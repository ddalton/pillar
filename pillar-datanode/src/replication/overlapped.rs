//! Overlapped replication strategy
//!
//! This implements SEDA-based (Staged Event-Driven Architecture) overlapped replication
//! where disk reads, RDMA writes, and acknowledgments are pipelined concurrently.
//!
//! **Based on:** SOR-HDFS (Staged Overlapped Replication) from OSU HiBD research
//!
//! **Performance expectations:**
//! - RDMA: 4.1x throughput improvement vs pipeline
//! - TCP: 1.5x throughput improvement vs pipeline
//!
//! **Characteristics:**
//! - Three overlapping stages:
//!   1. Disk read stage: Reads blocks from local storage
//!   2. Network write stage: Writes blocks to all replicas concurrently
//!   3. Acknowledgment stage: Tracks completion and reports to NameNode
//! - Maximizes pipeline parallelism
//! - Best for: large blocks (≥64MB) with RDMA-capable peers
//!
//! **Implementation:**
//! - Uses tokio channels to connect stages
//! - Each stage runs as an independent task
//! - Backpressure via bounded channels
//! - Graceful error handling with partial success

use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use bytes::Bytes;
use tracing::{debug, warn, error, info};

use super::{
    BlockReplicator, ReplicationRequest, ReplicationResult, ReplicationError, Result,
    ReplicationStrategy, TransportSelector, ReplicationTarget,
};
use crate::storage::{BlockStorage, block_storage::BlockId};
use crate::transport::DataTransport;

/// Chunk of block data in the pipeline
#[derive(Debug, Clone)]
struct BlockChunk {
    /// Chunk offset within the block
    offset: u64,

    /// Chunk data
    data: Bytes,

    /// Chunk sequence number (for ordering)
    seq_num: u64,
}

/// Write task result
#[derive(Debug)]
struct WriteResult {
    /// Target DataNode UUID
    target_uuid: String,

    /// Chunk sequence number
    seq_num: u64,

    /// Success or error
    result: std::result::Result<u64, ReplicationError>,
}

/// Overlapped replicator using SEDA-based pipeline
pub struct OverlappedReplicator {
    /// Chunk size for reading blocks (4MB for large blocks)
    chunk_size: usize,

    /// Channel capacity for buffering between stages
    channel_capacity: usize,
}

impl OverlappedReplicator {
    /// Create a new overlapped replicator
    pub fn new() -> Self {
        Self {
            chunk_size: 4 * 1024 * 1024, // 4 MB chunks for better RDMA performance
            channel_capacity: 16,         // Buffer up to 16 chunks (64MB in flight)
        }
    }

    /// Create with custom parameters (mainly for testing)
    #[allow(dead_code)]
    pub fn with_params(chunk_size: usize, channel_capacity: usize) -> Self {
        Self {
            chunk_size,
            channel_capacity,
        }
    }

    /// Stage 1: Disk read stage
    /// Reads the block in chunks and sends them to the write stage
    async fn disk_read_stage(
        block_storage: Arc<BlockStorage>,
        block_id: u64,
        gen_stamp: u64,
        block_size: u64,
        chunk_size: usize,
        tx: mpsc::Sender<BlockChunk>,
    ) -> Result<u64> {
        debug!(
            "Disk read stage starting for block {} (size: {} bytes, chunk_size: {} bytes)",
            block_id, block_size, chunk_size
        );

        let mut offset = 0u64;
        let mut seq_num = 0u64;
        let mut total_bytes_read = 0u64;

        while offset < block_size {
            let length = std::cmp::min(chunk_size as u64, block_size - offset);

            // Read chunk from storage
            let block = BlockId::new(block_id, gen_stamp);
            let data = block_storage
                .read_block(&block, offset, length)
                .map_err(|e| {
                    error!(
                        "Disk read failed for block {} at offset {}: {}",
                        block_id, offset, e
                    );
                    e
                })?;

            total_bytes_read += data.len() as u64;

            // Create chunk and send to write stage
            let chunk = BlockChunk {
                offset,
                data,
                seq_num,
            };

            // Send chunk (with backpressure)
            if let Err(e) = tx.send(chunk).await {
                error!("Failed to send chunk to write stage: {}", e);
                return Err(ReplicationError::IoError(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Write stage closed unexpectedly",
                )));
            }

            debug!(
                "Disk read stage: read chunk seq={}, offset={}, length={}",
                seq_num, offset, length
            );

            offset += length;
            seq_num += 1;
        }

        info!(
            "Disk read stage completed for block {}: {} bytes in {} chunks",
            block_id, total_bytes_read, seq_num
        );

        Ok(total_bytes_read)
    }

    /// Stage 2: Network write stage
    /// Receives chunks from disk stage and writes them to all targets concurrently
    async fn network_write_stage(
        block_id: u64,
        gen_stamp: u64,
        targets: Vec<(String, Arc<dyn DataTransport>, String)>, // (uuid, transport, addr)
        mut rx: mpsc::Receiver<BlockChunk>,
        tx: mpsc::Sender<WriteResult>,
    ) -> Result<u64> {
        debug!(
            "Network write stage starting for block {} with {} targets",
            block_id,
            targets.len()
        );

        let mut total_bytes_written = 0u64;
        let mut chunk_count = 0u64;

        // Process each chunk
        while let Some(chunk) = rx.recv().await {
            let seq_num = chunk.seq_num;
            let offset = chunk.offset;
            let data = chunk.data;
            let data_len = data.len() as u64;

            debug!(
                "Network write stage: processing chunk seq={}, offset={}, length={}",
                seq_num, offset, data_len
            );

            // Write this chunk to all targets concurrently
            let mut tasks = Vec::new();

            for (target_uuid, transport, addr) in &targets {
                let uuid_clone = target_uuid.clone();
                let transport_clone = transport.clone();
                let addr_clone = addr.clone();
                let data_clone = data.clone();

                let task = tokio::spawn(async move {
                    let result = transport_clone
                        .write_to_peer(&addr_clone, block_id, gen_stamp, offset, &data_clone)
                        .await
                        .map(|_| data_clone.len() as u64)
                        .map_err(|e| {
                            error!(
                                "Write failed for target {} at offset {}: {}",
                                uuid_clone, offset, e
                            );
                            ReplicationError::from(e)
                        });

                    WriteResult {
                        target_uuid: uuid_clone,
                        seq_num,
                        result,
                    }
                });

                tasks.push(task);
            }

            // Wait for all writes to complete for this chunk
            for task in tasks {
                match task.await {
                    Ok(write_result) => {
                        // Send result to ack stage
                        if let Err(e) = tx.send(write_result).await {
                            error!("Failed to send write result to ack stage: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Task join error in network write stage: {}", e);
                    }
                }
            }

            total_bytes_written += data_len * targets.len() as u64;
            chunk_count += 1;

            debug!(
                "Network write stage: completed chunk seq={} to {} targets",
                seq_num,
                targets.len()
            );
        }

        info!(
            "Network write stage completed for block {}: {} bytes across {} chunks",
            block_id, total_bytes_written, chunk_count
        );

        Ok(total_bytes_written)
    }

    /// Stage 3: Acknowledgment stage
    /// Tracks write results and accumulates statistics
    async fn ack_stage(
        block_id: u64,
        total_targets: usize,
        expected_chunks: usize,
        mut rx: mpsc::Receiver<WriteResult>,
    ) -> Result<(usize, Vec<String>)> {
        debug!(
            "Ack stage starting for block {}: {} targets, {} expected chunks",
            block_id, total_targets, expected_chunks
        );

        let mut successful_targets: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut errors = Vec::new();
        let mut processed_results = 0;
        let expected_results = total_targets * expected_chunks;

        while let Some(result) = rx.recv().await {
            processed_results += 1;

            match result.result {
                Ok(_bytes) => {
                    successful_targets.insert(result.target_uuid.clone());
                    debug!(
                        "Ack stage: successful write to {} for chunk seq={}",
                        result.target_uuid, result.seq_num
                    );
                }
                Err(e) => {
                    let err_msg = format!(
                        "Write failed for target {} at chunk seq={}: {}",
                        result.target_uuid, result.seq_num, e
                    );
                    warn!("{}", err_msg);
                    errors.push(err_msg);
                }
            }

            // Exit early if we've processed all expected results
            if processed_results >= expected_results {
                break;
            }
        }

        let successful_count = successful_targets.len();
        info!(
            "Ack stage completed for block {}: {}/{} targets successful, {} errors",
            block_id,
            successful_count,
            total_targets,
            errors.len()
        );

        Ok((successful_count, errors))
    }
}

impl Default for OverlappedReplicator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BlockReplicator for OverlappedReplicator {
    async fn replicate_block(
        &self,
        block_storage: Arc<BlockStorage>,
        request: ReplicationRequest,
        transport_selector: TransportSelector,
    ) -> Result<ReplicationResult> {
        let start_time = Instant::now();
        let block_id = request.block_id;
        let gen_stamp = request.gen_stamp;
        let block_size = request.block_size;
        let total_replicas = request.targets.len();

        info!(
            "Starting overlapped replication of block {} to {} targets (size: {} bytes)",
            block_id, total_replicas, block_size
        );

        // First, verify the block exists and get its actual size
        let metadata = block_storage.get_block_metadata(block_id)
            .ok_or_else(|| {
                error!("Block {} not found in storage", block_id);
                ReplicationError::BlockNotFound(block_id)
            })?;

        let actual_block_size = metadata.num_bytes;
        if actual_block_size == 0 {
            warn!("Block {} has zero size, skipping replication", block_id);
            return Ok(ReplicationResult {
                block_id,
                successful_replicas: 0,
                total_replicas,
                strategy_used: ReplicationStrategy::Overlapped,
                bytes_transferred: 0,
                duration_ms: start_time.elapsed().as_millis() as u64,
                errors: vec!["Block has zero size".to_string()],
            });
        }

        // Select transports for all targets
        let mut target_transports = Vec::new();
        let mut setup_errors = Vec::new();

        for target in request.targets.iter() {
            match transport_selector(target) {
                Some((transport, addr)) => {
                    target_transports.push((
                        target.datanode_uuid.clone(),
                        transport,
                        addr,
                    ));
                }
                None => {
                    let error_msg = format!(
                        "No suitable transport found for target {}",
                        target.datanode_uuid
                    );
                    warn!("{}", error_msg);
                    setup_errors.push(error_msg);
                }
            }
        }

        if target_transports.is_empty() {
            error!("No valid transports for any target");
            return Ok(ReplicationResult {
                block_id,
                successful_replicas: 0,
                total_replicas,
                strategy_used: ReplicationStrategy::Overlapped,
                bytes_transferred: 0,
                duration_ms: start_time.elapsed().as_millis() as u64,
                errors: setup_errors,
            });
        }

        // Calculate expected number of chunks
        let expected_chunks = ((actual_block_size + self.chunk_size as u64 - 1)
            / self.chunk_size as u64) as usize;

        // Create channels for pipeline stages
        let (read_tx, read_rx) = mpsc::channel::<BlockChunk>(self.channel_capacity);
        let (write_tx, write_rx) = mpsc::channel::<WriteResult>(self.channel_capacity * 4);

        // Spawn stage 1: Disk reads
        let storage_clone = block_storage.clone();
        let chunk_size = self.chunk_size;
        let read_task: JoinHandle<Result<u64>> = tokio::spawn(async move {
            Self::disk_read_stage(
                storage_clone,
                block_id,
                gen_stamp,
                actual_block_size,
                chunk_size,
                read_tx,
            )
            .await
        });

        // Spawn stage 2: Network writes
        let write_task: JoinHandle<Result<u64>> = tokio::spawn(async move {
            Self::network_write_stage(block_id, gen_stamp, target_transports, read_rx, write_tx)
                .await
        });

        // Spawn stage 3: Acknowledgments
        let ack_task: JoinHandle<Result<(usize, Vec<String>)>> = tokio::spawn(async move {
            Self::ack_stage(block_id, total_replicas, expected_chunks, write_rx).await
        });

        // Wait for all stages to complete
        let read_result = read_task.await.map_err(|e| {
            ReplicationError::JoinError(format!("Read task failed: {}", e))
        })??;

        let _write_result = write_task.await.map_err(|e| {
            ReplicationError::JoinError(format!("Write task failed: {}", e))
        })??;

        let (successful_replicas, mut ack_errors) = ack_task.await.map_err(|e| {
            ReplicationError::JoinError(format!("Ack task failed: {}", e))
        })??;

        // Combine setup errors and ack errors
        setup_errors.append(&mut ack_errors);
        let all_errors = setup_errors;

        let duration_ms = start_time.elapsed().as_millis() as u64;

        // Calculate total bytes transferred (actual bytes * number of successful replicas)
        let total_bytes_transferred = read_result * successful_replicas as u64;

        let result = ReplicationResult {
            block_id,
            successful_replicas,
            total_replicas,
            strategy_used: ReplicationStrategy::Overlapped,
            bytes_transferred: total_bytes_transferred,
            duration_ms,
            errors: all_errors,
        };

        // Check if we have enough successful replicas
        if successful_replicas == 0 {
            error!("All replicas failed for block {}", block_id);
            return Err(ReplicationError::AllReplicasFailed);
        } else if successful_replicas < total_replicas {
            warn!(
                "Partial replication for block {}: {}/{} replicas succeeded",
                block_id, successful_replicas, total_replicas
            );
        }

        info!(
            "Overlapped replication completed for block {}: {}/{} replicas in {}ms ({:.2} MB/s)",
            block_id,
            successful_replicas,
            total_replicas,
            duration_ms,
            result.throughput_mbps()
        );

        Ok(result)
    }

    fn strategy_name(&self) -> &'static str {
        "Overlapped"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlapped_replicator_creation() {
        let replicator = OverlappedReplicator::new();
        assert_eq!(replicator.chunk_size, 4 * 1024 * 1024);
        assert_eq!(replicator.channel_capacity, 16);
        assert_eq!(replicator.strategy_name(), "Overlapped");
    }

    #[test]
    fn test_overlapped_replicator_custom_params() {
        let replicator = OverlappedReplicator::with_params(2 * 1024 * 1024, 8);
        assert_eq!(replicator.chunk_size, 2 * 1024 * 1024);
        assert_eq!(replicator.channel_capacity, 8);
    }
}
