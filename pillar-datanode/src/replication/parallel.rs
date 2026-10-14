//! Parallel replication strategy
//!
//! This implements concurrent block replication where the source DataNode
//! writes to all target DataNodes simultaneously: DN1 → [DN2, DN3]
//!
//! **Based on:** OSU HiBD research
//!
//! **Performance expectations:**
//! - RDMA: 2.9x throughput improvement vs pipeline
//! - TCP: 1.3x throughput improvement vs pipeline
//!
//! **Characteristics:**
//! - All replicas receive data concurrently using tokio tasks
//! - Better network and CPU utilization
//! - Reduced overall replication time
//! - Best for: medium blocks (<64MB) with RDMA-capable peers
//!
//! **Implementation:**
//! - Read the block from disk once, on a blocking thread
//! - Fan each chunk out (zero-copy `Bytes`) to one streamed session per target
//! - Wait for all sessions to complete (or fail)
//! - Partial failures are handled gracefully

use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn, error};

use super::{
    spawn_block_reader, BlockReplicator, ReplicationRequest, ReplicationResult, ReplicationError,
    Result, ReplicationStrategy, TransportSelector,
};
use crate::storage::{BlockStorage, block_storage::BlockId};
use crate::transport::DataTransport;

/// Chunks buffered per target between the reader and the network session.
const CHANNEL_DEPTH: usize = 4;

/// Parallel replicator that transfers blocks concurrently to all targets
pub struct ParallelReplicator {
    /// Chunk size for reading blocks (1MB)
    chunk_size: usize,
}

impl ParallelReplicator {
    /// Create a new parallel replicator
    pub fn new() -> Self {
        Self {
            chunk_size: 1024 * 1024, // 1 MB chunks
        }
    }

    /// Create with custom chunk size (mainly for testing)
    #[allow(dead_code)]
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    /// Stream the block to one target from the shared reader's channel
    /// (runs in a tokio task)
    async fn replicate_to_target(
        transport: Arc<dyn DataTransport>,
        peer_addr: String,
        target_uuid: String,
        block_id: u64,
        gen_stamp: u64,
        chunks: crate::transport::BlockChunks,
    ) -> Result<u64> {
        debug!("Starting parallel replication of block {} to {}", block_id, peer_addr);

        let written = transport
            .send_block_to_peer(&peer_addr, block_id, gen_stamp, chunks)
            .await
            .map_err(|e| {
                error!("Failed to send block {} to {} ({}): {}", block_id, peer_addr, target_uuid, e);
                e
            })?;

        debug!(
            "Completed parallel replication of block {} to {} ({} bytes)",
            block_id, target_uuid, written
        );

        Ok(written)
    }
}

impl Default for ParallelReplicator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BlockReplicator for ParallelReplicator {
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

        debug!(
            "Starting parallel replication of block {} to {} targets (size: {} bytes)",
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
                strategy_used: ReplicationStrategy::Parallel,
                bytes_transferred: 0,
                duration_ms: start_time.elapsed().as_millis() as u64,
                errors: vec!["Block has zero size".to_string()],
            });
        }

        // Spawn concurrent replication tasks for all targets
        let mut tasks: Vec<JoinHandle<Result<(String, u64)>>> = Vec::new();
        let mut outputs = Vec::new();
        let mut errors = Vec::new();

        for target in request.targets.iter() {
            // Select the appropriate transport for this target
            let (transport, addr) = match transport_selector(target) {
                Some((t, a)) => (t, a),
                None => {
                    let error_msg = format!(
                        "No suitable transport found for target {}",
                        target.datanode_uuid
                    );
                    warn!("{}", error_msg);
                    errors.push(error_msg);
                    continue;
                }
            };

            // Each target gets its own session fed from the shared reader.
            let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
            outputs.push(tx);
            let target_uuid = target.datanode_uuid.clone();

            let task = tokio::spawn(async move {
                Self::replicate_to_target(transport, addr, target_uuid.clone(), block_id, gen_stamp, rx)
                    .await
                    .map(|bytes| (target_uuid, bytes))
            });

            tasks.push(task);
        }

        // Read the block once and fan it out to every target.
        let reader = spawn_block_reader(
            block_storage.clone(),
            BlockId::new(block_id, gen_stamp),
            actual_block_size,
            self.chunk_size,
            outputs,
        );

        debug!(
            "Spawned {} concurrent replication tasks for block {}",
            tasks.len(),
            block_id
        );

        // Wait for all tasks to complete
        let mut successful_replicas = 0;
        let mut total_bytes_transferred = 0u64;

        for task in tasks {
            match task.await {
                Ok(Ok((target_uuid, bytes_written))) => {
                    successful_replicas += 1;
                    total_bytes_transferred += bytes_written;
                    debug!(
                        "Successfully replicated block {} to {} ({} bytes)",
                        block_id, target_uuid, bytes_written
                    );
                }
                Ok(Err(e)) => {
                    let err_msg = format!(
                        "Failed to replicate block {}: {}",
                        block_id, e
                    );
                    warn!("{}", err_msg);
                    errors.push(err_msg);
                }
                Err(e) => {
                    let err_msg = format!(
                        "Task join error for block {}: {}",
                        block_id, e
                    );
                    error!("{}", err_msg);
                    errors.push(err_msg);
                }
            }
        }

        reader.await.map_err(|e| ReplicationError::JoinError(e.to_string()))?;
        let duration_ms = start_time.elapsed().as_millis() as u64;

        let result = ReplicationResult {
            block_id,
            successful_replicas,
            total_replicas,
            strategy_used: ReplicationStrategy::Parallel,
            bytes_transferred: total_bytes_transferred,
            duration_ms,
            errors,
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
            // We'll return success for partial replication but include errors
        }

        debug!(
            "Parallel replication completed for block {}: {}/{} replicas in {}ms ({:.2} MB/s)",
            block_id,
            successful_replicas,
            total_replicas,
            duration_ms,
            result.throughput_mbps()
        );

        Ok(result)
    }

    fn strategy_name(&self) -> &'static str {
        "Parallel"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_replicator_creation() {
        let replicator = ParallelReplicator::new();
        assert_eq!(replicator.chunk_size, 1024 * 1024);
        assert_eq!(replicator.strategy_name(), "Parallel");
    }

    #[test]
    fn test_parallel_replicator_custom_chunk() {
        let replicator = ParallelReplicator::with_chunk_size(512 * 1024);
        assert_eq!(replicator.chunk_size, 512 * 1024);
    }

    #[tokio::test]
    async fn test_parallel_empty_targets() {
        let replicator = ParallelReplicator::new();
        let block_storage = Arc::new(
            BlockStorage::new("/tmp/test-blocks-parallel", "/tmp/test-tmp-parallel")
                .expect("Failed to create block storage")
        );

        let request = ReplicationRequest {
            block_id: 12345,
            gen_stamp: 1,
            targets: vec![],
            block_size: 1024,
            priority: 1,
        };

        let transport_selector: TransportSelector = Arc::new(|_| None);
        let result = replicator.replicate_block(block_storage, request, transport_selector).await;

        // Should fail because the block doesn't exist
        assert!(result.is_err());
    }
}
