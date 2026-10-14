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
//! - Disk stage: one blocking reader, reading the block once
//! - Network stage: one streamed session per target, fed zero-copy `Bytes`
//!   through its own bounded channel (`channel_capacity` chunks deep)
//! - Ack stage: one end-of-block acknowledgement per session
//! - A target counts as replicated only when its session's final ack says
//!   the peer stored the whole block
//!
//! It differs from [`super::ParallelReplicator`] only in chunk size and
//! buffering depth, which let disk reads run further ahead of the network.

use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn, error, info};

use super::{
    spawn_block_reader, BlockReplicator, ReplicationRequest, ReplicationResult, ReplicationError,
    Result, ReplicationStrategy, TransportSelector,
};
use crate::storage::{BlockStorage, block_storage::BlockId};

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

        // Stage 2 (network): one streamed session per target, each fed by
        // its own bounded channel. Stage 3 (acks): each session's single
        // end-of-block acknowledgement, gathered below.
        let mut outputs = Vec::new();
        let mut sessions: Vec<JoinHandle<(String, crate::transport::Result<u64>)>> = Vec::new();
        for (target_uuid, transport, addr) in target_transports {
            let (tx, rx) = mpsc::channel(self.channel_capacity);
            outputs.push(tx);
            sessions.push(tokio::spawn(async move {
                let r = transport.send_block_to_peer(&addr, block_id, gen_stamp, rx).await;
                (target_uuid, r)
            }));
        }

        // Stage 1 (disk): read the block once and fan it out.
        let read_task = spawn_block_reader(
            block_storage.clone(),
            BlockId::new(block_id, gen_stamp),
            actual_block_size,
            self.chunk_size,
            outputs,
        );

        let mut successful_replicas = 0;
        let mut total_bytes_transferred = 0u64;
        for session in sessions {
            match session.await {
                Ok((uuid, Ok(bytes))) => {
                    successful_replicas += 1;
                    total_bytes_transferred += bytes;
                    debug!("Block {} replicated to {} ({} bytes)", block_id, uuid, bytes);
                }
                Ok((uuid, Err(e))) => {
                    let err_msg = format!("Write failed for target {}: {}", uuid, e);
                    warn!("{}", err_msg);
                    setup_errors.push(err_msg);
                }
                Err(e) => setup_errors.push(format!("Session task failed: {}", e)),
            }
        }
        read_task.await.map_err(|e| ReplicationError::JoinError(format!("Read task failed: {}", e)))?;
        let all_errors = setup_errors;

        let duration_ms = start_time.elapsed().as_millis() as u64;

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
