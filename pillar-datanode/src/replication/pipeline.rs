//! Pipeline replication strategy
//!
//! This implements the traditional HDFS pipeline replication where blocks are
//! transferred sequentially: DN1 → DN2 → DN3
//!
//! **Characteristics:**
//! - Sequential writes to each replica
//! - Works with any transport (TCP/RDMA)
//! - Lower throughput but simple and reliable
//! - Best for: small blocks, mixed clusters, or when RDMA is unavailable
//!
//! **Performance:**
//! - Baseline for comparison
//! - Simple error handling and retry logic

use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use tracing::{debug, warn, error};

use super::{
    BlockReplicator, ReplicationRequest, ReplicationResult, ReplicationError, Result,
    ReplicationStrategy, TransportSelector,
};
use crate::storage::{BlockStorage, block_storage::BlockId};
use crate::transport::DataTransport;

/// Pipeline replicator that transfers blocks sequentially to each target
pub struct PipelineReplicator {
    /// Chunk size for reading blocks (1MB)
    chunk_size: usize,
}

impl PipelineReplicator {
    /// Create a new pipeline replicator
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

    /// Replicate a block to a single target
    async fn replicate_to_target(
        &self,
        block_storage: &BlockStorage,
        transport: Arc<dyn DataTransport>,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        block_size: u64,
    ) -> Result<u64> {
        debug!(
            "Starting pipeline replication of block {} to {} (size: {} bytes)",
            block_id, peer_addr, block_size
        );

        let mut total_bytes_written = 0u64;
        let mut offset = 0u64;

        // Read and write block in chunks
        while offset < block_size {
            let length = std::cmp::min(self.chunk_size as u64, block_size - offset);

            // Read chunk from local storage
            let block = BlockId::new(block_id, gen_stamp);
            let data = block_storage
                .read_block(&block, offset, length)
                .map_err(|e| {
                    error!(
                        "Failed to read block {} at offset {}: {}",
                        block_id, offset, e
                    );
                    e
                })?;

            // Write chunk to peer
            transport
                .write_to_peer(peer_addr, block_id, gen_stamp, offset, &data)
                .await
                .map_err(|e| {
                    error!(
                        "Failed to write block {} to {} at offset {}: {}",
                        block_id, peer_addr, offset, e
                    );
                    e
                })?;

            total_bytes_written += data.len() as u64;
            offset += length;

            debug!(
                "Transferred chunk: offset={}, length={}, total={}",
                offset - length,
                length,
                total_bytes_written
            );
        }

        debug!(
            "Completed pipeline replication of block {} to {} ({} bytes)",
            block_id, peer_addr, total_bytes_written
        );

        Ok(total_bytes_written)
    }
}

impl Default for PipelineReplicator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BlockReplicator for PipelineReplicator {
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

        let mut successful_replicas = 0;
        let mut total_bytes_transferred = 0u64;
        let mut errors = Vec::new();

        debug!(
            "Starting pipeline replication of block {} to {} targets (size: {} bytes)",
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
                strategy_used: ReplicationStrategy::Pipeline,
                bytes_transferred: 0,
                duration_ms: start_time.elapsed().as_millis() as u64,
                errors: vec!["Block has zero size".to_string()],
            });
        }

        // Sequential replication to each target
        for (idx, target) in request.targets.iter().enumerate() {
            debug!(
                "Replicating to target {}/{}: {} ({})",
                idx + 1,
                total_replicas,
                target.datanode_uuid,
                target.hostname
            );

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

            // Replicate to this target
            match self.replicate_to_target(
                &block_storage,
                transport,
                &addr,
                block_id,
                gen_stamp,
                actual_block_size,
            ).await {
                Ok(bytes_written) => {
                    successful_replicas += 1;
                    total_bytes_transferred += bytes_written;
                    debug!(
                        "Successfully replicated block {} to {} ({} bytes)",
                        block_id, target.datanode_uuid, bytes_written
                    );
                }
                Err(e) => {
                    let err_msg = format!(
                        "Failed to replicate block {} to {}: {}",
                        block_id, target.datanode_uuid, e
                    );
                    warn!("{}", err_msg);
                    errors.push(err_msg);
                }
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;

        let result = ReplicationResult {
            block_id,
            successful_replicas,
            total_replicas,
            strategy_used: ReplicationStrategy::Pipeline,
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

        Ok(result)
    }

    fn strategy_name(&self) -> &'static str {
        "Pipeline"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::ReplicationTarget;

    #[test]
    fn test_pipeline_replicator_creation() {
        let replicator = PipelineReplicator::new();
        assert_eq!(replicator.chunk_size, 1024 * 1024);
        assert_eq!(replicator.strategy_name(), "Pipeline");
    }

    #[test]
    fn test_pipeline_replicator_custom_chunk() {
        let replicator = PipelineReplicator::with_chunk_size(512 * 1024);
        assert_eq!(replicator.chunk_size, 512 * 1024);
    }

    #[tokio::test]
    async fn test_pipeline_empty_targets() {
        let replicator = PipelineReplicator::new();
        let block_storage = Arc::new(
            BlockStorage::new("/tmp/test-blocks", "/tmp/test-tmp")
                .expect("Failed to create block storage")
        );

        let request = ReplicationRequest {
            block_id: 12345,
            gen_stamp: 1,
            targets: vec![],
            block_size: 1024,
            priority: 1,
        };

        let transport_selector: super::TransportSelector = Arc::new(|_| None);

        // This should fail with NoReplicas error before reaching the replicator
        // but let's test that the replicator handles it gracefully
        let result = replicator.replicate_block(block_storage, request, transport_selector).await;

        // The replicator will fail because the block doesn't exist
        // In a real scenario, the coordinator would check for empty targets first
        assert!(result.is_err());
    }

    #[test]
    fn test_replication_target_creation() {
        let target = ReplicationTarget {
            datanode_uuid: "dn1-uuid".to_string(),
            hostname: "datanode1.example.com".to_string(),
            ip_addr: "192.168.1.10".to_string(),
            xfer_port: 50010,
            rdma_port: 50011,
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        };

        assert_eq!(target.datanode_uuid, "dn1-uuid");
        assert!(target.supports_rdma());
        assert_eq!(
            target.get_address(crate::transport::TransportType::Tcp),
            "192.168.1.10:50010"
        );
    }
}
