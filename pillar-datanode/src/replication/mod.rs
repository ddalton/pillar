//! Block replication module
//!
//! This module implements three replication strategies inspired by OSU HiBD research:
//!
//! 1. **Pipeline Replication** (baseline):
//!    - Traditional HDFS sequential replication: DN1 → DN2 → DN3
//!    - Works with any transport (TCP/RDMA)
//!    - Best for small blocks or mixed clusters
//!
//! 2. **Parallel Replication** (OSU HiBD):
//!    - Concurrent writes to all replicas: DN1 → [DN2, DN3]
//!    - Expected improvement: 2.9x with RDMA, 1.3x with TCP
//!    - Best for medium blocks with RDMA-capable peers
//!
//! 3. **Overlapped Replication** (SOR-HDFS):
//!    - SEDA-based pipelining of disk reads, RDMA writes, and acknowledgments
//!    - Expected improvement: 4.1x with RDMA, 1.5x with TCP
//!    - Best for large blocks (>64MB) with RDMA-capable peers
//!
//! The module provides adaptive strategy selection based on:
//! - Block size
//! - Peer transport capabilities (RDMA vs TCP)
//! - Number of replicas
//! - Available resources

use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, info, warn};

pub mod pipeline;
pub mod parallel;
pub mod overlapped;
pub mod strategy;

pub use pipeline::PipelineReplicator;
pub use parallel::ParallelReplicator;
pub use overlapped::OverlappedReplicator;
pub use strategy::{ReplicationStrategy, StrategySelector};

use crate::storage::BlockStorage;
use crate::transport::{DataTransport, TransportType};

/// Errors that can occur during block replication
#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("Block not found: {0}")]
    BlockNotFound(u64),

    #[error("No replicas specified")]
    NoReplicas,

    #[error("Transport error: {0}")]
    TransportError(#[from] crate::transport::TransportError),

    #[error("Storage error: {0}")]
    StorageError(#[from] crate::storage::block_storage::BlockStorageError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Partial replication failure: {success}/{total} replicas succeeded")]
    PartialFailure { success: usize, total: usize },

    #[error("All replicas failed")]
    AllReplicasFailed,

    #[error("Task join error: {0}")]
    JoinError(String),
}

pub type Result<T> = std::result::Result<T, ReplicationError>;

/// Information about a target DataNode for replication
#[derive(Debug, Clone)]
pub struct ReplicationTarget {
    /// DataNode UUID
    pub datanode_uuid: String,

    /// Hostname of the DataNode
    pub hostname: String,

    /// IP address
    pub ip_addr: String,

    /// TCP data transfer port
    pub xfer_port: u32,

    /// RDMA port (0 if not available)
    pub rdma_port: u32,

    /// Supported transport types (e.g., ["tcp", "rdma"])
    pub supported_transports: Vec<String>,
}

impl ReplicationTarget {
    /// Get the address for a specific transport type
    pub fn get_address(&self, transport: TransportType) -> String {
        match transport {
            TransportType::Tcp => format!("{}:{}", self.ip_addr, self.xfer_port),
            #[cfg(feature = "rdma")]
            TransportType::Rdma => {
                if self.rdma_port > 0 {
                    format!("{}:{}", self.ip_addr, self.rdma_port)
                } else {
                    // Fallback to TCP if RDMA port not available
                    format!("{}:{}", self.ip_addr, self.xfer_port)
                }
            }
        }
    }

    /// Check if the target supports a specific transport
    pub fn supports_transport(&self, transport: TransportType) -> bool {
        let transport_name = transport.to_string();
        self.supported_transports.iter().any(|t| t == &transport_name)
    }

    /// Check if the target supports RDMA
    pub fn supports_rdma(&self) -> bool {
        self.rdma_port > 0 && self.supported_transports.iter().any(|t| t == "rdma")
    }
}

/// Block replication request
#[derive(Debug, Clone)]
pub struct ReplicationRequest {
    /// Block ID to replicate
    pub block_id: u64,

    /// Generation stamp
    pub gen_stamp: u64,

    /// Target DataNodes to replicate to
    pub targets: Vec<ReplicationTarget>,

    /// Expected block size (used for strategy selection)
    pub block_size: u64,

    /// Priority (higher = more urgent)
    pub priority: u8,
}

/// Result of a replication operation
#[derive(Debug)]
pub struct ReplicationResult {
    /// Block ID that was replicated
    pub block_id: u64,

    /// Number of successful replicas
    pub successful_replicas: usize,

    /// Total number of attempted replicas
    pub total_replicas: usize,

    /// Strategy used for replication
    pub strategy_used: ReplicationStrategy,

    /// Total bytes transferred
    pub bytes_transferred: u64,

    /// Time taken in milliseconds
    pub duration_ms: u64,

    /// Errors encountered (if any)
    pub errors: Vec<String>,
}

impl ReplicationResult {
    /// Check if replication was fully successful
    pub fn is_success(&self) -> bool {
        self.successful_replicas == self.total_replicas
    }

    /// Check if replication was partially successful
    pub fn is_partial(&self) -> bool {
        self.successful_replicas > 0 && self.successful_replicas < self.total_replicas
    }

    /// Get the throughput in MB/s
    pub fn throughput_mbps(&self) -> f64 {
        if self.duration_ms == 0 {
            return 0.0;
        }
        let bytes_per_sec = (self.bytes_transferred as f64 * 1000.0) / self.duration_ms as f64;
        bytes_per_sec / (1024.0 * 1024.0)
    }
}

/// Read a finalized block once, on a blocking thread, and send each chunk
/// to every sender in `outputs` (one per target's transfer session).
///
/// Chunks are `Bytes`, so fanning out is a reference-count bump, not a
/// copy. A target whose session has ended (its receiver dropped) is
/// skipped from then on; reading stops when no target is left. A read
/// error is delivered to every remaining target, which aborts their
/// sessions so no peer finalizes a short block. The bounded channels
/// pace the reader to the slowest live target.
pub(crate) fn spawn_block_reader(
    block_storage: Arc<BlockStorage>,
    block: crate::storage::block_storage::BlockId,
    block_size: u64,
    chunk_size: usize,
    mut outputs: Vec<tokio::sync::mpsc::Sender<std::io::Result<Bytes>>>,
) -> tokio::task::JoinHandle<u64> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let fail = |outputs: &mut Vec<tokio::sync::mpsc::Sender<std::io::Result<Bytes>>>, e: std::io::Error| {
            for tx in outputs.drain(..) {
                let _ = tx.blocking_send(Err(std::io::Error::new(e.kind(), e.to_string())));
            }
        };
        let mut file = match block_storage.open_for_read(&block, 0, block_size) {
            Ok((f, len)) if len == block_size => f,
            Ok((_, len)) => {
                let e = std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("block {} holds {} bytes, {} expected", block.block_id, len, block_size),
                );
                fail(&mut outputs, e);
                return 0;
            }
            Err(e) => {
                fail(&mut outputs, std::io::Error::other(e.to_string()));
                return 0;
            }
        };
        let mut read = 0u64;
        while read < block_size && !outputs.is_empty() {
            let n = (chunk_size as u64).min(block_size - read) as usize;
            let mut buf = bytes::BytesMut::zeroed(n);
            if let Err(e) = file.read_exact(&mut buf) {
                fail(&mut outputs, e);
                return read;
            }
            let chunk = buf.freeze();
            outputs.retain(|tx| tx.blocking_send(Ok(chunk.clone())).is_ok());
            read += n as u64;
        }
        read
    })
}

/// Transport selection function type
pub type TransportSelector = Arc<dyn Fn(&ReplicationTarget) -> Option<(Arc<dyn DataTransport>, String)> + Send + Sync>;

/// Trait for block replication implementations
#[async_trait]
pub trait BlockReplicator: Send + Sync {
    /// Replicate a block to the specified targets
    async fn replicate_block(
        &self,
        block_storage: Arc<BlockStorage>,
        request: ReplicationRequest,
        transport_selector: TransportSelector,
    ) -> Result<ReplicationResult>;

    /// Get the strategy name
    fn strategy_name(&self) -> &'static str;
}

/// Main replication coordinator that selects and executes the appropriate strategy
pub struct ReplicationCoordinator {
    /// Block storage reference
    block_storage: Arc<BlockStorage>,

    /// Available data transports
    transports: Vec<Arc<dyn DataTransport>>,

    /// Strategy selector
    strategy_selector: StrategySelector,

    /// Individual replicators
    pipeline_replicator: PipelineReplicator,
    parallel_replicator: ParallelReplicator,
    overlapped_replicator: OverlappedReplicator,
}

impl ReplicationCoordinator {
    /// Create a new replication coordinator
    pub fn new(
        block_storage: Arc<BlockStorage>,
        transports: Vec<Arc<dyn DataTransport>>,
    ) -> Self {
        let strategy_selector = StrategySelector::new();
        let pipeline_replicator = PipelineReplicator::new();
        let parallel_replicator = ParallelReplicator::new();
        let overlapped_replicator = OverlappedReplicator::new();

        Self {
            block_storage,
            transports,
            strategy_selector,
            pipeline_replicator,
            parallel_replicator,
            overlapped_replicator,
        }
    }

    /// Replicate a block using adaptive strategy selection
    pub async fn replicate(
        &self,
        request: ReplicationRequest,
    ) -> Result<ReplicationResult> {
        if request.targets.is_empty() {
            return Err(ReplicationError::NoReplicas);
        }

        // Select the best strategy based on the request
        let strategy = self.strategy_selector.select_strategy(
            &request.targets,
            request.block_size,
            &self.transports,
        );

        info!(
            "Replicating block {} to {} targets using {:?} strategy",
            request.block_id,
            request.targets.len(),
            strategy
        );

        // Create transport selector closure
        let transports = self.transports.clone();
        let transport_selector: TransportSelector = Arc::new(move |target: &ReplicationTarget| {
            // Try RDMA first if supported
            #[cfg(feature = "rdma")]
            if target.supports_rdma() {
                if let Some(transport) = transports.iter().find(|t| {
                    matches!(t.transport_type(), TransportType::Rdma)
                }) {
                    let addr = target.get_address(TransportType::Rdma);
                    return Some((transport.clone(), addr));
                }
            }

            // Fallback to TCP
            if let Some(transport) = transports.iter().find(|t| {
                matches!(t.transport_type(), TransportType::Tcp)
            }) {
                let addr = target.get_address(TransportType::Tcp);
                return Some((transport.clone(), addr));
            }

            None
        });

        // Execute the selected strategy
        let start = std::time::Instant::now();
        let result = match strategy {
            ReplicationStrategy::Pipeline => {
                self.pipeline_replicator
                    .replicate_block(self.block_storage.clone(), request, transport_selector)
                    .await?
            }
            ReplicationStrategy::Parallel => {
                self.parallel_replicator
                    .replicate_block(self.block_storage.clone(), request, transport_selector)
                    .await?
            }
            ReplicationStrategy::Overlapped => {
                self.overlapped_replicator
                    .replicate_block(self.block_storage.clone(), request, transport_selector)
                    .await?
            }
        };

        let duration = start.elapsed();
        info!(
            "Replication of block {} completed: {}/{} replicas in {:.2}s ({:.2} MB/s)",
            result.block_id,
            result.successful_replicas,
            result.total_replicas,
            duration.as_secs_f64(),
            result.throughput_mbps()
        );

        Ok(result)
    }

    /// Get the appropriate transport for a target
    pub(crate) fn select_transport_for_target(
        &self,
        target: &ReplicationTarget,
    ) -> Option<(Arc<dyn DataTransport>, String)> {
        // Try RDMA first if supported
        #[cfg(feature = "rdma")]
        if target.supports_rdma() {
            if let Some(transport) = self.transports.iter().find(|t| {
                matches!(t.transport_type(), TransportType::Rdma)
            }) {
                let addr = target.get_address(TransportType::Rdma);
                debug!(
                    "Selected RDMA transport for target {} at {}",
                    target.datanode_uuid, addr
                );
                return Some((transport.clone(), addr));
            }
        }

        // Fallback to TCP
        if let Some(transport) = self.transports.iter().find(|t| {
            matches!(t.transport_type(), TransportType::Tcp)
        }) {
            let addr = target.get_address(TransportType::Tcp);
            debug!(
                "Selected TCP transport for target {} at {}",
                target.datanode_uuid, addr
            );
            return Some((transport.clone(), addr));
        }

        warn!(
            "No suitable transport found for target {}",
            target.datanode_uuid
        );
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_target_address() {
        let target = ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "datanode1".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 50011,
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        };

        assert_eq!(target.get_address(TransportType::Tcp), "10.0.0.1:50010");

        #[cfg(feature = "rdma")]
        assert_eq!(target.get_address(TransportType::Rdma), "10.0.0.1:50011");
    }

    #[test]
    fn test_replication_target_transport_support() {
        let target = ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "datanode1".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 50011,
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        };

        assert!(target.supports_transport(TransportType::Tcp));
        assert!(target.supports_rdma());

        #[cfg(feature = "rdma")]
        assert!(target.supports_transport(TransportType::Rdma));
    }

    #[test]
    fn test_replication_target_no_rdma() {
        let target = ReplicationTarget {
            datanode_uuid: "dn2".to_string(),
            hostname: "datanode2".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        };

        assert!(target.supports_transport(TransportType::Tcp));
        assert!(!target.supports_rdma());

        #[cfg(feature = "rdma")]
        assert!(!target.supports_transport(TransportType::Rdma));
    }

    #[test]
    fn test_replication_result_success() {
        let result = ReplicationResult {
            block_id: 12345,
            successful_replicas: 3,
            total_replicas: 3,
            strategy_used: ReplicationStrategy::Parallel,
            bytes_transferred: 1024 * 1024 * 100, // 100 MB
            duration_ms: 1000, // 1 second
            errors: vec![],
        };

        assert!(result.is_success());
        assert!(!result.is_partial());
        assert_eq!(result.throughput_mbps(), 100.0);
    }

    #[test]
    fn test_replication_result_partial() {
        let result = ReplicationResult {
            block_id: 12345,
            successful_replicas: 2,
            total_replicas: 3,
            strategy_used: ReplicationStrategy::Pipeline,
            bytes_transferred: 1024 * 1024 * 50, // 50 MB
            duration_ms: 500,
            errors: vec!["Target 3 failed".to_string()],
        };

        assert!(!result.is_success());
        assert!(result.is_partial());
        assert_eq!(result.throughput_mbps(), 100.0);
    }
}
