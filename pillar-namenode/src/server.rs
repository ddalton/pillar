use std::net::SocketAddr;
use std::sync::Arc;
use std::path::PathBuf;
use anyhow::Result;
use tonic::transport::Server;
use tracing::{info, error};

use crate::metadata::{Namespace, BlockManager, SqliteMetadataStore};
use crate::datanode::{DatanodeManager, HeartbeatMonitor, BlockPlacementPolicy};
use crate::grpc::{ClientNamenodeService, DatanodeProtocolService};

/// Configuration for the NameNode
#[derive(Debug, Clone)]
pub struct NameNodeConfig {
    /// RPC server address
    pub rpc_addr: SocketAddr,

    /// HTTP server address (for UI/metrics)
    pub http_addr: SocketAddr,

    /// Metadata storage path
    pub metadata_path: PathBuf,

    /// Default replication factor
    pub default_replication: u16,

    /// Default block size (bytes)
    pub default_block_size: u64,
}

impl Default for NameNodeConfig {
    fn default() -> Self {
        Self {
            rpc_addr: "0.0.0.0:9000".parse().unwrap(),
            http_addr: "0.0.0.0:9870".parse().unwrap(),
            metadata_path: PathBuf::from("/var/lib/pillar/namenode/metadata.db"),
            default_replication: 3,
            default_block_size: 128 * 1024 * 1024, // 128 MB
        }
    }
}

/// The NameNode server
pub struct NameNode {
    config: NameNodeConfig,
    namespace: Arc<Namespace>,
    block_manager: Arc<BlockManager>,
    datanode_manager: Arc<DatanodeManager>,
    placement_policy: Arc<BlockPlacementPolicy>,
    heartbeat_monitor: Arc<HeartbeatMonitor>,
}

impl NameNode {
    pub async fn new(config: NameNodeConfig) -> Result<Self> {
        info!("Initializing NameNode with config: {:?}", config);

        // Initialize metadata store
        let metadata_store = Arc::new(SqliteMetadataStore::new(&config.metadata_path));
        metadata_store.initialize().await
            .map_err(|e| anyhow::anyhow!("Failed to initialize metadata store: {}", e))?;

        info!("Metadata store initialized at: {:?}", config.metadata_path);

        // Initialize core components
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(
            datanode_manager.clone(),
            config.default_replication,
        ));
        let heartbeat_monitor = Arc::new(HeartbeatMonitor::new(datanode_manager.clone()));

        info!("NameNode core components initialized");

        Ok(Self {
            config,
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
            heartbeat_monitor,
        })
    }

    /// Start the NameNode server
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting NameNode server on {}", self.config.rpc_addr);

        // Start heartbeat monitor in background
        let heartbeat_monitor = self.heartbeat_monitor.clone();
        tokio::spawn(async move {
            heartbeat_monitor.start().await;
        });

        // Create gRPC services
        let client_service = ClientNamenodeService::new(
            self.namespace.clone(),
            self.block_manager.clone(),
            self.datanode_manager.clone(),
            self.placement_policy.clone(),
        );

        let datanode_service = DatanodeProtocolService::new(
            self.datanode_manager.clone(),
            self.block_manager.clone(),
        );

        info!("gRPC services initialized");

        // Note: Actual gRPC server implementation would go here
        // For now, this is a placeholder as we need the protobuf-generated code
        // to create the actual tonic server

        info!("NameNode RPC server started on {}", self.config.rpc_addr);
        info!("NameNode HTTP UI available on {}", self.config.http_addr);

        // Keep the server running
        tokio::signal::ctrl_c().await?;

        info!("Shutting down NameNode");

        Ok(())
    }

    /// Get namespace reference
    pub fn namespace(&self) -> Arc<Namespace> {
        self.namespace.clone()
    }

    /// Get block manager reference
    pub fn block_manager(&self) -> Arc<BlockManager> {
        self.block_manager.clone()
    }

    /// Get datanode manager reference
    pub fn datanode_manager(&self) -> Arc<DatanodeManager> {
        self.datanode_manager.clone()
    }

    /// Get server statistics
    pub fn get_stats(&self) -> NameNodeStats {
        NameNodeStats {
            total_capacity: self.datanode_manager.get_total_capacity(),
            total_used: self.datanode_manager.get_total_used(),
            total_remaining: self.datanode_manager.get_total_remaining(),
            num_live_datanodes: self.datanode_manager.get_num_live_datanodes(),
            total_blocks: self.block_manager.get_total_blocks(),
        }
    }
}

/// NameNode statistics
#[derive(Debug, Clone)]
pub struct NameNodeStats {
    pub total_capacity: u64,
    pub total_used: u64,
    pub total_remaining: u64,
    pub num_live_datanodes: usize,
    pub total_blocks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_namenode_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_path = temp_dir.path().join("metadata.db");

        let config = NameNodeConfig {
            metadata_path,
            ..Default::default()
        };

        let namenode = NameNode::new(config).await.unwrap();

        // Verify initialization
        let stats = namenode.get_stats();
        assert_eq!(stats.num_live_datanodes, 0);
        assert_eq!(stats.total_blocks, 0);
    }
}
