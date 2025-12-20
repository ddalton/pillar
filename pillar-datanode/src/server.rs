use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::Result;
use tracing::{info, error};
use uuid::Uuid;

use crate::storage::{VolumeManager, BlockStorage, DataTransferServer};
use crate::grpc::{NamenodeClient, ClientDatanodeService};
use crate::grpc::namenode_client::DatanodeInfo;

/// Configuration for the DataNode
#[derive(Debug, Clone)]
pub struct DataNodeConfig {
    /// NameNode RPC address
    pub namenode_addr: String,

    /// DataNode UUID (auto-generated if not provided)
    pub uuid: Option<String>,

    /// Hostname
    pub hostname: String,

    /// RPC server address
    pub rpc_addr: SocketAddr,

    /// Data transfer server address (for block I/O)
    pub data_addr: SocketAddr,

    /// HTTP server address (for UI/metrics)
    pub http_addr: SocketAddr,

    /// Mount point for Flint volume
    pub volume_mount_point: PathBuf,

    /// Block pool ID
    pub block_pool_id: String,
}

impl Default for DataNodeConfig {
    fn default() -> Self {
        Self {
            namenode_addr: "pillar-namenode:9000".to_string(),
            uuid: None,
            hostname: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "localhost".to_string()),
            rpc_addr: "0.0.0.0:9867".parse().unwrap(),
            data_addr: "0.0.0.0:50010".parse().unwrap(),
            http_addr: "0.0.0.0:50075".parse().unwrap(),
            volume_mount_point: PathBuf::from("/mnt/pillar-data"),
            block_pool_id: "BP-pillar-001".to_string(),
        }
    }
}

/// The DataNode server
pub struct DataNode {
    config: DataNodeConfig,
    datanode_info: DatanodeInfo,
    volume_manager: Arc<VolumeManager>,
    block_storage: Arc<BlockStorage>,
    namenode_client: Arc<NamenodeClient>,
    data_transfer_server: Arc<DataTransferServer>,
    client_service: Arc<ClientDatanodeService>,
}

impl DataNode {
    pub async fn new(config: DataNodeConfig) -> Result<Self> {
        info!("Initializing DataNode with config: {:?}", config);

        // Generate UUID if not provided
        let uuid = config.uuid.clone().unwrap_or_else(|| Uuid::new_v4().to_string());

        // Initialize volume manager
        let volume_manager = Arc::new(VolumeManager::new(
            &config.volume_mount_point,
            config.block_pool_id.clone(),
        )?);

        info!("Volume manager initialized at: {:?}", config.volume_mount_point);

        // Initialize block storage
        let block_storage = Arc::new(BlockStorage::new(
            volume_manager.get_finalized_dir(),
            volume_manager.get_tmp_dir(),
        )?);

        info!("Block storage initialized ({} existing blocks)", block_storage.get_block_count());

        // Create datanode info
        let datanode_info = DatanodeInfo {
            uuid: uuid.clone(),
            hostname: config.hostname.clone(),
            rpc_port: config.rpc_addr.port(),
            data_port: config.data_addr.port(),
            http_port: config.http_addr.port(),
        };

        // Create NameNode client
        let namenode_client = Arc::new(NamenodeClient::new(
            config.namenode_addr.clone(),
            datanode_info.clone(),
            volume_manager.clone(),
            block_storage.clone(),
        ));

        // Create data transfer server
        let data_transfer_server = Arc::new(DataTransferServer::new(
            block_storage.clone(),
            config.data_addr,
        ));

        // Create client service
        let client_service = Arc::new(ClientDatanodeService::new(
            block_storage.clone(),
            volume_manager.clone(),
        ));

        info!("DataNode core components initialized");

        Ok(Self {
            config,
            datanode_info,
            volume_manager,
            block_storage,
            namenode_client,
            data_transfer_server,
            client_service,
        })
    }

    /// Start the DataNode server
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting DataNode server");

        // Register with NameNode
        self.namenode_client.register().await
            .map_err(|e| anyhow::anyhow!("Failed to register with NameNode: {}", e))?;

        // Send initial block report
        self.namenode_client.send_block_report().await
            .map_err(|e| anyhow::anyhow!("Failed to send block report: {}", e))?;

        // Start data transfer server in background
        let data_transfer_server = self.data_transfer_server.clone();
        tokio::spawn(async move {
            if let Err(e) = data_transfer_server.start().await {
                error!("Data transfer server failed: {}", e);
            }
        });

        // Start heartbeat loop in background
        let namenode_client = self.namenode_client.clone();
        tokio::spawn(async move {
            namenode_client.start_heartbeat_loop().await;
        });

        info!("DataNode RPC server started on {}", self.config.rpc_addr);
        info!("DataNode data transfer server started on {}", self.config.data_addr);
        info!("DataNode HTTP UI available on {}", self.config.http_addr);

        // Keep the server running
        tokio::signal::ctrl_c().await?;

        info!("Shutting down DataNode");

        Ok(())
    }

    /// Get DataNode statistics
    pub fn get_stats(&self) -> DataNodeStats {
        let volume_info = self.volume_manager.get_volume_info()
            .unwrap_or_else(|_| crate::storage::VolumeInfo {
                capacity_bytes: 0,
                used_bytes: 0,
                remaining_bytes: 0,
                block_pool_used_bytes: 0,
            });

        DataNodeStats {
            uuid: self.datanode_info.uuid.clone(),
            hostname: self.datanode_info.hostname.clone(),
            capacity: volume_info.capacity_bytes,
            used: volume_info.used_bytes,
            remaining: volume_info.remaining_bytes,
            block_pool_used: volume_info.block_pool_used_bytes,
            num_blocks: self.block_storage.get_block_count(),
        }
    }
}

/// DataNode statistics
#[derive(Debug, Clone)]
pub struct DataNodeStats {
    pub uuid: String,
    pub hostname: String,
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub block_pool_used: u64,
    pub num_blocks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_datanode_initialization() {
        let temp_dir = TempDir::new().unwrap();

        let config = DataNodeConfig {
            volume_mount_point: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let datanode = DataNode::new(config).await.unwrap();

        let stats = datanode.get_stats();
        assert!(!stats.uuid.is_empty());
        assert_eq!(stats.num_blocks, 0);
    }
}
