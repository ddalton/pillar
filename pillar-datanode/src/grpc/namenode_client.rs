use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use thiserror::Error;
use tracing::{info, warn, error, debug};
use tonic::transport::Channel;
use tonic::Request;

use pillar_proto::hdfs::datanode::datanode_protocol_service_client::DatanodeProtocolServiceClient;
use pillar_proto::hdfs::datanode::{
    DatanodeRegistrationProto,
    RegisterDatanodeRequestProto, HeartbeatRequestProto,
    BlockReportRequestProto,
    HeartbeatResponseProto, DatanodeCommandProto,
};
use pillar_proto::hdfs::{
    DatanodeIdProto, StorageInfoProto, ExportedBlockKeysProto,
    StorageReportProto,
};

use crate::storage::{VolumeManager, BlockStorage};

#[derive(Error, Debug)]
pub enum NamenodeClientError {
    #[error("Registration failed: {0}")]
    RegistrationFailed(String),

    #[error("Heartbeat failed: {0}")]
    HeartbeatFailed(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
}

pub type Result<T> = std::result::Result<T, NamenodeClientError>;

/// DataNode information
#[derive(Debug, Clone)]
pub struct DatanodeInfo {
    pub uuid: String,
    pub hostname: String,
    pub rpc_port: u16,
    pub data_port: u16,
    pub http_port: u16,
}

/// Client for communicating with the NameNode
pub struct NamenodeClient {
    namenode_addr: String,
    datanode_info: DatanodeInfo,
    volume_manager: Arc<VolumeManager>,
    block_storage: Arc<BlockStorage>,
    heartbeat_interval: Duration,
    block_pool_id: String,
}

impl NamenodeClient {
    pub fn new(
        namenode_addr: String,
        datanode_info: DatanodeInfo,
        volume_manager: Arc<VolumeManager>,
        block_storage: Arc<BlockStorage>,
    ) -> Self {
        let block_pool_id = volume_manager.get_block_pool_id().to_string();
        Self {
            namenode_addr,
            datanode_info,
            volume_manager,
            block_storage,
            heartbeat_interval: Duration::from_secs(3),
            block_pool_id,
        }
    }

    /// Create a gRPC client connection to the NameNode
    async fn create_client(&self) -> Result<DatanodeProtocolServiceClient<Channel>> {
        let addr = format!("http://{}", self.namenode_addr);
        let client = DatanodeProtocolServiceClient::connect(addr).await?;
        Ok(client)
    }

    /// Create DatanodeID protobuf message
    fn create_datanode_id_proto(&self) -> DatanodeIdProto {
        DatanodeIdProto {
            ip_addr: self.datanode_info.hostname.clone(),
            host_name: self.datanode_info.hostname.clone(),
            datanode_uuid: self.datanode_info.uuid.clone(),
            xfer_port: self.datanode_info.data_port as u32,
            info_port: self.datanode_info.http_port as u32,
            ipc_port: self.datanode_info.rpc_port as u32,
            info_secure_port: Some(0),
        }
    }

    /// Create DatanodeRegistration protobuf message
    fn create_registration_proto(&self, volume_info: &crate::storage::VolumeInfo) -> DatanodeRegistrationProto {
        use pillar_proto::hdfs::BlockKeyProto;

        DatanodeRegistrationProto {
            datanode_id: self.create_datanode_id_proto(),
            storage_info: StorageInfoProto {
                layout_version: 0,
                namespce_id: 0,  // Note: This is the actual field name in the protobuf (typo in Hadoop)
                cluster_id: self.block_pool_id.clone(),
                c_time: 0,
            },
            keys: ExportedBlockKeysProto {
                is_block_token_enabled: false,
                key_update_interval: 0,
                token_life_time: 0,
                current_key: BlockKeyProto {
                    key_id: 0,
                    expiry_date: 0,
                    key_bytes: None,
                },
                all_keys: vec![],
            },
            software_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Register with the NameNode
    pub async fn register(&self) -> Result<()> {
        info!("Registering with NameNode at {}", self.namenode_addr);

        let volume_info = self.volume_manager.get_volume_info()
            .map_err(|e| NamenodeClientError::RegistrationFailed(format!("Failed to get volume info: {}", e)))?;

        // Create gRPC client
        let mut client = self.create_client().await?;

        // Create registration request
        let registration = self.create_registration_proto(&volume_info);
        let request = Request::new(RegisterDatanodeRequestProto {
            registration: registration,
        });

        // Send registration request
        let response = client.register_datanode(request).await?;

        info!("Successfully registered with NameNode");
        info!("  UUID: {}", self.datanode_info.uuid);
        info!("  Hostname: {}", self.datanode_info.hostname);
        info!("  RPC Port: {}", self.datanode_info.rpc_port);
        info!("  Data Port: {}", self.datanode_info.data_port);
        info!("  Capacity: {} GB", volume_info.capacity_bytes / 1_000_000_000);

        Ok(())
    }

    /// Send heartbeat to NameNode
    pub async fn send_heartbeat(&self) -> Result<Vec<DatanodeCommand>> {
        debug!("Sending heartbeat to NameNode");

        let volume_info = self.volume_manager.get_volume_info()
            .map_err(|e| NamenodeClientError::HeartbeatFailed(format!("Failed to get volume info: {}", e)))?;

        // Create gRPC client
        let mut client = self.create_client().await?;

        // Create storage report
        let storage_report = StorageReportProto {
            storage_uuid: self.datanode_info.uuid.clone(),
            failed: Some(false),
            capacity: Some(volume_info.capacity_bytes),
            dfs_used: Some(volume_info.block_pool_used_bytes),
            remaining: Some(volume_info.remaining_bytes),
            block_pool_used: Some(volume_info.block_pool_used_bytes),
            storage: Some(pillar_proto::hdfs::DatanodeStorageProto {
                storage_uuid: self.datanode_info.uuid.clone(),
                state: Some(0), // NORMAL
                storage_type: Some(1), // DISK
            }),
            non_dfs_used: Some(volume_info.used_bytes - volume_info.block_pool_used_bytes),
            mount: None,
        };

        // Create heartbeat request
        let registration = self.create_registration_proto(&volume_info);
        let request = Request::new(HeartbeatRequestProto {
            registration: registration,
            reports: vec![storage_report],
            xmits_in_progress: Some(0),
            xceiver_count: Some(0),
            failed_volumes: Some(0),
            cache_capacity: Some(0),
            cache_used: Some(0),
            volume_failure_summary: None,
            request_full_block_report_lease: Some(false),
            slow_peers: vec![],
            slow_disks: vec![],
        });

        // Send heartbeat request
        let response = client.send_heartbeat(request).await?;
        let heartbeat_response = response.into_inner();

        debug!("Heartbeat sent successfully");
        debug!("  Capacity: {} GB", volume_info.capacity_bytes / 1_000_000_000);
        debug!("  Used: {} GB", volume_info.used_bytes / 1_000_000_000);
        debug!("  Remaining: {} GB", volume_info.remaining_bytes / 1_000_000_000);
        debug!("  Blocks: {}", self.block_storage.get_block_count());

        // Parse commands from response
        let commands = heartbeat_response.cmds
            .into_iter()
            .filter_map(|cmd| self.parse_command(cmd))
            .collect();

        Ok(commands)
    }

    /// Parse a DatanodeCommand from protobuf
    fn parse_command(&self, _cmd: DatanodeCommandProto) -> Option<DatanodeCommand> {
        // TODO: Implement command parsing
        // For MVP, we don't process commands yet
        None
    }

    /// Send block report to NameNode
    pub async fn send_block_report(&self) -> Result<()> {
        info!("Sending block report to NameNode");

        let blocks = self.block_storage.get_all_blocks();

        // Create gRPC client
        let mut client = self.create_client().await?;

        // Create volume info for registration
        let volume_info = self.volume_manager.get_volume_info()
            .map_err(|e| NamenodeClientError::HeartbeatFailed(format!("Failed to get volume info: {}", e)))?;

        // Create registration
        let registration = self.create_registration_proto(&volume_info);

        // Import the required protobuf types for block report
        use pillar_proto::hdfs::datanode::{
            StorageBlockReportProto,
        };
        use pillar_proto::hdfs::BlockProto;

        // Convert blocks to protobuf format - StorageBlockReportProto.blocks expects Vec<u64> of block IDs
        let block_ids: Vec<u64> = blocks
            .iter()
            .map(|(block_id, _num_bytes)| block_id.block_id)
            .collect();

        // Create storage block report
        let storage_report = StorageBlockReportProto {
            storage: pillar_proto::hdfs::DatanodeStorageProto {
                storage_uuid: self.datanode_info.uuid.clone(),
                state: Some(0), // NORMAL
                storage_type: Some(1), // DISK
            },
            blocks: block_ids,
            blocks_buffers: vec![],
            number_of_blocks: None,
        };

        // Create block report request
        let request = Request::new(BlockReportRequestProto {
            registration: registration,
            block_pool_id: self.block_pool_id.clone(),
            reports: vec![storage_report],
            context: None,
        });

        // Send block report request
        let response = client.block_report(request).await?;

        info!("Block report sent successfully ({} blocks)", blocks.len());

        Ok(())
    }

    /// Start the heartbeat loop
    pub async fn start_heartbeat_loop(self: Arc<Self>) {
        let mut interval = time::interval(self.heartbeat_interval);
        let mut first_heartbeat = true;

        loop {
            interval.tick().await;

            match self.send_heartbeat().await {
                Ok(commands) => {
                    if first_heartbeat {
                        info!("First heartbeat successful");
                        first_heartbeat = false;
                    }

                    // Process commands from NameNode
                    for command in commands {
                        if let Err(e) = self.handle_command(command).await {
                            error!("Error handling command: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Heartbeat failed: {}", e);
                }
            }
        }
    }

    /// Handle a command from the NameNode
    async fn handle_command(&self, command: DatanodeCommand) -> Result<()> {
        match command {
            DatanodeCommand::ReplicateBlocks { blocks, target } => {
                info!("Received command to replicate {} blocks to {}", blocks.len(), target);
                // TODO: Implement block replication
            }
            DatanodeCommand::InvalidateBlocks { blocks } => {
                info!("Received command to invalidate {} blocks", blocks.len());
                // TODO: Implement block deletion
            }
            DatanodeCommand::Shutdown => {
                warn!("Received shutdown command from NameNode");
                // TODO: Implement graceful shutdown
            }
        }

        Ok(())
    }
}

/// Commands that can be received from NameNode
#[derive(Debug, Clone)]
pub enum DatanodeCommand {
    ReplicateBlocks {
        blocks: Vec<u64>,
        target: String,
    },
    InvalidateBlocks {
        blocks: Vec<u64>,
    },
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[tokio::test]
    async fn test_namenode_client_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = Arc::new(VolumeManager::new(mount_point, "BP-test".to_string()).unwrap());

        let finalized_dir = volume_manager.get_finalized_dir();
        let tmp_dir = volume_manager.get_tmp_dir();
        let block_storage = Arc::new(BlockStorage::new(finalized_dir, tmp_dir).unwrap());

        let datanode_info = DatanodeInfo {
            uuid: "dn-test-001".to_string(),
            hostname: "localhost".to_string(),
            rpc_port: 9000,
            data_port: 50010,
            http_port: 50075,
        };

        let client = NamenodeClient::new(
            "localhost:9000".to_string(),
            datanode_info,
            volume_manager,
            block_storage,
        );

        assert_eq!(client.namenode_addr, "localhost:9000");
    }
}
