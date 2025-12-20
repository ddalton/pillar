use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use thiserror::Error;
use tracing::{info, warn, error, debug};

use crate::storage::{VolumeManager, BlockStorage};

#[derive(Error, Debug)]
pub enum NamenodeClientError {
    #[error("Registration failed: {0}")]
    RegistrationFailed(String),

    #[error("Heartbeat failed: {0}")]
    HeartbeatFailed(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),
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
}

impl NamenodeClient {
    pub fn new(
        namenode_addr: String,
        datanode_info: DatanodeInfo,
        volume_manager: Arc<VolumeManager>,
        block_storage: Arc<BlockStorage>,
    ) -> Self {
        Self {
            namenode_addr,
            datanode_info,
            volume_manager,
            block_storage,
            heartbeat_interval: Duration::from_secs(3),
        }
    }

    /// Register with the NameNode
    pub async fn register(&self) -> Result<()> {
        info!("Registering with NameNode at {}", self.namenode_addr);

        let volume_info = self.volume_manager.get_volume_info()
            .map_err(|e| NamenodeClientError::RegistrationFailed(format!("Failed to get volume info: {}", e)))?;

        // TODO: Implement actual gRPC call to NameNode
        // For now, this is a placeholder
        // In full implementation:
        // let mut client = DatanodeProtocolClient::connect(self.namenode_addr.clone()).await?;
        // let request = RegisterDatanodeRequest {
        //     registration: DatanodeRegistration {
        //         datanode_id: DatanodeID {
        //             uuid: self.datanode_info.uuid.clone(),
        //             hostname: self.datanode_info.hostname.clone(),
        //             rpc_port: self.datanode_info.rpc_port,
        //             ...
        //         },
        //         storage_info: StorageInfo {
        //             capacity: volume_info.capacity_bytes,
        //             ...
        //         },
        //     },
        // };
        // client.register_datanode(request).await?;

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

        // TODO: Implement actual gRPC call to NameNode
        // For now, this is a placeholder
        // In full implementation:
        // let request = HeartbeatRequest {
        //     registration: ...,
        //     reports: vec![StorageReport {
        //         capacity: volume_info.capacity_bytes,
        //         used: volume_info.used_bytes,
        //         remaining: volume_info.remaining_bytes,
        //         ...
        //     }],
        //     xmits_in_progress: 0,
        //     xceivers_count: 0,
        //     failed_volumes: 0,
        // };
        // let response = client.send_heartbeat(request).await?;
        // Parse commands from response

        debug!("Heartbeat sent successfully");
        debug!("  Capacity: {} GB", volume_info.capacity_bytes / 1_000_000_000);
        debug!("  Used: {} GB", volume_info.used_bytes / 1_000_000_000);
        debug!("  Remaining: {} GB", volume_info.remaining_bytes / 1_000_000_000);
        debug!("  Blocks: {}", self.block_storage.get_block_count());

        Ok(Vec::new()) // No commands for MVP
    }

    /// Send block report to NameNode
    pub async fn send_block_report(&self) -> Result<()> {
        info!("Sending block report to NameNode");

        let blocks = self.block_storage.get_all_blocks();

        // TODO: Implement actual gRPC call to NameNode
        // For now, this is a placeholder
        // In full implementation:
        // let request = BlockReportRequest {
        //     registration: ...,
        //     blocks: blocks.iter().map(|(block_id, num_bytes)| {
        //         BlockReportBlockProto {
        //             block_id: block_id.block_id,
        //             generation_stamp: block_id.generation_stamp,
        //             num_bytes: *num_bytes,
        //         }
        //     }).collect(),
        // };
        // client.block_report(request).await?;

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
