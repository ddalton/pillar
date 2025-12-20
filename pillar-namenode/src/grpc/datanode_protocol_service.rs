use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::datanode::DatanodeManager;
use crate::metadata::BlockManager;
use crate::metadata::block_manager::{BlockId, DatanodeId};
use crate::datanode::manager::StorageInfo;

/// DatanodeProtocol service - handles communication from DataNodes to NameNode
pub struct DatanodeProtocolService {
    datanode_manager: Arc<DatanodeManager>,
    block_manager: Arc<BlockManager>,
}

impl DatanodeProtocolService {
    pub fn new(
        datanode_manager: Arc<DatanodeManager>,
        block_manager: Arc<BlockManager>,
    ) -> Self {
        Self {
            datanode_manager,
            block_manager,
        }
    }

    /// Handle datanode registration
    pub async fn handle_register_datanode(
        &self,
        datanode_id_proto: &pillar_proto::hdfs::DatanodeIdProto,
    ) -> Result<(), Status> {
        // Convert protobuf to internal representation (includes RDMA capabilities)
        let datanode_id = DatanodeId::from_proto(datanode_id_proto);

        // Default capacity - will be updated on first heartbeat
        let capacity = 0;

        self.datanode_manager
            .register(datanode_id.clone(), capacity)
            .map_err(|e| Status::internal(format!("Registration failed: {}", e)))?;

        // Log registration with transport capabilities
        let transports_str = if datanode_id.supported_transports.is_empty() {
            "tcp (legacy)".to_string()
        } else {
            datanode_id.supported_transports.join(", ")
        };

        info!(
            "Registered datanode: {} ({}:{}) - transports: {}{}",
            datanode_id.uuid,
            datanode_id.hostname,
            datanode_id.data_port,
            transports_str,
            if datanode_id.rdma_port > 0 {
                format!(", RDMA port: {}", datanode_id.rdma_port)
            } else {
                String::new()
            }
        );

        Ok(())
    }

    /// Handle heartbeat from datanode
    pub async fn handle_heartbeat(
        &self,
        uuid: &str,
        capacity: u64,
        used: u64,
        remaining: u64,
        block_pool_used: u64,
    ) -> Result<Vec<DatanodeCommand>, Status> {
        let storage = StorageInfo {
            capacity,
            used,
            remaining,
            block_pool_used,
        };

        // Default values for optional fields
        let xceiver_count = 0;
        let failed_volumes = 0;

        self.datanode_manager
            .heartbeat(uuid, storage, xceiver_count, failed_volumes)
            .map_err(|e| Status::internal(format!("Heartbeat failed: {}", e)))?;

        // For now, return empty command list
        // In full implementation, this would return commands like:
        // - DNA_TRANSFER: replicate blocks
        // - DNA_INVALIDATE: delete blocks
        // - DNA_SHUTDOWN: shutdown datanode
        Ok(Vec::new())
    }

    /// Handle block report from datanode
    pub async fn handle_block_report(
        &self,
        uuid: String,
        blocks: Vec<(u64, u64, u64)>, // (block_id, generation_stamp, num_bytes)
    ) -> Result<(), Status> {
        let datanode_id = self.datanode_manager
            .get_datanode(&uuid)
            .ok_or_else(|| Status::not_found(format!("Datanode not found: {}", uuid)))?
            .id;

        info!("Received block report from {}: {} blocks", uuid, blocks.len());

        for (block_id, generation_stamp, num_bytes) in blocks {
            let block = BlockId::new(block_id, generation_stamp);

            // Report block to block manager
            self.block_manager.report_block(
                datanode_id.clone(),
                block,
                num_bytes,
            );

            // Add block to datanode's block list
            let _ = self.datanode_manager.add_block_to_datanode(&uuid, block);
        }

        Ok(())
    }

    /// Handle block received notification
    pub async fn handle_block_received(
        &self,
        uuid: String,
        block_id: u64,
        generation_stamp: u64,
        num_bytes: u64,
    ) -> Result<(), Status> {
        let block = BlockId::new(block_id, generation_stamp);

        let datanode_id = self.datanode_manager
            .get_datanode(&uuid)
            .ok_or_else(|| Status::not_found(format!("Datanode not found: {}", uuid)))?
            .id;

        // Add block location
        self.block_manager.add_block_location(block, datanode_id);

        // Add to datanode's block list
        self.datanode_manager
            .add_block_to_datanode(&uuid, block)
            .map_err(|e| Status::internal(format!("Failed to add block: {}", e)))?;

        info!("DataNode {} received block {}", uuid, block_id);

        Ok(())
    }

    /// Handle error report from datanode
    pub async fn handle_error_report(
        &self,
        uuid: String,
        error_message: String,
    ) -> Result<(), Status> {
        warn!("Error from datanode {}: {}", uuid, error_message);
        Ok(())
    }
}

/// Commands that NameNode can send to DataNode
#[derive(Debug, Clone)]
pub enum DatanodeCommand {
    /// Transfer blocks to other datanodes (for replication)
    TransferBlocks {
        blocks: Vec<BlockId>,
        targets: Vec<DatanodeId>,
    },

    /// Delete invalid blocks
    InvalidateBlocks {
        blocks: Vec<BlockId>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_datanode_registration() {
        let datanode_manager = Arc::new(DatanodeManager::new());
        let block_manager = Arc::new(BlockManager::new());
        let service = DatanodeProtocolService::new(datanode_manager.clone(), block_manager);

        let datanode_id_proto = pillar_proto::hdfs::DatanodeIdProto {
            ip_addr: "192.168.1.1".to_string(),
            host_name: "host1".to_string(),
            datanode_uuid: "dn1".to_string(),
            xfer_port: 50010,
            info_port: 50075,
            ipc_port: 9000,
            info_secure_port: Some(0),
            supported_transports: vec!["tcp".to_string()],
            rdma_port: Some(0),
        };

        service.handle_register_datanode(&datanode_id_proto).await.unwrap();

        let node = datanode_manager.get_datanode("dn1").unwrap();
        assert_eq!(node.id.uuid, "dn1");
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let datanode_manager = Arc::new(DatanodeManager::new());
        let block_manager = Arc::new(BlockManager::new());
        let service = DatanodeProtocolService::new(datanode_manager.clone(), block_manager);

        // Register first
        let datanode_id_proto = pillar_proto::hdfs::DatanodeIdProto {
            ip_addr: "192.168.1.1".to_string(),
            host_name: "host1".to_string(),
            datanode_uuid: "dn1".to_string(),
            xfer_port: 50010,
            info_port: 50075,
            ipc_port: 9000,
            info_secure_port: Some(0),
            supported_transports: vec!["tcp".to_string()],
            rdma_port: Some(0),
        };

        service.handle_register_datanode(&datanode_id_proto).await.unwrap();

        // Send heartbeat
        let commands = service.handle_heartbeat(
            "dn1",
            1_000_000_000, // capacity
            500_000_000,   // used
            500_000_000,   // remaining
            500_000_000,   // block_pool_used
        ).await.unwrap();

        assert_eq!(commands.len(), 0); // No commands for now
    }
}
