use std::sync::Arc;
use dashmap::DashMap;
use tonic::Status;
use tracing::{info, warn};

use crate::datanode::DatanodeManager;
use crate::metadata::BlockManager;
use crate::metadata::block_manager::{BlockId, DatanodeId};
use crate::datanode::manager::StorageInfo;

/// DatanodeProtocol service - handles communication from DataNodes to NameNode
pub struct DatanodeProtocolService {
    datanode_manager: Arc<DatanodeManager>,
    block_manager: Arc<BlockManager>,
    /// Commands waiting for each DataNode's next heartbeat, by uuid.
    pending_commands: DashMap<String, Vec<DatanodeCommand>>,
}

impl DatanodeProtocolService {
    pub fn new(
        datanode_manager: Arc<DatanodeManager>,
        block_manager: Arc<BlockManager>,
    ) -> Self {
        Self {
            datanode_manager,
            block_manager,
            pending_commands: DashMap::new(),
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

        // Hand over whatever the replication monitor queued for this node,
        // plus deletions of replicas whose blocks were removed.
        let mut commands = self.take_commands(uuid);
        let invalid = self.block_manager.take_invalidations(uuid);
        if !invalid.is_empty() {
            commands.push(DatanodeCommand::InvalidateBlocks { blocks: invalid });
        }
        Ok(commands)
    }

    /// Queue a command for `uuid`'s next heartbeat.
    pub fn queue_command(&self, uuid: &str, command: DatanodeCommand) {
        self.pending_commands.entry(uuid.to_string()).or_default().push(command);
    }

    /// Take the commands queued for `uuid`.
    pub fn take_commands(&self, uuid: &str) -> Vec<DatanodeCommand> {
        self.pending_commands.remove(uuid).map(|(_, v)| v).unwrap_or_default()
    }

    /// Number of DataNodes with commands waiting (for tests and metrics).
    pub fn pending_command_count(&self, uuid: &str) -> usize {
        self.pending_commands.get(uuid).map(|v| v.len()).unwrap_or(0)
    }

    fn datanode_id(&self, uuid: &str) -> Result<DatanodeId, Status> {
        self.datanode_manager
            .get_datanode(uuid)
            .map(|d| d.id)
            .ok_or_else(|| Status::not_found(format!("Datanode not found: {}", uuid)))
    }

    /// Handle block report from datanode
    pub async fn handle_block_report(
        &self,
        uuid: String,
        blocks: Vec<(u64, u64, u64)>, // (block_id, generation_stamp, num_bytes)
    ) -> Result<(), Status> {
        let datanode_id = self.datanode_id(&uuid)?;

        info!("Received block report from {}: {} blocks", uuid, blocks.len());

        for (block_id, generation_stamp, num_bytes) in blocks {
            self.record_replica(&uuid, &datanode_id, BlockId::new(block_id, generation_stamp), num_bytes);
        }

        Ok(())
    }

    /// Handle block received notification (incremental block report)
    pub async fn handle_block_received(
        &self,
        uuid: String,
        block_id: u64,
        generation_stamp: u64,
        num_bytes: u64,
    ) -> Result<(), Status> {
        let datanode_id = self.datanode_id(&uuid)?;
        self.record_replica(&uuid, &datanode_id, BlockId::new(block_id, generation_stamp), num_bytes);
        info!("DataNode {} received block {} ({} bytes)", uuid, block_id, num_bytes);
        Ok(())
    }

    /// Handle block deleted notification (incremental block report)
    pub async fn handle_block_deleted(
        &self,
        uuid: String,
        block_id: u64,
        generation_stamp: u64,
    ) -> Result<(), Status> {
        let datanode_id = self.datanode_id(&uuid)?;
        let block = BlockId::new(block_id, generation_stamp);
        self.block_manager.remove_block_location(&block, &datanode_id);
        let _ = self.datanode_manager.remove_block_from_datanode(&uuid, &block);
        info!("DataNode {} deleted block {}", uuid, block_id);
        Ok(())
    }

    /// Record that `uuid` holds a finalized replica. `report_block`
    /// de-duplicates locations, so repeated reports are idempotent.
    fn record_replica(&self, uuid: &str, datanode_id: &DatanodeId, block: BlockId, num_bytes: u64) {
        self.block_manager.report_block(datanode_id.clone(), block, num_bytes);
        let _ = self.datanode_manager.add_block_to_datanode(uuid, block);
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
    /// Copy blocks to other datanodes (for replication). Each entry is a
    /// block, its length, and the DataNodes that should receive it.
    TransferBlocks {
        transfers: Vec<(BlockId, u64, Vec<DatanodeId>)>,
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

        assert_eq!(commands.len(), 0);

        // A queued command is delivered once, on the next heartbeat.
        service.queue_command("dn1", DatanodeCommand::InvalidateBlocks { blocks: vec![BlockId::new(1, 1)] });
        let commands = service.handle_heartbeat("dn1", 1, 0, 1, 0).await.unwrap();
        assert_eq!(commands.len(), 1);
        assert!(service.handle_heartbeat("dn1", 1, 0, 1, 0).await.unwrap().is_empty());
    }
}
