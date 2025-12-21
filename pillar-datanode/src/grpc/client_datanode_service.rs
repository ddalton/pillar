use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn, debug};

use crate::storage::{BlockStorage, VolumeManager};
use crate::storage::block_storage::BlockId;

/// ClientDatanodeProtocol service - handles communication from clients to DataNode
pub struct ClientDatanodeService {
    block_storage: Arc<BlockStorage>,
    volume_manager: Arc<VolumeManager>,
}

impl ClientDatanodeService {
    pub fn new(
        block_storage: Arc<BlockStorage>,
        volume_manager: Arc<VolumeManager>,
    ) -> Self {
        Self {
            block_storage,
            volume_manager,
        }
    }

    /// Get the visible length of a replica
    pub async fn handle_get_replica_visible_length(
        &self,
        block_id: u64,
    ) -> Result<u64, Status> {
        let metadata = self.block_storage.get_block_metadata(block_id)
            .ok_or_else(|| Status::not_found(format!("Block not found: {}", block_id)))?;

        Ok(metadata.num_bytes)
    }

    /// Get block local path information
    pub async fn handle_get_block_local_path_info(
        &self,
        block_id: u64,
        generation_stamp: u64,
    ) -> Result<String, Status> {
        let metadata = self.block_storage.get_block_metadata(block_id)
            .ok_or_else(|| Status::not_found(format!("Block not found: {}", block_id)))?;

        if metadata.block_id.generation_stamp != generation_stamp {
            return Err(Status::invalid_argument("Generation stamp mismatch"));
        }

        Ok(metadata.file_path.to_string_lossy().to_string())
    }

    /// Refresh namenodes (reconfiguration)
    pub async fn handle_refresh_namenodes(&self) -> Result<(), Status> {
        info!("Refresh namenodes requested");
        // For MVP, this is a no-op
        // In full implementation, this would reload NameNode addresses
        Ok(())
    }

    /// Delete a block pool
    pub async fn handle_delete_block_pool(&self, block_pool_id: String) -> Result<(), Status> {
        warn!("Delete block pool requested: {}", block_pool_id);
        // For MVP, this is not implemented
        Err(Status::unimplemented("Delete block pool not yet implemented"))
    }

    /// Shutdown the datanode
    pub async fn handle_shutdown_datanode(&self, for_upgrade: bool) -> Result<(), Status> {
        warn!("Shutdown datanode requested (for_upgrade={})", for_upgrade);
        // For MVP, this is not implemented
        // In full implementation, this would initiate graceful shutdown
        Err(Status::unimplemented("Shutdown not yet implemented"))
    }

    /// Get HDFS block locations (for client-side caching)
    pub async fn handle_get_hdfs_block_locations(
        &self,
        block_ids: Vec<u64>,
    ) -> Result<Vec<BlockLocation>, Status> {
        let mut locations = Vec::new();

        for block_id in block_ids {
            if let Some(metadata) = self.block_storage.get_block_metadata(block_id) {
                locations.push(BlockLocation {
                    block_id: metadata.block_id.block_id,
                    generation_stamp: metadata.block_id.generation_stamp,
                    num_bytes: metadata.num_bytes,
                    path: metadata.file_path.to_string_lossy().to_string(),
                });
            }
        }

        Ok(locations)
    }

    /// Get volume information
    pub async fn handle_get_volume_info(&self) -> Result<VolumeStats, Status> {
        let volume_info = self.volume_manager.get_volume_info()
            .map_err(|e| Status::internal(format!("Failed to get volume info: {}", e)))?;

        Ok(VolumeStats {
            capacity: volume_info.capacity_bytes,
            used: volume_info.used_bytes,
            remaining: volume_info.remaining_bytes,
            block_pool_used: volume_info.block_pool_used_bytes,
            num_blocks: self.block_storage.get_block_count(),
        })
    }
}

/// Block location information
#[derive(Debug, Clone)]
pub struct BlockLocation {
    pub block_id: u64,
    pub generation_stamp: u64,
    pub num_bytes: u64,
    pub path: String,
}

/// Volume statistics
#[derive(Debug, Clone)]
pub struct VolumeStats {
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
    use std::fs;

    #[tokio::test]
    async fn test_client_datanode_service_volume_info() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = Arc::new(VolumeManager::new(mount_point, "BP-test".to_string()).unwrap());

        let finalized_dir = volume_manager.get_finalized_dir();
        let tmp_dir = volume_manager.get_tmp_dir();
        let block_storage = Arc::new(BlockStorage::new(finalized_dir, tmp_dir).unwrap());

        let service = ClientDatanodeService::new(block_storage.clone(), volume_manager);

        // Get volume info
        let stats = service.handle_get_volume_info().await.unwrap();
        assert!(stats.capacity > 0);
        assert_eq!(stats.num_blocks, 0);
    }

    #[tokio::test]
    async fn test_get_replica_visible_length_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = Arc::new(VolumeManager::new(mount_point, "BP-test".to_string()).unwrap());
        let finalized_dir = volume_manager.get_finalized_dir();
        let tmp_dir = volume_manager.get_tmp_dir();
        let block_storage = Arc::new(BlockStorage::new(finalized_dir, tmp_dir).unwrap());

        let service = ClientDatanodeService::new(block_storage, volume_manager);

        // Try to get non-existent block
        let result = service.handle_get_replica_visible_length(99999).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_block_local_path_info_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = Arc::new(VolumeManager::new(mount_point, "BP-test".to_string()).unwrap());
        let finalized_dir = volume_manager.get_finalized_dir();
        let tmp_dir = volume_manager.get_tmp_dir();
        let block_storage = Arc::new(BlockStorage::new(finalized_dir, tmp_dir).unwrap());

        let service = ClientDatanodeService::new(block_storage, volume_manager);

        // Try to get non-existent block
        let result = service.handle_get_block_local_path_info(99999, 1).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_refresh_namenodes() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = Arc::new(VolumeManager::new(mount_point, "BP-test".to_string()).unwrap());
        let finalized_dir = volume_manager.get_finalized_dir();
        let tmp_dir = volume_manager.get_tmp_dir();
        let block_storage = Arc::new(BlockStorage::new(finalized_dir, tmp_dir).unwrap());

        let service = ClientDatanodeService::new(block_storage, volume_manager);

        // Should succeed (no-op in MVP)
        let result = service.handle_refresh_namenodes().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_hdfs_block_locations_empty() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = Arc::new(VolumeManager::new(mount_point, "BP-test".to_string()).unwrap());
        let finalized_dir = volume_manager.get_finalized_dir();
        let tmp_dir = volume_manager.get_tmp_dir();
        let block_storage = Arc::new(BlockStorage::new(finalized_dir, tmp_dir).unwrap());

        let service = ClientDatanodeService::new(block_storage, volume_manager);

        // Request non-existent blocks
        let result = service.handle_get_hdfs_block_locations(vec![1, 2, 3]).await.unwrap();
        assert_eq!(result.len(), 0);
    }
}
