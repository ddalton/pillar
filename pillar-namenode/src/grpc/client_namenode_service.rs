use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::metadata::{Namespace, BlockManager};
use crate::metadata::namespace::{FsPermission, INode};
use crate::metadata::block_manager::{BlockId, DatanodeId};
use crate::datanode::{DatanodeManager, BlockPlacementPolicy};

/// ClientNamenodeProtocol service - handles communication from HDFS clients to NameNode
pub struct ClientNamenodeService {
    namespace: Arc<Namespace>,
    block_manager: Arc<BlockManager>,
    datanode_manager: Arc<DatanodeManager>,
    placement_policy: Arc<BlockPlacementPolicy>,
}

impl ClientNamenodeService {
    pub fn new(
        namespace: Arc<Namespace>,
        block_manager: Arc<BlockManager>,
        datanode_manager: Arc<DatanodeManager>,
        placement_policy: Arc<BlockPlacementPolicy>,
    ) -> Self {
        Self {
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        }
    }

    /// Get file/directory info
    pub async fn handle_get_file_info(&self, path: &str) -> Result<FileInfo, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("Path not found: {}", e)))?;

        let inode_guard = inode.read().await;

        Ok(FileInfo {
            path: path.to_string(),
            is_dir: inode_guard.is_dir,
            length: self.get_file_length(&*inode_guard).await,
            block_size: inode_guard.block_size,
            replication: inode_guard.replication,
            modification_time: inode_guard.mtime,
            access_time: inode_guard.atime,
            owner: inode_guard.owner.clone(),
            group: inode_guard.group.clone(),
            permissions: inode_guard.permissions.mode,
        })
    }

    /// List directory contents
    pub async fn handle_get_listing(&self, path: &str) -> Result<Vec<FileInfo>, Status> {
        let children = self.namespace
            .list_directory(path)
            .await
            .map_err(|e| Status::not_found(format!("Directory not found: {}", e)))?;

        let mut result = Vec::new();

        for child in children {
            let child_guard = child.read().await;
            let child_path = if path == "/" {
                format!("/{}", child_guard.name)
            } else {
                format!("{}/{}", path, child_guard.name)
            };

            result.push(FileInfo {
                path: child_path,
                is_dir: child_guard.is_dir,
                length: self.get_file_length(&*child_guard).await,
                block_size: child_guard.block_size,
                replication: child_guard.replication,
                modification_time: child_guard.mtime,
                access_time: child_guard.atime,
                owner: child_guard.owner.clone(),
                group: child_guard.group.clone(),
                permissions: child_guard.permissions.mode,
            });
        }

        Ok(result)
    }

    /// Create a directory
    pub async fn handle_mkdir(
        &self,
        path: &str,
        owner: String,
        group: String,
        permissions: u16,
    ) -> Result<(), Status> {
        self.namespace
            .mkdir(path, owner, group, FsPermission::new(permissions))
            .await
            .map_err(|e| Status::already_exists(format!("Failed to create directory: {}", e)))?;

        info!("Created directory: {}", path);

        Ok(())
    }

    /// Create a file
    pub async fn handle_create(
        &self,
        path: &str,
        owner: String,
        group: String,
        permissions: u16,
        replication: u16,
        block_size: u64,
    ) -> Result<(), Status> {
        self.namespace
            .create_file(
                path,
                owner,
                group,
                FsPermission::new(permissions),
                replication,
                block_size,
            )
            .await
            .map_err(|e| Status::already_exists(format!("Failed to create file: {}", e)))?;

        info!("Created file: {}", path);

        Ok(())
    }

    /// Delete a file or directory
    pub async fn handle_delete(&self, path: &str, recursive: bool) -> Result<bool, Status> {
        self.namespace
            .delete(path, recursive)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete: {}", e)))?;

        info!("Deleted: {}", path);

        Ok(true)
    }

    /// Rename a file or directory (atomic operation)
    pub async fn handle_rename(&self, src: &str, dst: &str) -> Result<bool, Status> {
        self.namespace
            .rename(src, dst)
            .await
            .map_err(|e| Status::internal(format!("Failed to rename: {}", e)))?;

        info!("Renamed: {} -> {}", src, dst);

        Ok(true)
    }

    /// Get block locations for a file
    pub async fn handle_get_block_locations(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<LocatedBlock>, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let inode_guard = inode.read().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot get blocks for directory"));
        }

        let blocks = inode_guard.blocks.as_ref()
            .ok_or_else(|| Status::internal("File has no blocks"))?;

        let mut result = Vec::new();

        for (idx, block) in blocks.iter().enumerate() {
            let block_id = BlockId::new(block.block_id, block.generation_stamp);

            let locations = self.block_manager
                .get_block_locations(&block_id)
                .unwrap_or_default();

            result.push(LocatedBlock {
                block_id: block.block_id,
                generation_stamp: block.generation_stamp,
                num_bytes: block.num_bytes,
                offset: idx as u64 * inode_guard.block_size,
                locations,
            });
        }

        Ok(result)
    }

    /// Add a new block to a file (called by client when writing)
    pub async fn handle_add_block(&self, path: &str) -> Result<LocatedBlock, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let mut inode_guard = inode.write().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot add block to directory"));
        }

        // Allocate new block
        let block_id = self.block_manager
            .allocate_block(inode_guard.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to allocate block: {}", e)))?;

        // Select datanodes for replica placement
        let targets = self.placement_policy
            .choose_targets(Some(inode_guard.replication))
            .map_err(|e| Status::resource_exhausted(format!("Failed to choose targets: {}", e)))?;

        // Add block to inode
        inode_guard.add_block(crate::metadata::namespace::BlockInfo {
            block_id: block_id.block_id,
            generation_stamp: block_id.generation_stamp,
            num_bytes: 0,
        });

        info!("Allocated block {} for file: {}", block_id.block_id, path);

        Ok(LocatedBlock {
            block_id: block_id.block_id,
            generation_stamp: block_id.generation_stamp,
            num_bytes: 0,
            offset: 0,
            locations: targets,
        })
    }

    /// Complete a file (finalize writes)
    pub async fn handle_complete(&self, path: &str) -> Result<bool, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let inode_guard = inode.read().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot complete directory"));
        }

        info!("Completed file: {}", path);

        Ok(true)
    }

    /// Append to an existing file
    pub async fn handle_append(&self, path: &str) -> Result<LocatedBlock, Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let inode_guard = inode.read().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot append to directory"));
        }

        // Get the last block if it exists and isn't full
        if let Some(blocks) = &inode_guard.blocks {
            if let Some(last_block) = blocks.last() {
                if last_block.num_bytes < inode_guard.block_size {
                    // Return the last block for appending
                    let block_id = BlockId::new(last_block.block_id, last_block.generation_stamp);
                    let locations = self.block_manager
                        .get_block_locations(&block_id)
                        .unwrap_or_default();

                    info!("Appending to existing block {} for file: {}", last_block.block_id, path);

                    return Ok(LocatedBlock {
                        block_id: last_block.block_id,
                        generation_stamp: last_block.generation_stamp,
                        num_bytes: last_block.num_bytes,
                        offset: (blocks.len() - 1) as u64 * inode_guard.block_size,
                        locations,
                    });
                }
            }
        }

        // If no partial block exists, allocate a new one (same as add_block)
        drop(inode_guard);
        self.handle_add_block(path).await
    }

    /// Abandon a block allocation (cleanup failed writes)
    pub async fn handle_abandon_block(&self, path: &str, block_id: u64) -> Result<(), Status> {
        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let mut inode_guard = inode.write().await;

        if let Some(blocks) = &mut inode_guard.blocks {
            blocks.retain(|b| b.block_id != block_id);
            info!("Abandoned block {} for file: {}", block_id, path);
        }

        Ok(())
    }

    /// Get DataNode report (cluster health)
    pub async fn handle_get_datanode_report(&self) -> Result<Vec<DatanodeReportInfo>, Status> {
        let datanodes = self.datanode_manager.get_live_datanodes();

        let mut report = Vec::new();
        for dn in datanodes {
            report.push(DatanodeReportInfo {
                hostname: dn.id.hostname.clone(),
                uuid: dn.id.uuid.clone(),
                capacity: dn.storage.capacity,
                used: dn.storage.used,
                remaining: dn.storage.remaining,
                num_blocks: dn.blocks.len() as u32,
                last_update: dn.last_update.elapsed().as_secs(),
                supported_transports: dn.id.supported_transports.clone(),
                rdma_port: dn.id.rdma_port,
            });
        }

        Ok(report)
    }

    /// Get filesystem stats from actual DataNodes
    pub async fn handle_get_fs_stats_real(&self) -> Result<FsStats, Status> {
        let total_capacity = self.datanode_manager.get_total_capacity();
        let total_used = self.datanode_manager.get_total_used();
        let total_remaining = self.datanode_manager.get_total_remaining();

        Ok(FsStats {
            capacity: total_capacity,
            used: total_used,
            remaining: total_remaining,
            under_replicated: 0, // TODO: Calculate from block manager
            corrupt_blocks: 0,
            missing_blocks: 0,
        })
    }

    /// Get file length
    async fn get_file_length(&self, inode: &INode) -> u64 {
        if inode.is_dir {
            return 0;
        }

        inode.blocks.as_ref()
            .map(|blocks| blocks.iter().map(|b| b.num_bytes).sum())
            .unwrap_or(0)
    }
}

/// DataNode report information
#[derive(Debug, Clone)]
pub struct DatanodeReportInfo {
    pub hostname: String,
    pub uuid: String,
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub num_blocks: u32,
    pub last_update: u64,
    pub supported_transports: Vec<String>,
    pub rdma_port: u16,
}

/// Filesystem statistics
#[derive(Debug, Clone)]
pub struct FsStats {
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub under_replicated: u64,
    pub corrupt_blocks: u64,
    pub missing_blocks: u64,
}

/// File information returned to client
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub is_dir: bool,
    pub length: u64,
    pub block_size: u64,
    pub replication: u16,
    pub modification_time: u64,
    pub access_time: u64,
    pub owner: String,
    pub group: String,
    pub permissions: u16,
}

/// Block location information
#[derive(Debug, Clone)]
pub struct LocatedBlock {
    pub block_id: u64,
    pub generation_stamp: u64,
    pub num_bytes: u64,
    pub offset: u64,
    pub locations: Vec<DatanodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_operations() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create directory
        service.handle_mkdir("/test", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();

        // Get file info
        let info = service.handle_get_file_info("/test").await.unwrap();
        assert!(info.is_dir);
        assert_eq!(info.path, "/test");

        // List directory
        let listing = service.handle_get_listing("/").await.unwrap();
        assert_eq!(listing.len(), 1);
    }

    #[tokio::test]
    async fn test_append_operations() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1)); // Use 1 for testing

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode for block allocation
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "localhost".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000, // 1GB
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,  // replication = 1 for test
            128 * 1024 * 1024,
        ).await.unwrap();

        // Add a partial block (not full)
        let block1 = service.handle_add_block("/test.txt").await.unwrap();

        // Simulate partial block by updating its size
        let file_inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut file_guard = file_inode.write().await;
            if let Some(blocks) = &mut file_guard.blocks {
                if let Some(last_block) = blocks.last_mut() {
                    last_block.num_bytes = 64 * 1024 * 1024; // Half full
                }
            }
        }

        // Append should return the existing partial block
        let append_block = service.handle_append("/test.txt").await.unwrap();
        assert_eq!(append_block.block_id, block1.block_id);
        assert_eq!(append_block.num_bytes, 64 * 1024 * 1024);

        // Mark block as full
        {
            let mut file_guard = file_inode.write().await;
            if let Some(blocks) = &mut file_guard.blocks {
                if let Some(last_block) = blocks.last_mut() {
                    last_block.num_bytes = 128 * 1024 * 1024; // Full
                }
            }
        }

        // Append should allocate a new block since last one is full
        let append_block2 = service.handle_append("/test.txt").await.unwrap();
        assert_ne!(append_block2.block_id, block1.block_id);
    }

    #[tokio::test]
    async fn test_abandon_block() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1)); // Use 1 for testing

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "localhost".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file and add blocks
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,  // replication = 1 for test
            128 * 1024 * 1024,
        ).await.unwrap();

        let block1 = service.handle_add_block("/test.txt").await.unwrap();
        let block2 = service.handle_add_block("/test.txt").await.unwrap();

        // Verify 2 blocks exist
        let file_inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let file_guard = file_inode.read().await;
            assert_eq!(file_guard.blocks.as_ref().unwrap().len(), 2);
        }

        // Abandon the second block
        service.handle_abandon_block("/test.txt", block2.block_id).await.unwrap();

        // Verify only 1 block remains
        {
            let file_guard = file_inode.read().await;
            assert_eq!(file_guard.blocks.as_ref().unwrap().len(), 1);
            assert_eq!(file_guard.blocks.as_ref().unwrap()[0].block_id, block1.block_id);
        }
    }

    #[tokio::test]
    async fn test_rename_service() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create directories and file
        service.handle_mkdir("/src", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();
        service.handle_mkdir("/dst", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();
        service.handle_create(
            "/src/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Rename file
        let result = service.handle_rename("/src/file.txt", "/dst/renamed.txt").await.unwrap();
        assert!(result);

        // Verify old path doesn't exist
        assert!(service.handle_get_file_info("/src/file.txt").await.is_err());

        // Verify new path exists
        let info = service.handle_get_file_info("/dst/renamed.txt").await.unwrap();
        assert_eq!(info.path, "/dst/renamed.txt");
        assert!(!info.is_dir);

        // Test directory rename
        service.handle_mkdir("/olddir", "user".to_string(), "group".to_string(), 0o755)
            .await
            .unwrap();
        service.handle_create(
            "/olddir/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        let result = service.handle_rename("/olddir", "/newdir").await.unwrap();
        assert!(result);

        // Verify directory rename worked
        assert!(service.handle_get_file_info("/olddir").await.is_err());
        assert!(service.handle_get_file_info("/newdir").await.is_ok());
        assert!(service.handle_get_file_info("/newdir/file.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_datanode_report() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace,
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Initially no datanodes
        let report = service.handle_get_datanode_report().await.unwrap();
        assert_eq!(report.len(), 0);

        // Register datanodes
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000, // 1GB
        ).unwrap();

        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            2_000_000_000, // 2GB
        ).unwrap();

        // Get report
        let report = service.handle_get_datanode_report().await.unwrap();
        assert_eq!(report.len(), 2);

        // Verify datanode info
        let dn1 = report.iter().find(|dn| dn.uuid == "dn1").unwrap();
        assert_eq!(dn1.hostname, "host1");
        assert_eq!(dn1.capacity, 1_000_000_000);

        let dn2 = report.iter().find(|dn| dn.uuid == "dn2").unwrap();
        assert_eq!(dn2.hostname, "host2");
        assert_eq!(dn2.capacity, 2_000_000_000);
    }

    #[tokio::test]
    async fn test_fs_stats() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager.clone(),
            placement_policy,
        );

        // Initially no capacity
        let stats = service.handle_get_fs_stats_real().await.unwrap();
        assert_eq!(stats.capacity, 0);
        assert_eq!(stats.used, 0);
        assert_eq!(stats.remaining, 0);

        // Register datanodes with different capacities
        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000, // 1GB
        ).unwrap();

        datanode_manager.register(
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            2_000_000_000, // 2GB
        ).unwrap();

        // Simulate heartbeat to update storage stats
        use crate::datanode::manager::StorageInfo;
        datanode_manager.heartbeat(
            "dn1",
            StorageInfo {
                capacity: 1_000_000_000,
                used: 100_000_000,
                remaining: 900_000_000,
                block_pool_used: 100_000_000,
            },
            0,
            0,
        ).unwrap();

        datanode_manager.heartbeat(
            "dn2",
            StorageInfo {
                capacity: 2_000_000_000,
                used: 500_000_000,
                remaining: 1_500_000_000,
                block_pool_used: 500_000_000,
            },
            0,
            0,
        ).unwrap();

        // Get aggregated stats
        let stats = service.handle_get_fs_stats_real().await.unwrap();
        assert_eq!(stats.capacity, 3_000_000_000);
        assert_eq!(stats.used, 600_000_000);
        assert_eq!(stats.remaining, 2_400_000_000);
    }
}
