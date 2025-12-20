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

    /// Rename a file or directory
    pub async fn handle_rename(&self, src: &str, dst: &str) -> Result<bool, Status> {
        // For MVP, implement as copy metadata + delete old
        // Full implementation would be atomic
        let src_inode = self.namespace
            .lookup(src)
            .await
            .map_err(|e| Status::not_found(format!("Source not found: {}", e)))?;

        let src_guard = src_inode.read().await;

        if src_guard.is_dir {
            return Err(Status::unimplemented("Directory rename not yet implemented"));
        }

        // Create new file at destination
        self.namespace
            .create_file(
                dst,
                src_guard.owner.clone(),
                src_guard.group.clone(),
                src_guard.permissions,
                src_guard.replication,
                src_guard.block_size,
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to create destination: {}", e)))?;

        // Copy blocks (simplified - would need to copy block list)
        // For MVP, this is a placeholder

        // Delete source
        drop(src_guard);
        self.namespace
            .delete(src, false)
            .await
            .map_err(|e| Status::internal(format!("Failed to delete source: {}", e)))?;

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
}
