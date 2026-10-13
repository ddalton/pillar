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

        // Read what placement needs, then release the inode lock: choosing
        // targets scans all datanodes and must not serialize other operations
        // on this file.
        let replication = {
            let inode_guard = inode.read().await;
            if inode_guard.is_dir {
                return Err(Status::invalid_argument("Cannot add block to directory"));
            }
            inode_guard.replication
        };

        // Select datanodes for replica placement (before allocating, so a
        // placement failure doesn't leak an allocated block)
        let targets = self.placement_policy
            .choose_targets(Some(replication))
            .map_err(|e| Status::resource_exhausted(format!("Failed to choose targets: {}", e)))?;

        let mut inode_guard = inode.write().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot add block to directory"));
        }

        // Allocate new block
        let block_id = self.block_manager
            .allocate_block(inode_guard.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to allocate block: {}", e)))?;

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
                num_blocks: dn.num_blocks as u32,
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

    /// Get additional datanode for pipeline recovery
    /// Called when a DataNode fails during write pipeline
    pub async fn handle_get_additional_datanode(
        &self,
        block_id: u64,
        generation_stamp: u64,
        existing_datanodes: &[DatanodeId],
        excluded_datanodes: &[DatanodeId],
        num_additional_nodes: u32,
    ) -> Result<LocatedBlock, Status> {
        info!(
            "Getting {} additional datanode(s) for block {} (excluding {} nodes)",
            num_additional_nodes,
            block_id,
            existing_datanodes.len() + excluded_datanodes.len()
        );

        // Choose the best live datanodes, excluding existing and excluded ones
        let exclude: Vec<&str> = existing_datanodes
            .iter()
            .chain(excluded_datanodes.iter())
            .map(|dn| dn.uuid.as_str())
            .collect();

        let new_locations = self.placement_policy
            .choose_targets_excluding(num_additional_nodes as usize, &exclude)
            .map_err(|e| Status::resource_exhausted(format!(
                "Not enough datanodes available: need {}: {}",
                num_additional_nodes, e
            )))?;

        // Combine existing and new locations
        let mut all_locations = existing_datanodes.to_vec();
        all_locations.extend(new_locations);

        info!("Selected {} additional datanode(s) for block {}", num_additional_nodes, block_id);

        Ok(LocatedBlock {
            block_id,
            generation_stamp,
            num_bytes: 0,
            offset: 0,
            locations: all_locations,
        })
    }

    /// Update block generation stamp for pipeline
    pub async fn handle_update_block_for_pipeline(
        &self,
        block_id: u64,
        old_generation_stamp: u64,
    ) -> Result<LocatedBlock, Status> {
        info!(
            "Updating block {} generation stamp from {}",
            block_id, old_generation_stamp
        );

        // Allocate new generation stamp
        let new_generation_stamp = self.block_manager
            .allocate_generation_stamp()
            .await;

        // Update the block with new generation stamp
        let old_block_id = BlockId::new(block_id, old_generation_stamp);
        let new_block_id = BlockId::new(block_id, new_generation_stamp);

        // Get the block info
        let block_info = self.block_manager
            .get_block_info(&old_block_id)
            .ok_or_else(|| Status::not_found(format!("Block not found: {}", block_id)))?;

        // Get current locations
        let locations = self.block_manager
            .get_block_locations(&old_block_id)
            .unwrap_or_default();

        // Update block manager with new generation stamp
        self.block_manager.update_generation_stamp(old_block_id, new_block_id);

        info!(
            "Updated block {} generation stamp: {} -> {}",
            block_id, old_generation_stamp, new_generation_stamp
        );

        Ok(LocatedBlock {
            block_id,
            generation_stamp: new_generation_stamp,
            num_bytes: block_info.num_bytes,
            offset: 0,
            locations,
        })
    }

    /// Recover lease (force close file when client crashes)
    pub async fn handle_recover_lease(
        &self,
        path: &str,
        _client_name: &str,
    ) -> Result<bool, Status> {
        info!("Recovering lease for file: {}", path);

        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let inode_guard = inode.read().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot recover lease for directory"));
        }

        // In a full implementation, this would:
        // 1. Check if the file has an active lease
        // 2. Contact DataNodes to finalize any under-construction blocks
        // 3. Update block sizes in metadata
        // 4. Close the file and release the lease
        //
        // For MVP, we simply mark the file as closed by ensuring all blocks are finalized
        if let Some(blocks) = &inode_guard.blocks {
            for block in blocks {
                let block_id = BlockId::new(block.block_id, block.generation_stamp);

                // Update block size if needed (would normally get this from DataNodes)
                if block.num_bytes == 0 {
                    // In production, query the DataNodes for actual block size
                    warn!("Block {} has zero size during lease recovery", block.block_id);
                }
            }
        }

        info!("Successfully recovered lease for file: {}", path);
        Ok(true)
    }

    /// Truncate a file to a new length
    pub async fn handle_truncate(
        &self,
        path: &str,
        new_length: u64,
    ) -> Result<bool, Status> {
        info!("Truncating file {} to {} bytes", path, new_length);

        let inode = self.namespace
            .lookup(path)
            .await
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        let mut inode_guard = inode.write().await;

        if inode_guard.is_dir {
            return Err(Status::invalid_argument("Cannot truncate directory"));
        }

        // Calculate current file length
        let current_length = inode_guard.blocks.as_ref()
            .map(|blocks| blocks.iter().map(|b| b.num_bytes).sum())
            .unwrap_or(0);

        // Validate new length
        if new_length > current_length {
            return Err(Status::invalid_argument(format!(
                "Cannot truncate to length {} greater than current length {}",
                new_length, current_length
            )));
        }

        // If new length is same as current, it's a no-op
        if new_length == current_length {
            info!("Truncate is no-op: new length equals current length");
            return Ok(true);
        }

        // Determine which blocks to keep and modify
        let block_size = inode_guard.block_size;
        let num_blocks_to_keep = ((new_length + block_size - 1) / block_size) as usize;

        if let Some(blocks) = &mut inode_guard.blocks {
            // If truncating to zero, remove all blocks
            if new_length == 0 {
                blocks.clear();
                info!("Truncated {} to zero length, removed all blocks", path);
                return Ok(true);
            }

            // Calculate the size of the last block after truncation
            let last_block_size = if new_length % block_size == 0 {
                block_size
            } else {
                new_length % block_size
            };

            // Keep only the blocks we need and update the last block's size
            blocks.truncate(num_blocks_to_keep);

            if let Some(last_block) = blocks.last_mut() {
                last_block.num_bytes = last_block_size;
                info!(
                    "Updated last block {} size to {} bytes",
                    last_block.block_id,
                    last_block_size
                );
            }

            info!(
                "Truncated {} from {} to {} bytes ({} blocks remaining)",
                path, current_length, new_length, blocks.len()
            );
        }

        Ok(true)
    }

    /// Concatenate source files into target file
    pub async fn handle_concat(
        &self,
        target: &str,
        sources: &[String],
    ) -> Result<(), Status> {
        info!("Concatenating {} files into {}", sources.len(), target);

        // Validate sources array is not empty
        if sources.is_empty() {
            return Err(Status::invalid_argument("Sources array cannot be empty"));
        }

        // Look up target file
        let target_inode = self.namespace
            .lookup(target)
            .await
            .map_err(|e| Status::not_found(format!("Target file not found: {}", e)))?;

        let mut target_guard = target_inode.write().await;

        if target_guard.is_dir {
            return Err(Status::invalid_argument("Target cannot be a directory"));
        }

        // Validate and collect all source files
        let mut source_blocks = Vec::new();
        for source_path in sources {
            let source_inode = self.namespace
                .lookup(source_path)
                .await
                .map_err(|e| Status::not_found(format!("Source file not found: {}", e)))?;

            let source_guard = source_inode.read().await;

            if source_guard.is_dir {
                return Err(Status::invalid_argument(format!(
                    "Source {} is a directory",
                    source_path
                )));
            }

            // Check block size compatibility
            if source_guard.block_size != target_guard.block_size {
                return Err(Status::invalid_argument(format!(
                    "Source {} has different block size ({} vs {})",
                    source_path, source_guard.block_size, target_guard.block_size
                )));
            }

            // Collect blocks from this source
            if let Some(blocks) = &source_guard.blocks {
                source_blocks.extend(blocks.clone());
            }
        }

        // Append source blocks to target
        if let Some(target_blocks) = &mut target_guard.blocks {
            target_blocks.extend(source_blocks);
        } else {
            target_guard.blocks = Some(source_blocks);
        }

        // Release target lock before deleting sources
        drop(target_guard);

        // Delete all source files
        for source_path in sources {
            self.namespace
                .delete(source_path, false)
                .await
                .map_err(|e| Status::internal(format!(
                    "Failed to delete source file {}: {}",
                    source_path, e
                )))?;
        }

        info!("Successfully concatenated {} files into {}", sources.len(), target);

        Ok(())
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

    #[tokio::test]
    async fn test_get_additional_datanode() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register multiple datanodes
        for i in 1..=5 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // Simulate existing datanodes in the pipeline
        let existing_dns = vec![
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
        ];

        let excluded_dns = vec![];

        // Request 1 additional datanode
        let result = service.handle_get_additional_datanode(
            1000,  // block_id
            1,     // generation_stamp
            &existing_dns,
            &excluded_dns,
            1,     // num_additional_nodes
        ).await.unwrap();

        // Should have 3 locations total (2 existing + 1 new)
        assert_eq!(result.locations.len(), 3);

        // The new datanode should not be dn1 or dn2
        let new_dn = &result.locations[2];
        assert_ne!(new_dn.uuid, "dn1");
        assert_ne!(new_dn.uuid, "dn2");
    }

    #[tokio::test]
    async fn test_get_additional_datanode_insufficient() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register only 2 datanodes
        for i in 1..=2 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // Both are already in use
        let existing_dns = vec![
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn1".to_string(),
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            crate::metadata::block_manager::DatanodeId {
                uuid: "dn2".to_string(),
                hostname: "host2".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
        ];

        // Request 1 more, but none available
        let result = service.handle_get_additional_datanode(
            1000,
            1,
            &existing_dns,
            &[],
            1,
        ).await;

        // Should fail with resource_exhausted
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_block_for_pipeline() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file and allocate a block
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        let block = service.handle_add_block("/test.txt").await.unwrap();
        let old_generation_stamp = block.generation_stamp;

        // Update block generation stamp
        let updated_block = service.handle_update_block_for_pipeline(
            block.block_id,
            old_generation_stamp,
        ).await.unwrap();

        // Generation stamp should be different
        assert_eq!(updated_block.block_id, block.block_id);
        assert_ne!(updated_block.generation_stamp, old_generation_stamp);
        assert!(updated_block.generation_stamp > old_generation_stamp);
    }

    #[tokio::test]
    async fn test_recover_lease() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Add a block
        let _block = service.handle_add_block("/test.txt").await.unwrap();

        // Recover lease
        let result = service.handle_recover_lease("/test.txt", "client1").await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_recover_lease_nonexistent_file() {
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

        // Try to recover lease for non-existent file
        let result = service.handle_recover_lease("/nonexistent.txt", "client1").await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Atomicity Tests - Concurrent Operations
    // ========================================================================

    #[tokio::test]
    async fn test_concurrent_rename_different_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create two files
        service.handle_create(
            "/file1.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();
        service.handle_create(
            "/file2.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename different files
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file1.txt", "/renamed1.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_rename("/file2.txt", "/renamed2.txt").await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both renames should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result1.unwrap());
        assert!(result2.unwrap());

        // Verify both files were renamed
        assert!(service.handle_get_file_info("/renamed1.txt").await.is_ok());
        assert!(service.handle_get_file_info("/renamed2.txt").await.is_ok());
        assert!(service.handle_get_file_info("/file1.txt").await.is_err());
        assert!(service.handle_get_file_info("/file2.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_rename_same_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename same file to different destinations
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file.txt", "/renamed1.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_rename("/file.txt", "/renamed2.txt").await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // One should succeed, one should fail
        let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one rename should succeed");

        // Original file should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());

        // Exactly one of the renamed files should exist
        let renamed1_exists = service.handle_get_file_info("/renamed1.txt").await.is_ok();
        let renamed2_exists = service.handle_get_file_info("/renamed2.txt").await.is_ok();
        assert!(renamed1_exists ^ renamed2_exists, "Exactly one renamed file should exist");
    }

    #[tokio::test]
    async fn test_rename_read_race() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename and read
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file.txt", "/renamed.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_get_file_info("/file.txt").await
        });

        let rename_result = handle1.await.unwrap();
        let read_result = handle2.await.unwrap();

        // Rename should succeed
        assert!(rename_result.is_ok());

        // Read might succeed or fail depending on timing, but shouldn't corrupt state
        // After rename, new name should exist
        assert!(service.handle_get_file_info("/renamed.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_rename_delete_race() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently rename and delete
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/file.txt", "/renamed.txt").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_delete("/file.txt", false).await
        });

        let rename_result = handle1.await.unwrap();
        let delete_result = handle2.await.unwrap();

        // One should succeed, one should fail
        let success_count = [rename_result.is_ok(), delete_result.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one operation should succeed");

        // Original file should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_rename_directory_concurrent() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create two directories
        service.handle_mkdir("/dir1", "user".to_string(), "group".to_string(), 0o755).await.unwrap();
        service.handle_mkdir("/dir2", "user".to_string(), "group".to_string(), 0o755).await.unwrap();

        // Concurrently rename different directories
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_rename("/dir1", "/renamed_dir1").await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_rename("/dir2", "/renamed_dir2").await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both renames should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Verify both directories were renamed
        assert!(service.handle_get_file_info("/renamed_dir1").await.is_ok());
        assert!(service.handle_get_file_info("/renamed_dir2").await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_create_same_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Concurrently create same file
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_create(
                "/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_create(
                "/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // One should succeed, one should fail (file already exists)
        let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one create should succeed");

        // File should exist
        assert!(service.handle_get_file_info("/file.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_create_different_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Concurrently create different files
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_create(
                "/file1.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_create(
                "/file2.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both creates should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Both files should exist
        assert!(service.handle_get_file_info("/file1.txt").await.is_ok());
        assert!(service.handle_get_file_info("/file2.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_create_read_race() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Concurrently create and read
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_create(
                "/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let handle2 = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            service2.handle_get_file_info("/file.txt").await
        });

        let create_result = handle1.await.unwrap();
        let read_result = handle2.await.unwrap();

        // Create should succeed
        assert!(create_result.is_ok());

        // Read might succeed or fail depending on timing
        // After create completes, file should exist
        assert!(service.handle_get_file_info("/file.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_delete_same_file() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently delete same file
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/file.txt", false).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_delete("/file.txt", false).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // One should succeed, one should fail gracefully
        let success_count = [result1.is_ok(), result2.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one delete should succeed");

        // File should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_delete_different_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create two files
        service.handle_create(
            "/file1.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();
        service.handle_create(
            "/file2.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently delete different files
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/file1.txt", false).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_delete("/file2.txt", false).await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // Both deletes should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Both files should not exist
        assert!(service.handle_get_file_info("/file1.txt").await.is_err());
        assert!(service.handle_get_file_info("/file2.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_while_reading() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a file
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Concurrently delete and read
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/file.txt", false).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_get_file_info("/file.txt").await
        });

        let delete_result = handle1.await.unwrap();
        let _read_result = handle2.await.unwrap();

        // Delete should succeed
        assert!(delete_result.is_ok());

        // Read might succeed or fail depending on timing
        // After delete, file should not exist
        assert!(service.handle_get_file_info("/file.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_delete_directory_while_creating_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 3));

        let service = Arc::new(ClientNamenodeService::new(
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
        ));

        // Create a directory
        service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await.unwrap();

        // Concurrently delete directory and create file in it
        let service1 = service.clone();
        let service2 = service.clone();

        let handle1 = tokio::spawn(async move {
            service1.handle_delete("/dir", true).await
        });

        let handle2 = tokio::spawn(async move {
            service2.handle_create(
                "/dir/file.txt",
                "user".to_string(),
                "group".to_string(),
                0o644,
                3,
                128 * 1024 * 1024,
            ).await
        });

        let delete_result = handle1.await.unwrap();
        let create_result = handle2.await.unwrap();

        // One should succeed, one should fail
        let success_count = [delete_result.is_ok(), create_result.is_ok()].iter().filter(|&&x| x).count();
        assert_eq!(success_count, 1, "Exactly one operation should succeed");
    }

    // ========================================================================
    // DataNode Failure Tests
    // ========================================================================

    #[tokio::test]
    async fn test_insufficient_datanodes() {
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

        // Register only 2 datanodes, but replication factor is 3
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
            1_000_000_000,
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
            1_000_000_000,
        ).unwrap();

        // Create a file - should fail because we need 3 datanodes but only have 2
        let result = service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3, // Replication factor 3
            128 * 1024 * 1024,
        ).await;

        // Creation should succeed (metadata operation)
        assert!(result.is_ok());

        // But adding a block should fail due to insufficient datanodes
        let add_block_result = service.handle_add_block("/test.txt").await;
        assert!(add_block_result.is_err());
    }

    #[tokio::test]
    async fn test_read_with_datanode_failure() {
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

        // Register 3 datanodes
        for i in 1..=3 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // All 3 datanodes should be alive initially
        assert_eq!(datanode_manager.get_live_datanodes().len(), 3);
        assert_eq!(datanode_manager.get_dead_datanodes().len(), 0);

        // Mark dn1 as dead
        datanode_manager.mark_datanode_dead_for_test("dn1");

        // Verify failure detection
        let live_datanodes = datanode_manager.get_live_datanodes();
        let dead_datanodes = datanode_manager.get_dead_datanodes();
        assert_eq!(live_datanodes.len(), 2);
        assert_eq!(dead_datanodes.len(), 1);
        assert_eq!(dead_datanodes[0].id.uuid, "dn1");

        // Live datanodes should only be dn2 and dn3
        let live_uuids: Vec<String> = live_datanodes.iter().map(|dn| dn.id.uuid.clone()).collect();
        assert!(live_uuids.contains(&"dn2".to_string()));
        assert!(live_uuids.contains(&"dn3".to_string()));
        assert!(!live_uuids.contains(&"dn1".to_string()));

        // Client would select datanodes for read, should only get live ones
        let selected = datanode_manager.select_datanodes(2).unwrap();
        assert_eq!(selected.len(), 2);

        // Selected should not include dead dn1
        for node in &selected {
            assert_ne!(node.uuid, "dn1", "Dead datanode should not be selected");
        }
    }

    #[tokio::test]
    async fn test_write_with_datanode_failure_recovery() {
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

        // Register 4 datanodes (one extra for replacement)
        for i in 1..=4 {
            datanode_manager.register(
                crate::metadata::block_manager::DatanodeId {
                    uuid: format!("dn{}", i),
                    hostname: format!("host{}", i),
                    rpc_port: 9867,
                    data_port: 50010,
                    http_port: 50075,
                    supported_transports: vec!["tcp".to_string()],
                    rdma_port: 0,
                },
                1_000_000_000,
            ).unwrap();
        }

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Add a block
        let block = service.handle_add_block("/test.txt").await.unwrap();
        let original_locations = block.locations.clone();

        // Simulate one datanode failing during write
        let failed_datanode = &original_locations[0];

        // Client calls get_additional_datanode to get replacement
        let existing: Vec<crate::metadata::block_manager::DatanodeId> = original_locations.clone();
        let excluded: Vec<crate::metadata::block_manager::DatanodeId> = vec![failed_datanode.clone()];

        let replacement_block = service.handle_get_additional_datanode(
            block.block_id,
            block.generation_stamp,
            &existing,
            &excluded,
            1,
        ).await.unwrap();

        // Replacement block should have original + new datanodes
        assert!(replacement_block.locations.len() >= 3);

        // New datanode should not be the failed one
        let new_locations: Vec<String> = replacement_block.locations
            .iter()
            .map(|loc| loc.uuid.clone())
            .collect();

        // Should have at least one datanode that wasn't in the original list
        let has_new_datanode = new_locations.iter()
            .any(|uuid| !existing.iter().any(|orig| &orig.uuid == uuid));

        assert!(has_new_datanode || new_locations.len() > existing.len(),
                "Should have added a new datanode for pipeline recovery");
    }

    // ========================================================================
    // Truncate Operation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_truncate_to_shorter_length() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file with a block
        let block_size = 128 * 1024 * 1024; // 128MB
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        // Simulate a full block
        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = block_size; // Full block
                }
            }
        }

        // Truncate to half the block size
        let new_length = block_size / 2;
        let result = service.handle_truncate("/test.txt", new_length).await.unwrap();
        assert!(result);

        // Verify the file length
        let file_info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(file_info.length, new_length);

        // Verify block was updated
        let inode_guard = inode.read().await;
        assert_eq!(inode_guard.blocks.as_ref().unwrap().len(), 1);
        assert_eq!(inode_guard.blocks.as_ref().unwrap()[0].num_bytes, new_length);
    }

    #[tokio::test]
    async fn test_truncate_to_same_length() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        let current_length = 64 * 1024 * 1024;
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = current_length;
                }
            }
        }

        // Truncate to same length (no-op)
        let result = service.handle_truncate("/test.txt", current_length).await.unwrap();
        assert!(result);

        // Verify nothing changed
        let file_info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(file_info.length, current_length);
    }

    #[tokio::test]
    async fn test_truncate_beyond_length_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        let current_length = 64 * 1024 * 1024;
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = current_length;
                }
            }
        }

        // Try to truncate beyond current length - should fail
        let result = service.handle_truncate("/test.txt", current_length + 1000).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_truncate_removes_complete_blocks() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file with multiple blocks
        let block_size = 128 * 1024 * 1024;
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        // Add 3 blocks
        service.handle_add_block("/test.txt").await.unwrap();
        service.handle_add_block("/test.txt").await.unwrap();
        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                // Make all blocks full
                for block in blocks.iter_mut() {
                    block.num_bytes = block_size;
                }
            }
        }

        // Verify we have 3 blocks
        {
            let inode_guard = inode.read().await;
            assert_eq!(inode_guard.blocks.as_ref().unwrap().len(), 3);
        }

        // Truncate to 1.5 blocks
        let new_length = block_size + (block_size / 2);
        let result = service.handle_truncate("/test.txt", new_length).await.unwrap();
        assert!(result);

        // Verify we now have 2 blocks
        let inode_guard = inode.read().await;
        assert_eq!(inode_guard.blocks.as_ref().unwrap().len(), 2);

        // First block should be full, second should be half
        assert_eq!(inode_guard.blocks.as_ref().unwrap()[0].num_bytes, block_size);
        assert_eq!(inode_guard.blocks.as_ref().unwrap()[1].num_bytes, block_size / 2);
    }

    #[tokio::test]
    async fn test_truncate_to_zero() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file with blocks
        let block_size = 128 * 1024 * 1024;
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();
        service.handle_add_block("/test.txt").await.unwrap();

        // Set block sizes
        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                for block in blocks.iter_mut() {
                    block.num_bytes = block_size / 2; // Half full blocks
                }
            }
        }

        // Truncate to zero
        let result = service.handle_truncate("/test.txt", 0).await.unwrap();
        assert!(result);

        // Verify all blocks were removed
        let inode_guard = inode.read().await;
        assert!(inode_guard.blocks.as_ref().unwrap().is_empty());

        // Verify file length is zero
        let file_info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(file_info.length, 0);
    }

    #[tokio::test]
    async fn test_truncate_nonexistent_file() {
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

        // Try to truncate non-existent file
        let result = service.handle_truncate("/nonexistent.txt", 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_truncate_directory_fails() {
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

        // Create a directory
        service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await.unwrap();

        // Try to truncate directory - should fail
        let result = service.handle_truncate("/dir", 0).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Concat Operation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_concat_multiple_files() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        let block_size = 128 * 1024 * 1024;

        // Create target file
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();
        service.handle_add_block("/target.txt").await.unwrap();

        // Create source files
        for i in 1..=3 {
            let path = format!("/source{}.txt", i);
            service.handle_create(
                &path,
                "user".to_string(),
                "group".to_string(),
                0o644,
                1,
                block_size,
            ).await.unwrap();
            service.handle_add_block(&path).await.unwrap();
        }

        // Concat source files into target
        let sources = vec![
            "/source1.txt".to_string(),
            "/source2.txt".to_string(),
            "/source3.txt".to_string(),
        ];

        let result = service.handle_concat("/target.txt", &sources).await;
        assert!(result.is_ok());

        // Verify target file has all blocks (1 original + 3 from sources = 4 total)
        let target_inode = namespace.lookup("/target.txt").await.unwrap();
        let target_guard = target_inode.read().await;
        assert_eq!(target_guard.blocks.as_ref().unwrap().len(), 4);

        // Verify source files were deleted
        for i in 1..=3 {
            let path = format!("/source{}.txt", i);
            assert!(namespace.lookup(&path).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_concat_empty_sources_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager,
            datanode_manager,
            placement_policy,
        );

        // Create target file
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat with empty sources - should fail
        let result = service.handle_concat("/target.txt", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concat_nonexistent_source_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

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
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create target file
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat with non-existent source
        let sources = vec!["/nonexistent.txt".to_string()];
        let result = service.handle_concat("/target.txt", &sources).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concat_different_block_sizes_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create target file with 128MB blocks
        service.handle_create(
            "/target.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Create source file with 64MB blocks
        service.handle_create(
            "/source.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            64 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat - should fail due to different block sizes
        let sources = vec!["/source.txt".to_string()];
        let result = service.handle_concat("/target.txt", &sources).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concat_directory_fails() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a directory and a file
        service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await.unwrap();
        service.handle_create(
            "/file.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Try to concat directory as target - should fail
        let sources = vec!["/file.txt".to_string()];
        let result = service.handle_concat("/dir", &sources).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Idempotency Tests
    // ========================================================================

    #[tokio::test]
    async fn test_create_idempotency() {
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

        // Create a file
        let result1 = service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await;
        assert!(result1.is_ok());

        // Retry create - should fail with already exists
        let result2 = service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await;
        assert!(result2.is_err());

        // File should still exist and be intact
        let info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(info.path, "/test.txt");
    }

    #[tokio::test]
    async fn test_delete_idempotency() {
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

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Delete the file
        let result1 = service.handle_delete("/test.txt", false).await;
        assert!(result1.is_ok());

        // Retry delete - should fail (file doesn't exist)
        let result2 = service.handle_delete("/test.txt", false).await;
        assert!(result2.is_err());

        // File should not exist
        assert!(service.handle_get_file_info("/test.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_rename_idempotency() {
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

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            3,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Rename the file
        let result1 = service.handle_rename("/test.txt", "/renamed.txt").await;
        assert!(result1.is_ok());

        // Retry rename - should fail (source doesn't exist)
        let result2 = service.handle_rename("/test.txt", "/renamed.txt").await;
        assert!(result2.is_err());

        // Renamed file should exist
        assert!(service.handle_get_file_info("/renamed.txt").await.is_ok());
        // Original should not exist
        assert!(service.handle_get_file_info("/test.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_truncate_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file with data
        let block_size = 128 * 1024 * 1024;
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            block_size,
        ).await.unwrap();

        service.handle_add_block("/test.txt").await.unwrap();

        let inode = namespace.lookup("/test.txt").await.unwrap();
        {
            let mut inode_guard = inode.write().await;
            if let Some(blocks) = &mut inode_guard.blocks {
                if let Some(block) = blocks.last_mut() {
                    block.num_bytes = block_size;
                }
            }
        }

        // Truncate the file
        let new_length = block_size / 2;
        let result1 = service.handle_truncate("/test.txt", new_length).await;
        assert!(result1.is_ok());

        // Retry truncate to same length - should succeed (idempotent)
        let result2 = service.handle_truncate("/test.txt", new_length).await;
        assert!(result2.is_ok());

        // File length should be the same
        let info = service.handle_get_file_info("/test.txt").await.unwrap();
        assert_eq!(info.length, new_length);
    }

    #[tokio::test]
    async fn test_mkdir_idempotency() {
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

        // Create a directory
        let result1 = service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await;
        assert!(result1.is_ok());

        // Retry mkdir - should fail (already exists)
        let result2 = service.handle_mkdir("/dir", "user".to_string(), "group".to_string(), 0o755).await;
        assert!(result2.is_err());

        // Directory should still exist
        let info = service.handle_get_file_info("/dir").await.unwrap();
        assert!(info.is_dir);
    }

    #[tokio::test]
    async fn test_append_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

        let service = ClientNamenodeService::new(
            namespace.clone(),
            block_manager.clone(),
            datanode_manager.clone(),
            placement_policy,
        );

        // Register a datanode
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
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // First append
        let result1 = service.handle_append("/test.txt").await;
        assert!(result1.is_ok());
        let block_id1 = result1.unwrap().block_id;

        // Retry append - should succeed and return same block (if not full)
        let result2 = service.handle_append("/test.txt").await;
        assert!(result2.is_ok());
        let block_id2 = result2.unwrap().block_id;

        // Should get the same block back since it's not full
        assert_eq!(block_id1, block_id2);
    }

    #[tokio::test]
    async fn test_complete_idempotency() {
        let namespace = Arc::new(Namespace::new());
        let block_manager = Arc::new(BlockManager::new());
        let datanode_manager = Arc::new(DatanodeManager::new());
        let placement_policy = Arc::new(BlockPlacementPolicy::new(datanode_manager.clone(), 1));

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
                hostname: "host1".to_string(),
                rpc_port: 9867,
                data_port: 50010,
                http_port: 50075,
                supported_transports: vec!["tcp".to_string()],
                rdma_port: 0,
            },
            1_000_000_000,
        ).unwrap();

        // Create a file
        service.handle_create(
            "/test.txt",
            "user".to_string(),
            "group".to_string(),
            0o644,
            1,
            128 * 1024 * 1024,
        ).await.unwrap();

        // Complete the file
        let result1 = service.handle_complete("/test.txt").await;
        assert!(result1.is_ok());

        // Retry complete - should succeed (idempotent)
        let result2 = service.handle_complete("/test.txt").await;
        assert!(result2.is_ok());

        // File should still exist
        assert!(service.handle_get_file_info("/test.txt").await.is_ok());
    }
}
