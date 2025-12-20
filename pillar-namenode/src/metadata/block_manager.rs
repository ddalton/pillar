use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlockManagerError {
    #[error("Block not found: {0}")]
    BlockNotFound(u64),

    #[error("Insufficient datanodes for replication")]
    InsufficientDatanodes,

    #[error("Invalid block size")]
    InvalidBlockSize,
}

pub type Result<T> = std::result::Result<T, BlockManagerError>;

/// Unique block identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId {
    pub block_id: u64,
    pub generation_stamp: u64,
}

impl BlockId {
    pub fn new(block_id: u64, generation_stamp: u64) -> Self {
        Self { block_id, generation_stamp }
    }
}

/// DataNode identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatanodeId {
    pub uuid: String,
    pub hostname: String,
    pub rpc_port: u16,
}

impl DatanodeId {
    pub fn new(uuid: String, hostname: String, rpc_port: u16) -> Self {
        Self { uuid, hostname, rpc_port }
    }
}

/// Information about a block
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub block_id: BlockId,
    pub num_bytes: u64,
    pub inode_id: u64,  // Which file this block belongs to
}

/// BlockManager manages block allocation and replica placement
pub struct BlockManager {
    /// All blocks: block_id -> BlockInfo
    blocks: DashMap<BlockId, BlockInfo>,

    /// Block locations: block_id -> Vec<DatanodeId>
    block_locations: DashMap<BlockId, Vec<DatanodeId>>,

    /// File to blocks mapping: inode_id -> Vec<BlockId>
    file_blocks: DashMap<u64, Vec<BlockId>>,

    /// Next block ID to allocate (lock-free atomic counter)
    next_block_id: AtomicU64,

    /// Next generation stamp (lock-free atomic counter)
    next_generation_stamp: AtomicU64,

    /// Default block size (128 MB)
    default_block_size: u64,
}

impl BlockManager {
    pub fn new() -> Self {
        Self {
            blocks: DashMap::new(),
            block_locations: DashMap::new(),
            file_blocks: DashMap::new(),
            next_block_id: AtomicU64::new(1),
            next_generation_stamp: AtomicU64::new(1000),
            default_block_size: 128 * 1024 * 1024, // 128 MB
        }
    }

    /// Allocate a new block for a file (lock-free using atomic operations)
    pub async fn allocate_block(&self, inode_id: u64) -> Result<BlockId> {
        let block_id = BlockId::new(
            self.next_block_id.fetch_add(1, Ordering::SeqCst),
            self.next_generation_stamp.fetch_add(1, Ordering::SeqCst),
        );

        let block_info = BlockInfo {
            block_id,
            num_bytes: 0, // Will be updated when block is finalized
            inode_id,
        };

        self.blocks.insert(block_id, block_info);

        // Add to file blocks
        self.file_blocks
            .entry(inode_id)
            .or_insert_with(Vec::new)
            .push(block_id);

        Ok(block_id)
    }

    /// Get block locations (which datanodes have this block)
    pub fn get_block_locations(&self, block_id: &BlockId) -> Option<Vec<DatanodeId>> {
        self.block_locations
            .get(block_id)
            .map(|entry| entry.value().clone())
    }

    /// Add a block location (datanode has a replica of this block)
    pub fn add_block_location(&self, block_id: BlockId, datanode: DatanodeId) {
        self.block_locations
            .entry(block_id)
            .or_insert_with(Vec::new)
            .push(datanode);
    }

    /// Remove a block location
    pub fn remove_block_location(&self, block_id: &BlockId, datanode: &DatanodeId) {
        if let Some(mut locations) = self.block_locations.get_mut(block_id) {
            locations.retain(|dn| dn != datanode);
        }
    }

    /// Get all blocks for a file
    pub fn get_file_blocks(&self, inode_id: u64) -> Option<Vec<BlockId>> {
        self.file_blocks
            .get(&inode_id)
            .map(|entry| entry.value().clone())
    }

    /// Update block size when finalized
    pub fn update_block_size(&self, block_id: &BlockId, num_bytes: u64) -> Result<()> {
        if let Some(mut block_info) = self.blocks.get_mut(block_id) {
            block_info.num_bytes = num_bytes;
            Ok(())
        } else {
            Err(BlockManagerError::BlockNotFound(block_id.block_id))
        }
    }

    /// Report a block from a datanode (during block report)
    pub fn report_block(&self, datanode: DatanodeId, block_id: BlockId, num_bytes: u64) {
        // Update or create block info
        if let Some(mut block_info) = self.blocks.get_mut(&block_id) {
            if block_info.num_bytes == 0 {
                block_info.num_bytes = num_bytes;
            }
        }

        // Add location if not already present
        let mut locations = self.block_locations
            .entry(block_id)
            .or_insert_with(Vec::new);

        if !locations.contains(&datanode) {
            locations.push(datanode);
        }
    }

    /// Delete all blocks for a file
    pub fn delete_file_blocks(&self, inode_id: u64) -> Vec<BlockId> {
        if let Some((_, blocks)) = self.file_blocks.remove(&inode_id) {
            for block_id in &blocks {
                self.blocks.remove(block_id);
                self.block_locations.remove(block_id);
            }
            blocks
        } else {
            Vec::new()
        }
    }

    /// Get block info
    pub fn get_block_info(&self, block_id: &BlockId) -> Option<BlockInfo> {
        self.blocks.get(block_id).map(|entry| entry.value().clone())
    }

    /// Get total number of blocks
    pub fn get_total_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Get default block size
    pub fn get_default_block_size(&self) -> u64 {
        self.default_block_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_block_allocation() {
        let bm = BlockManager::new();

        // Allocate block
        let block1 = bm.allocate_block(100).await.unwrap();
        assert_eq!(block1.block_id, 1);

        // Allocate another
        let block2 = bm.allocate_block(100).await.unwrap();
        assert_eq!(block2.block_id, 2);

        // Check file blocks
        let blocks = bm.get_file_blocks(100).unwrap();
        assert_eq!(blocks.len(), 2);
    }

    #[tokio::test]
    async fn test_block_locations() {
        let bm = BlockManager::new();
        let block = bm.allocate_block(100).await.unwrap();

        let dn1 = DatanodeId::new("dn1".to_string(), "host1".to_string(), 9000);
        let dn2 = DatanodeId::new("dn2".to_string(), "host2".to_string(), 9000);

        // Add locations
        bm.add_block_location(block, dn1.clone());
        bm.add_block_location(block, dn2.clone());

        // Get locations
        let locations = bm.get_block_locations(&block).unwrap();
        assert_eq!(locations.len(), 2);

        // Remove location
        bm.remove_block_location(&block, &dn1);
        let locations = bm.get_block_locations(&block).unwrap();
        assert_eq!(locations.len(), 1);
    }
}
