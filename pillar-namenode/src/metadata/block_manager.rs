use dashmap::DashMap;
use std::sync::{Arc, RwLock};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
    pub data_port: u16,
    pub http_port: u16,
    /// Supported transports (e.g., ["tcp", "rdma"])
    pub supported_transports: Vec<String>,
    /// RDMA port (0 if not available)
    pub rdma_port: u16,
}

impl DatanodeId {
    pub fn new(uuid: String, hostname: String, rpc_port: u16) -> Self {
        Self {
            uuid,
            hostname,
            rpc_port,
            data_port: 50010,      // Default HDFS data transfer port
            http_port: 50075,      // Default HDFS HTTP UI port
            supported_transports: vec!["tcp".to_string()],  // Default to TCP only
            rdma_port: 0,          // No RDMA by default
        }
    }

    /// Create a DatanodeId from protobuf DatanodeIDProto
    pub fn from_proto(proto: &pillar_proto::hdfs::DatanodeIdProto) -> Self {
        Self {
            uuid: proto.datanode_uuid.clone(),
            hostname: proto.host_name.clone(),
            rpc_port: proto.ipc_port as u16,
            data_port: proto.xfer_port as u16,
            http_port: proto.info_port as u16,
            supported_transports: proto.supported_transports.clone(),
            rdma_port: proto.rdma_port.unwrap_or(0) as u16,
        }
    }
}

/// Information about a block
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BlockInfo {
    pub block_id: BlockId,
    pub num_bytes: u64,
    pub inode_id: u64,  // Which file this block belongs to
}

/// Compact handle for an interned datanode (index into [`DatanodeTable`]).
type DnIndex = u32;

/// Interning table for datanode identities referenced by replica locations.
///
/// Each replica is stored as a 4-byte [`DnIndex`] instead of a full
/// `DatanodeId` (several heap-allocated strings). Indexes are keyed by
/// datanode UUID and never reused; if a datanode re-registers with new
/// addresses, its entry is updated in place so all replicas see the new
/// address.
struct DatanodeTable {
    by_uuid: DashMap<String, DnIndex>,
    entries: RwLock<Vec<Arc<DatanodeId>>>,
}

impl DatanodeTable {
    fn new() -> Self {
        Self {
            by_uuid: DashMap::new(),
            entries: RwLock::new(Vec::new()),
        }
    }

    fn read_entries(&self) -> std::sync::RwLockReadGuard<'_, Vec<Arc<DatanodeId>>> {
        self.entries.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_entries(&self) -> std::sync::RwLockWriteGuard<'_, Vec<Arc<DatanodeId>>> {
        self.entries.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Intern a datanode, returning its compact index.
    fn intern(&self, datanode: &DatanodeId) -> DnIndex {
        if let Some(idx) = self.by_uuid.get(&datanode.uuid).map(|e| *e.value()) {
            // Refresh the stored identity if the datanode's addresses changed.
            let stale = **self.read_entries().get(idx as usize).expect("interned index") != *datanode;
            if stale {
                self.write_entries()[idx as usize] = Arc::new(datanode.clone());
            }
            return idx;
        }

        // Lock order: by_uuid shard, then entries. Nothing takes them in the
        // opposite order.
        match self.by_uuid.entry(datanode.uuid.clone()) {
            dashmap::mapref::entry::Entry::Occupied(e) => *e.get(),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let mut entries = self.write_entries();
                let idx = DnIndex::try_from(entries.len()).expect("too many datanodes");
                entries.push(Arc::new(datanode.clone()));
                drop(entries);
                e.insert(idx);
                idx
            }
        }
    }

    /// Index of an already-interned datanode, by UUID.
    fn index_of(&self, uuid: &str) -> Option<DnIndex> {
        self.by_uuid.get(uuid).map(|e| *e.value())
    }

    fn resolve_all(&self, indexes: &[DnIndex]) -> Vec<Arc<DatanodeId>> {
        let entries = self.read_entries();
        indexes.iter().map(|&i| entries[i as usize].clone()).collect()
    }
}

/// BlockManager manages block allocation and replica placement
pub struct BlockManager {
    /// All blocks: block_id -> BlockInfo
    blocks: DashMap<BlockId, BlockInfo>,

    /// Block locations: block_id -> compact datanode indexes (see [`DatanodeTable`])
    block_locations: DashMap<BlockId, Vec<DnIndex>>,

    /// Interned datanode identities referenced by `block_locations`
    datanode_table: DatanodeTable,

    /// File to blocks mapping: inode_id -> Vec<BlockId>
    file_blocks: DashMap<u64, Vec<BlockId>>,

    /// Next block ID to allocate (lock-free atomic counter)
    next_block_id: AtomicU64,

    /// Next generation stamp (lock-free atomic counter)
    next_generation_stamp: AtomicU64,

    /// Default block size (128 MB)
    default_block_size: u64,

    /// Replicas of removed blocks still on DataNodes: uuid -> blocks to
    /// delete, handed out with that DataNode's next heartbeat.
    pending_invalidations: DashMap<String, Vec<BlockId>>,

    /// Blocks being written: the DataNodes of their pipeline (HDFS
    /// "expected locations"). Readers of a file still open ask them how much
    /// of the block is visible; no replica is reported until it is finished.
    expected_locations: DashMap<BlockId, Vec<DnIndex>>,
}

impl BlockManager {
    pub fn new() -> Self {
        Self {
            blocks: DashMap::new(),
            block_locations: DashMap::new(),
            datanode_table: DatanodeTable::new(),
            file_blocks: DashMap::new(),
            next_block_id: AtomicU64::new(1),
            next_generation_stamp: AtomicU64::new(1000),
            default_block_size: 128 * 1024 * 1024, // 128 MB
            pending_invalidations: DashMap::new(),
            expected_locations: DashMap::new(),
        }
    }

    /// The DataNodes writing `block` (its pipeline).
    pub fn set_expected_locations(&self, block: BlockId, nodes: &[DatanodeId]) {
        let idx = nodes.iter().map(|n| self.datanode_table.intern(n)).collect();
        self.expected_locations.insert(block, idx);
    }

    /// The DataNodes writing `block`, if it is being written.
    pub fn get_expected_locations(&self, block: &BlockId) -> Vec<DatanodeId> {
        let idx = self.expected_locations.get(block).map(|e| e.value().clone()).unwrap_or_default();
        self.datanode_table.resolve_all(&idx).into_iter().map(|d| (*d).clone()).collect()
    }

    /// One more DataNode writing `block` (it reported a replica being written).
    pub fn add_expected_location(&self, block: BlockId, node: &DatanodeId) {
        let idx = self.datanode_table.intern(node);
        let mut nodes = self.expected_locations.entry(block).or_default();
        if !nodes.contains(&idx) {
            nodes.push(idx);
        }
    }

    /// `block` is no longer being written.
    pub fn clear_expected_locations(&self, block: &BlockId) {
        self.expected_locations.remove(block);
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
        self.get_block_locations_shared(block_id)
            .map(|locs| locs.iter().map(|dn| (**dn).clone()).collect())
    }

    /// Like [`BlockManager::get_block_locations`] but returns shared handles,
    /// avoiding a deep copy of each `DatanodeId`.
    pub fn get_block_locations_shared(&self, block_id: &BlockId) -> Option<Vec<Arc<DatanodeId>>> {
        // Copy the (small) index list out so no map guard is held while resolving.
        let indexes = self.block_locations.get(block_id).map(|e| e.value().clone())?;
        Some(self.datanode_table.resolve_all(&indexes))
    }

    /// Number of known replicas of a block.
    pub fn get_replica_count(&self, block_id: &BlockId) -> usize {
        self.block_locations.get(block_id).map(|e| e.value().len()).unwrap_or(0)
    }

    /// Whether the given datanode (by UUID) is a known location of the block.
    pub fn has_block_location(&self, block_id: &BlockId, datanode_uuid: &str) -> bool {
        match self.datanode_table.index_of(datanode_uuid) {
            Some(idx) => self.block_locations
                .get(block_id)
                .map(|e| e.value().contains(&idx))
                .unwrap_or(false),
            None => false,
        }
    }

    /// Add a block location (datanode has a replica of this block).
    /// Adding the same datanode twice is a no-op.
    pub fn add_block_location(&self, block_id: BlockId, datanode: DatanodeId) {
        self.add_location_index(block_id, self.datanode_table.intern(&datanode));
    }

    fn add_location_index(&self, block_id: BlockId, idx: DnIndex) {
        let mut locations = self.block_locations
            .entry(block_id)
            .or_insert_with(Vec::new);
        if !locations.contains(&idx) {
            locations.push(idx);
        }
    }

    /// Remove a block location (matched by datanode UUID)
    pub fn remove_block_location(&self, block_id: &BlockId, datanode: &DatanodeId) {
        if let Some(idx) = self.datanode_table.index_of(&datanode.uuid) {
            if let Some(mut locations) = self.block_locations.get_mut(block_id) {
                locations.retain(|&i| i != idx);
            }
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
        self.add_location_index(block_id, self.datanode_table.intern(&datanode));
    }

    /// Queue deletion of `block`'s known replicas, before its locations are
    /// forgotten.
    fn invalidate_replicas(&self, block_id: &BlockId) {
        for dn in self.get_block_locations_shared(block_id).unwrap_or_default() {
            self.pending_invalidations.entry(dn.uuid.clone()).or_default().push(*block_id);
        }
    }

    /// Forget `datanode_uuid`'s replica of `block_id` and have the DataNode
    /// delete it (an excess replica, or a corrupt one).
    pub fn drop_replica(&self, block_id: &BlockId, datanode_uuid: &str) {
        if let Some(idx) = self.datanode_table.index_of(datanode_uuid) {
            if let Some(mut locations) = self.block_locations.get_mut(block_id) {
                locations.retain(|&i| i != idx);
            }
        }
        self.queue_invalidation(datanode_uuid, *block_id);
    }

    /// A replica reported corrupt (reportBadBlocks): drop it so the
    /// replication monitor makes a good copy, unless it is the block's last
    /// known replica, which HDFS keeps too (it may be partly readable).
    /// Returns whether it was dropped.
    pub fn drop_corrupt_replica(&self, block_id: &BlockId, datanode_uuid: &str) -> bool {
        let holders = self.get_block_locations_shared(block_id).unwrap_or_default();
        if holders.len() < 2 || !holders.iter().any(|h| h.uuid == datanode_uuid) {
            return false;
        }
        self.drop_replica(block_id, datanode_uuid);
        true
    }

    /// Tell `datanode_uuid` to delete its replica of `block_id`.
    pub fn queue_invalidation(&self, datanode_uuid: &str, block_id: BlockId) {
        let mut pending = self.pending_invalidations.entry(datanode_uuid.to_string()).or_default();
        if !pending.contains(&block_id) {
            pending.push(block_id);
        }
    }

    /// Replicas a DataNode should delete (blocks removed from the namespace).
    pub fn take_invalidations(&self, datanode_uuid: &str) -> Vec<BlockId> {
        self.pending_invalidations.remove(datanode_uuid).map(|(_, v)| v).unwrap_or_default()
    }

    /// Every block with its owning inode, length and known replica holders
    /// (for the replication monitor's scan).
    pub fn blocks_snapshot(&self) -> Vec<(BlockId, u64, u64, Vec<Arc<DatanodeId>>)> {
        let infos: Vec<(BlockId, u64, u64)> =
            self.blocks.iter().map(|e| (e.block_id, e.inode_id, e.num_bytes)).collect();
        infos
            .into_iter()
            .map(|(id, inode, len)| (id, inode, len, self.get_block_locations_shared(&id).unwrap_or_default()))
            .collect()
    }

    /// Delete all blocks for a file
    pub fn delete_file_blocks(&self, inode_id: u64) -> Vec<BlockId> {
        if let Some((_, blocks)) = self.file_blocks.remove(&inode_id) {
            for block_id in &blocks {
                self.invalidate_replicas(block_id);
                self.blocks.remove(block_id);
                self.block_locations.remove(block_id);
                self.expected_locations.remove(block_id);
            }
            blocks
        } else {
            Vec::new()
        }
    }

    /// Forget a single block (e.g. abandoned, or dropped by truncate).
    pub fn remove_block(&self, block_id: &BlockId) -> Option<BlockInfo> {
        self.invalidate_replicas(block_id);
        self.expected_locations.remove(block_id);
        let info = self.blocks.remove(block_id).map(|(_, info)| info);
        self.block_locations.remove(block_id);
        if let Some(info) = &info {
            if let Some(mut file_blocks) = self.file_blocks.get_mut(&info.inode_id) {
                file_blocks.retain(|b| b != block_id);
            }
        }
        info
    }

    /// Re-home all blocks of `from_inode` onto the end of `to_inode` (concat).
    pub fn move_file_blocks(&self, from_inode: u64, to_inode: u64) {
        if from_inode == to_inode {
            return;
        }
        if let Some((_, moved)) = self.file_blocks.remove(&from_inode) {
            for block_id in &moved {
                if let Some(mut info) = self.blocks.get_mut(block_id) {
                    info.inode_id = to_inode;
                }
            }
            self.file_blocks.entry(to_inode).or_default().extend(moved);
        }
    }

    /// The block with id `block_id`, whatever its generation stamp.
    pub fn find_block(&self, block_id: u64) -> Option<BlockId> {
        self.blocks.iter().find(|e| e.key().block_id == block_id).map(|e| *e.key())
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

    /// The id the next allocated block will get.
    pub fn next_block_id(&self) -> u64 {
        self.next_block_id.load(Ordering::SeqCst)
    }

    /// The next generation stamp to be handed out.
    pub fn next_generation_stamp(&self) -> u64 {
        self.next_generation_stamp.load(Ordering::SeqCst)
    }

    /// Never hand out ids below these (after loading persisted state).
    pub fn restore_counters(&self, next_block_id: u64, next_generation_stamp: u64) {
        self.next_block_id.fetch_max(next_block_id, Ordering::SeqCst);
        self.next_generation_stamp.fetch_max(next_generation_stamp, Ordering::SeqCst);
    }

    /// Register a recovered file's blocks (their locations come later, from
    /// block reports).
    pub fn restore_file(&self, inode_id: u64, blocks: &[crate::metadata::namespace::BlockInfo]) {
        if blocks.is_empty() {
            return;
        }
        let ids: Vec<BlockId> = blocks.iter().map(|b| BlockId::new(b.block_id, b.generation_stamp)).collect();
        for (b, id) in blocks.iter().zip(&ids) {
            self.blocks.insert(*id, BlockInfo { block_id: *id, num_bytes: b.num_bytes, inode_id });
        }
        self.file_blocks.insert(inode_id, ids);
    }

    /// Allocate a new generation stamp for pipeline updates
    pub async fn allocate_generation_stamp(&self) -> u64 {
        self.next_generation_stamp.fetch_add(1, Ordering::SeqCst)
    }

    /// Update block generation stamp (for pipeline recovery)
    pub fn update_generation_stamp(&self, old_block_id: BlockId, new_block_id: BlockId) {
        // Get the block info with old generation stamp
        if let Some(block_info) = self.blocks.remove(&old_block_id) {
            // Create new block info with updated generation stamp
            let new_block_info = BlockInfo {
                block_id: new_block_id,
                num_bytes: block_info.1.num_bytes,
                inode_id: block_info.1.inode_id,
            };

            // Insert with new generation stamp
            self.blocks.insert(new_block_id, new_block_info);

            // Update block locations if they exist
            if let Some(locations) = self.block_locations.remove(&old_block_id) {
                self.block_locations.insert(new_block_id, locations.1);
            }
            if let Some((_, pipeline)) = self.expected_locations.remove(&old_block_id) {
                self.expected_locations.insert(new_block_id, pipeline);
            }

            // Update file blocks mapping
            if let Some(mut file_blocks) = self.file_blocks.get_mut(&block_info.1.inode_id) {
                if let Some(pos) = file_blocks.iter().position(|b| *b == old_block_id) {
                    file_blocks[pos] = new_block_id;
                }
            }
        }
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

    #[tokio::test]
    async fn test_block_locations_are_interned_and_deduplicated() {
        let bm = BlockManager::new();
        let b1 = bm.allocate_block(100).await.unwrap();
        let b2 = bm.allocate_block(100).await.unwrap();

        let dn1 = DatanodeId::new("dn1".to_string(), "host1".to_string(), 9000);
        let dn2 = DatanodeId::new("dn2".to_string(), "host2".to_string(), 9000);

        // Duplicate adds/reports of the same datanode are ignored.
        bm.add_block_location(b1, dn1.clone());
        bm.add_block_location(b1, dn1.clone());
        bm.report_block(dn1.clone(), b1, 10);
        bm.report_block(dn2.clone(), b1, 10);
        bm.report_block(dn1.clone(), b2, 10);
        assert_eq!(bm.get_replica_count(&b1), 2);
        assert_eq!(bm.get_block_locations(&b1).unwrap(), vec![dn1.clone(), dn2.clone()]);
        assert!(bm.has_block_location(&b1, "dn2"));
        assert!(!bm.has_block_location(&b2, "dn2"));
        assert!(!bm.has_block_location(&b1, "unknown"));

        // Both blocks share one interned identity for dn1.
        let a = bm.get_block_locations_shared(&b1).unwrap();
        let b = bm.get_block_locations_shared(&b2).unwrap();
        assert!(Arc::ptr_eq(&a[0], &b[0]));

        // Re-registration with a new address is reflected for every replica.
        let mut dn1_moved = dn1.clone();
        dn1_moved.hostname = "host1-new".to_string();
        dn1_moved.data_port = 9866;
        bm.report_block(dn1_moved.clone(), b2, 10);
        assert_eq!(bm.get_block_locations(&b1).unwrap()[0], dn1_moved);
        assert_eq!(bm.get_block_locations(&b2).unwrap(), vec![dn1_moved.clone()]);

        // Removal matches by UUID, even with a stale DatanodeId.
        bm.remove_block_location(&b1, &dn1);
        assert_eq!(bm.get_block_locations(&b1).unwrap(), vec![dn2.clone()]);

        // Locations follow a generation-stamp bump.
        let b1_new = BlockId::new(b1.block_id, b1.generation_stamp + 100);
        bm.update_generation_stamp(b1, b1_new);
        assert!(bm.get_block_locations(&b1).is_none());
        assert_eq!(bm.get_block_locations(&b1_new).unwrap(), vec![dn2]);
    }

    #[tokio::test]
    async fn test_remove_and_move_blocks() {
        let bm = BlockManager::new();
        let a1 = bm.allocate_block(1).await.unwrap();
        let a2 = bm.allocate_block(1).await.unwrap();
        let c1 = bm.allocate_block(2).await.unwrap();
        bm.add_block_location(a2, DatanodeId::new("dn1".to_string(), "h".to_string(), 1));

        assert_eq!(bm.remove_block(&a2).unwrap().inode_id, 1);
        assert!(bm.get_block_info(&a2).is_none());
        assert!(bm.get_block_locations(&a2).is_none());
        assert_eq!(bm.get_file_blocks(1).unwrap(), vec![a1]);

        bm.move_file_blocks(2, 1);
        assert_eq!(bm.get_file_blocks(1).unwrap(), vec![a1, c1]);
        assert!(bm.get_file_blocks(2).is_none());
        assert_eq!(bm.get_block_info(&c1).unwrap().inode_id, 1);
    }

    #[tokio::test]
    async fn a_corrupt_replica_is_dropped_unless_it_is_the_last() {
        let bm = BlockManager::new();
        let b = bm.allocate_block(7).await.unwrap();
        bm.report_block(DatanodeId::new("dn1".into(), "h1".into(), 1), b, 10);
        bm.report_block(DatanodeId::new("dn2".into(), "h2".into(), 1), b, 10);
        assert!(!bm.drop_corrupt_replica(&b, "dn3"), "not a holder");
        assert!(bm.drop_corrupt_replica(&b, "dn1"));
        assert_eq!(bm.take_invalidations("dn1"), vec![b]);
        assert_eq!(bm.get_replica_count(&b), 1);
        assert!(!bm.drop_corrupt_replica(&b, "dn2"), "the last replica is kept");
        assert_eq!(bm.get_replica_count(&b), 1);
        assert!(bm.take_invalidations("dn2").is_empty());
    }

    #[tokio::test]
    async fn removing_a_block_queues_its_replicas_for_deletion() {
        let bm = BlockManager::new();
        let b = bm.allocate_block(7).await.unwrap();
        bm.report_block(DatanodeId::new("dn1".into(), "h1".into(), 1), b, 10);
        bm.report_block(DatanodeId::new("dn2".into(), "h2".into(), 1), b, 10);
        assert_eq!(bm.blocks_snapshot().len(), 1);
        assert_eq!(bm.delete_file_blocks(7), vec![b]);
        assert_eq!(bm.take_invalidations("dn1"), vec![b]);
        assert_eq!(bm.take_invalidations("dn2"), vec![b]);
        assert!(bm.take_invalidations("dn1").is_empty());
        assert!(bm.blocks_snapshot().is_empty());
    }
}
