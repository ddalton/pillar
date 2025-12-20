use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

use crate::metadata::block_manager::{BlockId, DatanodeId};

#[derive(Error, Debug)]
pub enum DatanodeManagerError {
    #[error("Datanode not found: {0}")]
    DatanodeNotFound(String),

    #[error("Datanode already registered: {0}")]
    DatanodeAlreadyRegistered(String),

    #[error("Insufficient datanodes for replication")]
    InsufficientDatanodes,
}

pub type Result<T> = std::result::Result<T, DatanodeManagerError>;

/// Storage information for a datanode
#[derive(Debug, Clone)]
pub struct StorageInfo {
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub block_pool_used: u64,
}

impl StorageInfo {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            used: 0,
            remaining: capacity,
            block_pool_used: 0,
        }
    }

    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.used as f64 / self.capacity as f64
        }
    }
}

/// Descriptor for a registered datanode
#[derive(Debug, Clone)]
pub struct DatanodeDescriptor {
    pub id: DatanodeId,
    pub storage: StorageInfo,
    pub last_update: Instant,
    pub last_block_report: Option<Instant>,
    pub blocks: HashSet<BlockId>,
    pub xceiver_count: u32,  // Number of active data transfers
    pub failed_volumes: u32,
}

impl DatanodeDescriptor {
    pub fn new(id: DatanodeId, capacity: u64) -> Self {
        Self {
            id,
            storage: StorageInfo::new(capacity),
            last_update: Instant::now(),
            last_block_report: None,
            blocks: HashSet::new(),
            xceiver_count: 0,
            failed_volumes: 0,
        }
    }

    pub fn is_alive(&self, stale_interval: Duration) -> bool {
        self.last_update.elapsed() < stale_interval
    }

    pub fn is_dead(&self, dead_interval: Duration) -> bool {
        self.last_update.elapsed() > dead_interval
    }

    pub fn update_heartbeat(&mut self, storage: StorageInfo, xceiver_count: u32, failed_volumes: u32) {
        self.last_update = Instant::now();
        self.storage = storage;
        self.xceiver_count = xceiver_count;
        self.failed_volumes = failed_volumes;
    }

    pub fn add_block(&mut self, block_id: BlockId) {
        self.blocks.insert(block_id);
    }

    pub fn remove_block(&mut self, block_id: &BlockId) {
        self.blocks.remove(block_id);
    }
}

/// Manages all registered datanodes
pub struct DatanodeManager {
    /// All registered datanodes: uuid -> DatanodeDescriptor
    datanodes: DashMap<String, DatanodeDescriptor>,

    /// Heartbeat interval (default: 3 seconds)
    heartbeat_interval: Duration,

    /// Stale datanode interval (default: 30 seconds)
    stale_interval: Duration,

    /// Dead datanode interval (default: 630 seconds = 10.5 minutes)
    dead_interval: Duration,
}

impl DatanodeManager {
    pub fn new() -> Self {
        Self {
            datanodes: DashMap::new(),
            heartbeat_interval: Duration::from_secs(3),
            stale_interval: Duration::from_secs(30),
            dead_interval: Duration::from_secs(630),
        }
    }

    /// Register a new datanode
    pub fn register(&self, id: DatanodeId, capacity: u64) -> Result<()> {
        let uuid = id.uuid.clone();

        if self.datanodes.contains_key(&uuid) {
            return Err(DatanodeManagerError::DatanodeAlreadyRegistered(uuid));
        }

        let descriptor = DatanodeDescriptor::new(id, capacity);
        self.datanodes.insert(uuid, descriptor);

        Ok(())
    }

    /// Process a heartbeat from a datanode
    pub fn heartbeat(
        &self,
        uuid: &str,
        storage: StorageInfo,
        xceiver_count: u32,
        failed_volumes: u32,
    ) -> Result<()> {
        if let Some(mut descriptor) = self.datanodes.get_mut(uuid) {
            descriptor.update_heartbeat(storage, xceiver_count, failed_volumes);
            Ok(())
        } else {
            Err(DatanodeManagerError::DatanodeNotFound(uuid.to_string()))
        }
    }

    /// Get a datanode by UUID
    pub fn get_datanode(&self, uuid: &str) -> Option<DatanodeDescriptor> {
        self.datanodes.get(uuid).map(|entry| entry.value().clone())
    }

    /// Get all live datanodes
    pub fn get_live_datanodes(&self) -> Vec<DatanodeDescriptor> {
        self.datanodes
            .iter()
            .filter(|entry| entry.value().is_alive(self.stale_interval))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all dead datanodes
    pub fn get_dead_datanodes(&self) -> Vec<DatanodeDescriptor> {
        self.datanodes
            .iter()
            .filter(|entry| entry.value().is_dead(self.dead_interval))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Select datanodes for block placement (simple round-robin for now)
    pub fn select_datanodes(&self, num_replicas: usize) -> Result<Vec<DatanodeId>> {
        let live_nodes = self.get_live_datanodes();

        if live_nodes.len() < num_replicas {
            return Err(DatanodeManagerError::InsufficientDatanodes);
        }

        // Simple selection: pick nodes with most remaining capacity
        let mut nodes = live_nodes;
        nodes.sort_by(|a, b| {
            b.storage.remaining.cmp(&a.storage.remaining)
        });

        Ok(nodes.into_iter()
            .take(num_replicas)
            .map(|desc| desc.id)
            .collect())
    }

    /// Add a block to a datanode's block list
    pub fn add_block_to_datanode(&self, uuid: &str, block_id: BlockId) -> Result<()> {
        if let Some(mut descriptor) = self.datanodes.get_mut(uuid) {
            descriptor.add_block(block_id);
            Ok(())
        } else {
            Err(DatanodeManagerError::DatanodeNotFound(uuid.to_string()))
        }
    }

    /// Remove a block from a datanode's block list
    pub fn remove_block_from_datanode(&self, uuid: &str, block_id: &BlockId) -> Result<()> {
        if let Some(mut descriptor) = self.datanodes.get_mut(uuid) {
            descriptor.remove_block(block_id);
            Ok(())
        } else {
            Err(DatanodeManagerError::DatanodeNotFound(uuid.to_string()))
        }
    }

    /// Get total capacity across all datanodes
    pub fn get_total_capacity(&self) -> u64 {
        self.datanodes
            .iter()
            .map(|entry| entry.value().storage.capacity)
            .sum()
    }

    /// Get total used space across all datanodes
    pub fn get_total_used(&self) -> u64 {
        self.datanodes
            .iter()
            .map(|entry| entry.value().storage.used)
            .sum()
    }

    /// Get total remaining space across all datanodes
    pub fn get_total_remaining(&self) -> u64 {
        self.datanodes
            .iter()
            .map(|entry| entry.value().storage.remaining)
            .sum()
    }

    /// Get number of live datanodes
    pub fn get_num_live_datanodes(&self) -> usize {
        self.get_live_datanodes().len()
    }

    /// Remove dead datanodes
    pub fn remove_dead_datanodes(&self) -> Vec<String> {
        let dead_nodes = self.get_dead_datanodes();
        let mut removed = Vec::new();

        for node in dead_nodes {
            self.datanodes.remove(&node.id.uuid);
            removed.push(node.id.uuid);
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datanode_registration() {
        let manager = DatanodeManager::new();

        let id = DatanodeId::new("dn1".to_string(), "host1".to_string(), 9000);
        manager.register(id.clone(), 1_000_000_000).unwrap();

        let descriptor = manager.get_datanode("dn1").unwrap();
        assert_eq!(descriptor.id.uuid, "dn1");
        assert_eq!(descriptor.storage.capacity, 1_000_000_000);
    }

    #[test]
    fn test_heartbeat() {
        let manager = DatanodeManager::new();

        let id = DatanodeId::new("dn1".to_string(), "host1".to_string(), 9000);
        manager.register(id, 1_000_000_000).unwrap();

        let storage = StorageInfo {
            capacity: 1_000_000_000,
            used: 500_000_000,
            remaining: 500_000_000,
            block_pool_used: 500_000_000,
        };

        manager.heartbeat("dn1", storage, 5, 0).unwrap();

        let descriptor = manager.get_datanode("dn1").unwrap();
        assert_eq!(descriptor.storage.used, 500_000_000);
        assert_eq!(descriptor.xceiver_count, 5);
    }
}
