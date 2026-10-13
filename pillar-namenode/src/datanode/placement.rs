use std::sync::Arc;

use super::manager::{DatanodeManager, DatanodeInfo};
use crate::metadata::block_manager::DatanodeId;

/// Block placement policy for replica selection
pub struct BlockPlacementPolicy {
    datanode_manager: Arc<DatanodeManager>,
    default_replication: u16,
}

impl BlockPlacementPolicy {
    pub fn new(datanode_manager: Arc<DatanodeManager>, default_replication: u16) -> Self {
        Self {
            datanode_manager,
            default_replication,
        }
    }

    /// Select datanodes for a new block
    ///
    /// Simple policy for MVP:
    /// 1. Select nodes with most available space
    /// 2. Avoid nodes with high xceiver count
    /// 3. Prefer nodes with no failed volumes
    ///
    /// Works on lightweight [`DatanodeInfo`] snapshots, so the cost is
    /// O(number of datanodes), independent of how many blocks they store.
    pub fn choose_targets(&self, num_replicas: Option<u16>) -> Result<Vec<DatanodeId>, String> {
        let num_replicas = num_replicas.unwrap_or(self.default_replication) as usize;
        self.choose_targets_excluding(num_replicas, &[])
    }

    /// Select `num_replicas` live datanodes, skipping any whose UUID is in `exclude_uuids`.
    pub fn choose_targets_excluding(
        &self,
        num_replicas: usize,
        exclude_uuids: &[&str],
    ) -> Result<Vec<DatanodeId>, String> {
        let mut candidates: Vec<(f64, DatanodeInfo)> = self.datanode_manager
            .get_live_datanodes()
            .into_iter()
            .filter(|node| !exclude_uuids.contains(&node.id.uuid.as_str()))
            .map(|node| (Self::score_datanode(&node), node))
            .collect();

        if candidates.len() < num_replicas {
            return Err(format!(
                "Not enough live datanodes: {} available, {} required",
                candidates.len(),
                num_replicas
            ));
        }

        // Highest score first
        candidates.sort_by(|a, b| b.0.total_cmp(&a.0));

        Ok(candidates.into_iter()
            .take(num_replicas)
            .map(|(_, node)| node.id)
            .collect())
    }

    /// Score a datanode for placement (higher is better)
    pub fn score_datanode(node: &DatanodeInfo) -> f64 {
        let mut score = 0.0;

        // Factor 1: Available space (0-100 points)
        let space_ratio = node.storage.remaining as f64 / node.storage.capacity.max(1) as f64;
        score += space_ratio * 100.0;

        // Factor 2: Low xceiver count (0-50 points)
        let xceiver_penalty = (node.xceiver_count as f64 * 2.0).min(50.0);
        score -= xceiver_penalty;

        // Factor 3: No failed volumes (0-50 points bonus)
        if node.failed_volumes == 0 {
            score += 50.0;
        } else {
            score -= node.failed_volumes as f64 * 25.0;
        }

        score
    }

    /// Get default replication factor
    pub fn get_default_replication(&self) -> u16 {
        self.default_replication
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::block_manager::{BlockId, DatanodeId};
    use crate::datanode::manager::StorageInfo;

    #[test]
    fn test_placement_policy() {
        let manager = Arc::new(DatanodeManager::new());

        // Register 3 datanodes
        for i in 1..=3 {
            let id = DatanodeId::new(
                format!("dn{}", i),
                format!("host{}", i),
                9000,
            );
            manager.register(id, 1_000_000_000).unwrap();
        }

        let policy = BlockPlacementPolicy::new(manager.clone(), 3);

        // Choose targets
        let targets = policy.choose_targets(None).unwrap();
        assert_eq!(targets.len(), 3);
    }

    #[test]
    fn test_placement_prefers_healthy_nodes_and_honours_exclusions() {
        let manager = Arc::new(DatanodeManager::new());
        for i in 1..=4 {
            let id = DatanodeId::new(format!("dn{}", i), format!("host{}", i), 9000);
            manager.register(id, 1_000).unwrap();
        }
        // dn1: nearly full; dn2: failed volumes; dn3/dn4 healthy and empty.
        let nearly_full = StorageInfo { capacity: 1_000, used: 990, remaining: 10, block_pool_used: 990 };
        manager.heartbeat("dn1", nearly_full, 0, 0).unwrap();
        manager.heartbeat("dn2", StorageInfo::new(1_000), 0, 2).unwrap();
        // Many blocks on a node must not affect placement.
        for b in 0..10_000 {
            manager.add_block_to_datanode("dn3", BlockId::new(b, 1)).unwrap();
        }

        let policy = BlockPlacementPolicy::new(manager.clone(), 3);
        let targets: Vec<String> = policy.choose_targets(Some(2)).unwrap()
            .into_iter().map(|d| d.uuid).collect();
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&"dn3".to_string()));
        assert!(targets.contains(&"dn4".to_string()));

        let targets = policy.choose_targets_excluding(2, &["dn3"]).unwrap();
        assert!(targets.iter().all(|d| d.uuid != "dn3"));
        assert_eq!(targets[0].uuid, "dn4");

        assert!(policy.choose_targets_excluding(4, &["dn3"]).is_err());
    }
}
