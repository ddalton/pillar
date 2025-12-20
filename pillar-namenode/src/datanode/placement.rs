use std::sync::Arc;

use super::manager::{DatanodeManager, DatanodeDescriptor};
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
    pub fn choose_targets(&self, num_replicas: Option<u16>) -> Result<Vec<DatanodeId>, String> {
        let num_replicas = num_replicas.unwrap_or(self.default_replication) as usize;

        let mut live_nodes = self.datanode_manager.get_live_datanodes();

        if live_nodes.len() < num_replicas {
            return Err(format!(
                "Not enough live datanodes: {} available, {} required",
                live_nodes.len(),
                num_replicas
            ));
        }

        // Score each node based on multiple factors
        live_nodes.sort_by(|a, b| {
            let score_a = self.score_datanode(a);
            let score_b = self.score_datanode(b);
            score_b.partial_cmp(&score_a).unwrap()
        });

        Ok(live_nodes.into_iter()
            .take(num_replicas)
            .map(|desc| desc.id)
            .collect())
    }

    /// Score a datanode for placement (higher is better)
    fn score_datanode(&self, node: &DatanodeDescriptor) -> f64 {
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
    use crate::metadata::block_manager::DatanodeId;
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
}
