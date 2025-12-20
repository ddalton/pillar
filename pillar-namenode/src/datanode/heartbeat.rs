use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{info, warn};

use super::manager::DatanodeManager;

/// Monitors datanode heartbeats and removes dead nodes
pub struct HeartbeatMonitor {
    datanode_manager: Arc<DatanodeManager>,
    check_interval: Duration,
}

impl HeartbeatMonitor {
    pub fn new(datanode_manager: Arc<DatanodeManager>) -> Self {
        Self {
            datanode_manager,
            check_interval: Duration::from_secs(60), // Check every minute
        }
    }

    /// Start the heartbeat monitoring loop
    pub async fn start(self: Arc<Self>) {
        let mut interval = time::interval(self.check_interval);

        loop {
            interval.tick().await;
            self.check_dead_datanodes().await;
        }
    }

    /// Check for dead datanodes and remove them
    async fn check_dead_datanodes(&self) {
        let dead_nodes = self.datanode_manager.remove_dead_datanodes();

        if !dead_nodes.is_empty() {
            warn!("Removed {} dead datanodes: {:?}", dead_nodes.len(), dead_nodes);
        }

        let num_live = self.datanode_manager.get_num_live_datanodes();
        info!("Heartbeat check: {} live datanodes", num_live);
    }
}
