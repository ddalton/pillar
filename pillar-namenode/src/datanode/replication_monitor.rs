//! NameNode replication monitor: keeps every block at its file's
//! replication factor (HDFS `RedundancyMonitor`).
//!
//! Each pass:
//! 1. **Dead DataNodes** (no heartbeat for the dead interval) are removed,
//!    and their replicas are dropped from the location map first, so those
//!    blocks are seen as under-replicated.
//! 2. **Under-replicated blocks** (fewer known replicas than the file's
//!    replication, but at least one) get a TRANSFER command queued for a
//!    live holder, naming new targets that do not already hold the block.
//!
//! A block must stay under-replicated for a grace period before it is
//! scheduled, so blocks whose write pipeline is still reporting replicas
//! are left alone; a scheduled block is not rescheduled until its transfer
//! has had time to finish. Deleted blocks are invalidated separately: the
//! BlockManager queues them and the heartbeat hands them out.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::{self, Instant};
use tracing::{debug, info, warn};

use super::{BlockPlacementPolicy, DatanodeManager};
use crate::grpc::datanode_protocol_service::DatanodeCommand;
use crate::grpc::DatanodeProtocolService;
use crate::metadata::block_manager::BlockId;
use crate::metadata::{BlockManager, Namespace};

/// HDFS `dfs.namenode.redundancy.interval.seconds`.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(3);
/// How long a block must look under-replicated before it is scheduled.
const DEFAULT_GRACE: Duration = Duration::from_secs(10);
/// HDFS `dfs.namenode.reconstruction.pending.timeout-sec` (5 minutes).
const DEFAULT_PENDING_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy)]
enum State {
    /// First seen under-replicated at this time.
    Seen(Instant),
    /// Transfer queued; do not reschedule before this time.
    Scheduled(Instant),
}

/// What one pass did (for logs and tests).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassStats {
    pub dead_datanodes: usize,
    pub under_replicated: usize,
    pub missing: usize,
    pub scheduled: usize,
}

pub struct ReplicationMonitor {
    namespace: Arc<Namespace>,
    block_manager: Arc<BlockManager>,
    datanode_manager: Arc<DatanodeManager>,
    placement: Arc<BlockPlacementPolicy>,
    commands: Arc<DatanodeProtocolService>,
    interval: Duration,
    grace: Duration,
    pending_timeout: Duration,
    state: Mutex<HashMap<BlockId, State>>,
}

impl ReplicationMonitor {
    pub fn new(
        namespace: Arc<Namespace>,
        block_manager: Arc<BlockManager>,
        datanode_manager: Arc<DatanodeManager>,
        placement: Arc<BlockPlacementPolicy>,
        commands: Arc<DatanodeProtocolService>,
    ) -> Self {
        Self {
            namespace,
            block_manager,
            datanode_manager,
            placement,
            commands,
            interval: DEFAULT_INTERVAL,
            grace: DEFAULT_GRACE,
            pending_timeout: DEFAULT_PENDING_TIMEOUT,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Override the pass interval, grace period and pending timeout.
    pub fn with_timing(mut self, interval: Duration, grace: Duration, pending_timeout: Duration) -> Self {
        self.interval = interval;
        self.grace = grace;
        self.pending_timeout = pending_timeout;
        self
    }

    pub async fn start(self: Arc<Self>) {
        let mut tick = time::interval(self.interval);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let stats = self.run_once().await;
            if stats.dead_datanodes + stats.scheduled + stats.missing > 0 {
                info!("Replication monitor: {:?}", stats);
            }
        }
    }

    /// One pass; see the module docs.
    pub async fn run_once(&self) -> PassStats {
        let mut stats = PassStats::default();

        // 1. Dead DataNodes: forget their replicas, then the nodes.
        for dead in self.datanode_manager.get_dead_datanodes() {
            for block in &dead.blocks {
                self.block_manager.remove_block_location(block, &dead.id);
            }
            warn!("DataNode {} is dead; {} replicas lost", dead.id.uuid, dead.blocks.len());
        }
        stats.dead_datanodes = self.datanode_manager.remove_dead_datanodes().len();

        // 2. Under-replicated blocks.
        let live: HashSet<String> =
            self.datanode_manager.get_live_datanodes().into_iter().map(|d| d.id.uuid).collect();
        let now = Instant::now();
        let mut replication_of: HashMap<u64, Option<u16>> = HashMap::new();
        let mut seen_now = HashSet::new();

        for (block, inode_id, num_bytes, holders) in self.block_manager.blocks_snapshot() {
            let expected = match replication_of.get(&inode_id) {
                Some(r) => *r,
                None => {
                    let r = match self.namespace.get_inode(inode_id) {
                        Some(inode) => Some(inode.read().await.replication),
                        None => None,
                    };
                    replication_of.insert(inode_id, r);
                    r
                }
            };
            let Some(expected) = expected.filter(|r| *r > 0) else { continue };
            let have = holders.len();
            if have >= expected as usize {
                continue;
            }
            if have == 0 {
                // Nothing to copy from (or not yet written).
                stats.missing += 1;
                continue;
            }
            stats.under_replicated += 1;
            seen_now.insert(block);

            let ready = {
                let mut st = self.state.lock().unwrap();
                match st.get(&block).copied() {
                    None => {
                        st.insert(block, State::Seen(now));
                        false
                    }
                    Some(State::Seen(t)) => now.duration_since(t) >= self.grace,
                    Some(State::Scheduled(until)) => now >= until,
                }
            };
            if !ready {
                continue;
            }

            let Some(source) = holders.iter().find(|h| live.contains(&h.uuid)) else {
                debug!("block {}: no live holder to copy from", block.block_id);
                continue;
            };
            let exclude: Vec<&str> = holders.iter().map(|h| h.uuid.as_str()).collect();
            let needed = expected as usize - have;
            let targets = (1..=needed)
                .rev()
                .find_map(|n| self.placement.choose_targets_excluding(n, &exclude).ok());
            let Some(targets) = targets else {
                debug!("block {}: no DataNode available to receive a replica", block.block_id);
                continue;
            };

            info!(
                "Block {} has {}/{} replicas: {} -> {:?}",
                block.block_id,
                have,
                expected,
                source.uuid,
                targets.iter().map(|t| t.uuid.as_str()).collect::<Vec<_>>()
            );
            self.commands.queue_command(
                &source.uuid,
                DatanodeCommand::TransferBlocks { transfers: vec![(block, num_bytes, targets)] },
            );
            self.state.lock().unwrap().insert(block, State::Scheduled(now + self.pending_timeout));
            stats.scheduled += 1;
        }

        // Blocks no longer under-replicated (or gone) leave the state map.
        self.state.lock().unwrap().retain(|b, _| seen_now.contains(b));
        stats
    }
}
