use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time;
use thiserror::Error;
use tracing::{info, warn, error, debug};
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use pillar_common::hdfs::block_list::{encode_block_list, ReportedBlock};
use pillar_proto::hdfs::datanode::datanode_protocol_service_client::DatanodeProtocolServiceClient;
use pillar_proto::hdfs::datanode::{
    block_command_proto, datanode_command_proto, received_deleted_block_info_proto,
    BlockCommandProto, BlockReceivedAndDeletedRequestProto, BlockReportRequestProto,
    DatanodeCommandProto, DatanodeRegistrationProto, HeartbeatRequestProto,
    ReceivedDeletedBlockInfoProto, RegisterDatanodeRequestProto, StorageBlockReportProto,
    StorageReceivedDeletedBlocksProto,
};
use pillar_proto::hdfs::{
    BlockKeyProto, BlockProto, DatanodeIdProto, DatanodeInfoProto, DatanodeStorageProto,
    ExportedBlockKeysProto, StorageInfoProto, StorageReportProto,
};

use crate::storage::{VolumeManager, BlockStorage};
use crate::storage::block_storage::{BlockEvent, BlockId};
use crate::replication::{ReplicationCoordinator, ReplicationRequest, ReplicationTarget};

/// Block transfers a DataNode runs at once (HDFS
/// `dfs.namenode.replication.max-streams` defaults to 2).
const MAX_CONCURRENT_TRANSFERS: usize = 2;

#[derive(Error, Debug)]
pub enum NamenodeClientError {
    #[error("Registration failed: {0}")]
    RegistrationFailed(String),

    #[error("Heartbeat failed: {0}")]
    HeartbeatFailed(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("Transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),
}

pub type Result<T> = std::result::Result<T, NamenodeClientError>;

/// DataNode information
#[derive(Debug, Clone)]
pub struct DatanodeInfo {
    pub uuid: String,
    pub hostname: String,
    pub rpc_port: u16,
    pub data_port: u16,
    pub http_port: u16,
    /// List of supported transports (e.g., ["tcp", "rdma"])
    pub supported_transports: Vec<String>,
    /// RDMA port (0 if RDMA not available)
    pub rdma_port: u16,
}

/// Client for communicating with the NameNode
pub struct NamenodeClient {
    namenode_addr: String,
    datanode_info: DatanodeInfo,
    volume_manager: Arc<VolumeManager>,
    block_storage: Arc<BlockStorage>,
    heartbeat_interval: Duration,
    block_report_interval: Duration,
    block_pool_id: String,
    /// One lazily-connected channel shared by every RPC; tonic reconnects it
    /// on demand, so heartbeats don't pay a TCP + HTTP/2 handshake each.
    client: DatanodeProtocolServiceClient<Channel>,
    replication_coordinator: Option<Arc<ReplicationCoordinator>>,
    transfer_slots: Arc<Semaphore>,
}

impl NamenodeClient {
    pub fn new(
        namenode_addr: String,
        datanode_info: DatanodeInfo,
        volume_manager: Arc<VolumeManager>,
        block_storage: Arc<BlockStorage>,
    ) -> Self {
        let block_pool_id = volume_manager.get_block_pool_id().to_string();
        let channel = Endpoint::from_shared(format!("http://{}", namenode_addr))
            .expect("NameNode address forms a valid URI")
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .connect_lazy();
        block_storage.enable_events();
        Self {
            namenode_addr,
            datanode_info,
            volume_manager,
            block_storage,
            heartbeat_interval: Duration::from_secs(3),
            // HDFS dfs.blockreport.intervalMsec default: 6 hours.
            block_report_interval: Duration::from_secs(6 * 3600),
            block_pool_id,
            client: DatanodeProtocolServiceClient::new(channel),
            replication_coordinator: None,
            transfer_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_TRANSFERS)),
        }
    }

    /// How often to heartbeat (default 3 s).
    pub fn set_heartbeat_interval(&mut self, interval: Duration) {
        self.heartbeat_interval = interval;
    }

    /// How often to send a full block report (incremental reports go with
    /// every heartbeat).
    pub fn set_block_report_interval(&mut self, interval: Duration) {
        self.block_report_interval = interval;
    }

    /// Set the replication coordinator (called after initialization)
    pub fn set_replication_coordinator(&mut self, coordinator: Arc<ReplicationCoordinator>) {
        self.replication_coordinator = Some(coordinator);
    }

    fn client(&self) -> DatanodeProtocolServiceClient<Channel> {
        self.client.clone()
    }

    /// Create DatanodeID protobuf message
    fn create_datanode_id_proto(&self) -> DatanodeIdProto {
        DatanodeIdProto {
            ip_addr: self.datanode_info.hostname.clone(),
            host_name: self.datanode_info.hostname.clone(),
            datanode_uuid: self.datanode_info.uuid.clone(),
            xfer_port: self.datanode_info.data_port as u32,
            info_port: self.datanode_info.http_port as u32,
            ipc_port: self.datanode_info.rpc_port as u32,
            info_secure_port: Some(0),
            // Advertise transport capabilities to NameNode
            supported_transports: self.datanode_info.supported_transports.clone(),
            rdma_port: Some(self.datanode_info.rdma_port as u32),
        }
    }

    /// Create DatanodeRegistration protobuf message
    fn create_registration_proto(&self) -> DatanodeRegistrationProto {
        DatanodeRegistrationProto {
            datanode_id: self.create_datanode_id_proto(),
            storage_info: StorageInfoProto {
                layout_version: 0,
                namespce_id: 0,  // Note: This is the actual field name in the protobuf (typo in Hadoop)
                cluster_id: self.block_pool_id.clone(),
                c_time: 0,
            },
            keys: ExportedBlockKeysProto {
                is_block_token_enabled: false,
                key_update_interval: 0,
                token_life_time: 0,
                current_key: BlockKeyProto {
                    key_id: 0,
                    expiry_date: 0,
                    key_bytes: None,
                },
                all_keys: vec![],
            },
            software_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    fn storage_proto(&self) -> DatanodeStorageProto {
        DatanodeStorageProto {
            storage_uuid: self.datanode_info.uuid.clone(),
            state: Some(0), // NORMAL
            storage_type: Some(1), // DISK
        }
    }

    fn volume_stats(&self) -> std::result::Result<crate::storage::VolumeInfo, String> {
        self.volume_manager
            .get_volume_stats(self.block_storage.total_bytes())
            .map_err(|e| format!("Failed to get volume info: {}", e))
    }

    /// Register with the NameNode
    pub async fn register(&self) -> Result<()> {
        info!("Registering with NameNode at {}", self.namenode_addr);

        let volume_info = self.volume_stats().map_err(NamenodeClientError::RegistrationFailed)?;

        let request = Request::new(RegisterDatanodeRequestProto {
            registration: self.create_registration_proto(),
        });
        self.client().register_datanode(request).await?;

        info!("Successfully registered with NameNode");
        info!("  UUID: {}", self.datanode_info.uuid);
        info!("  Hostname: {}", self.datanode_info.hostname);
        info!("  RPC Port: {}", self.datanode_info.rpc_port);
        info!("  Data Port: {}", self.datanode_info.data_port);
        info!("  Capacity: {} GB", volume_info.capacity_bytes / 1_000_000_000);

        Ok(())
    }

    /// Send heartbeat to NameNode
    pub async fn send_heartbeat(&self) -> Result<Vec<DatanodeCommand>> {
        debug!("Sending heartbeat to NameNode");

        let volume_info = self.volume_stats().map_err(NamenodeClientError::HeartbeatFailed)?;

        let storage_report = StorageReportProto {
            storage_uuid: self.datanode_info.uuid.clone(),
            failed: Some(false),
            capacity: Some(volume_info.capacity_bytes),
            dfs_used: Some(volume_info.block_pool_used_bytes),
            remaining: Some(volume_info.remaining_bytes),
            block_pool_used: Some(volume_info.block_pool_used_bytes),
            storage: Some(self.storage_proto()),
            non_dfs_used: Some(volume_info.used_bytes.saturating_sub(volume_info.block_pool_used_bytes)),
            mount: None,
        };

        let in_flight = (MAX_CONCURRENT_TRANSFERS - self.transfer_slots.available_permits()) as u32;
        let request = Request::new(HeartbeatRequestProto {
            registration: self.create_registration_proto(),
            reports: vec![storage_report],
            xmits_in_progress: Some(in_flight),
            xceiver_count: Some(0),
            failed_volumes: Some(0),
            cache_capacity: Some(0),
            cache_used: Some(0),
            volume_failure_summary: None,
            request_full_block_report_lease: Some(false),
            slow_peers: vec![],
            slow_disks: vec![],
        });

        let heartbeat_response = self.client().send_heartbeat(request).await?.into_inner();

        debug!(
            "Heartbeat sent: capacity {} GB, remaining {} GB, {} blocks",
            volume_info.capacity_bytes / 1_000_000_000,
            volume_info.remaining_bytes / 1_000_000_000,
            self.block_storage.get_block_count()
        );

        Ok(heartbeat_response.cmds.into_iter().filter_map(parse_command).collect())
    }

    /// Send a full block report to NameNode (HDFS BlockListAsLongs encoding,
    /// with each replica's length and generation stamp).
    pub async fn send_block_report(&self) -> Result<()> {
        let blocks: Vec<ReportedBlock> = self
            .block_storage
            .get_all_blocks()
            .into_iter()
            .map(|(id, num_bytes)| ReportedBlock {
                block_id: id.block_id,
                num_bytes,
                generation_stamp: id.generation_stamp,
            })
            .collect();

        info!("Sending block report to NameNode ({} blocks)", blocks.len());

        let storage_report = StorageBlockReportProto {
            storage: self.storage_proto(),
            blocks: encode_block_list(&blocks),
            blocks_buffers: vec![],
            number_of_blocks: Some(blocks.len() as u64),
        };

        let request = Request::new(BlockReportRequestProto {
            registration: self.create_registration_proto(),
            block_pool_id: self.block_pool_id.clone(),
            reports: vec![storage_report],
            context: None,
        });
        self.client().block_report(request).await?;

        info!("Block report sent successfully ({} blocks)", blocks.len());
        Ok(())
    }

    /// Report replicas finalized or deleted since the last call
    /// (`blockReceivedAndDeleted`, HDFS's incremental block report), so the
    /// NameNode learns of new replicas without waiting for a full report.
    pub async fn send_incremental_block_report(&self) -> Result<()> {
        let events = self.block_storage.take_events();
        if events.is_empty() {
            return Ok(());
        }
        let blocks = events
            .iter()
            .map(|e| {
                let (block, num_bytes, status) = match *e {
                    BlockEvent::Received { block, num_bytes } => {
                        (block, num_bytes, received_deleted_block_info_proto::BlockStatus::Received)
                    }
                    BlockEvent::Deleted { block } => {
                        (block, 0, received_deleted_block_info_proto::BlockStatus::Deleted)
                    }
                };
                ReceivedDeletedBlockInfoProto {
                    block: BlockProto {
                        block_id: block.block_id,
                        gen_stamp: block.generation_stamp,
                        num_bytes: Some(num_bytes),
                    },
                    status: status as i32,
                    delete_hint: None,
                }
            })
            .collect();

        #[allow(deprecated)]
        let request = Request::new(BlockReceivedAndDeletedRequestProto {
            registration: self.create_registration_proto(),
            block_pool_id: self.block_pool_id.clone(),
            blocks: vec![StorageReceivedDeletedBlocksProto {
                storage_uuid: self.datanode_info.uuid.clone(),
                blocks,
                storage: Some(self.storage_proto()),
            }],
        });

        if let Err(e) = self.client().block_received_and_deleted(request).await {
            // Keep them for the next attempt.
            self.block_storage.requeue_events(events);
            return Err(e.into());
        }
        debug!("Incremental block report sent");
        Ok(())
    }

    /// Start the heartbeat loop
    pub async fn start_heartbeat_loop(self: Arc<Self>) {
        let mut interval = time::interval(self.heartbeat_interval);
        let mut first_heartbeat = true;
        let mut last_full_report = time::Instant::now();

        loop {
            interval.tick().await;

            match self.send_heartbeat().await {
                Ok(commands) => {
                    if first_heartbeat {
                        info!("First heartbeat successful");
                        first_heartbeat = false;
                    }

                    for command in commands {
                        self.handle_command(command);
                    }
                }
                Err(e) => {
                    error!("Heartbeat failed: {}", e);
                }
            }

            if let Err(e) = self.send_incremental_block_report().await {
                warn!("Incremental block report failed: {}", e);
            }

            if last_full_report.elapsed() >= self.block_report_interval {
                match self.send_block_report().await {
                    Ok(()) => last_full_report = time::Instant::now(),
                    Err(e) => warn!("Full block report failed: {}", e),
                }
            }
        }
    }

    /// Handle a command from the NameNode. Transfers run in the background,
    /// at most `MAX_CONCURRENT_TRANSFERS` at a time, so a long replication
    /// never delays heartbeats.
    fn handle_command(self: &Arc<Self>, command: DatanodeCommand) {
        match command {
            DatanodeCommand::ReplicateBlocks { transfers } => {
                let coordinator = match &self.replication_coordinator {
                    Some(c) => c.clone(),
                    None => {
                        error!("Replication coordinator not initialized; dropping {} transfers", transfers.len());
                        return;
                    }
                };
                info!("Received command to replicate {} block(s)", transfers.len());

                for t in transfers {
                    let coordinator = coordinator.clone();
                    let slots = self.transfer_slots.clone();
                    let storage = self.block_storage.clone();
                    tokio::spawn(async move {
                        let _permit = slots.acquire_owned().await;
                        // The NameNode's length may be stale or zero; the
                        // local replica is authoritative.
                        let block_size = storage
                            .get_block_metadata(t.block_id)
                            .map(|m| m.num_bytes)
                            .unwrap_or(t.num_bytes);
                        let request = ReplicationRequest {
                            block_id: t.block_id,
                            gen_stamp: t.gen_stamp,
                            targets: t.targets,
                            block_size,
                            priority: 1,
                        };
                        match coordinator.replicate(request).await {
                            Ok(result) if result.is_success() => info!(
                                "Replicated block {} to {}/{} targets ({:.2} MB/s)",
                                t.block_id, result.successful_replicas, result.total_replicas,
                                result.throughput_mbps()
                            ),
                            Ok(result) => warn!(
                                "Partial replication of block {}: {}/{} targets; errors: {:?}",
                                t.block_id, result.successful_replicas, result.total_replicas, result.errors
                            ),
                            Err(e) => error!("Failed to replicate block {}: {}", t.block_id, e),
                        }
                    });
                }
            }
            DatanodeCommand::InvalidateBlocks { blocks } => {
                info!("Received command to invalidate {} block(s)", blocks.len());
                for (block_id, gen_stamp) in blocks {
                    let block = BlockId::new(block_id, gen_stamp);
                    match self.block_storage.get_block_metadata(block_id) {
                        // Only delete the replica the NameNode named.
                        Some(m) if m.block_id.generation_stamp == gen_stamp => {
                            if let Err(e) = self.block_storage.delete_block(&block) {
                                error!("Failed to delete block {}: {}", block_id, e);
                            }
                        }
                        Some(m) => warn!(
                            "Not invalidating block {}: local genstamp {} != {}",
                            block_id, m.block_id.generation_stamp, gen_stamp
                        ),
                        None => debug!("Block {} to invalidate is not here", block_id),
                    }
                }
            }
            DatanodeCommand::Shutdown => {
                warn!("Received shutdown command from NameNode (not implemented; ignoring)");
            }
        }
    }
}

/// One block to copy and the DataNodes to copy it to.
#[derive(Debug, Clone)]
pub struct BlockTransfer {
    pub block_id: u64,
    pub gen_stamp: u64,
    pub num_bytes: u64,
    pub targets: Vec<ReplicationTarget>,
}

/// Commands that can be received from NameNode
#[derive(Debug, Clone)]
pub enum DatanodeCommand {
    /// Copy blocks to other DataNodes (`BlockCommandProto` TRANSFER: each
    /// block has its own target list)
    ReplicateBlocks { transfers: Vec<BlockTransfer> },
    /// Delete replicas: (block_id, generation_stamp)
    InvalidateBlocks { blocks: Vec<(u64, u64)> },
    /// Shutdown the DataNode gracefully
    Shutdown,
}

fn replication_target(info: &DatanodeInfoProto) -> ReplicationTarget {
    let id = &info.id;
    let mut supported_transports = id.supported_transports.clone();
    if supported_transports.is_empty() {
        supported_transports.push("tcp".to_string());
    }
    ReplicationTarget {
        datanode_uuid: id.datanode_uuid.clone(),
        hostname: id.host_name.clone(),
        ip_addr: id.ip_addr.clone(),
        xfer_port: id.xfer_port,
        rdma_port: id.rdma_port.unwrap_or(0),
        supported_transports,
    }
}

/// Translate a `DatanodeCommandProto` into a [`DatanodeCommand`]. Command
/// types Pillar does not act on are logged and skipped.
pub fn parse_command(cmd: DatanodeCommandProto) -> Option<DatanodeCommand> {
    if cmd.cmd_type != datanode_command_proto::Type::BlockCommand as i32 {
        debug!("Ignoring DataNode command type {}", cmd.cmd_type);
        return None;
    }
    let BlockCommandProto { action, blocks, targets, .. } = cmd.blk_cmd?;
    match block_command_proto::Action::try_from(action) {
        Ok(block_command_proto::Action::Transfer) => {
            if blocks.len() != targets.len() {
                warn!("TRANSFER command with {} blocks but {} target lists; ignoring", blocks.len(), targets.len());
                return None;
            }
            let transfers = blocks
                .iter()
                .zip(&targets)
                .map(|(b, t)| BlockTransfer {
                    block_id: b.block_id,
                    gen_stamp: b.gen_stamp,
                    num_bytes: b.num_bytes.unwrap_or(0),
                    targets: t.datanodes.iter().map(replication_target).collect(),
                })
                .collect();
            Some(DatanodeCommand::ReplicateBlocks { transfers })
        }
        Ok(block_command_proto::Action::Invalidate) => Some(DatanodeCommand::InvalidateBlocks {
            blocks: blocks.iter().map(|b| (b.block_id, b.gen_stamp)).collect(),
        }),
        Ok(block_command_proto::Action::Shutdown) => Some(DatanodeCommand::Shutdown),
        Err(_) => {
            warn!("Unknown block command action {}", action);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_namenode_client_creation() {
        let temp_dir = TempDir::new().unwrap();
        let mount_point = temp_dir.path();

        let volume_manager = Arc::new(VolumeManager::new(mount_point, "BP-test".to_string()).unwrap());

        let finalized_dir = volume_manager.get_finalized_dir();
        let tmp_dir = volume_manager.get_tmp_dir();
        let block_storage = Arc::new(BlockStorage::new(finalized_dir, tmp_dir).unwrap());

        let datanode_info = DatanodeInfo {
            uuid: "dn-test-001".to_string(),
            hostname: "localhost".to_string(),
            rpc_port: 9000,
            data_port: 50010,
            http_port: 50075,
            supported_transports: vec!["tcp".to_string()],
            rdma_port: 0,
        };

        let client = NamenodeClient::new(
            "localhost:9000".to_string(),
            datanode_info,
            volume_manager,
            block_storage,
        );

        assert_eq!(client.namenode_addr, "localhost:9000");
    }

    fn dn(uuid: &str, port: u32) -> DatanodeInfoProto {
        DatanodeInfoProto {
            id: DatanodeIdProto {
                ip_addr: "10.0.0.1".into(),
                host_name: "h".into(),
                datanode_uuid: uuid.into(),
                xfer_port: port,
                info_port: 0,
                ipc_port: 0,
                info_secure_port: None,
                supported_transports: vec![],
                rdma_port: None,
            },
            ..Default::default()
        }
    }

    fn block_cmd(action: block_command_proto::Action, blocks: Vec<BlockProto>, targets: Vec<Vec<DatanodeInfoProto>>) -> DatanodeCommandProto {
        DatanodeCommandProto {
            cmd_type: datanode_command_proto::Type::BlockCommand as i32,
            blk_cmd: Some(BlockCommandProto {
                action: action as i32,
                block_pool_id: "BP-1".into(),
                blocks,
                targets: targets
                    .into_iter()
                    .map(|d| pillar_proto::hdfs::DatanodeInfosProto { datanodes: d })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn bp(id: u64, gs: u64, len: u64) -> BlockProto {
        BlockProto { block_id: id, gen_stamp: gs, num_bytes: Some(len) }
    }

    #[test]
    fn a_transfer_command_keeps_each_blocks_own_targets() {
        let cmd = block_cmd(
            block_command_proto::Action::Transfer,
            vec![bp(1, 10, 100), bp(2, 20, 200)],
            vec![vec![dn("a", 50010)], vec![dn("b", 50011), dn("c", 50012)]],
        );
        let Some(DatanodeCommand::ReplicateBlocks { transfers }) = parse_command(cmd) else {
            panic!("not a transfer");
        };
        assert_eq!(transfers.len(), 2);
        assert_eq!((transfers[0].block_id, transfers[0].gen_stamp, transfers[0].num_bytes), (1, 10, 100));
        assert_eq!(transfers[0].targets.len(), 1);
        assert_eq!(transfers[1].targets.iter().map(|t| t.xfer_port).collect::<Vec<_>>(), [50011, 50012]);
        assert_eq!(transfers[1].targets[0].supported_transports, ["tcp"]);
    }

    #[test]
    fn invalidate_and_malformed_commands() {
        let inv = block_cmd(block_command_proto::Action::Invalidate, vec![bp(5, 50, 0)], vec![]);
        assert!(matches!(
            parse_command(inv),
            Some(DatanodeCommand::InvalidateBlocks { blocks }) if blocks == [(5, 50)]
        ));
        let mismatched = block_cmd(block_command_proto::Action::Transfer, vec![bp(1, 1, 1)], vec![]);
        assert!(parse_command(mismatched).is_none());
        let other = DatanodeCommandProto {
            cmd_type: datanode_command_proto::Type::FinalizeCommand as i32,
            ..Default::default()
        };
        assert!(parse_command(other).is_none());
    }
}
