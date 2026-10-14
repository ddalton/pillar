use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use pillar_proto::hadoop::hdfs::datanode::*;
use pillar_proto::hadoop::hdfs::datanode::datanode_protocol_service_server;
use pillar_proto::hadoop::hdfs::{
    VersionRequestProto, VersionResponseProto, NamespaceInfoProto, StorageInfoProto,
    DatanodeIdProto, BlockProto, DatanodeInfoProto, DatanodeStorageProto,
    StorageReportProto, NnhaStatusHeartbeatProto,
};
use crate::grpc::DatanodeProtocolService;
use crate::grpc::datanode_protocol_service::DatanodeCommand;
use crate::metadata::block_manager::{BlockId, DatanodeId};
use pillar_common::hdfs::block_list::decode_block_list;
use pillar_proto::hadoop::hdfs::DatanodeInfosProto;

fn datanode_info(id: &DatanodeId) -> DatanodeInfoProto {
    DatanodeInfoProto {
        id: DatanodeIdProto {
            // Pillar tracks DataNodes by hostname; it is also what they
            // register as their ipAddr.
            ip_addr: id.hostname.clone(),
            host_name: id.hostname.clone(),
            datanode_uuid: id.uuid.clone(),
            xfer_port: id.data_port as u32,
            info_port: id.http_port as u32,
            ipc_port: id.rpc_port as u32,
            info_secure_port: Some(0),
            supported_transports: id.supported_transports.clone(),
            rdma_port: Some(id.rdma_port as u32),
        },
        ..Default::default()
    }
}

fn block_proto(block: &crate::metadata::block_manager::BlockId, num_bytes: u64) -> BlockProto {
    BlockProto { block_id: block.block_id, gen_stamp: block.generation_stamp, num_bytes: Some(num_bytes) }
}

/// Encode a queued command as a `BlockCommandProto`. For TRANSFER,
/// `targets[i]` lists the DataNodes that should receive `blocks[i]`.
pub(crate) fn encode_command(command: &DatanodeCommand, block_pool_id: &str) -> DatanodeCommandProto {
    let (action, blocks, targets) = match command {
        DatanodeCommand::TransferBlocks { transfers } => (
            block_command_proto::Action::Transfer,
            transfers.iter().map(|(b, len, _)| block_proto(b, *len)).collect(),
            transfers
                .iter()
                .map(|(_, _, dns)| DatanodeInfosProto { datanodes: dns.iter().map(datanode_info).collect() })
                .collect(),
        ),
        DatanodeCommand::InvalidateBlocks { blocks } => (
            block_command_proto::Action::Invalidate,
            blocks.iter().map(|b| block_proto(b, 0)).collect(),
            Vec::new(),
        ),
        DatanodeCommand::Register => {
            return DatanodeCommandProto {
                cmd_type: datanode_command_proto::Type::RegisterCommand as i32,
                register_cmd: Some(RegisterCommandProto {}),
                ..Default::default()
            };
        }
        DatanodeCommand::RecoverBlocks { blocks } => {
            let blocks = blocks
                .iter()
                .map(|(b, new_gs, pipeline)| pillar_proto::hdfs::RecoveringBlockProto {
                    new_gen_stamp: *new_gs,
                    block: pillar_proto::hdfs::LocatedBlockProto {
                        b: pillar_proto::hdfs::ExtendedBlockProto {
                            pool_id: block_pool_id.to_string(),
                            block_id: b.block_id,
                            generation_stamp: b.generation_stamp,
                            num_bytes: Some(0),
                        },
                        offset: 0,
                        locs: pipeline.iter().map(datanode_info).collect(),
                        corrupt: false,
                        block_token: Default::default(),
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect();
            return DatanodeCommandProto {
                cmd_type: datanode_command_proto::Type::BlockRecoveryCommand as i32,
                recovery_cmd: Some(BlockRecoveryCommandProto { blocks }),
                ..Default::default()
            };
        }
    };
    DatanodeCommandProto {
        cmd_type: datanode_command_proto::Type::BlockCommand as i32,
        blk_cmd: Some(BlockCommandProto {
            action: action as i32,
            block_pool_id: block_pool_id.to_string(),
            blocks,
            targets,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// gRPC service implementation for DatanodeProtocol
/// This handles communication from DataNodes to NameNode
pub struct DatanodeProtocolServer {
    service: Arc<DatanodeProtocolService>,
}

impl DatanodeProtocolServer {
    pub fn new(service: Arc<DatanodeProtocolService>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl datanode_protocol_service_server::DatanodeProtocolService for DatanodeProtocolServer {
    /// Register a DataNode with the NameNode
    async fn register_datanode(
        &self,
        request: Request<RegisterDatanodeRequestProto>,
    ) -> Result<Response<RegisterDatanodeResponseProto>, Status> {
        let req = request.into_inner();
        let registration = &req.registration;

        let datanode_id = &registration.datanode_id;

        info!(
            "register_datanode: {} ({})",
            datanode_id.datanode_uuid,
            datanode_id.host_name
        );

        let storage = &registration.storage_info;
        if let Err(e) = self.service.check_identity(&storage.cluster_id, storage.namespce_id) {
            warn!("Refusing DataNode {}: {}", datanode_id.datanode_uuid, e.message());
            return Err(e);
        }

        match self.service
            .handle_register_datanode(datanode_id)
            .await
        {
            Ok(()) => {
                let response = RegisterDatanodeResponseProto {
                    registration: req.registration.clone(),
                };
                Ok(Response::new(response))
            }
            Err(e) => Err(e),
        }
    }

    /// Send heartbeat from DataNode to NameNode
    async fn send_heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let req = request.into_inner();
        let registration = &req.registration;

        let datanode_id = &registration.datanode_id;

        let uuid = &datanode_id.datanode_uuid;

        // Extract storage information from reports
        let (capacity, dfs_used, remaining, block_pool_used) = if let Some(report) = req.reports.first() {
            (
                report.capacity.unwrap_or(0),
                report.dfs_used.unwrap_or(0),
                report.remaining.unwrap_or(0),
                report.block_pool_used.unwrap_or(0),
            )
        } else {
            (0, 0, 0, 0)
        };

        // Log only occasionally to avoid spam
        if rand::random::<u8>() % 100 == 0 {
            info!(
                "heartbeat: {} (capacity={}, used={}, remaining={})",
                uuid, capacity, dfs_used, remaining
            );
        }

        match self.service
            .handle_heartbeat(uuid, capacity, dfs_used, remaining, block_pool_used)
            .await
        {
            Ok(commands) => {
                let block_pool_id = &registration.storage_info.cluster_id;
                let response = HeartbeatResponseProto {
                    cmds: commands.iter().map(|c| encode_command(c, block_pool_id)).collect(),
                    ha_status: NnhaStatusHeartbeatProto::default(),
                    rolling_upgrade_status: None,
                    full_block_report_lease_id: Some(0),
                    is_slownode: Some(false),
                    rolling_upgrade_status_v2: None,
                };
                Ok(Response::new(response))
            }
            Err(e) => Err(e),
        }
    }

    /// Block report from DataNode
    async fn block_report(
        &self,
        request: Request<BlockReportRequestProto>,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        let req = request.into_inner();
        let registration = &req.registration;

        let datanode_id = &registration.datanode_id;

        let uuid = &datanode_id.datanode_uuid;
        let num_blocks: usize = req.reports.iter().map(|r| r.blocks.len()).sum();

        info!("block_report: {} ({} blocks)", uuid, num_blocks);

        // Each storage's blocks are HDFS BlockListAsLongs:
        // (blockId, numBytes, genStamp) per replica.
        let mut blocks = Vec::new();
        for report in &req.reports {
            let decoded = decode_block_list(&report.blocks)
                .map_err(|e| Status::invalid_argument(format!("bad block report from {}: {}", uuid, e)))?;
            blocks.extend(decoded.into_iter().map(|b| (b.block_id, b.generation_stamp, b.num_bytes, b.under_construction)));
        }

        match self.service.handle_block_report(uuid.clone(), blocks).await {
            Ok(()) => {
                let response = BlockReportResponseProto {
                    cmd: None, // No commands for now
                };
                Ok(Response::new(response))
            }
            Err(e) => Err(e),
        }
    }

    /// Cache report (for memory caching feature)
    async fn cache_report(
        &self,
        _request: Request<CacheReportRequestProto>,
    ) -> Result<Response<CacheReportResponseProto>, Status> {
        // Simplified - cache management not implemented in MVP
        Ok(Response::new(CacheReportResponseProto { cmd: None }))
    }

    /// Block received and deleted notifications
    async fn block_received_and_deleted(
        &self,
        request: Request<BlockReceivedAndDeletedRequestProto>,
    ) -> Result<Response<BlockReceivedAndDeletedResponseProto>, Status> {
        use received_deleted_block_info_proto::BlockStatus;

        let req = request.into_inner();
        let uuid = req.registration.datanode_id.datanode_uuid;
        for storage in req.blocks {
            for info in storage.blocks {
                let b = info.block;
                match BlockStatus::try_from(info.status) {
                    Ok(BlockStatus::Received) => {
                        self.service
                            .handle_block_received(uuid.clone(), b.block_id, b.gen_stamp, b.num_bytes.unwrap_or(0))
                            .await?
                    }
                    Ok(BlockStatus::Deleted) => {
                        self.service.handle_block_deleted(uuid.clone(), b.block_id, b.gen_stamp).await?
                    }
                    // RECEIVING: replica under construction, not yet readable.
                    Ok(BlockStatus::Receiving) => {}
                    Err(_) => warn!("{}: unknown block status {} for block {}", uuid, info.status, b.block_id),
                }
            }
        }
        Ok(Response::new(BlockReceivedAndDeletedResponseProto {}))
    }

    /// Error report from DataNode
    async fn error_report(
        &self,
        request: Request<ErrorReportRequestProto>,
    ) -> Result<Response<ErrorReportResponseProto>, Status> {
        let req = request.into_inner();
        warn!(
            "error_report from DataNode: code={}, msg={}",
            req.error_code, req.msg
        );
        Ok(Response::new(ErrorReportResponseProto {}))
    }

    /// Version request
    async fn version_request(
        &self,
        _request: Request<VersionRequestProto>,
    ) -> Result<Response<VersionResponseProto>, Status> {
        let id = self.service.identity();
        Ok(Response::new(VersionResponseProto {
            info: NamespaceInfoProto {
                build_version: env!("CARGO_PKG_VERSION").to_string(),
                unused: 0,
                block_pool_id: id.block_pool_id.clone(),
                storage_info: StorageInfoProto {
                    layout_version: 1,
                    namespce_id: id.namespace_id,  // Note: proto has typo "namespce_id"
                    cluster_id: id.cluster_id.clone(),
                    c_time: 0,
                },
                software_version: env!("CARGO_PKG_VERSION").to_string(),
                capabilities: Some(0),
                state: None,
            },
        }))
    }

    /// Commit block synchronization
    async fn commit_block_synchronization(
        &self,
        request: Request<CommitBlockSynchronizationRequestProto>,
    ) -> Result<Response<CommitBlockSynchronizationResponseProto>, Status> {
        let req = request.into_inner();
        let block = BlockId::new(req.block.block_id, req.block.generation_stamp);
        let holders: Vec<String> = req.new_taragets.iter().map(|t| t.datanode_uuid.clone()).collect();
        info!(
            "commitBlockSynchronization: block {} -> stamp {}, {} bytes, on {:?}{}",
            block.block_id, req.new_gen_stamp, req.new_length, holders, if req.delete_block { " (delete)" } else { "" }
        );
        self.service
            .handle_commit_block_synchronization(block, req.new_gen_stamp, req.new_length, req.close_file, req.delete_block, &holders)
            .await?;
        Ok(Response::new(CommitBlockSynchronizationResponseProto {}))
    }

    /// Report bad blocks from DataNode
    async fn report_bad_blocks(
        &self,
        request: Request<ReportBadBlocksRequestProto>,
    ) -> Result<Response<ReportBadBlocksResponseProto>, Status> {
        // A DataNode found replicas that fail their checksums: drop them
        // (unless one is a block's last replica) so good copies are made.
        for lb in request.into_inner().blocks {
            let block = BlockId::new(lb.b.block_id, lb.b.generation_stamp);
            for dn in lb.locs {
                let dropped = self.service.drop_corrupt_replica(&block, &dn.id.datanode_uuid);
                warn!(
                    "DataNode reports block {} on {} corrupt; {}",
                    block.block_id,
                    dn.id.datanode_uuid,
                    if dropped { "deleting it" } else { "kept (unknown or last replica)" }
                );
            }
        }
        Ok(Response::new(ReportBadBlocksResponseProto {}))
    }
}
