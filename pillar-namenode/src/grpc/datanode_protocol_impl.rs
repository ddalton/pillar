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
            Ok(_commands) => {
                let response = HeartbeatResponseProto {
                    cmds: vec![], // No commands for now
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

        // Extract block information
        // Blocks are encoded as uint64 values in the report
        // For now, use simplified block ID extraction
        let mut blocks = Vec::new();
        for report in req.reports {
            // Each block is represented as a u64 - simplified implementation
            for &block_id in &report.blocks {
                blocks.push((
                    block_id,
                    0, // generation stamp - would be in full implementation
                    0, // num_bytes - would be in full implementation
                ));
            }
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
        _request: Request<BlockReceivedAndDeletedRequestProto>,
    ) -> Result<Response<BlockReceivedAndDeletedResponseProto>, Status> {
        // Simplified implementation
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
        Ok(Response::new(VersionResponseProto {
            info: NamespaceInfoProto {
                build_version: env!("CARGO_PKG_VERSION").to_string(),
                unused: 0,
                block_pool_id: "BP-pillar-001".to_string(),
                storage_info: StorageInfoProto {
                    layout_version: 1,
                    namespce_id: 1,  // Note: proto has typo "namespce_id"
                    cluster_id: "pillar-cluster".to_string(),
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
        _request: Request<CommitBlockSynchronizationRequestProto>,
    ) -> Result<Response<CommitBlockSynchronizationResponseProto>, Status> {
        // Simplified implementation for block recovery
        Ok(Response::new(CommitBlockSynchronizationResponseProto {}))
    }

    /// Report bad blocks from DataNode
    async fn report_bad_blocks(
        &self,
        _request: Request<ReportBadBlocksRequestProto>,
    ) -> Result<Response<ReportBadBlocksResponseProto>, Status> {
        // Log and acknowledge bad block reports
        Ok(Response::new(ReportBadBlocksResponseProto {}))
    }
}
