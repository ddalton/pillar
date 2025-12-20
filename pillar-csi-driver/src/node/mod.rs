use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::identity::csi::{
    node_server::Node,
    NodeStageVolumeRequest, NodeStageVolumeResponse,
    NodeUnstageVolumeRequest, NodeUnstageVolumeResponse,
    NodePublishVolumeRequest, NodePublishVolumeResponse,
    NodeUnpublishVolumeRequest, NodeUnpublishVolumeResponse,
    NodeGetCapabilitiesRequest, NodeGetCapabilitiesResponse,
    NodeGetInfoRequest, NodeGetInfoResponse,
    NodeGetVolumeStatsRequest, NodeGetVolumeStatsResponse,
    NodeExpandVolumeRequest, NodeExpandVolumeResponse,
    node_service_capability::{Rpc, rpc::Type as RpcType},
    NodeServiceCapability,
};

/// Node service (minimal - applications use HDFS protocol directly)
///
/// This service is kept minimal because:
/// - Applications connect via HDFS client libraries (hdfs://pillar-namenode:9000/...)
/// - No filesystem mounts needed
/// - CSI is only for infrastructure management
pub struct NodeService {
    node_id: String,
}

impl NodeService {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("NodeStageVolume called (no-op): {}", req.volume_id);

        // No-op: Applications use HDFS protocol, not filesystem mounts
        let response = NodeStageVolumeResponse {};

        Ok(Response::new(response))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("NodeUnstageVolume called (no-op): {}", req.volume_id);

        // No-op
        let response = NodeUnstageVolumeResponse {};

        Ok(Response::new(response))
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("NodePublishVolume called (no-op): {} -> {}", req.volume_id, req.target_path);

        // No-op: Applications use HDFS client libraries directly
        // They connect to: hdfs://pillar-namenode:9000/pillar/volumes/<volume-id>
        let response = NodePublishVolumeResponse {};

        Ok(Response::new(response))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("NodeUnpublishVolume called (no-op): {}", req.volume_id);

        // No-op
        let response = NodeUnpublishVolumeResponse {};

        Ok(Response::new(response))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        info!("NodeGetCapabilities called");

        // Minimal capabilities since we don't do actual mounts
        let capabilities = vec![
            NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(Rpc {
                    r#type: RpcType::StageUnstageVolume as i32,
                })),
            },
        ];

        let response = NodeGetCapabilitiesResponse { capabilities };

        Ok(Response::new(response))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        info!("NodeGetInfo called");

        let response = NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 0, // No limit (not doing mounts)
            accessible_topology: None,
        };

        Ok(Response::new(response))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let req = request.into_inner();
        warn!("NodeGetVolumeStats not implemented: {}", req.volume_id);

        Err(Status::unimplemented("Volume stats not supported"))
    }

    async fn node_expand_volume(
        &self,
        request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        warn!("NodeExpandVolume not implemented: {}", req.volume_id);

        Err(Status::unimplemented("Volume expansion not supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_get_info() {
        let service = NodeService::new("test-node".to_string());
        let request = Request::new(NodeGetInfoRequest {});

        let response = service.node_get_info(request).await.unwrap();
        let info = response.into_inner();

        assert_eq!(info.node_id, "test-node");
    }
}
