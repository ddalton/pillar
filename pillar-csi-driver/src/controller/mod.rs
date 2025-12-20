use std::collections::HashMap;
use tonic::{Request, Response, Status};
use tracing::{info, warn};
use uuid::Uuid;

use crate::identity::csi::{
    controller_server::Controller,
    CreateVolumeRequest, CreateVolumeResponse,
    DeleteVolumeRequest, DeleteVolumeResponse,
    ControllerGetCapabilitiesRequest, ControllerGetCapabilitiesResponse,
    ValidateVolumeCapabilitiesRequest, ValidateVolumeCapabilitiesResponse,
    ListVolumesRequest, ListVolumesResponse,
    GetCapacityRequest, GetCapacityResponse,
    ControllerExpandVolumeRequest, ControllerExpandVolumeResponse,
    CreateSnapshotRequest, CreateSnapshotResponse,
    DeleteSnapshotRequest, DeleteSnapshotResponse,
    ListSnapshotsRequest, ListSnapshotsResponse,
    ControllerServiceCapability,
    Volume,
    validate_volume_capabilities_response,
};

/// Controller service manages HDFS namespace (administrative operations)
///
/// Note: This is NOT for mounting HDFS into pods. Applications use native HDFS protocol.
/// This CSI driver manages the HDFS cluster infrastructure only.
pub struct ControllerService {
    namenode_addr: String,
}

impl ControllerService {
    pub fn new(namenode_addr: String) -> Self {
        Self { namenode_addr }
    }

    /// Create HDFS namespace directory (administrative operation)
    async fn create_hdfs_namespace(&self, volume_id: &str, _capacity_bytes: i64) -> Result<(), Status> {
        info!("Creating HDFS namespace for volume: {}", volume_id);

        // TODO: Connect to NameNode gRPC and create directory
        // For MVP, this is a placeholder
        // In full implementation:
        // let mut client = ClientNamenodeProtocolClient::connect(self.namenode_addr.clone()).await?;
        // client.mkdirs(MkdirsRequest {
        //     src: format!("/pillar/volumes/{}", volume_id),
        //     masked_permission: FsPermission { perm: 0o755 },
        //     create_parent: true,
        // }).await?;

        info!("HDFS namespace created: /pillar/volumes/{}", volume_id);

        Ok(())
    }

    /// Delete HDFS namespace directory
    async fn delete_hdfs_namespace(&self, volume_id: &str) -> Result<(), Status> {
        info!("Deleting HDFS namespace for volume: {}", volume_id);

        // TODO: Connect to NameNode gRPC and delete directory
        // In full implementation:
        // let mut client = ClientNamenodeProtocolClient::connect(self.namenode_addr.clone()).await?;
        // client.delete(DeleteRequest {
        //     src: format!("/pillar/volumes/{}", volume_id),
        //     recursive: true,
        // }).await?;

        info!("HDFS namespace deleted: /pillar/volumes/{}", volume_id);

        Ok(())
    }
}

#[tonic::async_trait]
impl Controller for ControllerService {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("CreateVolume called: {}", req.name);

        // Generate volume ID
        let volume_id = format!("pillar-{}", Uuid::new_v4());

        // Get requested capacity (default 100GB if not specified)
        let capacity_bytes = req.capacity_range
            .as_ref()
            .and_then(|r| Some(r.required_bytes))
            .unwrap_or(100 * 1024 * 1024 * 1024); // 100GB

        // Extract parameters
        let replication_factor = req.parameters.get("replicationFactor")
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(3);

        info!("Volume parameters: capacity={}GB, replication={}",
            capacity_bytes / (1024*1024*1024), replication_factor);

        // Create HDFS namespace directory
        self.create_hdfs_namespace(&volume_id, capacity_bytes).await?;

        // Build volume context - applications will use this to connect
        let mut volume_context = HashMap::new();
        volume_context.insert("namenode".to_string(), self.namenode_addr.clone());
        volume_context.insert("path".to_string(), format!("/pillar/volumes/{}", volume_id));
        volume_context.insert("replicationFactor".to_string(), replication_factor.to_string());

        let volume = Volume {
            volume_id: volume_id.clone(),
            capacity_bytes,
            volume_context,
            content_source: None,
            accessible_topology: vec![],
        };

        let response = CreateVolumeResponse {
            volume: Some(volume),
        };

        info!("HDFS volume created: {} (path: /pillar/volumes/{})", volume_id, volume_id);

        Ok(Response::new(response))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("DeleteVolume called: {}", req.volume_id);

        // Delete HDFS namespace directory
        self.delete_hdfs_namespace(&req.volume_id).await?;

        let response = DeleteVolumeResponse {};

        info!("HDFS volume deleted: {}", req.volume_id);

        Ok(Response::new(response))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        info!("ControllerGetCapabilities called");

        use crate::identity::csi;
        let capabilities = vec![
            ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(csi::controller_service_capability::Rpc {
                    r#type: csi::controller_service_capability::rpc::Type::CreateDeleteVolume as i32,
                })),
            },
            ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(csi::controller_service_capability::Rpc {
                    r#type: csi::controller_service_capability::rpc::Type::ListVolumes as i32,
                })),
            },
        ];

        let response = ControllerGetCapabilitiesResponse { capabilities };

        Ok(Response::new(response))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        info!("ValidateVolumeCapabilitiesRequest called: {}", req.volume_id);

        // Pillar HDFS supports ReadWriteMany (distributed filesystem)
        let response = ValidateVolumeCapabilitiesResponse {
            confirmed: Some(validate_volume_capabilities_response::Confirmed {
                volume_context: req.volume_context,
                volume_capabilities: req.volume_capabilities,
                parameters: req.parameters.clone(),
                mutable_parameters: req.parameters,
            }),
            message: "".to_string(),
        };

        Ok(Response::new(response))
    }

    async fn list_volumes(
        &self,
        _request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        info!("ListVolumes called");

        // TODO: Query NameNode for list of volumes
        // For MVP, return empty list

        let response = ListVolumesResponse {
            entries: vec![],
            next_token: "".to_string(),
        };

        Ok(Response::new(response))
    }

    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        info!("GetCapacity called");

        // TODO: Query NameNode for actual cluster capacity
        // For MVP, return placeholder capacity

        let response = GetCapacityResponse {
            available_capacity: 10_000_000_000_000, // 10TB placeholder
            maximum_volume_size: None,
            minimum_volume_size: None,
        };

        Ok(Response::new(response))
    }

    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        warn!("ControllerExpandVolume not implemented: {}", req.volume_id);

        Err(Status::unimplemented("Volume expansion not yet supported"))
    }

    // Remaining methods are not needed for HDFS CSI
    async fn controller_publish_volume(
        &self,
        _request: Request<crate::identity::csi::ControllerPublishVolumeRequest>,
    ) -> Result<Response<crate::identity::csi::ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented("Not required - applications use HDFS protocol"))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: Request<crate::identity::csi::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<crate::identity::csi::ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented("Not required - applications use HDFS protocol"))
    }

    async fn create_snapshot(
        &self,
        _request: Request<crate::identity::csi::CreateSnapshotRequest>,
    ) -> Result<Response<crate::identity::csi::CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("Snapshots not yet supported"))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<crate::identity::csi::DeleteSnapshotRequest>,
    ) -> Result<Response<crate::identity::csi::DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("Snapshots not yet supported"))
    }

    async fn list_snapshots(
        &self,
        _request: Request<crate::identity::csi::ListSnapshotsRequest>,
    ) -> Result<Response<crate::identity::csi::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("Snapshots not yet supported"))
    }

    async fn controller_get_volume(
        &self,
        _request: Request<crate::identity::csi::ControllerGetVolumeRequest>,
    ) -> Result<Response<crate::identity::csi::ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("Not yet supported"))
    }

    async fn controller_modify_volume(
        &self,
        _request: Request<crate::identity::csi::ControllerModifyVolumeRequest>,
    ) -> Result<Response<crate::identity::csi::ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("Not yet supported"))
    }

    async fn get_snapshot(
        &self,
        _request: Request<crate::identity::csi::GetSnapshotRequest>,
    ) -> Result<Response<crate::identity::csi::GetSnapshotResponse>, Status> {
        Err(Status::unimplemented("Snapshots not yet supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_controller_capabilities() {
        let service = ControllerService::new("pillar-namenode:9000".to_string());
        let request = Request::new(ControllerGetCapabilitiesRequest {});

        let response = service.controller_get_capabilities(request).await.unwrap();
        let caps = response.into_inner();

        assert!(!caps.capabilities.is_empty());
    }
}
