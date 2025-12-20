use tonic::{Request, Response, Status};
use tracing::info;

// CSI protobuf generated code
pub mod csi {
    tonic::include_proto!("csi.v1");
}

use csi::{
    identity_server::Identity,
    GetPluginInfoRequest, GetPluginInfoResponse,
    GetPluginCapabilitiesRequest, GetPluginCapabilitiesResponse,
    ProbeRequest, ProbeResponse,
    PluginCapability,
};

const DRIVER_NAME: &str = "pillar.csi.hdfs.io";
const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct IdentityService;

impl IdentityService {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        info!("GetPluginInfo called");

        let response = GetPluginInfoResponse {
            name: DRIVER_NAME.to_string(),
            vendor_version: DRIVER_VERSION.to_string(),
            manifest: Default::default(),
        };

        Ok(Response::new(response))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        info!("GetPluginCapabilities called");

        let capabilities = vec![
            PluginCapability {
                r#type: Some(csi::plugin_capability::Type::Service(csi::plugin_capability::Service {
                    r#type: csi::plugin_capability::service::Type::ControllerService as i32,
                })),
            },
            PluginCapability {
                r#type: Some(csi::plugin_capability::Type::Service(csi::plugin_capability::Service {
                    r#type: csi::plugin_capability::service::Type::VolumeAccessibilityConstraints as i32,
                })),
            },
        ];

        let response = GetPluginCapabilitiesResponse { capabilities };

        Ok(Response::new(response))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        info!("Probe called");

        let response = ProbeResponse {
            ready: Some(true),
        };

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_plugin_info() {
        let service = IdentityService::new();
        let request = Request::new(GetPluginInfoRequest {});

        let response = service.get_plugin_info(request).await.unwrap();
        let info = response.into_inner();

        assert_eq!(info.name, DRIVER_NAME);
        assert_eq!(info.vendor_version, DRIVER_VERSION);
    }

    #[tokio::test]
    async fn test_probe() {
        let service = IdentityService::new();
        let request = Request::new(ProbeRequest {});

        let response = service.probe(request).await.unwrap();
        let probe = response.into_inner();

        assert_eq!(probe.ready, Some(true));
    }
}
