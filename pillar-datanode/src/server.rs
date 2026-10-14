use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::Result;
use tracing::{info, error, warn};
use uuid::Uuid;

use crate::storage::{VolumeManager, BlockStorage};
use crate::transport::tcp::TcpTransport;
use crate::transport::selector::TransportSelector;
use crate::replication::ReplicationCoordinator;
use crate::grpc::{NamenodeClient, ClientDatanodeService};
use crate::grpc::namenode_client::DatanodeInfo;

/// Configuration for the DataNode
#[derive(Debug, Clone)]
pub struct DataNodeConfig {
    /// NameNode RPC address
    pub namenode_addr: String,

    /// DataNode UUID (auto-generated if not provided)
    pub uuid: Option<String>,

    /// Hostname
    pub hostname: String,

    /// RPC server address
    pub rpc_addr: SocketAddr,

    /// Kerberos for the IPC port: the keytab and the DataNode's principal
    /// (`dn/<host>@<REALM>`). Without them, SIMPLE.
    pub kerberos_keytab: Option<std::path::PathBuf>,
    pub kerberos_principal: Option<String>,

    /// Data transfer server address (for block I/O)
    pub data_addr: SocketAddr,

    /// HTTP server address (for UI/metrics)
    pub http_addr: SocketAddr,

    /// Mount point for Flint volume
    pub volume_mount_point: PathBuf,

    /// Block pool ID
    pub block_pool_id: String,

    /// Handler count (RPC threads)
    pub handler_count: Option<u32>,

    /// Maximum xceiver count (concurrent transfers)
    pub max_xceiver_count: Option<u32>,

    /// Maximum transfer threads
    pub max_transfer_threads: Option<u32>,

    /// Heartbeat interval (seconds)
    pub heartbeat_interval: Option<u64>,

    /// Block report interval (milliseconds)
    pub block_report_interval: Option<u64>,

    /// Reserved disk space (bytes)
    pub du_reserved_bytes: Option<u64>,

    /// Transport mode: "auto", "tcp", "rdma"
    /// - "auto": Enable RDMA if hardware available, fallback to TCP (default)
    /// - "tcp": Force TCP only (disable RDMA)
    /// - "rdma": Force RDMA, fail if unavailable
    pub transport: String,

    /// RDMA data transfer port (when RDMA enabled)
    #[cfg(feature = "rdma")]
    pub rdma_addr: SocketAddr,

    /// RDMA queue depth
    #[cfg(feature = "rdma")]
    pub rdma_queue_depth: u32,

    /// RDMA max inline data size (bytes)
    #[cfg(feature = "rdma")]
    pub rdma_max_inline_data: u32,

    /// RDMA buffer pool size (pre-allocated buffers)
    #[cfg(feature = "rdma")]
    pub rdma_buffer_pool_size: usize,
}

impl Default for DataNodeConfig {
    fn default() -> Self {
        Self {
            namenode_addr: "pillar-namenode:9000".to_string(),
            uuid: None,
            hostname: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "localhost".to_string()),
            rpc_addr: "0.0.0.0:9867".parse().unwrap(),
            kerberos_keytab: None,
            kerberos_principal: None,
            data_addr: "0.0.0.0:50010".parse().unwrap(),
            http_addr: "0.0.0.0:50075".parse().unwrap(),
            volume_mount_point: PathBuf::from("/mnt/pillar-data"),
            block_pool_id: "BP-pillar-001".to_string(),
            handler_count: None,
            max_xceiver_count: None,
            max_transfer_threads: None,
            heartbeat_interval: None,
            block_report_interval: None,
            du_reserved_bytes: None,
            transport: "auto".to_string(),
            #[cfg(feature = "rdma")]
            rdma_addr: "0.0.0.0:50011".parse().unwrap(),
            #[cfg(feature = "rdma")]
            rdma_queue_depth: 128,
            #[cfg(feature = "rdma")]
            rdma_max_inline_data: 256,
            #[cfg(feature = "rdma")]
            rdma_buffer_pool_size: 1024,
        }
    }
}

impl DataNodeConfig {
    /// Load configuration from environment variables with fallbacks
    pub fn from_env() -> Self {
        use std::env;

        let mut config = Self::default();

        // Read from environment variables
        if let Ok(val) = env::var("HDFS_NAMENODE_RPC_ADDRESS") {
            config.namenode_addr = val;
        }

        if let Ok(val) = env::var("HDFS_BLOCK_POOL_ID") {
            config.block_pool_id = val;
        }

        if let Ok(val) = env::var("DATANODE_VOLUME_MOUNT_POINT") {
            config.volume_mount_point = PathBuf::from(val);
        }

        if let Ok(val) = env::var("DATANODE_KERBEROS_KEYTAB") {
            config.kerberos_keytab = Some(val.into());
        }
        if let Ok(val) = env::var("DATANODE_KERBEROS_PRINCIPAL") {
            config.kerberos_principal = Some(val);
        }

        if let Ok(val) = env::var("DATANODE_RPC_PORT") {
            if let Ok(port) = val.parse::<u16>() {
                config.rpc_addr = format!("0.0.0.0:{}", port).parse().unwrap();
            }
        }

        if let Ok(val) = env::var("DATANODE_DATA_PORT") {
            if let Ok(port) = val.parse::<u16>() {
                config.data_addr = format!("0.0.0.0:{}", port).parse().unwrap();
            }
        }

        if let Ok(val) = env::var("DATANODE_HTTP_PORT") {
            if let Ok(port) = val.parse::<u16>() {
                config.http_addr = format!("0.0.0.0:{}", port).parse().unwrap();
            }
        }

        if let Ok(val) = env::var("DATANODE_HANDLER_COUNT") {
            if let Ok(count) = val.parse() {
                config.handler_count = Some(count);
            }
        }

        if let Ok(val) = env::var("DATANODE_MAX_XCEIVER_COUNT") {
            if let Ok(count) = val.parse() {
                config.max_xceiver_count = Some(count);
            }
        }

        if let Ok(val) = env::var("DATANODE_MAX_TRANSFER_THREADS") {
            if let Ok(count) = val.parse() {
                config.max_transfer_threads = Some(count);
            }
        }

        if let Ok(val) = env::var("DATANODE_HEARTBEAT_INTERVAL") {
            if let Ok(interval) = val.parse() {
                config.heartbeat_interval = Some(interval);
            }
        }

        if let Ok(val) = env::var("DATANODE_BLOCK_REPORT_INTERVAL") {
            if let Ok(interval) = val.parse() {
                config.block_report_interval = Some(interval);
            }
        }

        if let Ok(val) = env::var("DATANODE_DU_RESERVED_BYTES") {
            if let Ok(bytes) = val.parse() {
                config.du_reserved_bytes = Some(bytes);
            }
        }

        // Transport configuration
        if let Ok(val) = env::var("DATANODE_TRANSPORT") {
            config.transport = val;
        }

        #[cfg(feature = "rdma")]
        {
            if let Ok(val) = env::var("DATANODE_RDMA_PORT") {
                if let Ok(port) = val.parse::<u16>() {
                    config.rdma_addr = format!("0.0.0.0:{}", port).parse().unwrap();
                }
            }

            if let Ok(val) = env::var("DATANODE_RDMA_QUEUE_DEPTH") {
                if let Ok(depth) = val.parse() {
                    config.rdma_queue_depth = depth;
                }
            }

            if let Ok(val) = env::var("DATANODE_RDMA_MAX_INLINE_DATA") {
                if let Ok(size) = val.parse() {
                    config.rdma_max_inline_data = size;
                }
            }

            if let Ok(val) = env::var("DATANODE_RDMA_BUFFER_POOL_SIZE") {
                if let Ok(size) = val.parse() {
                    config.rdma_buffer_pool_size = size;
                }
            }
        }

        // Get hostname from environment if available (useful in Kubernetes)
        if let Ok(val) = env::var("NODE_NAME") {
            config.hostname = val;
        } else if let Ok(val) = env::var("POD_NAME") {
            config.hostname = val;
        }

        config
    }
}

/// The DataNode server
pub struct DataNode {
    config: DataNodeConfig,
    datanode_info: DatanodeInfo,
    volume_manager: Arc<VolumeManager>,
    block_storage: Arc<BlockStorage>,
    namenode_client: Arc<NamenodeClient>,
    transport_selector: Arc<TransportSelector>,
    client_service: Arc<ClientDatanodeService>,
}

impl DataNode {
    pub async fn new(config: DataNodeConfig) -> Result<Self> {
        info!("Initializing DataNode with config: {:?}", config);

        // Generate UUID if not provided
        let uuid = config.uuid.clone().unwrap_or_else(|| Uuid::new_v4().to_string());

        // Initialize volume manager
        let volume_manager = Arc::new(VolumeManager::new(
            &config.volume_mount_point,
            config.block_pool_id.clone(),
        )?);

        info!("Volume manager initialized at: {:?}", config.volume_mount_point);

        // Initialize block storage
        let block_storage = Arc::new(BlockStorage::new(
            volume_manager.get_finalized_dir(),
            volume_manager.get_tmp_dir(),
        )?);

        info!("Block storage initialized ({} existing blocks)", block_storage.get_block_count());

        // Create TCP transport
        let tcp_transport = Arc::new(TcpTransport::new(
            block_storage.clone(),
            config.data_addr,
        ));

        // Create transport selector with optional RDMA
        let transport_selector = {
            #[cfg(feature = "rdma")]
            {
                use crate::transport::rdma::RdmaTransport;

                // Check if RDMA should be enabled based on config
                let enable_rdma = match config.transport.as_str() {
                    "rdma" => true,  // Force RDMA
                    "auto" => true,  // Try RDMA, will fallback to TCP if it fails
                    "tcp" => false,  // Force TCP only
                    _ => {
                        warn!("Unknown transport mode '{}', defaulting to TCP", config.transport);
                        false
                    }
                };

                if enable_rdma {
                    match RdmaTransport::new(
                        block_storage.clone(),
                        config.rdma_addr,
                        config.rdma_queue_depth,
                        config.rdma_max_inline_data,
                        config.rdma_buffer_pool_size,
                    ) {
                        Ok(rdma_transport) => {
                            info!("RDMA transport enabled on {}", config.rdma_addr);
                            Arc::new(TransportSelector::new_with_rdma(
                                tcp_transport,
                                Arc::new(rdma_transport),
                            ))
                        }
                        Err(e) => {
                            if config.transport == "rdma" {
                                // RDMA was explicitly requested but failed
                                return Err(anyhow::anyhow!("Failed to initialize RDMA (required by config): {}", e));
                            } else {
                                // Auto mode: fallback to TCP
                                warn!("RDMA initialization failed, falling back to TCP: {}", e);
                                Arc::new(TransportSelector::new_tcp_only(tcp_transport))
                            }
                        }
                    }
                } else {
                    info!("Using TCP-only transport (RDMA disabled by config)");
                    Arc::new(TransportSelector::new_tcp_only(tcp_transport))
                }
            }

            #[cfg(not(feature = "rdma"))]
            {
                if config.transport != "tcp" && config.transport != "auto" {
                    warn!("RDMA requested but not compiled in (use --features rdma), using TCP");
                }
                Arc::new(TransportSelector::new_tcp_only(tcp_transport))
            }
        };

        // Create client service
        let client_service = Arc::new(ClientDatanodeService::new(
            block_storage.clone(),
            volume_manager.clone(),
        ));

        info!("DataNode core components initialized");

        // Determine which transports are actually available
        let supported_transports = transport_selector.supported_transports();
        let rdma_port = if supported_transports.contains(&"rdma".to_string()) {
            #[cfg(feature = "rdma")]
            { config.rdma_addr.port() }
            #[cfg(not(feature = "rdma"))]
            { 0 }
        } else {
            0
        };

        // Create datanode info with detected transport capabilities
        let datanode_info = DatanodeInfo {
            uuid: uuid.clone(),
            hostname: config.hostname.clone(),
            rpc_port: config.rpc_addr.port(),
            data_port: config.data_addr.port(),
            http_port: config.http_addr.port(),
            supported_transports: supported_transports.clone(),
            rdma_port,
        };

        // Replication coordinator: runs the NameNode's TRANSFER commands over
        // whichever transports this DataNode has.
        #[allow(unused_mut)]
        let mut transports = vec![transport_selector.tcp_transport()];
        #[cfg(feature = "rdma")]
        if let Some(rdma) = transport_selector.rdma_transport() {
            transports.push(rdma);
        }
        let replication_coordinator = Arc::new(ReplicationCoordinator::new(block_storage.clone(), transports));

        // Create NameNode client
        let mut namenode_client = NamenodeClient::new(
            config.namenode_addr.clone(),
            datanode_info.clone(),
            volume_manager.clone(),
            block_storage.clone(),
        );
        namenode_client.set_replication_coordinator(replication_coordinator);
        if let Some(secs) = config.heartbeat_interval {
            namenode_client.set_heartbeat_interval(std::time::Duration::from_secs(secs.max(1)));
        }
        if let Some(ms) = config.block_report_interval {
            namenode_client.set_block_report_interval(std::time::Duration::from_millis(ms));
        }
        let namenode_client = Arc::new(namenode_client);

        // Log transport configuration
        if supported_transports.contains(&"rdma".to_string()) {
            #[cfg(feature = "rdma")]
            info!("Transport: TCP on {} + RDMA on {}", config.data_addr, config.rdma_addr);

            #[cfg(not(feature = "rdma"))]
            info!("Transport: TCP on {} + RDMA (not compiled)", config.data_addr);
        } else {
            info!("Transport: TCP on {}", config.data_addr);
        }
        info!("Advertising transports to NameNode: {:?}", supported_transports);

        Ok(Self {
            config,
            datanode_info,
            volume_manager,
            block_storage,
            namenode_client,
            transport_selector,
            client_service,
        })
    }

    /// Start the DataNode server
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting DataNode server");

        // Register with the NameNode, waiting for it if it is not up yet
        // (in Kubernetes the pods start together). A volume that belongs to
        // another namespace is fatal.
        loop {
            match self.namenode_client.register().await {
                Ok(()) => break,
                Err(e @ crate::grpc::namenode_client::NamenodeClientError::IncompatibleStorage(_)) => {
                    return Err(anyhow::anyhow!("Refusing to join the NameNode: {}", e));
                }
                Err(e) => {
                    warn!("Registration with the NameNode failed ({}); retrying in 2s", e);
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }

        // Send initial block report
        self.namenode_client.send_block_report().await
            .map_err(|e| anyhow::anyhow!("Failed to send block report: {}", e))?;

        // The IPC port: ClientDatanodeProtocol (getReplicaVisibleLength,
        // for readers of files still being written).
        let auth = match (&self.config.kerberos_keytab, &self.config.kerberos_principal) {
            (Some(keytab), Some(principal)) => pillar_common::ipc::Authentication::Kerberos(
                pillar_common::ipc::KerberosServer::new(keytab, principal)
                    .map_err(|e| anyhow::anyhow!("Kerberos: {}", e))?,
            ),
            (None, None) => pillar_common::ipc::Authentication::Simple,
            _ => anyhow::bail!("Kerberos needs both a keytab and a principal"),
        };
        let ipc = crate::ipc::serve(self.config.rpc_addr, self.block_storage.clone(), auth)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to bind the IPC port {}: {}", self.config.rpc_addr, e))?;
        info!("DataNode IPC (ClientDatanodeProtocol) on {}", ipc);

        // Start transport servers (TCP, and RDMA if available)
        self.transport_selector.start_all_servers().await;

        // Start heartbeat loop in background
        let namenode_client = self.namenode_client.clone();
        tokio::spawn(async move {
            namenode_client.start_heartbeat_loop().await;
        });

        info!("DataNode data transfer server started on {}", self.config.data_addr);
        info!("DataNode HTTP UI available on {}", self.config.http_addr);
        info!("Supported transports: {}", self.transport_selector.supported_transports().join(", "));

        // Keep the server running
        tokio::signal::ctrl_c().await?;

        info!("Shutting down DataNode");

        Ok(())
    }

    /// Get DataNode statistics
    pub fn get_stats(&self) -> DataNodeStats {
        let volume_info = self.volume_manager.get_volume_info()
            .unwrap_or_else(|_| crate::storage::VolumeInfo {
                capacity_bytes: 0,
                used_bytes: 0,
                remaining_bytes: 0,
                block_pool_used_bytes: 0,
            });

        DataNodeStats {
            uuid: self.datanode_info.uuid.clone(),
            hostname: self.datanode_info.hostname.clone(),
            capacity: volume_info.capacity_bytes,
            used: volume_info.used_bytes,
            remaining: volume_info.remaining_bytes,
            block_pool_used: volume_info.block_pool_used_bytes,
            num_blocks: self.block_storage.get_block_count(),
        }
    }
}

/// DataNode statistics
#[derive(Debug, Clone)]
pub struct DataNodeStats {
    pub uuid: String,
    pub hostname: String,
    pub capacity: u64,
    pub used: u64,
    pub remaining: u64,
    pub block_pool_used: u64,
    pub num_blocks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_datanode_initialization() {
        let temp_dir = TempDir::new().unwrap();

        let config = DataNodeConfig {
            volume_mount_point: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let datanode = DataNode::new(config).await.unwrap();

        let stats = datanode.get_stats();
        assert!(!stats.uuid.is_empty());
        assert_eq!(stats.num_blocks, 0);
    }
}
