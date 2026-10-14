use std::net::SocketAddr;
use std::sync::Arc;
use std::path::PathBuf;
use anyhow::Result;
use tonic::transport::Server;
use tracing::{info, error};

use crate::metadata::{Namespace, BlockManager, EditLog, EtcdMetadataStore};
use crate::datanode::{DatanodeManager, BlockPlacementPolicy, ReplicationMonitor};
use crate::grpc::{ClientNamenodeService, DatanodeProtocolService};

/// Configuration for the NameNode
#[derive(Debug, Clone)]
pub struct NameNodeConfig {
    /// RPC server address
    pub rpc_addr: SocketAddr,

    /// HTTP server address (for UI/metrics)
    pub http_addr: SocketAddr,

    /// Etcd endpoints for metadata storage (HA)
    pub etcd_endpoints: Vec<String>,

    /// Default replication factor
    pub default_replication: u16,

    /// Default block size (bytes)
    pub default_block_size: u64,

    /// Handler count (RPC threads)
    pub handler_count: Option<u32>,

    /// Service handler count
    pub service_handler_count: Option<u32>,

    /// Heartbeat recheck interval (milliseconds)
    pub heartbeat_recheck_interval: Option<u64>,

    /// Stale datanode interval (milliseconds)
    pub stale_datanode_interval: Option<u64>,

    /// Where the namespace is persisted (fsimage + edit log), HDFS
    /// `dfs.namenode.name.dir`. `None` keeps it in memory only.
    pub name_dir: Option<PathBuf>,

    /// Checkpoint after this many transactions...
    pub checkpoint_txns: u64,

    /// ...or this long after the last checkpoint, if anything changed.
    pub checkpoint_period: std::time::Duration,

    /// Cluster id to format a new name directory with (HDFS
    /// `namenode -format -clusterid`); must match an existing one.
    pub cluster_id: Option<String>,

    /// A DataNode silent this long is stale, and this long dead (HDFS
    /// defaults: 30 s, 10.5 min).
    pub datanode_stale_interval: std::time::Duration,
    pub datanode_dead_interval: std::time::Duration,

    /// Write leases: another client may take an open file over after the
    /// soft limit; the NameNode recovers it after the hard limit.
    pub lease_soft_limit: std::time::Duration,
    pub lease_hard_limit: std::time::Duration,
}

impl Default for NameNodeConfig {
    fn default() -> Self {
        Self {
            rpc_addr: "0.0.0.0:9000".parse().unwrap(),
            http_addr: "0.0.0.0:9870".parse().unwrap(),
            etcd_endpoints: vec!["localhost:2379".to_string()],
            default_replication: 3,
            default_block_size: 128 * 1024 * 1024, // 128 MB
            handler_count: None,
            service_handler_count: None,
            heartbeat_recheck_interval: None,
            stale_datanode_interval: None,
            name_dir: None,
            checkpoint_txns: 1_000_000,
            checkpoint_period: std::time::Duration::from_secs(3600),
            cluster_id: None,
            datanode_stale_interval: std::time::Duration::from_secs(30),
            datanode_dead_interval: std::time::Duration::from_secs(630),
            lease_soft_limit: crate::metadata::lease_manager::DEFAULT_SOFT_LIMIT,
            lease_hard_limit: crate::metadata::lease_manager::DEFAULT_HARD_LIMIT,
        }
    }
}

impl NameNodeConfig {
    /// Load configuration from environment variables with fallbacks
    pub fn from_env() -> Self {
        use std::env;

        let mut config = Self::default();

        // Read from environment variables
        if let Ok(val) = env::var("HDFS_REPLICATION") {
            if let Ok(replication) = val.parse() {
                config.default_replication = replication;
            }
        }

        if let Ok(val) = env::var("HDFS_BLOCK_SIZE") {
            if let Ok(block_size_mb) = val.parse::<u64>() {
                config.default_block_size = block_size_mb * 1024 * 1024;
            }
        }

        if let Ok(val) = env::var("ETCD_ENDPOINTS") {
            config.etcd_endpoints = val.split(',').map(|s| s.to_string()).collect();
        }

        if let Ok(val) = env::var("NAMENODE_RPC_PORT") {
            if let Ok(port) = val.parse::<u16>() {
                config.rpc_addr = format!("0.0.0.0:{}", port).parse().unwrap();
            }
        }

        if let Ok(val) = env::var("NAMENODE_HTTP_PORT") {
            if let Ok(port) = val.parse::<u16>() {
                config.http_addr = format!("0.0.0.0:{}", port).parse().unwrap();
            }
        }

        if let Ok(val) = env::var("NAMENODE_NAME_DIR") {
            if !val.is_empty() {
                config.name_dir = Some(PathBuf::from(val));
            }
        }

        if let Ok(val) = env::var("NAMENODE_HANDLER_COUNT") {
            if let Ok(count) = val.parse() {
                config.handler_count = Some(count);
            }
        }

        if let Ok(val) = env::var("NAMENODE_SERVICE_HANDLER_COUNT") {
            if let Ok(count) = val.parse() {
                config.service_handler_count = Some(count);
            }
        }

        if let Ok(val) = env::var("NAMENODE_HEARTBEAT_RECHECK_INTERVAL") {
            if let Ok(interval) = val.parse() {
                config.heartbeat_recheck_interval = Some(interval);
            }
        }

        if let Ok(val) = env::var("NAMENODE_STALE_DATANODE_INTERVAL") {
            if let Ok(interval) = val.parse() {
                config.stale_datanode_interval = Some(interval);
            }
        }

        config
    }
}

/// The NameNode server
pub struct NameNode {
    config: NameNodeConfig,
    namespace: Arc<Namespace>,
    block_manager: Arc<BlockManager>,
    datanode_manager: Arc<DatanodeManager>,
    placement_policy: Arc<BlockPlacementPolicy>,
    edit_log: Arc<EditLog>,
    identity: pillar_common::hdfs::storage_identity::StorageIdentity,
}

impl NameNode {
    pub async fn new(config: NameNodeConfig) -> Result<Self> {
        info!("Initializing NameNode with config: {:?}", config);

        // Initialize etcd metadata store for HA. It is not yet consulted for
        // namespace state, so "none" (or no endpoints) skips it, e.g. for
        // local testing without etcd.
        let etcd_disabled = config.etcd_endpoints.is_empty()
            || config.etcd_endpoints.iter().all(|e| e.is_empty() || e == "none");
        if etcd_disabled {
            info!("Metadata store: etcd disabled");
        } else {
            let metadata_store = Arc::new(
                EtcdMetadataStore::new(config.etcd_endpoints.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to connect to etcd: {}", e))?
            );

            metadata_store.initialize().await
                .map_err(|e| anyhow::anyhow!("Failed to initialize metadata store: {}", e))?;

            info!("Metadata store initialized with etcd: {:?}", config.etcd_endpoints);
        }

        // Initialize core components, recovering the namespace if it is
        // persisted.
        let block_manager = Arc::new(BlockManager::new());
        let (namespace, edit_log, identity) = match &config.name_dir {
            Some(dir) => {
                let (log, recovered) = EditLog::open_with(dir, config.cluster_id.as_deref())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to load namespace from {}: {}", dir.display(), e))?;
                recovered.restore_blocks(&block_manager).await;
                info!(
                    "Namespace loaded from {}: {} inodes, {} blocks, transaction {}",
                    dir.display(),
                    recovered.namespace.inode_count(),
                    block_manager.get_total_blocks(),
                    recovered.txid
                );
                (recovered.namespace, log, recovered.identity)
            }
            None => {
                tracing::warn!("No name directory: the namespace is kept in memory and lost on restart");
                let identity = crate::metadata::edit_log::new_identity(config.cluster_id.as_deref());
                (Namespace::new(), EditLog::in_memory(), identity)
            }
        };
        info!("Namespace identity: {}", identity);
        let namespace = Arc::new(namespace);
        let edit_log = Arc::new(edit_log);
        let datanode_manager = Arc::new(DatanodeManager::with_intervals(
            config.datanode_stale_interval,
            config.datanode_dead_interval,
        ));
        let placement_policy = Arc::new(BlockPlacementPolicy::new(
            datanode_manager.clone(),
            config.default_replication,
        ));

        info!("NameNode core components initialized");

        Ok(Self {
            config,
            namespace,
            block_manager,
            datanode_manager,
            placement_policy,
            edit_log,
            identity,
        })
    }

    /// Start the NameNode server
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting NameNode server on {}", self.config.rpc_addr);


        // Create gRPC services
        let client_service = ClientNamenodeService::new(
            self.namespace.clone(),
            self.block_manager.clone(),
            self.datanode_manager.clone(),
            self.placement_policy.clone(),
        )
        .with_edit_log(self.edit_log.clone())
        .with_defaults(self.config.default_block_size, self.config.default_replication)
        .with_block_pool_id(self.identity.block_pool_id.clone())
        .with_leases(Arc::new(crate::metadata::lease_manager::LeaseManager::new(
            self.config.lease_soft_limit,
            self.config.lease_hard_limit,
        )));
        let open = client_service.restore_leases().await;
        if open > 0 {
            info!("{} files are open for writing; their writers have {:?} to renew", open, self.config.lease_hard_limit);
        }

        if self.edit_log.is_durable() {
            tokio::spawn(run_checkpoints(
                self.edit_log.clone(),
                self.namespace.clone(),
                self.block_manager.clone(),
                self.config.checkpoint_txns,
                self.config.checkpoint_period,
            ));
        }

        let datanode_service = DatanodeProtocolService::new(
            self.datanode_manager.clone(),
            self.block_manager.clone(),
        )
        .with_identity(self.identity.clone());

        info!("gRPC services initialized");

        // Create high-performance gRPC server
        use pillar_proto::hadoop::hdfs::{
            client_namenode_protocol_server::ClientNamenodeProtocolServer as ClientNamenodeProtoServer,
            datanode::datanode_protocol_service_server::DatanodeProtocolServiceServer as DatanodeProtoServer,
        };
        use crate::grpc::{ClientNamenodeProtocolServer, DatanodeProtocolServer};
        use tower::ServiceBuilder;

        // Wrap services in Arc for the gRPC implementation
        let client_service_arc = Arc::new(client_service);

        // Lease monitor (HDFS LeaseManager.Monitor): recover files whose
        // writer stopped renewing past the hard limit.
        tokio::spawn({
            let service = client_service_arc.clone();
            async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
                loop {
                    tick.tick().await;
                    service.check_leases().await;
                }
            }
        });
        let datanode_service_arc = Arc::new(datanode_service);

        // Replication monitor: dead DataNodes and under-replicated blocks
        // (replaces the old heartbeat monitor, which dropped dead nodes but
        // left their replicas in the location map).
        let replication_monitor = Arc::new(ReplicationMonitor::new(
            self.namespace.clone(),
            self.block_manager.clone(),
            self.datanode_manager.clone(),
            self.placement_policy.clone(),
            datanode_service_arc.clone(),
        ));
        tokio::spawn(replication_monitor.start());

        // Create protocol servers. Hadoop IPC clients get their own front end
        // over the same ClientNamenodeService.
        let ipc_server = Arc::new(ClientNamenodeProtocolServer::new(client_service_arc.clone()));
        let client_proto_server = ClientNamenodeProtocolServer::new(client_service_arc);
        let datanode_proto_server = DatanodeProtocolServer::new(datanode_service_arc);

        info!("Starting RPC server (Hadoop IPC + gRPC) on {}", self.config.rpc_addr);
        let incoming = crate::ipc::bind_shared(self.config.rpc_addr, ipc_server).await?;

        // Build server with performance optimizations
        let server = tonic::transport::Server::builder()
            // High concurrency settings
            .concurrency_limit_per_connection(1024)
            // HTTP/2 caps a frame at 2^24-1 octets; h2 panics on anything
            // larger (the old 256 MiB value broke every gRPC connection).
            // Messages larger than a frame are split across frames anyway.
            .max_frame_size(Some(4 * 1024 * 1024))
            // Connection keep-alive
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .tcp_nodelay(true)
            // HTTP/2 settings for performance
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
            .http2_adaptive_window(Some(true))
            // Add services
            .add_service(ClientNamenodeProtoServer::new(client_proto_server))
            .add_service(DatanodeProtoServer::new(datanode_proto_server))
            .serve_with_incoming_shutdown(incoming, async {
                tokio::signal::ctrl_c().await.ok();
                info!("Shutdown signal received");
            });

        info!("NameNode RPC server started on {}", self.config.rpc_addr);
        info!("NameNode HTTP UI available on {}", self.config.http_addr);
        info!("NameNode ready to accept connections");

        // Start server
        server.await?;

        info!("Shutting down NameNode");

        Ok(())
    }

    /// Get namespace reference
    pub fn namespace(&self) -> Arc<Namespace> {
        self.namespace.clone()
    }

    /// Get block manager reference
    pub fn block_manager(&self) -> Arc<BlockManager> {
        self.block_manager.clone()
    }

    /// Get datanode manager reference
    pub fn datanode_manager(&self) -> Arc<DatanodeManager> {
        self.datanode_manager.clone()
    }

    /// Get server statistics
    pub fn get_stats(&self) -> NameNodeStats {
        NameNodeStats {
            total_capacity: self.datanode_manager.get_total_capacity(),
            total_used: self.datanode_manager.get_total_used(),
            total_remaining: self.datanode_manager.get_total_remaining(),
            num_live_datanodes: self.datanode_manager.get_num_live_datanodes(),
            total_blocks: self.block_manager.get_total_blocks(),
        }
    }
}

/// Fold the edit log into a new fsimage after `txns` transactions, or
/// after `period` if anything changed, so restarts replay a bounded log.
async fn run_checkpoints(
    log: Arc<EditLog>,
    namespace: Arc<Namespace>,
    blocks: Arc<BlockManager>,
    txns: u64,
    period: std::time::Duration,
) {
    let check = period.min(std::time::Duration::from_secs(10));
    let mut last = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(check).await;
        let since = log.txns_since_checkpoint();
        if since >= txns.max(1) || (since > 0 && last.elapsed() >= period) {
            if let Err(e) = log.checkpoint(&namespace, &blocks).await {
                error!("Checkpoint failed: {}", e);
            }
            last = tokio::time::Instant::now();
        }
    }
}

/// NameNode statistics
#[derive(Debug, Clone)]
pub struct NameNodeStats {
    pub total_capacity: u64,
    pub total_used: u64,
    pub total_remaining: u64,
    pub num_live_datanodes: usize,
    pub total_blocks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // TODO: Update this test for etcd-based configuration
    // #[tokio::test]
    // async fn test_namenode_initialization() {
    //     let config = NameNodeConfig::default();
    //     let namenode = NameNode::new(config).await.unwrap();
    //     // Verify initialization
    //     let stats = namenode.get_stats();
    //     assert_eq!(stats.num_live_datanodes, 0);
    //     assert_eq!(stats.total_blocks, 0);
    // }
}
