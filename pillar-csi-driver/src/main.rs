mod identity;
mod controller;
mod node;

use std::path::Path;
use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use identity::IdentityService;
use identity::csi::{
    identity_server::IdentityServer,
    controller_server::ControllerServer,
    node_server::NodeServer,
};
use controller::ControllerService;
use node::NodeService;

#[derive(Parser, Debug)]
#[clap(name = "pillar-csi-driver")]
#[clap(about = "Pillar CSI Driver - Manages HDFS namespace for Kubernetes", long_about = None)]
struct Args {
    /// CSI driver mode (controller, node, or all)
    #[clap(long, default_value = "all")]
    mode: String,

    /// CSI socket path
    #[clap(long, default_value = "/var/lib/kubelet/plugins/pillar.csi.hdfs.io/csi.sock")]
    socket: String,

    /// NameNode address for HDFS operations
    #[clap(long, default_value = "pillar-namenode:9000")]
    namenode: String,

    /// Node name (for node plugin)
    #[clap(long)]
    node_name: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[clap(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse arguments
    let args = Args::parse();

    // Initialize logging
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .init();

    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║         Pillar CSI Driver - Starting                     ║");
    info!("╚═══════════════════════════════════════════════════════════╝");
    info!("  Version:  {}", env!("CARGO_PKG_VERSION"));
    info!("  Mode:     {}", args.mode);
    info!("  Socket:   {}", args.socket);
    info!("  NameNode: {}", args.namenode);

    // Get node name
    let node_id = args.node_name.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown-node".to_string())
    });
    info!("  Node ID:  {}", node_id);

    // Remove existing socket if it exists
    let socket_path = Path::new(&args.socket);
    if socket_path.exists() {
        std::fs::remove_file(socket_path)?;
        info!("Removed existing socket file");
    }

    // Create parent directory if needed
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create UDS endpoint
    let uds = tokio::net::UnixListener::bind(&args.socket)?;
    let uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    info!("CSI socket created at: {}", args.socket);

    // Build server based on mode
    let identity_service = IdentityService::new();
    let mut router = Server::builder()
        .add_service(IdentityServer::new(identity_service));
    info!("✓ Identity service enabled");

    // Controller service (if mode is "controller" or "all")
    if args.mode == "controller" || args.mode == "all" {
        let controller_service = ControllerService::new(args.namenode.clone());
        router = router.add_service(ControllerServer::new(controller_service));
        info!("✓ Controller service enabled (manages HDFS namespace)");
    }

    // Node service (if mode is "node" or "all")
    if args.mode == "node" || args.mode == "all" {
        let node_service = NodeService::new(node_id.clone());
        router = router.add_service(NodeServer::new(node_service));
        info!("✓ Node service enabled (minimal - apps use HDFS protocol)");
    }

    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║         Pillar CSI Driver - Ready                        ║");
    info!("╚═══════════════════════════════════════════════════════════╝");
    info!("");
    info!("NOTE: Applications access HDFS via native protocol:");
    info!("  hdfs://pillar-namenode:9000/pillar/volumes/<volume-id>");
    info!("");

    // Start server
    router
        .serve_with_incoming(uds_stream)
        .await
        .map_err(|e| anyhow::anyhow!("CSI server failed: {}", e))?;

    Ok(())
}
