mod storage;
mod grpc;
mod server;
mod transport;
mod replication;
mod ipc;
mod recovery;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing::{info, error};
use tracing_subscriber::{fmt, EnvFilter};

use server::{DataNode, DataNodeConfig};

#[derive(Parser, Debug)]
#[clap(name = "pillar-datanode")]
#[clap(about = "Pillar HDFS DataNode - Drop-in replacement for Apache Hadoop HDFS DataNode in Rust", long_about = None)]
struct Args {
    /// NameNode address
    #[clap(long, default_value = "pillar-namenode:9000")]
    namenode: String,

    /// DataNode UUID (auto-generated if not provided)
    #[clap(long)]
    uuid: Option<String>,

    /// Hostname (auto-detected if not provided)
    #[clap(long)]
    hostname: Option<String>,

    /// RPC server address
    #[clap(long, default_value = "0.0.0.0:9867")]
    rpc_addr: String,

    /// Data transfer server address
    #[clap(long, default_value = "0.0.0.0:50010")]
    data_addr: String,

    /// HTTP server address (UI and metrics)
    #[clap(long, default_value = "0.0.0.0:50075")]
    http_addr: String,

    /// Flint volume mount point
    #[clap(long, default_value = "/mnt/pillar-data")]
    volume_mount_point: PathBuf,

    /// Block pool ID
    #[clap(long, default_value = "BP-pillar-001")]
    block_pool_id: String,

    /// Transport mode (auto, tcp, rdma)
    #[clap(long, default_value = "auto")]
    transport: String,

    /// RDMA port (when RDMA enabled)
    #[cfg(feature = "rdma")]
    #[clap(long, default_value = "50011")]
    rdma_port: u16,

    /// RDMA queue depth
    #[cfg(feature = "rdma")]
    #[clap(long, default_value = "128")]
    rdma_queue_depth: u32,

    /// Kerberos keytab for the IPC port (with --kerberos-principal).
    #[clap(long)]
    kerberos_keytab: Option<std::path::PathBuf>,

    /// The DataNode's Kerberos principal, dn/<host>@<REALM>.
    #[clap(long)]
    kerberos_principal: Option<String>,

    /// Log level (trace, debug, info, warn, error)
    #[clap(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
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

    info!("Starting Pillar HDFS DataNode");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));

    // Load configuration from environment variables first
    let mut config = DataNodeConfig::from_env();

    // Override with command-line arguments (CLI args take precedence)
    config.namenode_addr = args.namenode;
    config.uuid = args.uuid;

    // Get hostname from args, env, or auto-detect
    if let Some(hostname) = args.hostname {
        config.hostname = hostname;
    }
    // Otherwise, config.hostname is already set from env or auto-detected

    config.rpc_addr = args.rpc_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid RPC address: {}", e))?;
    config.data_addr = args.data_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid data address: {}", e))?;
    config.http_addr = args.http_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid HTTP address: {}", e))?;
    config.volume_mount_point = args.volume_mount_point;
    config.block_pool_id = args.block_pool_id;
    config.transport = args.transport;
    if args.kerberos_keytab.is_some() {
        config.kerberos_keytab = args.kerberos_keytab;
    }
    if args.kerberos_principal.is_some() {
        config.kerberos_principal = args.kerberos_principal;
    }

    #[cfg(feature = "rdma")]
    {
        config.rdma_addr = format!("0.0.0.0:{}", args.rdma_port).parse()
            .map_err(|e| anyhow::anyhow!("Invalid RDMA port: {}", e))?;
        config.rdma_queue_depth = args.rdma_queue_depth;
    }

    info!("Configuration loaded from environment and CLI arguments");

    // Create and start DataNode
    let datanode = Arc::new(DataNode::new(config).await?);

    info!("DataNode initialized successfully");

    // Print startup banner
    print_banner(&datanode);

    // Start the server
    if let Err(e) = datanode.start().await {
        error!("DataNode failed: {}", e);
        return Err(e);
    }

    Ok(())
}

fn print_banner(datanode: &DataNode) {
    let stats = datanode.get_stats();

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║         Pillar HDFS DataNode - Ready for Service         ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  Version:  {}                                       ║", env!("CARGO_PKG_VERSION"));
    println!("║  Protocol: Apache Hadoop HDFS (Drop-in Replacement)      ║");
    println!("║  Language: Rust (High Performance, Zero-Cost Abstractions)║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  DataNode Info:                                           ║");
    println!("║    UUID:       {}                              ║", &stats.uuid[..36.min(stats.uuid.len())]);
    println!("║    Hostname:   {:45} ║", stats.hostname);
    println!("║    Blocks:     {:>6}                                        ║", stats.num_blocks);
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  Storage:                                                 ║");
    println!("║    Capacity:   {} GB                          ║", stats.capacity / 1_000_000_000);
    println!("║    Used:       {} GB                          ║", stats.used / 1_000_000_000);
    println!("║    Remaining:  {} GB                          ║", stats.remaining / 1_000_000_000);
    println!("║    Block Pool: {} GB                          ║", stats.block_pool_used / 1_000_000_000);
    println!("╚═══════════════════════════════════════════════════════════╝\n");
}
