mod metadata;
mod datanode;
mod grpc;
mod ipc;
mod server;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing::{info, error};
use tracing_subscriber::{fmt, EnvFilter};

use server::{NameNode, NameNodeConfig};

#[derive(Parser, Debug)]
#[clap(name = "pillar-namenode")]
#[clap(about = "Pillar HDFS NameNode - Drop-in replacement for Apache Hadoop HDFS NameNode in Rust", long_about = None)]
struct Args {
    /// RPC server address
    #[clap(long, default_value = "0.0.0.0:9000")]
    rpc_addr: String,

    /// HTTP server address (UI and metrics)
    #[clap(long, default_value = "0.0.0.0:9870")]
    http_addr: String,

    /// Etcd endpoints (comma-separated) for HA metadata storage
    #[clap(long, default_value = "localhost:2379", value_delimiter = ',')]
    etcd_endpoints: Vec<String>,

    /// Default replication factor
    #[clap(long, default_value = "3")]
    replication: u16,

    /// Default block size in MB
    #[clap(long, default_value = "128")]
    block_size_mb: u64,

    /// Directory for the persisted namespace (fsimage + edit log). Without
    /// it the namespace lives in memory only and is lost on restart.
    #[clap(long)]
    name_dir: Option<std::path::PathBuf>,

    /// Cluster id for a newly formatted name directory (default: random).
    /// An already formatted directory must carry this id.
    #[clap(long)]
    cluster_id: Option<String>,

    /// Kerberos keytab for Hadoop IPC clients (with --kerberos-principal).
    #[clap(long)]
    kerberos_keytab: Option<std::path::PathBuf>,

    /// The NameNode's Kerberos principal, nn/<host>@<REALM>.
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

    info!("Starting Pillar HDFS NameNode");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));

    // Load configuration from environment variables first
    let mut config = NameNodeConfig::from_env();

    // Override with command-line arguments (CLI args take precedence)
    config.rpc_addr = args.rpc_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid RPC address: {}", e))?;
    config.http_addr = args.http_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid HTTP address: {}", e))?;
    config.etcd_endpoints = args.etcd_endpoints;
    config.default_replication = args.replication;
    config.default_block_size = args.block_size_mb * 1024 * 1024;
    if args.name_dir.is_some() {
        config.name_dir = args.name_dir;
    }
    if args.cluster_id.is_some() {
        config.cluster_id = args.cluster_id;
    }
    if args.kerberos_keytab.is_some() {
        config.kerberos_keytab = args.kerberos_keytab;
    }
    if args.kerberos_principal.is_some() {
        config.kerberos_principal = args.kerberos_principal;
    }

    info!("Configuration loaded from environment and CLI arguments");
    info!("Etcd endpoints: {:?}", config.etcd_endpoints);

    // Create and start NameNode
    let namenode = Arc::new(NameNode::new(config).await?);

    info!("NameNode initialized successfully");

    // Print startup banner
    print_banner(&namenode);

    // Start the server
    if let Err(e) = namenode.start().await {
        error!("NameNode failed: {}", e);
        return Err(e);
    }

    Ok(())
}

fn print_banner(namenode: &NameNode) {
    let stats = namenode.get_stats();

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║         Pillar HDFS NameNode - Ready for Service         ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  Version:  {}                                       ║", env!("CARGO_PKG_VERSION"));
    println!("║  Protocol: Apache Hadoop HDFS (Drop-in Replacement)      ║");
    println!("║  Language: Rust (High Performance, Zero-Cost Abstractions)║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  Status:                                                  ║");
    println!("║    Live DataNodes: {:>3}                                  ║", stats.num_live_datanodes);
    println!("║    Total Blocks:   {:>6}                               ║", stats.total_blocks);
    println!("║    Capacity:       {} GB                          ║", stats.total_capacity / 1_000_000_000);
    println!("║    Used:           {} GB                          ║", stats.total_used / 1_000_000_000);
    println!("║    Remaining:      {} GB                          ║", stats.total_remaining / 1_000_000_000);
    println!("╚═══════════════════════════════════════════════════════════╝\n");
}
