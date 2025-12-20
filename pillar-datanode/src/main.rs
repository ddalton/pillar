mod storage;
mod grpc;
mod server;

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

    // Parse addresses
    let rpc_addr: SocketAddr = args.rpc_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid RPC address: {}", e))?;
    let data_addr: SocketAddr = args.data_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid data address: {}", e))?;
    let http_addr: SocketAddr = args.http_addr.parse()
        .map_err(|e| anyhow::anyhow!("Invalid HTTP address: {}", e))?;

    // Get hostname
    let hostname = args.hostname.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "localhost".to_string())
    });

    // Create configuration
    let config = DataNodeConfig {
        namenode_addr: args.namenode,
        uuid: args.uuid,
        hostname,
        rpc_addr,
        data_addr,
        http_addr,
        volume_mount_point: args.volume_mount_point,
        block_pool_id: args.block_pool_id,
    };

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
