use clap::Parser;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "pillar-datanode")]
#[command(about = "Pillar HDFS DataNode - Rust implementation of Apache Hadoop HDFS DataNode", long_about = None)]
struct Args {
    /// DataNode RPC port
    #[arg(long, default_value = "50010")]
    port: u16,

    /// Data transfer port
    #[arg(long, default_value = "50020")]
    data_port: u16,

    /// NameNode address (host:port)
    #[arg(long, default_value = "localhost:9000")]
    namenode: String,

    /// Storage directory (must be a mounted Flint volume)
    #[arg(long, default_value = "/mnt/pillar-data")]
    storage_dir: String,

    /// DataNode ID (hostname or UUID)
    #[arg(long)]
    datanode_id: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pillar_datanode=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    let datanode_id = args.datanode_id.unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
    });

    info!("Starting Pillar HDFS DataNode");
    info!("  DataNode ID: {}", datanode_id);
    info!("  RPC Port: {}", args.port);
    info!("  Data Transfer Port: {}", args.data_port);
    info!("  NameNode: {}", args.namenode);
    info!("  Storage directory: {}", args.storage_dir);

    // TODO: Initialize DataNode components
    // - Volume manager (verify Flint volume is mounted)
    // - Block storage
    // - Registration with NameNode
    // - Heartbeat loop
    // - gRPC services (ClientDatanodeProtocol)
    // - Data transfer server

    info!("DataNode initialization complete");
    info!("Registering with NameNode at {}", args.namenode);

    // Keep the server running
    tokio::signal::ctrl_c().await?;
    info!("Shutting down DataNode");

    Ok(())
}
