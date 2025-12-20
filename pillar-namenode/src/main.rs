use clap::Parser;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "pillar-namenode")]
#[command(about = "Pillar HDFS NameNode - Rust implementation of Apache Hadoop HDFS NameNode", long_about = None)]
struct Args {
    /// NameNode RPC port
    #[arg(long, default_value = "9000")]
    port: u16,

    /// Metadata storage backend (sqlite or etcd)
    #[arg(long, default_value = "sqlite")]
    metadata_backend: String,

    /// Metadata storage path (for sqlite) or etcd endpoints (comma-separated)
    #[arg(long, default_value = "/var/lib/pillar/metadata")]
    metadata_path: String,

    /// Block pool ID
    #[arg(long, default_value = "BP-pillar-001")]
    block_pool_id: String,

    /// Cluster ID
    #[arg(long, default_value = "CL-pillar")]
    cluster_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pillar_namenode=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    info!("Starting Pillar HDFS NameNode");
    info!("  Port: {}", args.port);
    info!("  Metadata backend: {}", args.metadata_backend);
    info!("  Metadata path: {}", args.metadata_path);
    info!("  Block pool ID: {}", args.block_pool_id);
    info!("  Cluster ID: {}", args.cluster_id);

    info!("NameNode initialization complete");
    info!("Waiting for DataNode registrations...");

    // TODO: Initialize NameNode components
    // - Metadata store
    // - Namespace
    // - Block manager
    // - DataNode manager
    // - gRPC services (ClientNamenodeProtocol, DatanodeProtocol)

    // Keep the server running
    tokio::signal::ctrl_c().await?;
    info!("Shutting down NameNode");

    Ok(())
}
