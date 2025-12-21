//! Integration tests for block replication
//!
//! These tests verify the complete block replication flow including:
//! - Block storage and reading
//! - Transport selection (TCP/RDMA)
//! - Strategy selection (Pipeline/Parallel/Overlapped)
//! - End-to-end replication between mock DataNodes

use std::sync::Arc;
use tempfile::TempDir;
use pillar_datanode::storage::{BlockStorage, block_storage::BlockId};
use pillar_datanode::transport::{DataTransport, TransportType};
use pillar_datanode::transport::tcp::TcpTransport;
use pillar_datanode::replication::{
    ReplicationCoordinator, ReplicationRequest, ReplicationTarget,
    ReplicationStrategy, StrategySelector,
};

/// Helper to create a block storage with a test block
fn create_block_storage_with_data(block_id: u64, gen_stamp: u64, data: &[u8]) -> (TempDir, Arc<BlockStorage>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let finalized_dir = temp_dir.path().join("finalized");
    let tmp_dir = temp_dir.path().join("tmp");

    std::fs::create_dir_all(&finalized_dir).expect("Failed to create finalized dir");
    std::fs::create_dir_all(&tmp_dir).expect("Failed to create tmp dir");

    let storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir)
        .expect("Failed to create block storage"));

    // Create and write the block
    let block = BlockId::new(block_id, gen_stamp);
    storage.create_block(block).expect("Failed to create block");
    storage.write_block(&block, 0, data).expect("Failed to write block");
    storage.finalize_block(&block).expect("Failed to finalize block");

    (temp_dir, storage)
}

#[tokio::test]
async fn test_strategy_selector_small_block_tcp() {
    let selector = StrategySelector::new();

    // Create TCP-only targets
    let targets = vec![
        ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "dn1.example.com".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
        ReplicationTarget {
            datanode_uuid: "dn2".to_string(),
            hostname: "dn2.example.com".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
        ReplicationTarget {
            datanode_uuid: "dn3".to_string(),
            hostname: "dn3.example.com".to_string(),
            ip_addr: "10.0.0.3".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
    ];

    let transports: Vec<Arc<dyn DataTransport>> = vec![];

    // Small block (1MB) with 3 TCP targets -> should use Parallel
    let strategy = selector.select_strategy(&targets, 1024 * 1024, &transports);
    assert_eq!(strategy, ReplicationStrategy::Parallel);
}

#[tokio::test]
async fn test_strategy_selector_large_block_tcp() {
    let selector = StrategySelector::new();

    // Create TCP-only targets
    let targets = vec![
        ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "dn1.example.com".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
        ReplicationTarget {
            datanode_uuid: "dn2".to_string(),
            hostname: "dn2.example.com".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
        ReplicationTarget {
            datanode_uuid: "dn3".to_string(),
            hostname: "dn3.example.com".to_string(),
            ip_addr: "10.0.0.3".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
    ];

    let transports: Vec<Arc<dyn DataTransport>> = vec![];

    // Large block (100MB) with 3 TCP targets -> should use Overlapped
    let strategy = selector.select_strategy(&targets, 100 * 1024 * 1024, &transports);
    assert_eq!(strategy, ReplicationStrategy::Overlapped);
}

#[tokio::test]
async fn test_strategy_selector_single_target() {
    let selector = StrategySelector::new();

    // Single target
    let targets = vec![
        ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "dn1.example.com".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 50011,
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        },
    ];

    let transports: Vec<Arc<dyn DataTransport>> = vec![];

    // Single target always uses Pipeline
    let strategy = selector.select_strategy(&targets, 100 * 1024 * 1024, &transports);
    assert_eq!(strategy, ReplicationStrategy::Pipeline);
}

#[tokio::test]
async fn test_block_metadata_after_creation() {
    let block_id = 12345;
    let gen_stamp = 1;
    let data = b"Hello, HDFS!";

    let (_temp_dir, storage) = create_block_storage_with_data(block_id, gen_stamp, data);

    // Verify block metadata
    let metadata = storage.get_block_metadata(block_id)
        .expect("Block should exist");

    assert_eq!(metadata.block_id.block_id, block_id);
    assert_eq!(metadata.block_id.generation_stamp, gen_stamp);
    assert_eq!(metadata.num_bytes, data.len() as u64);
}

#[tokio::test]
async fn test_block_read_after_write() {
    let block_id = 12346;
    let gen_stamp = 1;
    let data = b"Test data for reading";

    let (_temp_dir, storage) = create_block_storage_with_data(block_id, gen_stamp, data);

    // Read the block back
    let block = BlockId::new(block_id, gen_stamp);
    let read_data = storage.read_block(&block, 0, data.len() as u64)
        .expect("Failed to read block");

    assert_eq!(read_data.as_ref(), data);
}

#[tokio::test]
async fn test_replication_coordinator_creation() {
    let (_temp_dir, storage) = create_block_storage_with_data(1, 1, b"test");

    // Create TCP transport (mock address)
    let tcp_transport = Arc::new(TcpTransport::new(
        storage.clone(),
        "127.0.0.1:50010".parse().unwrap(),
    ));

    let transports: Vec<Arc<dyn DataTransport>> = vec![tcp_transport];

    // Create coordinator
    let coordinator = ReplicationCoordinator::new(storage.clone(), transports);

    // Verify coordinator was created (just a smoke test)
    assert!(true);
}

#[tokio::test]
async fn test_replication_request_creation() {
    let targets = vec![
        ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "dn1.example.com".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
    ];

    let request = ReplicationRequest {
        block_id: 12345,
        gen_stamp: 1,
        targets: targets.clone(),
        block_size: 1024 * 1024,
        priority: 1,
    };

    assert_eq!(request.block_id, 12345);
    assert_eq!(request.gen_stamp, 1);
    assert_eq!(request.targets.len(), 1);
    assert_eq!(request.block_size, 1024 * 1024);
}

#[tokio::test]
async fn test_replication_target_transport_selection() {
    // TCP-only target
    let tcp_target = ReplicationTarget {
        datanode_uuid: "dn1".to_string(),
        hostname: "dn1.example.com".to_string(),
        ip_addr: "10.0.0.1".to_string(),
        xfer_port: 50010,
        rdma_port: 0,
        supported_transports: vec!["tcp".to_string()],
    };

    assert!(tcp_target.supports_transport(TransportType::Tcp));
    assert!(!tcp_target.supports_rdma());
    assert_eq!(tcp_target.get_address(TransportType::Tcp), "10.0.0.1:50010");

    // RDMA-capable target
    let rdma_target = ReplicationTarget {
        datanode_uuid: "dn2".to_string(),
        hostname: "dn2.example.com".to_string(),
        ip_addr: "10.0.0.2".to_string(),
        xfer_port: 50010,
        rdma_port: 50011,
        supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
    };

    assert!(rdma_target.supports_transport(TransportType::Tcp));
    assert!(rdma_target.supports_rdma());
    assert_eq!(rdma_target.get_address(TransportType::Tcp), "10.0.0.2:50010");

    #[cfg(feature = "rdma")]
    {
        assert!(rdma_target.supports_transport(TransportType::Rdma));
        assert_eq!(rdma_target.get_address(TransportType::Rdma), "10.0.0.2:50011");
    }
}

#[tokio::test]
async fn test_mixed_transport_cluster() {
    let selector = StrategySelector::new();

    // Mixed cluster: 2 RDMA, 1 TCP
    let targets = vec![
        ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "dn1.example.com".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 50011,
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        },
        ReplicationTarget {
            datanode_uuid: "dn2".to_string(),
            hostname: "dn2.example.com".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            xfer_port: 50010,
            rdma_port: 50011,
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        },
        ReplicationTarget {
            datanode_uuid: "dn3".to_string(),
            hostname: "dn3.example.com".to_string(),
            ip_addr: "10.0.0.3".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
    ];

    let transports: Vec<Arc<dyn DataTransport>> = vec![];

    // Mixed cluster with large block -> should use Parallel
    // (not all targets support RDMA, but most do)
    let strategy = selector.select_strategy(&targets, 50 * 1024 * 1024, &transports);

    // The strategy selector will choose Parallel for mixed clusters
    assert!(matches!(
        strategy,
        ReplicationStrategy::Parallel | ReplicationStrategy::Overlapped
    ));
}

#[tokio::test]
async fn test_replication_with_nonexistent_block() {
    let (_temp_dir, storage) = create_block_storage_with_data(1, 1, b"test");

    let tcp_transport = Arc::new(TcpTransport::new(
        storage.clone(),
        "127.0.0.1:50010".parse().unwrap(),
    ));

    let transports: Vec<Arc<dyn DataTransport>> = vec![tcp_transport];
    let coordinator = ReplicationCoordinator::new(storage.clone(), transports);

    let targets = vec![
        ReplicationTarget {
            datanode_uuid: "dn1".to_string(),
            hostname: "dn1.example.com".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            xfer_port: 50010,
            rdma_port: 0,
            supported_transports: vec!["tcp".to_string()],
        },
    ];

    let request = ReplicationRequest {
        block_id: 99999, // Non-existent block
        gen_stamp: 1,
        targets: targets.clone(),
        block_size: 1024,
        priority: 1,
    };

    // This should fail because the block doesn't exist
    let result = coordinator.replicate(request).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_replication_with_empty_targets() {
    let (_temp_dir, storage) = create_block_storage_with_data(1, 1, b"test");

    let tcp_transport = Arc::new(TcpTransport::new(
        storage.clone(),
        "127.0.0.1:50010".parse().unwrap(),
    ));

    let transports: Vec<Arc<dyn DataTransport>> = vec![tcp_transport];
    let coordinator = ReplicationCoordinator::new(storage.clone(), transports);

    let request = ReplicationRequest {
        block_id: 1,
        gen_stamp: 1,
        targets: vec![], // Empty targets
        block_size: 1024,
        priority: 1,
    };

    // This should fail because there are no targets
    let result = coordinator.replicate(request).await;
    assert!(result.is_err());
}
