//! The replication control loop end to end, in one process: a real
//! NameNode DatanodeProtocol gRPC server, and DataNodes with real storage,
//! TCP data servers and `NamenodeClient`s heartbeating to it.
//!
//! The NameNode queues TRANSFER / INVALIDATE commands; the DataNodes pick
//! them up on heartbeat, move or delete the data, and report back with
//! incremental block reports, which must update the NameNode's replica map.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use pillar_datanode::grpc::namenode_client::{DatanodeInfo, NamenodeClient};
use pillar_datanode::replication::ReplicationCoordinator;
use pillar_datanode::storage::block_storage::BlockId as DnBlockId;
use pillar_datanode::storage::{BlockStorage, VolumeManager};
use pillar_datanode::transport::tcp::TcpTransport;
use pillar_datanode::transport::DataTransport;
use pillar_namenode::datanode::DatanodeManager;
use pillar_namenode::grpc::datanode_protocol_service::DatanodeCommand;
use pillar_namenode::grpc::{DatanodeProtocolServer, DatanodeProtocolService};
use pillar_namenode::metadata::block_manager::BlockId;
use pillar_namenode::metadata::BlockManager;
use pillar_proto::hadoop::hdfs::datanode::datanode_protocol_service_server::DatanodeProtocolServiceServer;
use tempfile::TempDir;

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

struct NameNode {
    addr: SocketAddr,
    service: Arc<DatanodeProtocolService>,
    blocks: Arc<BlockManager>,
    datanodes: Arc<DatanodeManager>,
}

async fn start_namenode() -> NameNode {
    let datanodes = Arc::new(DatanodeManager::new());
    let blocks = Arc::new(BlockManager::new());
    let service = Arc::new(DatanodeProtocolService::new(datanodes.clone(), blocks.clone()));
    let addr = free_addr();
    let server = DatanodeProtocolServiceServer::new(DatanodeProtocolServer::new(service.clone()));
    tokio::spawn(tonic::transport::Server::builder().add_service(server).serve(addr));
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    NameNode { addr, service, blocks, datanodes }
}

struct DataNode {
    _dir: TempDir,
    volumes: Arc<VolumeManager>,
    storage: Arc<BlockStorage>,
}

async fn start_datanode(nn: &NameNode, uuid: &str) -> DataNode {
    let dir = TempDir::new().unwrap();
    let volumes = Arc::new(VolumeManager::new(dir.path(), "BP-test".to_string()).unwrap());
    let storage = Arc::new(BlockStorage::new(volumes.get_finalized_dir(), volumes.get_tmp_dir()).unwrap());
    let data_addr = free_addr();
    let tcp = Arc::new(TcpTransport::new(storage.clone(), data_addr));
    tokio::spawn(tcp.clone().start_server());

    let info = DatanodeInfo {
        uuid: uuid.to_string(),
        hostname: "127.0.0.1".to_string(),
        rpc_port: 0,
        data_port: data_addr.port(),
        http_port: 0,
        supported_transports: vec!["tcp".to_string()],
        rdma_port: 0,
    };
    let mut client = NamenodeClient::new(nn.addr.to_string(), info, volumes.clone(), storage.clone());
    client.set_replication_coordinator(Arc::new(ReplicationCoordinator::new(
        storage.clone(),
        vec![tcp as Arc<dyn DataTransport>],
    )));
    client.set_heartbeat_interval(Duration::from_millis(50));
    let client = Arc::new(client);
    client.register().await.unwrap();
    tokio::spawn(client.start_heartbeat_loop());
    DataNode { _dir: dir, volumes, storage }
}

/// Make `block` part of the namespace (as addBlock would), so replicas of
/// it are kept; replicas of unknown blocks are deleted.
fn known(nn: &NameNode, inode: u64, block: BlockId, num_bytes: u64) {
    use pillar_namenode::metadata::namespace::BlockInfo;
    nn.blocks.restore_file(
        inode,
        &[BlockInfo { block_id: block.block_id, generation_stamp: block.generation_stamp, num_bytes }],
    );
}

fn holders(nn: &NameNode, block: &BlockId) -> Vec<String> {
    let mut v: Vec<String> = nn
        .blocks
        .get_block_locations(block)
        .unwrap_or_default()
        .into_iter()
        .map(|d| d.uuid)
        .collect();
    v.sort();
    v
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_queued_transfer_creates_a_replica_the_namenode_then_knows_about() {
    let nn = start_namenode().await;
    let dn1 = start_datanode(&nn, "dn1").await;
    let dn2 = start_datanode(&nn, "dn2").await;

    // dn1 receives a block (as if a client had written it).
    let data: Vec<u8> = (0..3 * 1024 * 1024u32).map(|i| (i * 7 + 3) as u8).collect();
    known(&nn, 100, BlockId::new(1, 1001), data.len() as u64);
    let local = DnBlockId::new(1, 1001);
    {
        use std::io::Write;
        dn1.storage.begin_write(&local).unwrap().write_all(&data).unwrap();
    }
    dn1.storage.finalize_block(&local).unwrap();

    // The incremental block report tells the NameNode, with the genstamp.
    let block = BlockId::new(1, 1001);
    eventually("NameNode learns dn1 holds the block", || holders(&nn, &block) == ["dn1"]).await;

    // NameNode decides dn2 needs a copy.
    let dn2_id = nn.datanodes.get_datanode("dn2").unwrap().id;
    nn.service.queue_command(
        "dn1",
        DatanodeCommand::TransferBlocks { transfers: vec![(block, data.len() as u64, vec![dn2_id])] },
    );

    eventually("dn2 reports the new replica", || holders(&nn, &block) == ["dn1", "dn2"]).await;
    let copy = dn2.storage.read_block(&local, 0, u64::MAX).unwrap();
    assert!(copy[..] == data[..], "replica on dn2 differs");

    // And deletion flows the same way.
    nn.service.queue_command("dn2", DatanodeCommand::InvalidateBlocks { blocks: vec![block] });
    eventually("dn2's replica is deleted and unreported", || {
        holders(&nn, &block) == ["dn1"] && dn2.storage.get_block_metadata(1).is_none()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_block_report_carries_generation_stamps_and_lengths() {
    let nn = start_namenode().await;
    let dn = start_datanode(&nn, "dn1").await;
    known(&nn, 100, BlockId::new(9, 4242), 1000);
    known(&nn, 101, BlockId::new(10, 5000), 1000);
    // A known block; one the namespace never had (or deleted); and a stale
    // generation stamp of a known block.
    for b in [DnBlockId::new(9, 4242), DnBlockId::new(77, 1), DnBlockId::new(10, 4999)] {
        use std::io::Write;
        dn.storage.begin_write(&b).unwrap().write_all(&[1u8; 1000]).unwrap();
        dn.storage.finalize_block(&b).unwrap();
    }
    // Bypass the incremental path: a fresh client sends only a full report.
    let _ = dn.storage.take_events();
    let volumes = dn.volumes.clone();
    let info = DatanodeInfo {
        uuid: "dn1".into(),
        hostname: "127.0.0.1".into(),
        rpc_port: 0,
        data_port: 1,
        http_port: 0,
        supported_transports: vec!["tcp".into()],
        rdma_port: 0,
    };
    let client = NamenodeClient::new(nn.addr.to_string(), info, volumes, dn.storage.clone());
    client.send_block_report().await.unwrap();

    assert_eq!(holders(&nn, &BlockId::new(9, 4242)), ["dn1"]);
    assert!(holders(&nn, &BlockId::new(9, 0)).is_empty(), "reported under genstamp 0");

    // The unknown and the stale replicas are deleted on the next heartbeat;
    // the known one stays.
    assert!(holders(&nn, &BlockId::new(77, 1)).is_empty());
    assert!(holders(&nn, &BlockId::new(10, 4999)).is_empty());
    eventually("unknown and stale replicas are deleted", || {
        dn.storage.get_block_metadata(77).is_none() && dn.storage.get_block_metadata(10).is_none()
    })
    .await;
    assert!(dn.storage.get_block_metadata(9).is_some());
}
