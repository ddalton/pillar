//! Interop with an independent HDFS client: `hdfs-native` (pure Rust,
//! implements Hadoop IPC and DataTransferProtocol from the Hadoop protos)
//! against an in-process Pillar cluster — a NameNode serving Hadoop IPC and
//! gRPC on one port, and three DataNodes registered with it.
//!
//! This is the same path `hadoop fs -put / -cat` takes: IPC calls to the
//! NameNode (create, addBlock, complete, getBlockLocations, ...) and block
//! data over DataTransferProtocol with a three-node write pipeline.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use pillar_datanode::grpc::namenode_client::{DatanodeInfo, NamenodeClient};
use pillar_datanode::replication::ReplicationCoordinator;
use pillar_datanode::storage::{BlockStorage, VolumeManager};
use pillar_datanode::transport::tcp::TcpTransport;
use pillar_datanode::transport::DataTransport;
use pillar_namenode::datanode::{BlockPlacementPolicy, DatanodeManager, ReplicationMonitor};
use pillar_namenode::grpc::{
    ClientNamenodeProtocolServer, ClientNamenodeService, DatanodeProtocolServer, DatanodeProtocolService,
};
use pillar_namenode::metadata::{BlockManager, Namespace};
use pillar_proto::hadoop::hdfs::client_namenode_protocol_server::ClientNamenodeProtocolServer as ClientProtoServer;
use pillar_proto::hadoop::hdfs::datanode::datanode_protocol_service_server::DatanodeProtocolServiceServer;
use tempfile::TempDir;

const MB: usize = 1024 * 1024;

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

struct Cluster {
    nn_addr: SocketAddr,
    blocks: Arc<BlockManager>,
    datanodes: Arc<DatanodeManager>,
    _dns: Vec<(TempDir, Arc<BlockStorage>)>,
    /// Per DataNode: its heartbeat loop and data server, to simulate a crash.
    tasks: Vec<Vec<tokio::task::JoinHandle<()>>>,
}

impl Cluster {
    /// Stop DataNode `i` as if it had crashed: no heartbeats, no data port.
    fn kill(&self, i: usize) {
        for t in &self.tasks[i] {
            t.abort();
        }
    }
}

async fn start_cluster(n_datanodes: usize) -> Cluster {
    start_cluster_with(n_datanodes, false).await
}

/// `fast_failure`: DataNodes are declared dead after 1 s without a
/// heartbeat and the replication monitor runs every 100 ms.
async fn start_cluster_with(n_datanodes: usize, fast_failure: bool) -> Cluster {
    // NameNode: same wiring as NameNode::start, minus etcd.
    let namespace = Arc::new(Namespace::new());
    let blocks = Arc::new(BlockManager::new());
    let datanodes = Arc::new(if fast_failure {
        DatanodeManager::with_intervals(Duration::from_millis(500), Duration::from_secs(1))
    } else {
        DatanodeManager::new()
    });
    let placement = Arc::new(BlockPlacementPolicy::new(datanodes.clone(), 3));
    let client_service =
        Arc::new(ClientNamenodeService::new(namespace.clone(), blocks.clone(), datanodes.clone(), placement.clone()));
    let dn_service = Arc::new(DatanodeProtocolService::new(datanodes.clone(), blocks.clone()));
    let monitor = ReplicationMonitor::new(namespace, blocks.clone(), datanodes.clone(), placement, dn_service.clone());
    let monitor = if fast_failure {
        monitor.with_timing(Duration::from_millis(100), Duration::from_millis(300), Duration::from_secs(10))
    } else {
        monitor
    };
    tokio::spawn(Arc::new(monitor).start());

    let nn_addr = free_addr();
    let ipc = Arc::new(ClientNamenodeProtocolServer::new(client_service.clone()));
    let incoming = pillar_namenode::ipc::bind_shared(nn_addr, ipc).await.unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ClientProtoServer::new(ClientNamenodeProtocolServer::new(client_service)))
            .add_service(DatanodeProtocolServiceServer::new(DatanodeProtocolServer::new(dn_service)))
            .serve_with_incoming(incoming),
    );

    let mut dns = Vec::new();
    let mut tasks = Vec::new();
    for i in 0..n_datanodes {
        let dir = TempDir::new().unwrap();
        let volumes = Arc::new(VolumeManager::new(dir.path(), "BP-test".to_string()).unwrap());
        let storage = Arc::new(BlockStorage::new(volumes.get_finalized_dir(), volumes.get_tmp_dir()).unwrap());
        let data_addr = free_addr();
        let tcp = Arc::new(TcpTransport::new(storage.clone(), data_addr));
        let server = tokio::spawn({
            let tcp = tcp.clone();
            async move {
                let _ = tcp.start_server().await;
            }
        });
        let info = DatanodeInfo {
            uuid: format!("dn{i}"),
            hostname: "127.0.0.1".into(),
            rpc_port: 0,
            data_port: data_addr.port(),
            http_port: 0,
            supported_transports: vec!["tcp".into()],
            rdma_port: 0,
        };
        let mut client = NamenodeClient::new(nn_addr.to_string(), info, volumes, storage.clone());
        client.set_replication_coordinator(Arc::new(ReplicationCoordinator::new(
            storage.clone(),
            vec![tcp as Arc<dyn DataTransport>],
        )));
        client.set_heartbeat_interval(Duration::from_millis(100));
        let client = Arc::new(client);
        let mut registered = false;
        for _ in 0..100 {
            if client.register().await.is_ok() {
                registered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(registered, "dn{i} could not register");
        let heartbeats = tokio::spawn(client.start_heartbeat_loop());
        tasks.push(vec![server, heartbeats]);
        dns.push((dir, storage));
    }
    // Live = has heartbeated with capacity.
    for _ in 0..100 {
        if datanodes.get_live_datanodes().len() == n_datanodes {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Cluster { nn_addr, blocks, datanodes, _dns: dns, tasks }
}

fn client(c: &Cluster) -> Client {
    ClientBuilder::new().with_url(format!("hdfs://{}", c.nn_addr)).build().unwrap()
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i as u64 * 2654435761) >> 7) as u8 ^ seed).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hadoop_client_writes_and_reads_a_multi_block_file_through_a_three_node_pipeline() {
    let c = start_cluster(3).await;
    assert_eq!(c.datanodes.get_live_datanodes().len(), 3);
    let fs = client(&c);

    fs.mkdirs("/test", 0o755, true).await.unwrap();

    // 2.5 blocks of 1 MiB, replicated 3 ways.
    let data = pattern(5 * MB / 2, 0x5A);
    let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(3), ..Default::default() };
    let mut w = fs.create("/test/file.bin", opts).await.unwrap();
    w.write_bytes(Bytes::from(data.clone())).await.unwrap();
    w.close().await.unwrap();

    let status = fs.get_file_info("/test/file.bin").await.unwrap();
    assert_eq!(status.length, data.len());
    assert!(!status.isdir);

    let r = fs.read("/test/file.bin").await.unwrap();
    assert_eq!(r.file_length(), data.len());
    let all = r.read_range(0, data.len()).await.unwrap();
    assert!(all[..] == data[..], "file read back differs");
    // A range crossing a block boundary, not chunk-aligned.
    let mid = r.read_range(MB - 1000, 3000).await.unwrap();
    assert_eq!(&mid[..], &data[MB - 1000..MB + 2000]);

    // Every block landed on all three DataNodes (incremental reports).
    let listing = fs.list_status("/test", false).await.unwrap();
    assert_eq!(listing.len(), 1);
    assert_eq!(c.blocks.get_total_blocks(), 3);
    // The pipeline forwarded every packet: each DataNode holds all three
    // blocks, byte for byte the file's length, with an HDFS .meta beside each.
    for (i, (dir, storage)) in c._dns.iter().enumerate() {
        assert_eq!(storage.get_block_count(), 3, "dn{i} block count");
        assert_eq!(storage.total_bytes(), data.len() as u64, "dn{i} bytes");
        let metas = walk(dir.path()).into_iter().filter(|p| p.extension().is_some_and(|e| e == "meta")).count();
        assert_eq!(metas, 3, "dn{i} meta files");
    }
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn namespace_operations_over_hadoop_ipc() {
    let c = start_cluster(1).await;
    let fs = client(&c);

    fs.mkdirs("/a/b", 0o755, true).await.unwrap();
    let opts = WriteOptions { replication: Some(1), ..Default::default() };
    let mut w = fs.create("/a/b/small.txt", opts.clone()).await.unwrap();
    w.write_bytes(Bytes::from_static(b"hello pillar")).await.unwrap();
    w.close().await.unwrap();

    // Creating it again without overwrite fails the way Hadoop does.
    assert!(fs.create("/a/b/small.txt", opts).await.is_err());

    fs.rename("/a/b/small.txt", "/a/moved.txt", false).await.unwrap();
    assert!(fs.get_file_info("/a/b/small.txt").await.is_err());
    let r = fs.read("/a/moved.txt").await.unwrap();
    assert_eq!(&r.read_range(0, 12).await.unwrap()[..], b"hello pillar");

    let names: Vec<String> = fs.list_status("/a", false).await.unwrap().into_iter().map(|s| s.path).collect();
    assert_eq!(names.len(), 2, "{names:?}");

    assert!(fs.delete("/a", true).await.unwrap());
    assert!(fs.get_file_info("/a").await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_and_hadoop_ipc_share_the_port() {
    // The DataNodes above register over gRPC on the same port the Hadoop
    // client uses; make that explicit.
    let c = start_cluster(2).await;
    assert_eq!(c.datanodes.get_live_datanodes().len(), 2);
    let fs = client(&c);
    assert!(fs.get_file_info("/").await.unwrap().isdir);
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..300 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_replica_is_re_replicated_and_deleted_files_are_cleaned_up() {
    let c = start_cluster_with(3, true).await;
    let fs = client(&c);

    let data = pattern(300 * 1024, 0x33);
    let opts = WriteOptions { replication: Some(2), ..Default::default() };
    let mut w = fs.create("/r.bin", opts).await.unwrap();
    w.write_bytes(Bytes::from(data.clone())).await.unwrap();
    w.close().await.unwrap();

    let snapshot = c.blocks.blocks_snapshot();
    assert_eq!(snapshot.len(), 1);
    let (block, _, _, holders) = &snapshot[0];
    let mut holders: Vec<usize> = holders.iter().map(|h| h.uuid[2..].parse().unwrap()).collect();
    holders.sort();
    assert_eq!(holders.len(), 2);
    let spare = (0..3).find(|i| !holders.contains(i)).unwrap();

    // A holder crashes. The monitor declares it dead, drops its replica and
    // copies the block from the survivor to the spare DataNode.
    c.kill(holders[0]);
    let spare_storage = c._dns[spare].1.clone();
    eventually("the spare DataNode receives a replica", || spare_storage.get_block_count() == 1).await;
    let mut want = vec![format!("dn{}", holders[1]), format!("dn{spare}")];
    want.sort();
    eventually("the NameNode counts two live replicas again", || {
        let mut now: Vec<String> =
            c.blocks.get_block_locations(block).unwrap_or_default().into_iter().map(|d| d.uuid).collect();
        now.sort();
        now == want
    })
    .await;
    let copy = spare_storage.read_block(&pillar_datanode::storage::block_storage::BlockId::new(block.block_id, block.generation_stamp), 0, u64::MAX).unwrap();
    assert!(copy[..] == data[..], "re-replicated copy differs");

    // Readable with one of the original holders gone.
    let r = fs.read("/r.bin").await.unwrap();
    assert!(r.read_range(0, data.len()).await.unwrap()[..] == data[..]);

    // Deleting the file deletes the replicas on the live DataNodes.
    assert!(fs.delete("/r.bin", false).await.unwrap());
    let (a, b) = (c._dns[holders[1]].1.clone(), spare_storage.clone());
    eventually("live replicas are invalidated", || a.get_block_count() == 0 && b.get_block_count() == 0).await;
}

/// Metadata calls that used to answer success and change nothing:
/// setReplication (now adds and deletes replicas), setPermission, setOwner,
/// setTimes, getContentSummary; and append, which used to leave an empty
/// block that kept complete() from ever succeeding.
#[tokio::test(flavor = "multi_thread")]
async fn metadata_calls_take_effect_and_append_starts_a_new_block() {
    let c = start_cluster_with(3, true).await;
    let fs = client(&c);
    let replicas = |c: &Cluster| -> Vec<usize> { c._dns.iter().map(|(_, s)| s.get_block_count()).collect() };

    // Replication 1 -> 3 -> 2.
    let data = pattern(MB + 5, 0x11);
    let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(1), ..Default::default() };
    let mut w = fs.create("/r.bin", opts).await.unwrap();
    w.write_bytes(Bytes::from(data.clone())).await.unwrap();
    w.close().await.unwrap();
    eventually("one replica of each block", || replicas(&c).iter().sum::<usize>() == 2).await;
    assert!(fs.set_replication("/r.bin", 3).await.unwrap());
    eventually("re-replicated to 3", || replicas(&c) == vec![2, 2, 2]).await;
    assert!(fs.set_replication("/r.bin", 2).await.unwrap());
    eventually("excess replicas deleted", || replicas(&c).iter().sum::<usize>() == 4).await;
    assert_eq!(fs.get_file_info("/r.bin").await.unwrap().replication, Some(2));
    assert_eq!(fs.read("/r.bin").await.unwrap().read_range(0, data.len()).await.unwrap()[..], data[..]);

    // Permission, owner, times.
    fs.set_permission("/r.bin", 0o600).await.unwrap();
    fs.set_owner("/r.bin", Some("alice"), Some("eng")).await.unwrap();
    fs.set_times("/r.bin", 1_000_000, 2_000_000).await.unwrap();
    let st = fs.get_file_info("/r.bin").await.unwrap();
    assert_eq!((st.permission, st.owner.as_str(), st.group.as_str()), (0o600, "alice", "eng"));
    assert_eq!((st.modification_time, st.access_time), (1_000_000, 2_000_000));

    // Content summary.
    fs.mkdirs("/sum/d", 0o755, true).await.unwrap();
    for (p, n) in [("/sum/a", 10usize), ("/sum/d/b", 32)] {
        let mut w = fs.create(p, WriteOptions { replication: Some(1), ..Default::default() }).await.unwrap();
        w.write_bytes(Bytes::from(vec![7u8; n])).await.unwrap();
        w.close().await.unwrap();
    }
    let s = fs.get_content_summary("/sum").await.unwrap();
    assert_eq!((s.length, s.file_count, s.directory_count), (42, 2, 2));

    // Append to a file whose last block is full starts a new block.
    let head = pattern(MB, 0x21);
    let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(3), ..Default::default() };
    let mut w = fs.create("/app.bin", opts).await.unwrap();
    w.write_bytes(Bytes::from(head.clone())).await.unwrap();
    w.close().await.unwrap();
    let tail = pattern(1000, 0x22);
    let mut w = fs.append("/app.bin").await.unwrap();
    w.write_bytes(Bytes::from(tail.clone())).await.unwrap();
    w.close().await.unwrap();
    let r = fs.read("/app.bin").await.unwrap();
    assert_eq!(r.file_length(), MB + 1000);
    let all = r.read_range(0, MB + 1000).await.unwrap();
    assert!(all[..MB] == head[..] && all[MB..] == tail[..], "appended file differs");
    // Its last block is now partial. Extending it is refused with a message
    // naming NEW_BLOCK, and hdfs-native then appends in a new block (as it
    // does for erasure-coded files): three blocks, contents intact.
    let more = pattern(500, 0x23);
    let mut w = fs.append("/app.bin").await.unwrap();
    w.write_bytes(Bytes::from(more.clone())).await.unwrap();
    w.close().await.unwrap();
    let r = fs.read("/app.bin").await.unwrap();
    assert_eq!(r.file_length(), MB + 1500);
    let all = r.read_range(0, MB + 1500).await.unwrap();
    assert!(all[..MB] == head[..] && all[MB..MB + 1000] == tail[..] && all[MB + 1000..] == more[..]);
    let blocks = fs.get_file_info("/app.bin").await.unwrap();
    assert_eq!(blocks.length, MB + 1500);
}
