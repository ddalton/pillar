//! A NameNode crash loses nothing that was acknowledged: the namespace is
//! recovered from its name directory (fsimage + edit log), DataNodes are
//! told to re-register and report their replicas, and an independent HDFS
//! client (`hdfs-native`) reads every file back.
//!
//! The NameNode runs on its own tokio runtime; shutting that runtime down
//! drops all of its tasks, listener and name-directory lock at once, as a
//! killed process would, and edits not yet synced are lost with it.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use pillar_datanode::grpc::namenode_client::{DatanodeInfo, NamenodeClient};
use pillar_datanode::replication::ReplicationCoordinator;
use pillar_datanode::storage::{BlockStorage, VolumeManager};
use pillar_datanode::transport::tcp::TcpTransport;
use pillar_datanode::transport::DataTransport;
use pillar_namenode::datanode::DatanodeManager;
use pillar_namenode::metadata::{BlockManager, Namespace};
use pillar_namenode::{NameNode, NameNodeConfig};
use tempfile::TempDir;

const MB: usize = 1024 * 1024;

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i as u64 * 2654435761) >> 7) as u8 ^ seed).collect()
}

/// A NameNode on its own runtime, plus handles to inspect it.
struct RunningNameNode {
    rt: tokio::runtime::Runtime,
    namespace: Arc<Namespace>,
    blocks: Arc<BlockManager>,
    datanodes: Arc<DatanodeManager>,
}

async fn start_namenode(addr: SocketAddr, name_dir: &Path) -> RunningNameNode {
    start_namenode_with(addr, name_dir, NameNodeConfig::default()).await
}

/// `base` supplies everything but the addresses, etcd and name directory.
async fn start_namenode_with(addr: SocketAddr, name_dir: &Path, base: NameNodeConfig) -> RunningNameNode {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let config = NameNodeConfig {
        rpc_addr: addr,
        http_addr: free_addr(),
        etcd_endpoints: vec!["none".into()],
        name_dir: Some(PathBuf::from(name_dir)),
        ..base
    };
    let nn = Arc::new(rt.spawn(NameNode::new(config)).await.unwrap().unwrap());
    let (namespace, blocks, datanodes) = (nn.namespace(), nn.block_manager(), nn.datanode_manager());
    // Only the runtime holds the NameNode, so shutting it down frees all.
    rt.spawn(async move {
        nn.start().await.unwrap();
    });
    RunningNameNode { rt, namespace, blocks, datanodes }
}

impl RunningNameNode {
    async fn crash(self) {
        let rt = self.rt;
        tokio::task::spawn_blocking(move || rt.shutdown_timeout(Duration::from_secs(5))).await.unwrap();
    }
}

/// Three DataNodes heartbeating every 100 ms. They outlive NameNode restarts.
async fn start_datanodes(nn_addr: SocketAddr, n: usize) -> Vec<(TempDir, Arc<BlockStorage>)> {
    let mut dns = Vec::new();
    for i in 0..n {
        let dir = TempDir::new().unwrap();
        let volumes = Arc::new(VolumeManager::new(dir.path(), "BP-test".to_string()).unwrap());
        let storage = Arc::new(BlockStorage::new(volumes.get_finalized_dir(), volumes.get_tmp_dir()).unwrap());
        let data_addr = free_addr();
        let tcp = Arc::new(TcpTransport::new(storage.clone(), data_addr));
        tokio::spawn({
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
        tokio::spawn(client.start_heartbeat_loop());
        dns.push((dir, storage));
    }
    dns
}

fn client(addr: SocketAddr) -> Client {
    ClientBuilder::new().with_url(format!("hdfs://{addr}")).build().unwrap()
}

async fn write(fs: &Client, path: &str, data: &[u8]) {
    let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(3), ..Default::default() };
    let mut w = fs.create(path, opts).await.unwrap();
    w.write_bytes(Bytes::copy_from_slice(data)).await.unwrap();
    w.close().await.unwrap();
}

async fn read(fs: &Client, path: &str) -> Vec<u8> {
    let r = fs.read(path).await.unwrap();
    r.read_range(0, r.file_length()).await.unwrap().to_vec()
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..400 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {what}");
}

#[tokio::test(flavor = "multi_thread")]
async fn files_survive_a_namenode_crash_and_restart() {
    let name_dir = TempDir::new().unwrap();
    let nn_addr = free_addr();
    let nn = start_namenode(nn_addr, name_dir.path()).await;
    let _dns = start_datanodes(nn_addr, 3).await;
    eventually("three live DataNodes", || nn.datanodes.get_live_datanodes().len() == 3).await;

    let big = pattern(5 * MB / 2, 0x5A);
    let small = b"written before the crash".to_vec();
    let fs = client(nn_addr);
    fs.mkdirs("/data/nested", 0o755, true).await.unwrap();
    write(&fs, "/data/big.bin", &big).await;
    write(&fs, "/data/tmp.txt", &small).await;
    fs.rename("/data/tmp.txt", "/data/nested/small.txt", false).await.unwrap();
    write(&fs, "/data/doomed", b"deleted before the crash").await;
    fs.delete("/data/doomed", false).await.unwrap();
    let blocks_before = nn.blocks.get_total_blocks();
    assert_eq!(blocks_before, 4);
    drop(fs);

    nn.crash().await;
    let nn = start_namenode(nn_addr, name_dir.path()).await;

    // The namespace is back before any DataNode reports...
    assert_eq!(nn.blocks.get_total_blocks(), blocks_before);
    // ...and the DataNodes re-register and report every replica.
    eventually("DataNodes re-register", || nn.datanodes.get_live_datanodes().len() == 3).await;
    eventually("all replicas reported", || {
        nn.blocks.blocks_snapshot().iter().all(|(_, _, _, holders)| holders.len() == 3)
    })
    .await;

    let fs = client(nn_addr);
    assert_eq!(read(&fs, "/data/big.bin").await, big);
    assert_eq!(read(&fs, "/data/nested/small.txt").await, small);
    assert!(fs.get_file_info("/data/tmp.txt").await.is_err());
    assert!(fs.get_file_info("/data/doomed").await.is_err());
    let names: Vec<String> = fs.list_status("/data", false).await.unwrap().into_iter().map(|s| s.path).collect();
    assert_eq!(names.len(), 2, "{names:?}");

    // New writes get fresh block ids and work as before.
    let after = pattern(MB + 17, 0x77);
    write(&fs, "/data/after.bin", &after).await;
    assert_eq!(read(&fs, "/data/after.bin").await, after);
    assert_eq!(nn.blocks.get_total_blocks(), blocks_before + 2);

    // A second crash, right after those writes: still all there.
    drop(fs);
    nn.crash().await;
    let nn = start_namenode(nn_addr, name_dir.path()).await;
    eventually("DataNodes re-register again", || nn.datanodes.get_live_datanodes().len() == 3).await;
    let fs = client(nn_addr);
    assert_eq!(read(&fs, "/data/after.bin").await, after);
    assert_eq!(read(&fs, "/data/big.bin").await, big);
    nn.crash().await;
}

/// A NameNode on a fresh (or wrong) name directory has a different
/// identity: the DataNodes refuse to join it, so it never sees their
/// replicas and cannot have them deleted as unknown. Back on the right
/// directory they rejoin, and a replica the namespace does not know about
/// (here planted while the NameNode was down) is deleted.
#[tokio::test(flavor = "multi_thread")]
async fn datanodes_refuse_a_namenode_with_another_identity() {
    let name_dir = TempDir::new().unwrap();
    let nn_addr = free_addr();
    let nn = start_namenode(nn_addr, name_dir.path()).await;
    let dns = start_datanodes(nn_addr, 3).await;
    eventually("three live DataNodes", || nn.datanodes.get_live_datanodes().len() == 3).await;
    let data = pattern(3 * MB / 2, 0x42);
    write(&client(nn_addr), "/precious.bin", &data).await;
    let per_dn: Vec<usize> = dns.iter().map(|(_, s)| s.get_block_count()).collect();
    assert_eq!(per_dn, vec![2, 2, 2]);
    nn.crash().await;

    // The wrong directory: formatted afresh, new identity.
    let other_dir = TempDir::new().unwrap();
    let nn = start_namenode(nn_addr, other_dir.path()).await;
    tokio::time::sleep(Duration::from_millis(1500)).await; // ~15 heartbeats
    assert!(nn.datanodes.get_live_datanodes().is_empty(), "a DataNode joined a foreign namespace");
    assert_eq!(nn.blocks.blocks_snapshot().len(), 0);
    let still: Vec<usize> = dns.iter().map(|(_, s)| s.get_block_count()).collect();
    assert_eq!(still, per_dn, "replicas changed while refusing the foreign NameNode");
    nn.crash().await;

    // An orphan: a replica of a block the namespace never had.
    {
        use pillar_datanode::storage::block_storage::BlockId as DnBlockId;
        use std::io::Write;
        let orphan = DnBlockId::new(999_999, 1);
        dns[0].1.begin_write(&orphan).unwrap().write_all(b"orphan").unwrap();
        dns[0].1.finalize_block(&orphan).unwrap();
    }

    let nn = start_namenode(nn_addr, name_dir.path()).await;
    eventually("DataNodes rejoin their own NameNode", || nn.datanodes.get_live_datanodes().len() == 3).await;
    eventually("the orphan replica is deleted", || dns[0].1.get_block_metadata(999_999).is_none()).await;
    assert_eq!(read(&client(nn_addr), "/precious.bin").await, data);
    let after: Vec<usize> = dns.iter().map(|(_, s)| s.get_block_count()).collect();
    assert_eq!(after, per_dn);
    nn.crash().await;
}

/// A writer that dies without closing leaves its file open. The open state
/// survives a NameNode crash (the holder's lease is restored), and once the
/// hard limit passes without renewals the NameNode recovers the file: block
/// recovery settles the half-written last block at what its DataNodes all
/// hold, and the file closes.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_writers_file_is_recovered_even_across_a_restart() {
    let leases = || NameNodeConfig {
        lease_soft_limit: Duration::from_millis(500),
        lease_hard_limit: Duration::from_secs(2),
        ..Default::default()
    };
    let name_dir = TempDir::new().unwrap();
    let nn_addr = free_addr();
    let nn = start_namenode_with(nn_addr, name_dir.path(), leases()).await;
    let _dns = start_datanodes(nn_addr, 3).await;
    eventually("three live DataNodes", || nn.datanodes.get_live_datanodes().len() == 3).await;

    let is_open = |ns: Arc<Namespace>| async move {
        ns.lookup("/open.bin").await.unwrap().read().await.under_construction.clone()
    };
    let data = pattern(5 * MB / 2, 0x31);
    {
        let writer = client(nn_addr);
        let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(3), ..Default::default() };
        let mut w = writer.create("/open.bin", opts).await.unwrap();
        w.write_bytes(Bytes::from(data.clone())).await.unwrap();
        // The first two blocks are full: the DataNodes finish and report them.
        eventually("two finished blocks", || {
            nn.blocks.blocks_snapshot().iter().filter(|(_, _, _, h)| h.len() == 3).count() >= 2
        })
        .await;
        // The writer dies: no close(), no more renewals.
        drop(w);
        drop(writer);
    }
    let holder = is_open(nn.namespace.clone()).await.expect("open while its writer is alive").holder;

    nn.crash().await;
    let nn = start_namenode_with(nn_addr, name_dir.path(), leases()).await;
    let reopened = is_open(nn.namespace.clone()).await.expect("still open after the restart");
    assert_eq!(reopened.holder, holder);

    // The lease monitor recovers it once the restored lease passes the hard limit.
    let mut closed = false;
    for _ in 0..100 {
        if is_open(nn.namespace.clone()).await.is_none() {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(closed, "the file was never recovered");
    eventually("DataNodes re-register", || nn.datanodes.get_live_datanodes().len() == 3).await;
    let fs = client(nn_addr);
    let back = read(&fs, "/open.bin").await;
    // Block recovery: the DataNodes' block reports (replicas being written)
    // gave the restarted NameNode the last block's pipeline; the primary
    // agreed with the others on what all of them hold, so the file keeps
    // the two finished blocks and whatever of the third reached every
    // DataNode, every byte the writer's.
    assert!(back.len() > 2 * MB && back.len() <= data.len(), "recovered {} bytes", back.len());
    assert!(back[..] == data[..back.len()], "recovered bytes differ from what was written");
    let inode = nn.namespace.lookup("/open.bin").await.unwrap();
    let blocks = inode.read().await.blocks.clone().unwrap();
    assert_eq!(blocks.len(), 3);
    assert!(blocks[2].generation_stamp > blocks[1].generation_stamp + 1, "the last block got a recovery stamp");
    // Closed: anyone may append now.
    let mut w = fs.append("/open.bin").await.unwrap();
    w.write_bytes(Bytes::from_static(b"after recovery")).await.unwrap();
    w.close().await.unwrap();
    assert_eq!(read(&fs, "/open.bin").await.len(), back.len() + 14);
    nn.crash().await;
}

/// Block recovery without a restart: the NameNode knows the last block's
/// pipeline from addBlock, and when the dead writer's lease passes the hard
/// limit it has one of those DataNodes settle the block instead of dropping
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_writers_last_block_is_recovered_by_its_datanodes() {
    let config = NameNodeConfig {
        lease_soft_limit: Duration::from_millis(300),
        lease_hard_limit: Duration::from_secs(2),
        ..Default::default()
    };
    let name_dir = TempDir::new().unwrap();
    let nn_addr = free_addr();
    let nn = start_namenode_with(nn_addr, name_dir.path(), config).await;
    let _dns = start_datanodes(nn_addr, 3).await;
    eventually("three live DataNodes", || nn.datanodes.get_live_datanodes().len() == 3).await;

    let data = pattern(MB + MB / 2, 0x41);
    {
        let writer = client(nn_addr);
        let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(3), ..Default::default() };
        let mut w = writer.create("/half.bin", opts).await.unwrap();
        w.write_bytes(Bytes::from(data.clone())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(w);
        drop(writer);
    }
    let open = || async {
        nn.namespace.lookup("/half.bin").await.unwrap().read().await.under_construction.is_some()
    };
    assert!(open().await);
    let mut closed = false;
    for _ in 0..100 {
        if !open().await {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(closed, "the file was never recovered");
    let fs = client(nn_addr);
    let back = read(&fs, "/half.bin").await;
    assert!(back.len() > MB, "the half-written block is kept, not dropped ({} bytes)", back.len());
    assert!(back[..] == data[..back.len()]);
    let inode = nn.namespace.lookup("/half.bin").await.unwrap();
    let blocks = inode.read().await.blocks.clone().unwrap();
    assert!(blocks[1].generation_stamp > blocks[0].generation_stamp + 1, "a recovery stamp: {blocks:?}");
    nn.crash().await;
}
