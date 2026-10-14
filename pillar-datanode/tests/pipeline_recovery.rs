//! Write pipeline recovery: a DataNode dies while a client is writing a
//! block through it. The client (hdfs-native, which follows HDFS's
//! DataStreamer) gets a new generation stamp from the NameNode, rebuilds
//! the pipeline from the surviving DataNodes, which reopen their replicas
//! being written and cut them back to the acknowledged length, replays the
//! unacknowledged packets and finishes the file.
//!
//! Each DataNode runs on its own tokio runtime, so killing one drops its
//! listener and every connection it is serving at once, as a crashed
//! process would.

use std::net::SocketAddr;
use std::path::PathBuf;
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

struct Cluster {
    nn_addr: SocketAddr,
    _nn_rt: tokio::runtime::Runtime,
    _name_dir: TempDir,
    namespace: Arc<Namespace>,
    blocks: Arc<BlockManager>,
    datanodes: Arc<DatanodeManager>,
    dns: Vec<DataNodeProc>,
}

struct DataNodeProc {
    rt: Option<tokio::runtime::Runtime>,
    storage: Arc<BlockStorage>,
    _dir: TempDir,
}

impl DataNodeProc {
    async fn kill(&mut self) {
        let rt = self.rt.take().expect("running");
        tokio::task::spawn_blocking(move || rt.shutdown_timeout(Duration::from_secs(5))).await.unwrap();
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
}

/// DataNode `i` (uuid `dn<i>`) on `dir`, which may hold the replicas of an
/// earlier run, with a fresh data port.
fn spawn_datanode(i: usize, dir: TempDir, nn_addr: SocketAddr) -> DataNodeProc {
    let volumes = Arc::new(VolumeManager::new(dir.path(), "BP-test".to_string()).unwrap());
    let storage = Arc::new(BlockStorage::new(volumes.get_finalized_dir(), volumes.get_tmp_dir()).unwrap());
    let rt = runtime();
    let data_addr = free_addr();
    let info = DatanodeInfo {
        uuid: format!("dn{i}"),
        hostname: "127.0.0.1".into(),
        rpc_port: 0,
        data_port: data_addr.port(),
        http_port: 0,
        supported_transports: vec!["tcp".into()],
        rdma_port: 0,
    };
    let (storage2, nn) = (storage.clone(), nn_addr.to_string());
    rt.spawn(async move {
        let tcp = Arc::new(TcpTransport::new(storage2.clone(), data_addr));
        tokio::spawn(tcp.clone().start_server());
        let mut client = NamenodeClient::new(nn, info, volumes, storage2.clone());
        client.set_replication_coordinator(Arc::new(ReplicationCoordinator::new(
            storage2,
            vec![tcp as Arc<dyn DataTransport>],
        )));
        client.set_heartbeat_interval(Duration::from_millis(100));
        let client = Arc::new(client);
        while client.register().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        client.start_heartbeat_loop().await;
    });
    DataNodeProc { rt: Some(rt), storage, _dir: dir }
}

async fn start_cluster(n: usize) -> Cluster {
    let name_dir = TempDir::new().unwrap();
    let nn_addr = free_addr();
    let nn_rt = runtime();
    let config = NameNodeConfig {
        rpc_addr: nn_addr,
        http_addr: free_addr(),
        etcd_endpoints: vec!["none".into()],
        // hdfs-native acts as the local OS user, who owns nothing here.
        permissions: pillar_namenode::permission::Permissions::disabled(),
        name_dir: Some(PathBuf::from(name_dir.path())),
        // Notice a dead DataNode within a second, so new blocks avoid it.
        datanode_stale_interval: Duration::from_millis(500),
        datanode_dead_interval: Duration::from_secs(1),
        ..Default::default()
    };
    let nn = Arc::new(nn_rt.spawn(NameNode::new(config)).await.unwrap().unwrap());
    let (namespace, blocks, datanodes) = (nn.namespace(), nn.block_manager(), nn.datanode_manager());
    nn_rt.spawn(async move {
        nn.start().await.unwrap();
    });

    let mut dns = Vec::new();
    for i in 0..n {
        dns.push(spawn_datanode(i, TempDir::new().unwrap(), nn_addr));
    }
    for _ in 0..200 {
        if datanodes.get_live_datanodes().len() == n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(datanodes.get_live_datanodes().len(), n);
    Cluster { nn_addr, _nn_rt: nn_rt, _name_dir: name_dir, namespace, blocks, datanodes, dns }
}

impl Cluster {
    /// Stop every runtime off the async context (dropping a runtime inside
    /// one panics).
    async fn shutdown(self) {
        tokio::task::spawn_blocking(move || {
            let Cluster { _nn_rt, dns, .. } = self;
            for dn in dns {
                if let Some(rt) = dn.rt {
                    rt.shutdown_timeout(Duration::from_secs(5));
                }
            }
            _nn_rt.shutdown_timeout(Duration::from_secs(5));
        })
        .await
        .unwrap();
    }
}

fn client(c: &Cluster) -> Client {
    ClientBuilder::new().with_url(format!("hdfs://{}", c.nn_addr)).build().unwrap()
}

/// A client that never replaces a failed DataNode: the pipeline just
/// continues on the survivors (Java's default for a write that was not
/// hflushed).
fn client_without_replacement(c: &Cluster) -> Client {
    ClientBuilder::new()
        .with_url(format!("hdfs://{}", c.nn_addr))
        .with_config([("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")])
        .build()
        .unwrap()
}

async fn read(fs: &Client, path: &str) -> Vec<u8> {
    let r = fs.read(path).await.unwrap();
    r.read_range(0, r.file_length()).await.unwrap().to_vec()
}

/// Write 3.5 blocks with replication 3 on `datanodes` DataNodes, killing
/// the `nth` DataNode of the second block's pipeline in the middle of it;
/// the write must complete and read back. With a spare DataNode the client
/// replaces the dead one (the default policy, since the block holds data):
/// a survivor copies its replica to the newcomer before the pipeline is
/// rebuilt. Without one, `replace` is false and it carries on with two.
async fn write_through_a_datanode_failure(datanodes: usize, nth: usize, replace: bool) {
    let mut c = start_cluster(datanodes).await;
    let fs = if replace { client(&c) } else { client_without_replacement(&c) };
    let data = pattern(7 * MB / 2, (datanodes * 10 + nth) as u8);
    let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(3), ..Default::default() };
    let mut w = fs.create("/f.bin", opts).await.unwrap();
    // One and a half blocks, then a DataNode of the open block's pipeline
    // (the ones holding a replica being written) dies.
    w.write_bytes(Bytes::copy_from_slice(&data[..3 * MB / 2])).await.unwrap();
    let mut pipeline = Vec::new();
    for _ in 0..100 {
        let open = c.namespace.lookup("/f.bin").await.unwrap().read().await.blocks.clone().unwrap();
        if let Some(b) = open.get(1) {
            pipeline = (0..datanodes).filter(|i| c.dns[*i].storage.find_rbw(b.block_id).is_some()).collect();
            if pipeline.len() == 3 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(pipeline.len(), 3, "the second block's pipeline");
    let victim = pipeline[nth];
    c.dns[victim].kill().await;
    // Until the NameNode notices, it may place the next block on the dead
    // DataNode; the Java client retries with it excluded, hdfs-native does
    // not, so let the NameNode notice before the next block.
    for _ in 0..100 {
        if c.datanodes.get_live_datanodes().len() == datanodes - 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    w.write_bytes(Bytes::copy_from_slice(&data[3 * MB / 2..])).await.unwrap();
    w.close().await.expect("the write survives a DataNode failure");

    assert_eq!(read(&fs, "/f.bin").await, data);
    let inode = c.namespace.lookup("/f.bin").await.unwrap();
    let blocks = inode.read().await.blocks.clone().unwrap();
    assert_eq!(blocks.len(), 4);
    assert!(inode.read().await.under_construction.is_none());
    // Every finalized replica on a live DataNode has the block's current
    // stamp and length; the interrupted block has 3 of them with a
    // replacement, 2 without.
    let interrupted = &blocks[1];
    let mut holders = 0;
    for (i, dn) in c.dns.iter().enumerate().filter(|(i, _)| *i != victim) {
        for b in &blocks {
            if let Some(replica) = dn.storage.get_block_metadata(b.block_id) {
                assert_eq!(replica.block_id.generation_stamp, b.generation_stamp, "dn{i} block {} stamp", b.block_id);
                assert_eq!(replica.num_bytes, b.num_bytes, "dn{i} block {} length", b.block_id);
                holders += (b.block_id == interrupted.block_id) as usize;
            }
        }
    }
    assert_eq!(holders, if replace { 3 } else { 2 }, "replicas of the interrupted block");
    // The interrupted block was given a new generation stamp: the dead
    // DataNode's replica (old stamp) would be deleted if it came back.
    let stamps: Vec<u64> = blocks.iter().map(|b| b.generation_stamp).collect();
    assert!(stamps.windows(2).any(|w| w[1] > w[0] + 1), "no block was recovered: {stamps:?}");
    let _ = &c.blocks;
    drop(fs);
    c.shutdown().await;
}

// The pipeline's DataNode order is the placement's, so the three positions
// are covered by picking the 1st/2nd/3rd holder in DataNode order across runs.

#[tokio::test(flavor = "multi_thread")]
async fn losing_a_datanode_mid_block_with_a_replacement() {
    for nth in 0..3 {
        write_through_a_datanode_failure(4, nth, true).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn losing_a_datanode_mid_block_without_a_replacement() {
    for nth in 0..3 {
        write_through_a_datanode_failure(3, nth, false).await;
    }
}

/// The DataNode killed mid-write comes back with its replicas (as a
/// restarted pod on its volume does): its stale replica of the interrupted
/// block must not reach readers, and the block is re-replicated to it with
/// the right content. Every replica of every block, on every DataNode,
/// must hold exactly the file's bytes.
#[tokio::test(flavor = "multi_thread")]
async fn a_datanode_killed_mid_write_comes_back() {
    for nth in 0..3 {
        let mut c = start_cluster(3).await;
        let fs = client_without_replacement(&c);
        let data = pattern(7 * MB / 2, 40 + nth as u8);
        let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(3), ..Default::default() };
        let mut w = fs.create("/f.bin", opts).await.unwrap();
        w.write_bytes(Bytes::copy_from_slice(&data[..3 * MB / 2])).await.unwrap();
        let mut pipeline = Vec::new();
        for _ in 0..100 {
            let open = c.namespace.lookup("/f.bin").await.unwrap().read().await.blocks.clone().unwrap();
            if let Some(b) = open.get(1) {
                pipeline = (0..3).filter(|i| c.dns[*i].storage.find_rbw(b.block_id).is_some()).collect();
                if pipeline.len() == 3 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(pipeline.len(), 3);
        let victim = pipeline[nth];
        c.dns[victim].kill().await;
        for _ in 0..100 {
            if c.datanodes.get_live_datanodes().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        w.write_bytes(Bytes::copy_from_slice(&data[3 * MB / 2..])).await.unwrap();
        w.close().await.unwrap();

        // Back, on its old volume.
        let placeholder = DataNodeProc { rt: None, storage: c.dns[victim].storage.clone(), _dir: TempDir::new().unwrap() };
        let old = std::mem::replace(&mut c.dns[victim], placeholder);
        c.dns[victim] = spawn_datanode(victim, old._dir, c.nn_addr);
        let blocks = c.namespace.lookup("/f.bin").await.unwrap().read().await.blocks.clone().unwrap();
        let mut replicated = false;
        for _ in 0..600 {
            if blocks.iter().all(|b| c.dns.iter().all(|d| d.storage.get_block_metadata(b.block_id).map_or(false, |m| m.block_id.generation_stamp == b.generation_stamp))) {
                replicated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(replicated, "every block on all three DataNodes again");
        let mut offset = 0usize;
        for b in &blocks {
            let want = &data[offset..offset + b.num_bytes as usize];
            for (i, dn) in c.dns.iter().enumerate() {
                let got = dn.storage.read_block(&pillar_datanode::storage::block_storage::BlockId::new(b.block_id, b.generation_stamp), 0, b.num_bytes).unwrap();
                assert!(&got[..] == want, "dn{i} (victim dn{victim}) block {} at offset {} differs", b.block_id, offset);
            }
            offset += b.num_bytes as usize;
        }
        for _ in 0..5 {
            assert_eq!(read(&fs, "/f.bin").await, data, "a read of the file (victim dn{victim})");
        }
        drop(fs);
        c.shutdown().await;
    }
}
