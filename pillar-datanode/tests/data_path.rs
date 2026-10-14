//! End-to-end tests of the DataNode TCP data path: a block replicated to a
//! live `TcpTransport` server must be readable back, byte for byte.
//!
//! The `#[ignore]`d `throughput_*` tests are the loopback benchmark:
//!
//!     cargo test --release -p pillar-datanode --test data_path -- --ignored --nocapture

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pillar_datanode::replication::{
    BlockReplicator, OverlappedReplicator, ParallelReplicator, PipelineReplicator,
    ReplicationRequest, ReplicationTarget, TransportSelector,
};
use pillar_datanode::storage::{block_storage::BlockId, BlockStorage};
use pillar_datanode::transport::tcp::TcpTransport;
use pillar_datanode::transport::DataTransport;
use tempfile::TempDir;

const MB: usize = 1024 * 1024;

/// Deterministic, incompressible-looking test data (xorshift64).
fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    let mut v = Vec::with_capacity(len + 8);
    while v.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.extend_from_slice(&x.to_le_bytes());
    }
    v.truncate(len);
    v
}

fn storage() -> (TempDir, Arc<BlockStorage>) {
    let dir = TempDir::new().unwrap();
    let finalized = dir.path().join("finalized");
    let tmp = dir.path().join("tmp");
    std::fs::create_dir_all(&finalized).unwrap();
    std::fs::create_dir_all(&tmp).unwrap();
    let s = Arc::new(BlockStorage::new(&finalized, &tmp).unwrap());
    (dir, s)
}

/// A source DataNode's storage holding one finalized block.
fn source_with_block(block_id: u64, gen_stamp: u64, data: &[u8]) -> (TempDir, Arc<BlockStorage>) {
    let (dir, s) = storage();
    let b = BlockId::new(block_id, gen_stamp);
    s.create_block(b).unwrap();
    s.write_block(&b, 0, data).unwrap();
    s.finalize_block(&b).unwrap();
    (dir, s)
}

/// A target DataNode: empty storage plus a TCP data server on a free port.
struct Node {
    _dir: TempDir,
    storage: Arc<BlockStorage>,
    transport: Arc<TcpTransport>,
    addr: SocketAddr,
}

async fn start_node() -> Node {
    let (dir, storage) = storage();
    let addr = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let transport = Arc::new(TcpTransport::new(storage.clone(), addr));
    tokio::spawn(transport.clone().start_server());
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Node { _dir: dir, storage, transport, addr }
}

fn target(n: &Node, uuid: &str) -> ReplicationTarget {
    ReplicationTarget {
        datanode_uuid: uuid.into(),
        hostname: "localhost".into(),
        ip_addr: "127.0.0.1".into(),
        xfer_port: n.addr.port() as u32,
        rdma_port: 0,
        supported_transports: vec!["tcp".into()],
    }
}

fn selector(client: Arc<TcpTransport>) -> TransportSelector {
    Arc::new(move |t: &ReplicationTarget| {
        Some((client.clone() as Arc<dyn DataTransport>, format!("{}:{}", t.ip_addr, t.xfer_port)))
    })
}

async fn replicate(
    replicator: &dyn BlockReplicator,
    source: Arc<BlockStorage>,
    client: Arc<TcpTransport>,
    targets: &[&Node],
    block_id: u64,
    gen_stamp: u64,
    size: usize,
) {
    let request = ReplicationRequest {
        block_id,
        gen_stamp,
        targets: targets.iter().enumerate().map(|(i, n)| target(n, &format!("dn{i}"))).collect(),
        block_size: size as u64,
        priority: 1,
    };
    let result = replicator.replicate_block(source, request, selector(client)).await.unwrap();
    assert!(result.is_success(), "replication reported {:?}", result);
}

/// Read the whole block back from `node` over TCP and compare.
async fn assert_block_on(node: &Node, reader: &TcpTransport, block_id: u64, gen_stamp: u64, want: &[u8]) {
    let got = reader
        .read_from_peer(&node.addr.to_string(), block_id, gen_stamp, 0, want.len() as u64)
        .await
        .unwrap_or_else(|e| panic!("block {block_id} not readable on {}: {e}", node.addr));
    assert_eq!(got.len(), want.len(), "block {block_id}: length");
    if got[..] != want[..] {
        let first = got.iter().zip(want).position(|(a, b)| a != b).unwrap();
        panic!("block {block_id}: content differs from byte {first}");
    }
    let meta = node.storage.get_block_metadata(block_id).expect("finalized on target");
    assert_eq!(meta.num_bytes, want.len() as u64);
}

async fn round_trip(replicator: &dyn BlockReplicator, n_targets: usize) {
    // 5.5 MB: several chunks at every replicator's chunk size, plus a tail.
    let size = 5 * MB + MB / 2;
    let data = pattern(size, 7);
    let (_src_dir, source) = source_with_block(42, 1001, &data);
    let mut nodes = Vec::new();
    for _ in 0..n_targets {
        nodes.push(start_node().await);
    }
    let client = nodes[0].transport.clone();
    let refs: Vec<&Node> = nodes.iter().collect();
    replicate(replicator, source, client.clone(), &refs, 42, 1001, size).await;
    for n in &nodes {
        assert_block_on(n, &client, 42, 1001, &data).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_replication_delivers_a_multi_chunk_block_intact() {
    round_trip(&PipelineReplicator::with_chunk_size(MB), 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parallel_replication_delivers_a_multi_chunk_block_intact() {
    round_trip(&ParallelReplicator::with_chunk_size(MB), 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn overlapped_replication_delivers_a_multi_chunk_block_intact() {
    round_trip(&OverlappedReplicator::with_params(MB, 4), 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_past_the_end_returns_only_what_exists() {
    let data = pattern(3 * MB, 3);
    let (_d, source) = source_with_block(7, 1, &data);
    let node = start_node().await;
    replicate(&PipelineReplicator::with_chunk_size(MB), source, node.transport.clone(), &[&node], 7, 1, data.len()).await;

    let addr = node.addr.to_string();
    let tail = node.transport.read_from_peer(&addr, 7, 1, 2 * MB as u64, 10 * MB as u64).await.unwrap();
    assert_eq!(&tail[..], &data[2 * MB..]);
    // An absurd length must not make the server allocate it.
    let all = node.transport.read_from_peer(&addr, 7, 1, 0, u64::MAX).await.unwrap();
    assert_eq!(all.len(), data.len());
    assert!(node.transport.read_from_peer(&addr, 7, 1, 4 * MB as u64, 1).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn reading_a_missing_block_is_an_error_not_a_hang() {
    let node = start_node().await;
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        node.transport.read_from_peer(&node.addr.to_string(), 999, 1, 0, 10),
    )
    .await
    .expect("no reply within 5 s");
    assert!(r.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn many_small_reads_complete_quickly() {
    let data = pattern(MB, 11);
    let (_d, source) = source_with_block(8, 1, &data);
    let node = start_node().await;
    replicate(&PipelineReplicator::with_chunk_size(MB), source, node.transport.clone(), &[&node], 8, 1, data.len()).await;

    let addr = node.addr.to_string();
    let start = Instant::now();
    for i in 0..200u64 {
        let off = (i * 4096) % (MB as u64 - 4096);
        let got = node.transport.read_from_peer(&addr, 8, 1, off, 4096).await.unwrap();
        assert_eq!(&got[..], &data[off as usize..off as usize + 4096]);
    }
    let per_read = start.elapsed() / 200;
    eprintln!("4 KiB read latency (mean of 200): {per_read:?}");
    assert!(per_read < Duration::from_millis(20), "{per_read:?} per 4 KiB read");
}

// ---------------------------------------------------------------- benchmark

async fn throughput(name: &str, replicator: &dyn BlockReplicator, n_targets: usize) {
    let size = 128 * MB;
    let data = pattern(size, 99);
    let (_d, source) = source_with_block(1, 1, &data);
    let mut nodes = Vec::new();
    for _ in 0..n_targets {
        nodes.push(start_node().await);
    }
    let client = nodes[0].transport.clone();
    let refs: Vec<&Node> = nodes.iter().collect();

    let t = Instant::now();
    replicate(replicator, source, client.clone(), &refs, 1, 1, size).await;
    let write = t.elapsed();

    let t = Instant::now();
    let got = client.read_from_peer(&nodes[0].addr.to_string(), 1, 1, 0, size as u64).await;
    let read = t.elapsed();
    let intact = matches!(&got, Ok(b) if b[..] == data[..]);

    let mbps = |d: Duration, bytes: usize| bytes as f64 / MB as f64 / d.as_secs_f64();
    eprintln!(
        "{name:<11} 128 MiB -> {n_targets} target(s): write {:>7.1} MB/s ({:?}), read {:>7.1} MB/s, read-back {}",
        mbps(write, size * n_targets),
        write,
        mbps(read, size),
        if intact { "intact" } else { "FAILED" },
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn throughput_pipeline() {
    throughput("pipeline", &PipelineReplicator::new(), 1).await;
    throughput("pipeline", &PipelineReplicator::new(), 2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn throughput_parallel() {
    throughput("parallel", &ParallelReplicator::new(), 2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn throughput_overlapped() {
    throughput("overlapped", &OverlappedReplicator::new(), 2).await;
}

// ------------------------------------------------------------ failure paths

#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_transfer_leaves_no_block_behind() {
    let node = start_node().await;
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(Ok(bytes::Bytes::from(pattern(MB, 5)))).await.unwrap();
    tx.send(Err(std::io::Error::other("source disk failed"))).await.unwrap();
    drop(tx);

    let r = node.transport.send_block_to_peer(&node.addr.to_string(), 77, 1, rx).await;
    assert!(r.is_err(), "an aborted session must not report success");

    // The receiver notices the dropped connection asynchronously, then
    // joins its disk writer and unlinks the file: on a loaded CI runner
    // that has taken over a second, so allow ten.
    for _ in 0..1000 {
        if std::fs::read_dir(node._dir.path().join("tmp")).unwrap().next().is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(node.storage.get_block_metadata(77).is_none(), "partial block was finalized");
    assert_eq!(std::fs::read_dir(node._dir.path().join("tmp")).unwrap().count(), 0, "partial files left in tmp");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_transfer_cannot_clobber_a_finalized_replica() {
    let node = start_node().await;
    let addr = node.addr.to_string();
    let good = pattern(2 * MB, 21);
    node.transport.write_to_peer(&addr, 5, 1, good.clone().into()).await.unwrap();

    let r = node.transport.write_to_peer(&addr, 5, 1, pattern(MB, 22).into()).await;
    assert!(r.is_err(), "second write of a finalized block must be refused");
    assert_block_on(&node, &node.transport, 5, 1, &good).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_connection_serves_many_requests() {
    let node = start_node().await;
    let addr = node.addr.to_string();
    let data = pattern(MB, 31);
    node.transport.write_to_peer(&addr, 9, 1, data.clone().into()).await.unwrap();

    // A missing block is answered in-band and the connection stays usable.
    let mut s = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    use pillar_datanode::transport::wire;
    for i in 0..5u64 {
        let got = wire::read_block(&mut s, 9, 1, i * 1000, 1000).await.unwrap();
        assert_eq!(&got[..], &data[(i * 1000) as usize..(i * 1000 + 1000) as usize]);
        assert!(matches!(
            wire::read_block(&mut s, 12345, 1, 0, 10).await,
            Err(pillar_datanode::transport::TransportError::BlockNotFound(12345))
        ));
    }
}
