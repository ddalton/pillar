//! Data transfer encryption between DataNodes (`--encrypt-data-transfer`,
//! `--data-transfer-protection`): replica copies and reads over Pillar's
//! data protocol go through the SASL handshake, AES/CTR for privacy, with
//! tokens the DataNodes mint from the NameNode's block keys; a connection
//! that skips the handshake is refused. The Hadoop client's side is covered
//! by `tests/interop/run-java-interop.sh` (ENCRYPT=aes|rc4) and
//! `run-java-kerberos.sh` (DATA_PROTECTION=...).
//!
//! `throughput` (ignored by default) measures what encryption costs:
//! `cargo test --release -p pillar-datanode --test data_encryption -- --ignored --nocapture`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use pillar_common::hdfs::block_token::BlockTokenSecretManager;
use pillar_datanode::storage::BlockStorage;
use pillar_datanode::transport::sasl::DataSecurity;
use pillar_datanode::transport::tcp::TcpTransport;
use pillar_datanode::transport::DataTransport;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MB: usize = 1024 * 1024;

struct Node {
    _dir: TempDir,
    storage: Arc<BlockStorage>,
    transport: Arc<TcpTransport>,
    addr: SocketAddr,
}

/// A DataNode with the NameNode's block keys (`keys`) and `security`.
async fn node(keys: &BlockTokenSecretManager, security: DataSecurity) -> Node {
    let dir = TempDir::new().unwrap();
    let (finalized, tmp) = (dir.path().join("finalized"), dir.path().join("tmp"));
    std::fs::create_dir_all(&finalized).unwrap();
    std::fs::create_dir_all(&tmp).unwrap();
    let storage = Arc::new(BlockStorage::new(&finalized, &tmp).unwrap());
    storage.tokens.update(&keys.export());
    storage.data_security.set(security).unwrap();
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

fn keys() -> BlockTokenSecretManager {
    BlockTokenSecretManager::new("BP-test", Duration::from_secs(3600), Duration::from_secs(3600))
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i as u64 * 2654435761) >> 7) as u8 ^ seed).collect()
}

fn encrypted() -> DataSecurity {
    DataSecurity { encrypt: true, protection: None }
}

#[tokio::test(flavor = "multi_thread")]
async fn datanodes_copy_and_read_replicas_over_encrypted_connections() {
    let k = keys();
    for security in [encrypted(), DataSecurity { encrypt: false, protection: Some("integrity".parse().unwrap()) }] {
        let (a, b) = (node(&k, security.clone()).await, node(&k, security.clone()).await);
        let data = pattern(3 * MB + 77, 5);
        // a copies a replica to b, then reads it back, both through the
        // handshake (and a pooled connection for the read).
        let stored = a.transport.write_to_peer(&b.addr.to_string(), 42, 1001, Bytes::from(data.clone())).await.unwrap();
        assert_eq!(stored, data.len() as u64);
        assert!(b.storage.get_block_metadata(42).is_some(), "{security:?}: b has the replica");
        let back = a.transport.read_from_peer(&b.addr.to_string(), 42, 1001, 0, data.len() as u64).await.unwrap();
        assert!(back[..] == data[..], "{security:?}: the replica reads back intact");
        let part = a.transport.read_from_peer(&b.addr.to_string(), 42, 1001, MB as u64, 1000).await.unwrap();
        assert_eq!(&part[..], &data[MB..MB + 1000]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_without_the_handshake_is_refused() {
    let k = keys();
    let b = node(&k, encrypted()).await;
    // Hadoop's DataTransferProtocol, unencrypted: READ_BLOCK.
    let mut s = tokio::net::TcpStream::connect(b.addr).await.unwrap();
    s.write_all(&[0, 28, 81]).await.unwrap();
    let mut buf = [0u8; 16];
    assert_eq!(s.read(&mut buf).await.unwrap_or(0), 0, "closed without an answer");
    // A DataNode that does not encrypt cannot copy a replica to it.
    let plain = node(&k, DataSecurity::default()).await;
    let r = plain.transport.write_to_peer(&b.addr.to_string(), 7, 1, Bytes::from(vec![1u8; 1000])).await;
    assert!(r.is_err(), "an unencrypted copy is refused");
    assert!(b.storage.get_block_metadata(7).is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn another_clusters_datanode_cannot_connect() {
    let (ours, theirs) = (keys(), keys());
    let b = node(&ours, encrypted()).await;
    let intruder = node(&theirs, encrypted()).await;
    let r = intruder.transport.write_to_peer(&b.addr.to_string(), 9, 1, Bytes::from(vec![2u8; 1000])).await;
    assert!(r.is_err(), "keys of another cluster are refused");
    assert!(b.storage.get_block_metadata(9).is_none());
}

/// What encryption costs on one connection: copy and read 256 MiB.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn throughput() {
    let k = keys();
    let data = Bytes::from(pattern(256 * MB, 3));
    let cases = [
        ("plain", DataSecurity::default()),
        ("AES/CTR", encrypted()),
        ("SASL integrity", DataSecurity { encrypt: false, protection: Some("integrity".parse().unwrap()) }),
    ];
    for (name, security) in cases {
        let (a, b) = (node(&k, security.clone()).await, node(&k, security).await);
        let t = Instant::now();
        a.transport.write_to_peer(&b.addr.to_string(), 1, 1, data.clone()).await.unwrap();
        let write = t.elapsed();
        let t = Instant::now();
        let back = a.transport.read_from_peer(&b.addr.to_string(), 1, 1, 0, data.len() as u64).await.unwrap();
        let read = t.elapsed();
        assert_eq!(back.len(), data.len());
        let mbps = |d: Duration| 256.0 / d.as_secs_f64();
        println!("{name:>15}: write {:7.0} MiB/s, read {:7.0} MiB/s", mbps(write), mbps(read));
    }
}
