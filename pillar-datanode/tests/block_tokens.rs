//! Block access tokens: with them on, the NameNode hands DataNodes its
//! block keys and every block it returns to a client carries a token; the
//! DataNodes refuse any data transfer without a valid token for that block
//! and the access it needs, on Hadoop's DataTransferProtocol and on
//! Pillar's own data protocol alike, and mint their own for the copies
//! they make.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hdfs_native::{Client, ClientBuilder, WriteOptions};
use pillar_common::hdfs::block_token::mode;
use pillar_datanode::grpc::namenode_client::{DatanodeInfo, NamenodeClient};
use pillar_datanode::replication::ReplicationCoordinator;
use pillar_datanode::storage::{BlockStorage, VolumeManager};
use pillar_datanode::transport::hdfs_dtp::{read_delimited, write_delimited};
use pillar_datanode::transport::tcp::TcpTransport;
use pillar_datanode::transport::{wire, DataTransport, TransportError};
use pillar_namenode::metadata::Namespace;
use pillar_namenode::{NameNode, NameNodeConfig};
use pillar_proto::hadoop::common::TokenProto;
use pillar_proto::hdfs::{
    BaseHeaderProto, BlockOpResponseProto, ClientOperationHeaderProto, ExtendedBlockProto, OpReadBlockProto, Status,
};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

const MB: usize = 1024 * 1024;

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| ((i as u64 * 2654435761) >> 7) as u8 ^ seed).collect()
}

struct Cluster {
    nn_addr: SocketAddr,
    _name_dir: TempDir,
    namespace: Arc<Namespace>,
    dns: Vec<(SocketAddr, Arc<BlockStorage>, TempDir)>,
}

async fn start_cluster(n: usize) -> Cluster {
    let name_dir = TempDir::new().unwrap();
    let nn_addr = free_addr();
    let config = NameNodeConfig {
        rpc_addr: nn_addr,
        http_addr: free_addr(),
        etcd_endpoints: vec!["none".into()],
        name_dir: Some(PathBuf::from(name_dir.path())),
        block_access_tokens: true,
        ..Default::default()
    };
    let nn = Arc::new(NameNode::new(config).await.unwrap());
    let namespace = nn.namespace();
    let datanodes = nn.datanode_manager();
    tokio::spawn(async move { nn.start().await.unwrap() });

    let mut dns = Vec::new();
    for i in 0..n {
        let dir = TempDir::new().unwrap();
        let volumes = Arc::new(VolumeManager::new(dir.path(), "BP-test".to_string()).unwrap());
        let storage = Arc::new(BlockStorage::new(volumes.get_finalized_dir(), volumes.get_tmp_dir()).unwrap());
        let data_addr = free_addr();
        let tcp = Arc::new(TcpTransport::new(storage.clone(), data_addr));
        tokio::spawn(tcp.clone().start_server());
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
        while client.register().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::spawn(client.start_heartbeat_loop());
        dns.push((data_addr, storage, dir));
    }
    for _ in 0..200 {
        if datanodes.get_live_datanodes().len() == n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(datanodes.get_live_datanodes().len(), n);
    Cluster { nn_addr, _name_dir: name_dir, namespace, dns }
}

fn client(c: &Cluster) -> Client {
    ClientBuilder::new().with_url(format!("hdfs://{}", c.nn_addr)).build().unwrap()
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..300 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

/// A raw DataTransferProtocol READ_BLOCK of `block` with `token`: the
/// status the DataNode answers with.
async fn dtp_read(addr: SocketAddr, block: ExtendedBlockProto, token: Option<TokenProto>) -> Status {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(&[0, 28, 81]).await.unwrap(); // version 28, READ_BLOCK
    let len = block.num_bytes.unwrap_or(0);
    let op = OpReadBlockProto {
        header: ClientOperationHeaderProto {
            base_header: BaseHeaderProto { block, token, trace_info: None },
            client_name: "intruder".into(),
        },
        offset: 0,
        len,
        send_checksums: Some(true),
        caching_strategy: None,
    };
    write_delimited(&mut s, &op).await.unwrap();
    s.flush().await.unwrap();
    let r: BlockOpResponseProto = read_delimited(&mut s).await.unwrap();
    Status::try_from(r.status).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn data_transfers_need_a_valid_token_for_the_block() {
    let c = start_cluster(3).await;
    eventually("the DataNodes to receive block keys", || c.dns.iter().all(|d| d.1.tokens.enabled())).await;

    // A Hadoop client gets tokens from the NameNode with every block and
    // writes and reads as before.
    let fs = client(&c);
    let data = pattern(5 * MB / 2, 7);
    let opts = WriteOptions { block_size: Some(MB as u64), replication: Some(2), ..Default::default() };
    let mut w = fs.create("/t.bin", opts).await.unwrap();
    w.write_bytes(Bytes::copy_from_slice(&data)).await.unwrap();
    w.close().await.unwrap();
    let r = fs.read("/t.bin").await.unwrap();
    assert_eq!(r.read_range(0, data.len()).await.unwrap().to_vec(), data);

    let blocks = c.namespace.lookup("/t.bin").await.unwrap().read().await.blocks.clone().unwrap();
    let b = &blocks[0];
    let (addr, storage, _) = c.dns.iter().find(|d| d.1.get_block_metadata(b.block_id).is_some()).unwrap();
    let other = c.dns.iter().map(|d| &d.1).find(|s| s.get_block_metadata(b.block_id).is_none()).unwrap();
    let ext = ExtendedBlockProto {
        pool_id: String::new(),
        block_id: b.block_id,
        generation_stamp: b.generation_stamp,
        num_bytes: Some(b.num_bytes),
    };

    // DataTransferProtocol: no token, a token for another block, a write
    // token, a tampered one: all refused. A good read token works.
    assert_eq!(dtp_read(*addr, ext.clone(), None).await, Status::ErrorAccessToken);
    let for_other = storage.tokens.mint(blocks[1].block_id, mode::READ);
    assert_eq!(dtp_read(*addr, ext.clone(), for_other).await, Status::ErrorAccessToken);
    let write_only = storage.tokens.mint(b.block_id, mode::WRITE);
    assert_eq!(dtp_read(*addr, ext.clone(), write_only).await, Status::ErrorAccessToken);
    let mut forged = storage.tokens.mint(b.block_id, mode::READ).unwrap();
    forged.password[0] ^= 1;
    assert_eq!(dtp_read(*addr, ext.clone(), Some(forged)).await, Status::ErrorAccessToken);
    // Any DataNode's token verifies on any other: they share the keys.
    let good = other.tokens.mint(b.block_id, mode::READ);
    assert_eq!(dtp_read(*addr, ext.clone(), good).await, Status::Success);

    // Pillar's data protocol on the same port: the same rule.
    let peer = addr.to_string();
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let refused = wire::read_block(&mut s, b.block_id, b.generation_stamp, 0, 100, None).await;
    assert!(matches!(refused, Err(TransportError::ProtocolError(ref m)) if m.contains("block access token")), "{refused:?}");
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let token = other.tokens.mint(b.block_id, mode::READ);
    let got = wire::read_block(&mut s, b.block_id, b.generation_stamp, 0, 100, token).await.unwrap();
    assert_eq!(&got[..], &data[..100]);
    assert!(wire::replica_info(&peer, b.block_id, None).await.is_err());
    assert!(wire::replica_info(&peer, b.block_id, other.tokens.mint(b.block_id, mode::READ)).await.unwrap().is_some());

    // Re-replication: DataNodes copy blocks to each other with tokens
    // they mint themselves.
    assert!(fs.set_replication("/t.bin", 3).await.unwrap());
    eventually("every block on all three DataNodes", || {
        blocks.iter().all(|b| c.dns.iter().all(|d| d.1.get_block_metadata(b.block_id).is_some()))
    })
    .await;
}
