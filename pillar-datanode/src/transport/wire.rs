//! Pillar's DataNode block-transfer protocol, shared by the TCP and RDMA
//! (currently TCP-fallback) transports.
//!
//! This is Pillar's own DataNode-to-DataNode protocol, not Hadoop's
//! DataTransferProtocol. Integers are big-endian.
//!
//! A connection carries any number of requests, one after another, so
//! clients can keep connections open and reuse them.
//!
//! ```text
//! request    op(u8) block_id(u64) gen_stamp(u64) offset(u64) length(u64)   33 octets
//!
//! READ  (op 81)  reply: status(u8)=0 len(u64) <len octets>      len = min(length, size - offset)
//!                       status(u8)≠0 msg_len(u16) <msg>
//!
//! WRITE (op 80)  offset and length are 0 (reserved); the request is
//!                followed by packets and an end marker:
//!                packet: len(u32, 1..=MAX_PACKET) <len octets>
//!                end:    len(u32) = 0
//!                reply:  status(u8)=0 stored(u64) | status(u8)≠0 msg_len(u16) <msg>
//! ```
//!
//! A WRITE carries a complete block in one session. The receiver writes
//! the packets sequentially into one open temp file and finalizes the block
//! when the end marker arrives, so the block is readable as soon as the
//! sender sees `status = 0`. Packets are not acknowledged one by one; TCP
//! flow control provides backpressure. A sender that fails part-way closes
//! the connection without an end marker, and the receiver discards the
//! partial block.
//!
//! Disk I/O runs on blocking threads connected to the socket by bounded
//! channels, so disk and network overlap in both directions.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use pillar_common::hdfs::block_token::mode;
use pillar_proto::hadoop::common::TokenProto;
use prost::Message;

use super::{BlockChunks, DataTransferOp, Result, TransportError};
use crate::storage::block_storage::{BlockId, BlockStorage, BlockStorageError};

/// Largest WRITE packet accepted; bounds per-connection memory.
pub const MAX_PACKET: usize = 4 * 1024 * 1024;

/// Size of the chunks a READ reply is streamed in.
pub const READ_CHUNK: usize = 1024 * 1024;

/// Chunks buffered between the disk thread and the socket.
const PIPE_DEPTH: usize = 4;

/// Idle connections kept per peer by [`ConnPool`].
const MAX_IDLE_PER_PEER: usize = 8;

pub const STATUS_OK: u8 = 0;
pub const STATUS_ERROR: u8 = 1;
pub const STATUS_NOT_FOUND: u8 = 2;

const HEADER_LEN: usize = 33;

/// A decoded request header.
///
/// On the wire the fixed header is followed by the block access token (u16
/// length, then an encoded `TokenProto`; length 0 when tokens are off).
/// Every operation carries one: the data port is shared with Hadoop
/// clients and must not offer a way around their tokens.
#[derive(Debug, Clone)]
pub struct Header {
    pub op: u8,
    pub block_id: u64,
    pub gen_stamp: u64,
    pub offset: u64,
    pub length: u64,
    pub token: Option<TokenProto>,
}

impl Header {
    fn encode(&self) -> Vec<u8> {
        let token = self.token.as_ref().map(|t| t.encode_to_vec()).unwrap_or_default();
        let mut b = Vec::with_capacity(HEADER_LEN + 2 + token.len());
        b.push(self.op);
        for v in [self.block_id, self.gen_stamp, self.offset, self.length] {
            b.extend_from_slice(&v.to_be_bytes());
        }
        b.extend_from_slice(&(token.len() as u16).to_be_bytes());
        b.extend_from_slice(&token);
        b
    }

    fn decode(op: u8, rest: &[u8; HEADER_LEN - 1]) -> Self {
        let u = |i: usize| u64::from_be_bytes(rest[i..i + 8].try_into().unwrap());
        Header { op, block_id: u(0), gen_stamp: u(8), offset: u(16), length: u(24), token: None }
    }
}

/// TCP_NODELAY on every data socket: requests and replies are single
/// writes, and Nagle + delayed ACK would otherwise add ~40 ms to small
/// requests on a reused connection.
pub fn tune(socket: &TcpStream) {
    if let Err(e) = socket.set_nodelay(true) {
        warn!("set_nodelay failed: {}", e);
    }
}

pub async fn connect(peer_addr: &str) -> Result<TcpStream> {
    let socket = TcpStream::connect(peer_addr).await.map_err(|e| {
        TransportError::ConnectionError(format!("Failed to connect to {}: {}", peer_addr, e))
    })?;
    tune(&socket);
    Ok(socket)
}

// ------------------------------------------------------------------ server

/// Serve requests on one connection until the peer closes it. A
/// connection that opens with the SASL magic number is authenticated (and
/// encrypted, if negotiated) first; one that does not is refused when this
/// DataNode requires data transfer security.
pub async fn serve_connection<S>(mut socket: S, storage: Arc<BlockStorage>, peer: SocketAddr) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let first = match socket.read_u8().await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    if first == (super::sasl::MAGIC >> 24) as u8 {
        let mut rest = [0u8; 3];
        socket.read_exact(&mut rest).await?;
        if u32::from_be_bytes([first, rest[0], rest[1], rest[2]]) != super::sasl::MAGIC {
            return Err(TransportError::ProtocolError("bad SASL magic number".into()));
        }
        let (secured, _) = super::sasl::accept(socket, &storage).await.map_err(|e| {
            warn!("Data transfer handshake with {} failed: {}", peer, e);
            e
        })?;
        return serve_requests(secured, storage, peer, None).await;
    }
    if super::sasl::security_of(&storage).required() {
        warn!("Refusing an unencrypted data transfer connection from {} (encryption or protection is required)", peer);
        return Ok(());
    }
    serve_requests(socket, storage, peer, Some(first)).await
}

async fn serve_requests<S>(mut socket: S, storage: Arc<BlockStorage>, peer: SocketAddr, mut first: Option<u8>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    loop {
        let op = match first.take() {
            Some(op) => op,
            None => match socket.read_u8().await {
                Ok(op) => op,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e.into()),
            },
        };
        // Hadoop DataTransferProtocol: a 2-byte version whose first octet
        // is 0, never a Pillar op code.
        if op == 0 {
            super::hdfs_dtp::serve(&mut socket, &storage, peer).await?;
            continue;
        }
        let mut rest = [0u8; HEADER_LEN - 1];
        socket.read_exact(&mut rest).await?;
        let mut h = Header::decode(op, &rest);
        let token_len = socket.read_u16().await? as usize;
        if token_len > 0 {
            let mut t = vec![0u8; token_len];
            socket.read_exact(&mut t).await?;
            h.token = TokenProto::decode(&t[..]).ok();
        }
        // The access each operation needs from the block's token.
        let needed = match DataTransferOp::from_u8(op) {
            Some(DataTransferOp::ReadBlock | DataTransferOp::ReplicaInfo) => mode::READ,
            _ => mode::WRITE,
        };
        if let Err(e) = storage.tokens.verify(h.token.as_ref(), h.block_id, needed) {
            warn!("Refusing op {} on block {} from {}: {}", op, h.block_id, peer, e);
            reply_error(&mut socket, STATUS_ERROR, &format!("block access token: {}", e)).await?;
            return Ok(());
        }

        match DataTransferOp::from_u8(op) {
            Some(DataTransferOp::ReadBlock) => serve_read(&mut socket, &storage, h).await?,
            Some(DataTransferOp::WriteBlock) => {
                // A failed write leaves unread packets on the connection, so
                // it cannot be reused: reply and close.
                if !serve_write(&mut socket, &storage, h, peer).await? {
                    return Ok(());
                }
            }
            Some(DataTransferOp::ReplicaInfo) => match storage.recovery_info(h.block_id) {
                Some((finalized, id, len)) => {
                    let mut b = [0u8; 18];
                    b[0] = STATUS_OK;
                    b[1] = finalized as u8;
                    b[2..10].copy_from_slice(&id.generation_stamp.to_be_bytes());
                    b[10..18].copy_from_slice(&len.to_be_bytes());
                    socket.write_all(&b).await?;
                    socket.flush().await?;
                }
                None => reply_error(&mut socket, STATUS_NOT_FOUND, &format!("no replica of block {}", h.block_id)).await?,
            },
            Some(DataTransferOp::RecoverReplica) => {
                let s = storage.clone();
                let (id, gs, len) = (h.block_id, h.gen_stamp, h.length);
                let r = tokio::task::spawn_blocking(move || super::hdfs_dtp::finalize_recovered(&s, id, gs, len))
                    .await
                    .map_err(|e| TransportError::ProtocolError(e.to_string()))?;
                match r {
                    Ok(stored) => {
                        let mut b = [0u8; 9];
                        b[0] = STATUS_OK;
                        b[1..].copy_from_slice(&stored.to_be_bytes());
                        socket.write_all(&b).await?;
                        socket.flush().await?;
                    }
                    Err(e) => reply_error(&mut socket, STATUS_ERROR, &e).await?,
                }
            }
            other => {
                let msg = format!("operation {:?} ({}) not supported", other, op);
                reply_error(&mut socket, STATUS_ERROR, &msg).await?;
                return Ok(());
            }
        }
    }
}

async fn reply_error<S: AsyncWrite + Unpin>(socket: &mut S, status: u8, msg: &str) -> Result<()> {
    let msg = &msg.as_bytes()[..msg.len().min(u16::MAX as usize)];
    let mut b = Vec::with_capacity(3 + msg.len());
    b.push(status);
    b.extend_from_slice(&(msg.len() as u16).to_be_bytes());
    b.extend_from_slice(msg);
    socket.write_all(&b).await?;
    socket.flush().await?;
    Ok(())
}

async fn serve_read<S>(socket: &mut S, storage: &Arc<BlockStorage>, h: Header) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let block = BlockId::new(h.block_id, h.gen_stamp);
    let opened = {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || storage.open_for_read(&block, h.offset, h.length))
            .await
            .map_err(|e| TransportError::ProtocolError(e.to_string()))?
    };
    let (file, len) = match opened {
        Ok(v) => v,
        Err(BlockStorageError::BlockNotFound(_)) => {
            return reply_error(socket, STATUS_NOT_FOUND, &format!("block {} not found", h.block_id)).await;
        }
        Err(e) => return reply_error(socket, STATUS_ERROR, &e.to_string()).await,
    };

    let mut head = [0u8; 9];
    head[0] = STATUS_OK;
    head[1..].copy_from_slice(&len.to_be_bytes());
    socket.write_all(&head).await?;

    let mut chunks = spawn_file_reader(file, len, READ_CHUNK);
    let mut sent = 0u64;
    while let Some(chunk) = chunks.recv().await {
        // A disk error after the header is out cannot be reported in-band;
        // returning closes the connection and the client sees a short read.
        let chunk = chunk?;
        sent += chunk.len() as u64;
        socket.write_all(&chunk).await?;
    }
    socket.flush().await?;
    if sent != len {
        return Err(TransportError::ProtocolError(format!(
            "block {}: sent {} of {} bytes",
            h.block_id, sent, len
        )));
    }
    debug!("Served {} bytes of block {}", len, h.block_id);
    Ok(())
}

/// Read `len` bytes from `file` on a blocking thread, as a stream of chunks.
pub fn spawn_file_reader(mut file: std::fs::File, len: u64, chunk: usize) -> BlockChunks {
    let (tx, rx) = mpsc::channel(PIPE_DEPTH);
    tokio::task::spawn_blocking(move || {
        let mut left = len;
        while left > 0 {
            let n = (chunk as u64).min(left) as usize;
            let mut buf = BytesMut::zeroed(n);
            let r = file.read_exact(&mut buf).map(|_| buf.freeze());
            let failed = r.is_err();
            if tx.blocking_send(r).is_err() || failed {
                return;
            }
            left -= n as u64;
        }
    });
    rx
}

/// Receive a WRITE session. Returns whether the connection can be reused.
async fn serve_write<S>(socket: &mut S, storage: &Arc<BlockStorage>, h: Header, peer: SocketAddr) -> Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let block = BlockId::new(h.block_id, h.gen_stamp);
    let file = {
        let storage = storage.clone();
        tokio::task::spawn_blocking(move || storage.begin_write(&block))
            .await
            .map_err(|e| TransportError::ProtocolError(e.to_string()))?
    };
    let file = match file {
        Ok(f) => f,
        Err(e) => {
            reply_error(socket, STATUS_ERROR, &e.to_string()).await?;
            return Ok(false);
        }
    };

    // Socket -> channel -> blocking disk writer, so the next packet is read
    // while the previous one is being written.
    let (tx, mut rx) = mpsc::channel::<Bytes>(PIPE_DEPTH);
    let writer = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
        let mut file = file;
        let mut written = 0u64;
        while let Some(packet) = rx.blocking_recv() {
            file.write_all(&packet)?;
            written += packet.len() as u64;
        }
        file.flush()?;
        Ok(written)
    });

    let received: Result<()> = async {
        loop {
            let len = socket.read_u32().await? as usize;
            if len == 0 {
                return Ok(());
            }
            if len > MAX_PACKET {
                return Err(TransportError::ProtocolError(format!("packet of {} bytes exceeds {}", len, MAX_PACKET)));
            }
            let mut buf = BytesMut::zeroed(len);
            socket.read_exact(&mut buf).await?;
            if tx.send(buf.freeze()).await.is_err() {
                // The disk writer stopped; its error is reported below.
                return Ok(());
            }
        }
    }
    .await;
    drop(tx);
    let written = writer.await.map_err(|e| TransportError::ProtocolError(e.to_string()))?;

    let outcome = match (received, written) {
        (Ok(()), Ok(n)) => {
            let storage = storage.clone();
            tokio::task::spawn_blocking(move || storage.finalize_block(&block))
                .await
                .map_err(|e| TransportError::ProtocolError(e.to_string()))?
                .map(|stored| (stored, n))
                .map_err(|e| e.to_string())
        }
        (Err(e), _) => Err(format!("receive failed: {}", e)),
        (_, Err(e)) => Err(format!("disk write failed: {}", e)),
    };

    match outcome {
        Ok((stored, _)) => {
            let mut b = [0u8; 9];
            b[0] = STATUS_OK;
            b[1..].copy_from_slice(&stored.to_be_bytes());
            socket.write_all(&b).await?;
            socket.flush().await?;
            debug!("Received block {} ({} bytes) from {}", h.block_id, stored, peer);
            Ok(true)
        }
        Err(msg) => {
            storage.abort_write(&block);
            warn!("Write of block {} from {} failed: {}", h.block_id, peer, msg);
            // Best effort: the peer may already be gone.
            let _ = reply_error(socket, STATUS_ERROR, &msg).await;
            Ok(false)
        }
    }
}

// ------------------------------------------------------------------ client

/// A connection to another DataNode, through the data transfer handshake
/// when this DataNode requires one (authenticating with a token for
/// `block_id` it mints).
pub async fn connect_secure(peer: &str, storage: &BlockStorage, block_id: u64) -> Result<super::sasl::DataStream> {
    let s = connect(peer).await?;
    Ok(super::sasl::connect(s, storage, block_id).await?.0)
}

/// Block recovery: the replica of `block_id` at `peer` as (finalized?,
/// generation stamp, length), or `None` if it has none.
pub async fn replica_info(storage: &BlockStorage, peer: &str, block_id: u64, token: Option<TokenProto>) -> Result<Option<(bool, u64, u64)>> {
    let mut s = connect_secure(peer, storage, block_id).await?;
    let h = Header { op: DataTransferOp::ReplicaInfo.to_u8(), block_id, gen_stamp: 0, offset: 0, length: 0, token };
    s.write_all(&h.encode()).await?;
    s.flush().await?;
    let status = s.read_u8().await?;
    if status == STATUS_OK {
        let finalized = s.read_u8().await? != 0;
        let gs = s.read_u64().await?;
        let len = s.read_u64().await?;
        return Ok(Some((finalized, gs, len)));
    }
    let n = s.read_u16().await? as usize;
    let mut msg = vec![0u8; n];
    s.read_exact(&mut msg).await?;
    match status {
        STATUS_NOT_FOUND => Ok(None),
        _ => Err(TransportError::ProtocolError(String::from_utf8_lossy(&msg).into_owned())),
    }
}

/// Block recovery: have `peer` cut its replica of `block_id` to `len`,
/// give it `recovery_gs` and finalize it. Returns the finalized length.
pub async fn recover_replica(storage: &BlockStorage, peer: &str, block_id: u64, recovery_gs: u64, len: u64, token: Option<TokenProto>) -> Result<u64> {
    let mut s = connect_secure(peer, storage, block_id).await?;
    let h = Header { op: DataTransferOp::RecoverReplica.to_u8(), block_id, gen_stamp: recovery_gs, offset: 0, length: len, token };
    s.write_all(&h.encode()).await?;
    s.flush().await?;
    read_status(&mut s, block_id).await
}

async fn read_status<S: AsyncRead + Unpin>(socket: &mut S, block_id: u64) -> Result<u64> {
    let status = socket.read_u8().await?;
    if status == STATUS_OK {
        return Ok(socket.read_u64().await?);
    }
    let n = socket.read_u16().await? as usize;
    let mut msg = vec![0u8; n];
    socket.read_exact(&mut msg).await?;
    let msg = String::from_utf8_lossy(&msg).into_owned();
    Err(match status {
        STATUS_NOT_FOUND => TransportError::BlockNotFound(block_id),
        _ => TransportError::ProtocolError(msg),
    })
}

/// READ on an open connection.
pub async fn read_block<S>(socket: &mut S, block_id: u64, gen_stamp: u64, offset: u64, length: u64, token: Option<TokenProto>) -> Result<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let h = Header { op: DataTransferOp::ReadBlock.to_u8(), block_id, gen_stamp, offset, length, token };
    socket.write_all(&h.encode()).await?;
    socket.flush().await?;
    let len = read_status(socket, block_id).await?;
    if len > length {
        return Err(TransportError::ProtocolError(format!("peer sent {} bytes, {} requested", len, length)));
    }
    let mut data = BytesMut::zeroed(len as usize);
    socket.read_exact(&mut data).await?;
    Ok(data.freeze())
}

/// WRITE session on an open connection: stream `chunks` as one block.
///
/// An `Err` chunk aborts the session without an end marker, so the peer
/// discards what it received.
pub async fn send_block<S>(socket: &mut S, block_id: u64, gen_stamp: u64, mut chunks: BlockChunks, token: Option<TokenProto>) -> Result<u64>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let h = Header { op: DataTransferOp::WriteBlock.to_u8(), block_id, gen_stamp, offset: 0, length: 0, token };
    socket.write_all(&h.encode()).await?;
    let mut sent = 0u64;
    while let Some(chunk) = chunks.recv().await {
        let mut chunk = chunk?;
        while !chunk.is_empty() {
            let part = chunk.split_to(chunk.len().min(MAX_PACKET));
            sent += part.len() as u64;
            let len = (part.len() as u32).to_be_bytes();
            // One buffer chain, so the length prefix and payload go out together.
            socket.write_all_buf(&mut Buf::chain(&len[..], part)).await?;
        }
    }
    socket.write_all(&0u32.to_be_bytes()).await?;
    socket.flush().await?;
    let stored = read_status(socket, block_id).await?;
    if stored != sent {
        return Err(TransportError::ProtocolError(format!(
            "peer stored {} bytes of block {}, {} sent",
            stored, block_id, sent
        )));
    }
    Ok(stored)
}

/// Idle connections to peers, reused across requests.
#[derive(Default)]
pub struct ConnPool {
    idle: Mutex<HashMap<String, Vec<super::sasl::DataStream>>>,
}

impl ConnPool {
    /// An idle connection to `peer`, if one is pooled.
    pub fn take(&self, peer: &str) -> Option<super::sasl::DataStream> {
        self.idle.lock().unwrap().get_mut(peer).and_then(Vec::pop)
    }

    /// Return a connection after a request that left it clean.
    pub fn put(&self, peer: &str, socket: super::sasl::DataStream) {
        let mut idle = self.idle.lock().unwrap();
        let v = idle.entry(peer.to_string()).or_default();
        if v.len() < MAX_IDLE_PER_PEER {
            v.push(socket);
        }
    }

    /// READ through the pool. A pooled connection may have been closed by
    /// the peer while idle, so a failure on one is retried once on a fresh
    /// connection (READ is idempotent).
    pub async fn read_block(
        &self,
        storage: &BlockStorage,
        peer: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
        token: Option<TokenProto>,
    ) -> Result<Bytes> {
        if let Some(mut s) = self.take(peer) {
            match read_block(&mut s, block_id, gen_stamp, offset, length, token.clone()).await {
                Ok(data) => {
                    self.put(peer, s);
                    return Ok(data);
                }
                Err(e @ TransportError::BlockNotFound(_)) => {
                    // The peer answered cleanly: the connection is fine.
                    self.put(peer, s);
                    return Err(e);
                }
                // The peer answered but the stream may hold unread bytes.
                Err(e @ TransportError::ProtocolError(_)) => return Err(e),
                Err(e) => debug!("pooled connection to {} failed ({}); reconnecting", peer, e),
            }
        }
        let mut s = connect_secure(peer, storage, block_id).await?;
        let r = read_block(&mut s, block_id, gen_stamp, offset, length, token).await;
        if matches!(r, Ok(_) | Err(TransportError::BlockNotFound(_))) {
            self.put(peer, s);
        }
        r
    }

    /// WRITE session on a fresh connection (a session consumes its chunk
    /// stream, so it cannot be retried on a stale pooled one); the
    /// connection is pooled afterwards for reuse by reads.
    pub async fn send_block(
        &self,
        storage: &BlockStorage,
        peer: &str,
        block_id: u64,
        gen_stamp: u64,
        chunks: BlockChunks,
        token: Option<TokenProto>,
    ) -> Result<u64> {
        let mut s = connect_secure(peer, storage, block_id).await?;
        let stored = send_block(&mut s, block_id, gen_stamp, chunks, token).await?;
        self.put(peer, s);
        Ok(stored)
    }
}
