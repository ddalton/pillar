//! RDMA-based data transport implementation using ibverbs
//!
//! This module implements the RDMA transport layer for high-performance block I/O
//! using the `ibverbs` crate (0.9), which provides Rust bindings to libibverbs.
//!
//! ## Current status
//!
//! Block data currently moves over TCP using the shared wire protocol
//! (`transport/wire.rs`); see `RDMA_IMPLEMENTATION_PLAN.md`. The verbs
//! resources below (device context, protection domain, registered buffer pool,
//! queue-pair setup, SEND/RECV posting and completion polling) are the
//! scaffolding the RDMA data path will be built on. They are initialised by
//! [`RdmaTransport::new`] but are not yet used to move block data.
//!
//! ## RDMA Architecture (ibverbs 0.9 model)
//!
//! - **Context / Protection Domain (PD)**: opened once per transport. Every
//!   other verbs object borrows from these, so they are given a `'static`
//!   lifetime (see [`RdmaContext`]).
//! - **Memory Regions (MR)**: `MemoryRegion<u8>` owns its registered buffer
//!   (allocated by `ProtectionDomain::allocate`). Buffers are pooled to keep
//!   registration off the data path.
//! - **Queue Pairs (QP)**: a QP is created as a `PreparedQueuePair`, its
//!   `QueuePairEndpoint` is exchanged with the peer out of band (over any
//!   byte stream, e.g. TCP), and `handshake` moves it to RTS.
//! - **Completion Queues (CQ)**: one CQ per connection serves both the send
//!   and the receive queue; completions are demultiplexed by `wr_id`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use ibverbs::{
    ibv_qp_type, ibv_wc, CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair,
    QueuePairEndpoint,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::wire::{self, ConnPool};
use super::{BlockChunks, DataTransport, Result, TransportError, TransportType};
use crate::storage::block_storage::BlockStorage;

/// How long to wait for a single work completion before giving up.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on the size of a serialized `QueuePairEndpoint` during the handshake.
const MAX_ENDPOINT_MSG: u32 = 4096;

fn verbs_err(what: &str, e: impl std::fmt::Display) -> TransportError {
    TransportError::RdmaError(format!("{}: {}", what, e))
}

/// RDMA transport configuration
#[derive(Debug, Clone)]
#[allow(dead_code)] // only `listen_addr` is used until the RDMA data path lands
pub struct RdmaConfig {
    pub listen_addr: SocketAddr,
    pub queue_depth: u32,
    /// Requested inline-data size. ibverbs 0.9 does not expose
    /// `max_inline_data` on its `QueuePairBuilder` (it is fixed at 0), so this
    /// is currently informational only.
    pub max_inline_data: u32,
    pub buffer_pool_size: usize,
}

/// RDMA device context and protection domain.
///
/// In ibverbs 0.9 a `ProtectionDomain<'ctx>` borrows its `Context`, and queue
/// pairs / completion queues borrow those in turn. The context and PD are
/// therefore leaked to obtain `'static` references: they are created once per
/// transport and live for the lifetime of the DataNode process, and leaking
/// them avoids self-referential structs. (The device is closed by the OS at
/// process exit.)
struct RdmaContext {
    context: &'static Context,
    pd: &'static ProtectionDomain<'static>,
    device_name: String,
}

impl RdmaContext {
    /// Open the first available RDMA device and allocate a protection domain.
    ///
    /// In production you may want to select a specific device by name.
    fn new() -> Result<Self> {
        let devices = ibverbs::devices().map_err(|e| verbs_err("Failed to query RDMA devices", e))?;

        let device = devices.get(0).ok_or_else(|| {
            TransportError::RdmaError(
                "No RDMA devices found. Ensure RDMA hardware is installed and drivers are loaded."
                    .to_string(),
            )
        })?;

        let device_name = device
            .name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "<unnamed>".to_string());

        info!("Opening RDMA device: {}", device_name);

        // `open` also queries port 1 and fails unless the port is ACTIVE/ARMED.
        let context = device
            .open()
            .map_err(|e| verbs_err(&format!("Failed to open RDMA device {}", device_name), e))?;
        let context: &'static Context = Box::leak(Box::new(context));

        let pd = context
            .alloc_pd()
            .map_err(|e| verbs_err("Failed to allocate protection domain", e))?;
        let pd: &'static ProtectionDomain<'static> = Box::leak(Box::new(pd));

        info!("RDMA device {} initialized with protection domain", device_name);

        Ok(Self {
            context,
            pd,
            device_name,
        })
    }

    /// Pick a GID table index for global routing (required for RoCE).
    ///
    /// `QueuePairBuilder::set_gid_index` indexes `Context::gid_table()` and
    /// `PreparedQueuePair::endpoint` asserts that the entry at that position
    /// carries the same `gid_index`, so only such entries are eligible.
    fn gid_index(&self) -> Option<u32> {
        self.context
            .gid_table()
            .iter()
            .enumerate()
            .find(|(pos, entry)| entry.gid_index == *pos as u32)
            .map(|(pos, _)| pos as u32)
    }
}

/// Pool of pre-registered RDMA buffers.
///
/// `MemoryRegion<u8>` owns its buffer, so a pooled buffer *is* its memory
/// region. Pre-registering avoids the cost of `ibv_reg_mr` on every I/O.
#[allow(dead_code)] // not on the data path yet; see module docs
struct RdmaMemoryPool {
    buffers: Mutex<Vec<MemoryRegion<u8>>>,
    buffer_size: usize,
    pool_size: usize,
}

#[allow(dead_code)] // not on the data path yet; see module docs
impl RdmaMemoryPool {
    fn new(pd: &ProtectionDomain<'_>, pool_size: usize, buffer_size: usize) -> Result<Self> {
        info!(
            "Creating RDMA memory pool: {} buffers of {} bytes",
            pool_size, buffer_size
        );

        let mut buffers = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            match pd.allocate::<u8>(buffer_size) {
                Ok(mr) => buffers.push(mr),
                Err(e) => {
                    warn!(
                        "Failed to allocate RDMA buffer {}/{}: {}",
                        i + 1,
                        pool_size,
                        e
                    );
                    break;
                }
            }
        }

        if buffers.is_empty() {
            return Err(TransportError::RdmaError(
                "Failed to allocate any RDMA buffers".to_string(),
            ));
        }

        info!("Allocated {} RDMA buffers", buffers.len());

        Ok(Self {
            buffers: Mutex::new(buffers),
            buffer_size,
            pool_size,
        })
    }

    fn get_buffer(&self) -> Result<MemoryRegion<u8>> {
        self.buffers
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| TransportError::RdmaError("RDMA buffer pool exhausted".to_string()))
    }

    /// Return a buffer to the pool. Only call this once no work request
    /// referencing the buffer is outstanding (the `RdmaConnection` send/recv
    /// helpers only hand buffers back after their completion was reaped).
    fn return_buffer(&self, buffer: MemoryRegion<u8>) {
        let mut buffers = self.buffers.lock().unwrap();
        if buffers.len() < self.pool_size && buffer.len() == self.buffer_size {
            buffers.push(buffer);
        }
    }

    fn available(&self) -> usize {
        self.buffers.lock().unwrap().len()
    }
}

/// Write a `QueuePairEndpoint` as a length-prefixed JSON frame.
async fn write_endpoint<S: AsyncWrite + Unpin>(stream: &mut S, ep: &QueuePairEndpoint) -> Result<()> {
    let buf = serde_json::to_vec(ep)
        .map_err(|e| TransportError::ProtocolError(format!("encode QP endpoint: {}", e)))?;
    stream.write_u32(buf.len() as u32).await?;
    stream.write_all(&buf).await?;
    stream.flush().await?;
    Ok(())
}

/// Read a `QueuePairEndpoint` written by [`write_endpoint`].
async fn read_endpoint<S: AsyncRead + Unpin>(stream: &mut S) -> Result<QueuePairEndpoint> {
    let len = stream.read_u32().await?;
    if len > MAX_ENDPOINT_MSG {
        return Err(TransportError::ProtocolError(format!(
            "QP endpoint message too large: {} bytes",
            len
        )));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf)
        .map_err(|e| TransportError::ProtocolError(format!("decode QP endpoint: {}", e)))
}

/// An established RC queue pair to a peer DataNode.
///
/// The QP borrows its completion queue. ibverbs 0.9 expresses that with
/// lifetimes, so the CQ is boxed (stable address) and the borrow is extended
/// to `'static`. **Field order matters:** `qp` is declared before `cq` so it is
/// dropped (`ibv_destroy_qp`) before the CQ it references (`ibv_destroy_cq`).
#[allow(dead_code)] // not on the data path yet; see module docs
struct RdmaConnection {
    peer_addr: String,
    remote: QueuePairEndpoint,
    /// `post_send`/`post_receive` take `&mut QueuePair`; posting never blocks.
    qp: Mutex<QueuePair<'static>>,
    cq: Box<CompletionQueue<'static>>,
    /// Completions reaped by one waiter on behalf of another, keyed by `wr_id`.
    stash: Mutex<HashMap<u64, ibv_wc>>,
    next_wr_id: AtomicU64,
}

#[allow(dead_code)] // not on the data path yet; see module docs
impl RdmaConnection {
    /// Create an RC queue pair and connect it to the peer.
    ///
    /// The QP endpoints (QPN, LID, optional GID) are exchanged over `stream`,
    /// which can be any reliable byte stream to the peer (e.g. a TCP
    /// connection). Both sides run this same function.
    async fn establish<S: AsyncRead + AsyncWrite + Unpin>(
        peer_addr: &str,
        ctx: &RdmaContext,
        queue_depth: u32,
        stream: &mut S,
    ) -> Result<Self> {
        debug!("Establishing RDMA connection to {}", peer_addr);

        let queue_depth = queue_depth.max(1);
        // One CQ serves both the send and receive queues.
        let cq_entries = i32::try_from(queue_depth.saturating_mul(2)).unwrap_or(i32::MAX);
        let cq = Box::new(
            ctx.context
                .create_cq(cq_entries, 0)
                .map_err(|e| verbs_err("Failed to create completion queue", e))?,
        );
        // SAFETY: the CQ lives on the heap and is never moved out of its Box.
        // The only borrower is the QP created below, which is always dropped
        // before `cq`: as a local (declared after `cq`) and as a field of
        // `RdmaConnection` (declared before `cq`).
        let cq_ref: &'static CompletionQueue<'static> =
            unsafe { &*(cq.as_ref() as *const CompletionQueue<'static>) };

        let mut builder = ctx.pd.create_qp(cq_ref, cq_ref, ibv_qp_type::IBV_QPT_RC);
        builder
            .set_max_send_wr(queue_depth)
            .set_max_recv_wr(queue_depth)
            .allow_remote_rw();
        if let Some(idx) = ctx.gid_index() {
            builder.set_gid_index(idx);
        }
        let prepared = builder
            .build()
            .map_err(|e| verbs_err("Failed to create queue pair", e))?;

        let local = prepared.endpoint();
        write_endpoint(stream, &local).await?;
        let remote = read_endpoint(stream).await?;
        debug!("RDMA QP endpoints: local={:?} remote={:?}", local, remote);

        // INIT -> RTR -> RTS
        let qp = prepared
            .handshake(remote)
            .map_err(|e| verbs_err("QP handshake failed", e))?;

        info!(
            "RDMA connection established to {} (local QPN {}, remote QPN {})",
            peer_addr, local.num, remote.num
        );

        Ok(Self {
            peer_addr: peer_addr.to_string(),
            remote,
            qp: Mutex::new(qp),
            cq,
            stash: Mutex::new(HashMap::new()),
            next_wr_id: AtomicU64::new(1),
        })
    }

    fn next_wr_id(&self) -> u64 {
        self.next_wr_id.fetch_add(1, Ordering::Relaxed)
    }

    /// SEND the first `len` bytes of `mr` and wait for its completion.
    ///
    /// The buffer is moved into the operation and handed back once the work
    /// completion has been reaped, which is what ibverbs requires before an
    /// MR may be reused or dropped. If the caller's future is cancelled, the
    /// blocking poll task keeps ownership until the completion arrives.
    async fn send(self: &Arc<Self>, mut mr: MemoryRegion<u8>, len: usize) -> Result<MemoryRegion<u8>> {
        if len == 0 || len > mr.len() {
            return Err(TransportError::RdmaError(format!(
                "invalid SEND length {} for {}-byte buffer",
                len,
                mr.len()
            )));
        }
        let wr_id = self.next_wr_id();
        // SAFETY: `mr` is not touched again until its completion is reaped in
        // `wait_completion`, which owns it until then.
        unsafe { self.qp.lock().unwrap().post_send(&mut mr, ..len, wr_id) }
            .map_err(|e| verbs_err("Failed to post SEND", e))?;
        debug!("RDMA SEND posted to {} ({} bytes)", self.peer_addr, len);

        let (mr, _) = self.clone().wait_completion(wr_id, mr, "SEND").await?;
        Ok(mr)
    }

    /// Post a RECV into `mr` and wait for a message to land in it.
    /// Returns the buffer and the number of bytes received.
    async fn recv(self: &Arc<Self>, mut mr: MemoryRegion<u8>) -> Result<(MemoryRegion<u8>, usize)> {
        let wr_id = self.next_wr_id();
        // SAFETY: as in `send`.
        unsafe { self.qp.lock().unwrap().post_receive(&mut mr, .., wr_id) }
            .map_err(|e| verbs_err("Failed to post RECV", e))?;
        debug!("RDMA RECV posted from {}", self.peer_addr);

        self.clone().wait_completion(wr_id, mr, "RECV").await
    }

    /// Wait for the completion of `wr_id` on a blocking thread, so tokio
    /// worker threads are never spun.
    async fn wait_completion(
        self: Arc<Self>,
        wr_id: u64,
        mr: MemoryRegion<u8>,
        op: &'static str,
    ) -> Result<(MemoryRegion<u8>, usize)> {
        tokio::task::spawn_blocking(move || match self.poll_completion(wr_id) {
            Ok(wc) => match wc.error() {
                None => Ok((mr, wc.len())),
                Some((status, vendor_err)) => Err(TransportError::RdmaError(format!(
                    "{} to {} failed: status {} (vendor_err {})",
                    op, self.peer_addr, status, vendor_err
                ))),
            },
            Err(e) => {
                // The work request may still be outstanding, so the HCA may
                // still DMA into/out of this buffer. Deregistering or freeing
                // it would be unsound; leak it instead.
                std::mem::forget(mr);
                Err(e)
            }
        })
        .await
        .map_err(|e| TransportError::RdmaError(format!("completion task failed: {}", e)))?
    }

    /// Blocking poll for the completion of `wr_id`, with backoff: a short
    /// spin, then yielding, then sleeping with exponentially growing delay.
    fn poll_completion(&self, wr_id: u64) -> Result<ibv_wc> {
        let deadline = Instant::now() + COMPLETION_TIMEOUT;
        let mut completions = [ibv_wc::default(); 16];
        let mut attempt: u32 = 0;
        let mut sleep = Duration::from_micros(10);

        loop {
            {
                // Hold the stash lock while polling so a completion for another
                // waiter is always either in the CQ or in the stash.
                let mut stash = self.stash.lock().unwrap();
                if let Some(wc) = stash.remove(&wr_id) {
                    return Ok(wc);
                }
                let polled = self
                    .cq
                    .poll(&mut completions)
                    .map_err(|e| verbs_err("Failed to poll CQ", e))?;
                let mut found = None;
                for wc in polled.iter() {
                    if wc.wr_id() == wr_id {
                        found = Some(*wc);
                    } else {
                        stash.insert(wc.wr_id(), *wc);
                    }
                }
                if let Some(wc) = found {
                    return Ok(wc);
                }
            }

            if Instant::now() >= deadline {
                return Err(TransportError::RdmaError(format!(
                    "timed out waiting for completion of wr_id {} on connection to {}",
                    wr_id, self.peer_addr
                )));
            }

            attempt += 1;
            if attempt < 64 {
                std::hint::spin_loop();
            } else if attempt < 128 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(sleep);
                sleep = (sleep * 2).min(Duration::from_millis(1));
            }
        }
    }
}

/// RDMA connections to peer DataNodes, keyed by peer address.
#[allow(dead_code)] // not on the data path yet; see module docs
struct RdmaConnectionPool {
    connections: RwLock<HashMap<String, Arc<RdmaConnection>>>,
    context: Arc<RdmaContext>,
    queue_depth: u32,
    max_connections: usize,
}

#[allow(dead_code)] // not on the data path yet; see module docs
impl RdmaConnectionPool {
    fn new(context: Arc<RdmaContext>, queue_depth: u32, max_connections: usize) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            context,
            queue_depth,
            max_connections,
        }
    }

    async fn get(&self, peer_addr: &str) -> Option<Arc<RdmaConnection>> {
        self.connections.read().await.get(peer_addr).cloned()
    }

    /// Return the pooled connection to `peer_addr`, or establish one by
    /// exchanging QP endpoints over `stream`.
    ///
    /// The caller supplies the out-of-band stream: the DataNode's data port
    /// currently speaks only the TCP wire protocol, which has no QP-handshake
    /// message yet, so there is no default control channel to dial.
    async fn get_or_establish<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        peer_addr: &str,
        stream: &mut S,
    ) -> Result<Arc<RdmaConnection>> {
        if let Some(conn) = self.get(peer_addr).await {
            return Ok(conn);
        }

        let conn = Arc::new(
            RdmaConnection::establish(peer_addr, &self.context, self.queue_depth, stream).await?,
        );

        let mut connections = self.connections.write().await;
        if connections.len() >= self.max_connections {
            warn!("RDMA connection pool full, evicting oldest connection");
            // TODO: Implement LRU eviction
        }
        connections.insert(peer_addr.to_string(), conn.clone());
        Ok(conn)
    }
}

/// RDMA-based data transport
///
/// This implements the DataTransport trait for both server-side (serving
/// requests) and client-side (connecting to peers) operations.
pub struct RdmaTransport {
    block_storage: Arc<BlockStorage>,
    config: RdmaConfig,
    context: Arc<RdmaContext>,
    #[allow(dead_code)] // not on the data path yet; see module docs
    memory_pool: Arc<RdmaMemoryPool>,
    #[allow(dead_code)] // not on the data path yet; see module docs
    connection_pool: Arc<RdmaConnectionPool>,
    /// Pooled TCP connections for the interim TCP data path.
    tcp_pool: ConnPool,
}

impl RdmaTransport {
    pub fn new(
        block_storage: Arc<BlockStorage>,
        listen_addr: SocketAddr,
        queue_depth: u32,
        max_inline_data: u32,
        buffer_pool_size: usize,
    ) -> Result<Self> {
        info!("Initializing RDMA transport on {}", listen_addr);
        info!(
            "RDMA config: queue_depth={}, max_inline={}, buffers={}",
            queue_depth, max_inline_data, buffer_pool_size
        );

        let context = Arc::new(RdmaContext::new()?);

        // Buffers sized for typical block chunks
        let buffer_size = 1024 * 1024; // 1MB buffers
        let memory_pool = Arc::new(RdmaMemoryPool::new(context.pd, buffer_pool_size, buffer_size)?);

        let connection_pool = Arc::new(RdmaConnectionPool::new(
            context.clone(),
            queue_depth,
            256, // Max 256 peer connections
        ));

        Ok(Self {
            block_storage,
            config: RdmaConfig {
                listen_addr,
                queue_depth,
                max_inline_data,
                buffer_pool_size,
            },
            context,
            memory_pool,
            connection_pool,
            tcp_pool: ConnPool::default(),
        })
    }
}

#[async_trait]
impl DataTransport for RdmaTransport {
    // Data moves over TCP with the shared wire protocol until RDMA
    // READ/WRITE is implemented (RDMA_IMPLEMENTATION_PLAN.md).
    async fn read_from_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        debug!("Reading block {} from peer {} (RDMA transport, TCP data path)", block_id, peer_addr);
        self.tcp_pool.read_block(peer_addr, block_id, gen_stamp, offset, length).await
    }

    async fn send_block_to_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        chunks: BlockChunks,
    ) -> Result<u64> {
        debug!("Sending block {} to peer {} (RDMA transport, TCP data path)", block_id, peer_addr);
        self.tcp_pool.send_block(peer_addr, block_id, gen_stamp, chunks).await
    }

    fn transport_type(&self) -> TransportType {
        TransportType::Rdma
    }

    fn listen_addr(&self) -> String {
        self.config.listen_addr.to_string()
    }

    async fn health_check(&self) -> Result<()> {
        debug!(
            "RDMA health check: device {} OK, {} registered buffers available",
            self.context.device_name,
            self.memory_pool.available()
        );
        Ok(())
    }

    async fn start_server(self: Arc<Self>) -> Result<()> {
        // TODO: Use RDMA CM for connection management in production
        // For now, use TCP listener for connection setup
        let listener = TcpListener::bind(self.config.listen_addr).await?;

        info!("RDMA data transfer server listening on {}", self.config.listen_addr);
        info!("RDMA device: {}", self.context.device_name);

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    debug!("Accepted RDMA connection from {}", peer_addr);

                    wire::tune(&socket);
                    let storage = self.block_storage.clone();
                    tokio::spawn(async move {
                        if let Err(e) = wire::serve_connection(socket, storage, peer_addr).await {
                            warn!("Error handling RDMA connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting RDMA connection: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_rdma_transport_creation() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50011".parse().unwrap();

        // Requires RDMA hardware; without it, initialisation fails cleanly.
        match RdmaTransport::new(block_storage, addr, 128, 256, 16) {
            Ok(transport) => {
                assert_eq!(transport.transport_type(), TransportType::Rdma);
                assert_eq!(transport.listen_addr(), "127.0.0.1:50011");
                assert!(transport.health_check().await.is_ok());
            }
            Err(e) => {
                println!("RDMA init failed (expected without hardware): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_endpoint_exchange_roundtrip() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let ep = QueuePairEndpoint {
            num: 0x1234,
            lid: 7,
            gid: None,
        };
        write_endpoint(&mut a, &ep).await.unwrap();
        let got = read_endpoint(&mut b).await.unwrap();
        assert_eq!(got, ep);
    }

    #[tokio::test]
    async fn test_endpoint_rejects_oversized_frame() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_u32(MAX_ENDPOINT_MSG + 1).await.unwrap();
        assert!(read_endpoint(&mut b).await.is_err());
    }
}
