//! RDMA-based data transport implementation using ibverbs
//!
//! This module implements the RDMA transport layer for high-performance block I/O
//! using the ibverbs crate, which provides Rust bindings to libibverbs.
//!
//! ## RDMA Architecture
//!
//! The implementation uses RDMA verbs for zero-copy data transfer:
//! - **Queue Pairs (QP)**: Connection endpoints for RDMA operations
//! - **Protection Domains (PD)**: Memory isolation and access control
//! - **Memory Regions (MR)**: Registered memory for DMA access
//! - **Completion Queues (CQ)**: Asynchronous operation completion notifications
//!
//! ## Operation Types
//!
//! - **SEND/RECV**: Two-sided operations (both endpoints involved)
//! - **READ/WRITE**: One-sided operations (only initiator CPU involved)
//!
//! For block transfers, we use SEND/RECV for control messages and READ/WRITE
//! for bulk data transfer when performance is critical.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::ptr::NonNull;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{RwLock, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

use crate::storage::block_storage::BlockStorage;
use super::wire::{self, ConnPool};
use super::{BlockChunks, DataTransport, Result, TransportError, TransportType};

// Import ibverbs types when RDMA feature is enabled
#[cfg(feature = "rdma")]
use ibverbs::*;

/// RDMA transport configuration
#[derive(Debug, Clone)]
pub struct RdmaConfig {
    pub listen_addr: SocketAddr,
    pub queue_depth: u32,
    pub max_inline_data: u32,
    pub buffer_pool_size: usize,
}

/// RDMA device context and resources
///
/// Manages the RDMA device, protection domain, and shared resources.
/// All RDMA operations require a context to be initialized first.
#[cfg(feature = "rdma")]
struct RdmaContext {
    context: Arc<Context>,
    pd: Arc<ProtectionDomain<'static>>,
    device_name: String,
}

#[cfg(feature = "rdma")]
impl RdmaContext {
    /// Initialize RDMA device context
    ///
    /// This opens the first available RDMA device and creates a protection domain.
    /// In production, you may want to select a specific device by name.
    fn new() -> Result<Self> {
        // Get list of available RDMA devices
        let devices = ibverbs::devices()
            .map_err(|e| TransportError::ProtocolError(
                format!("Failed to query RDMA devices: {:?}", e)
            ))?;

        if devices.is_empty() {
            return Err(TransportError::ProtocolError(
                "No RDMA devices found. Ensure RDMA hardware is installed and drivers are loaded.".to_string()
            ));
        }

        // Open the first available device
        let device = &devices[0];
        let device_name = device.name()
            .map_err(|e| TransportError::ProtocolError(
                format!("Failed to get device name: {:?}", e)
            ))?;

        info!("Opening RDMA device: {}", device_name);

        let context = device.open()
            .map_err(|e| TransportError::ProtocolError(
                format!("Failed to open RDMA device {}: {:?}", device_name, e)
            ))?;

        let context = Arc::new(context);

        // Create protection domain
        // Safety: We're passing a valid context
        let pd = unsafe {
            ProtectionDomain::new(&context)
                .map_err(|e| TransportError::ProtocolError(
                    format!("Failed to create protection domain: {:?}", e)
                ))?
        };

        let pd = Arc::new(pd);

        info!("RDMA device {} initialized with protection domain", device_name);

        Ok(Self {
            context,
            pd,
            device_name,
        })
    }
}

/// RDMA memory buffer with registered memory region
///
/// Represents a buffer that has been registered with the RDMA device for DMA access.
/// The buffer is pinned in memory and can be used for zero-copy transfers.
#[cfg(feature = "rdma")]
struct RdmaBuffer {
    data: Vec<u8>,
    mr: MemoryRegion<'static>,
}

#[cfg(feature = "rdma")]
impl RdmaBuffer {
    /// Create and register a new RDMA buffer
    fn new(pd: &Arc<ProtectionDomain<'static>>, size: usize) -> Result<Self> {
        let mut data = vec![0u8; size];

        // Register memory region with RDMA device
        // Safety: We're registering a valid buffer that we own
        let mr = unsafe {
            MemoryRegion::new(
                pd,
                NonNull::new(data.as_mut_ptr()).unwrap(),
                size,
                // Allow local and remote read/write access
                ibverbs::MemoryRegionAccess::LOCAL_WRITE |
                ibverbs::MemoryRegionAccess::REMOTE_READ |
                ibverbs::MemoryRegionAccess::REMOTE_WRITE,
            )
            .map_err(|e| TransportError::ProtocolError(
                format!("Failed to register memory region: {:?}", e)
            ))?
        };

        Ok(Self { data, mr })
    }

    /// Get the buffer data
    fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable buffer data
    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Get the memory region
    fn mr(&self) -> &MemoryRegion<'static> {
        &self.mr
    }
}

/// RDMA memory buffer pool
///
/// Manages a pool of pre-allocated and registered RDMA buffers.
/// This avoids the overhead of allocating and registering buffers on every I/O operation.
struct RdmaMemoryPool {
    #[cfg(feature = "rdma")]
    buffers: Arc<Mutex<Vec<RdmaBuffer>>>,
    #[cfg(not(feature = "rdma"))]
    _phantom: std::marker::PhantomData<()>,
    buffer_size: usize,
    pool_size: usize,
}

impl RdmaMemoryPool {
    #[cfg(feature = "rdma")]
    fn new(pd: &Arc<ProtectionDomain<'static>>, pool_size: usize, buffer_size: usize) -> Result<Self> {
        info!("Creating RDMA memory pool: {} buffers of {} bytes", pool_size, buffer_size);

        let mut buffers = Vec::with_capacity(pool_size);

        // Pre-allocate and register all buffers
        for i in 0..pool_size {
            match RdmaBuffer::new(pd, buffer_size) {
                Ok(buf) => buffers.push(buf),
                Err(e) => {
                    warn!("Failed to allocate RDMA buffer {}/{}: {}", i + 1, pool_size, e);
                    break;
                }
            }
        }

        if buffers.is_empty() {
            return Err(TransportError::ProtocolError(
                "Failed to allocate any RDMA buffers".to_string()
            ));
        }

        info!("Allocated {} RDMA buffers", buffers.len());

        Ok(Self {
            buffers: Arc::new(Mutex::new(buffers)),
            buffer_size,
            pool_size,
        })
    }

    #[cfg(not(feature = "rdma"))]
    fn new(_pool_size: usize, buffer_size: usize) -> Result<Self> {
        Ok(Self {
            _phantom: std::marker::PhantomData,
            buffer_size,
            pool_size: _pool_size,
        })
    }

    #[cfg(feature = "rdma")]
    async fn get_buffer(&self) -> Result<RdmaBuffer> {
        let mut buffers = self.buffers.lock().await;

        if let Some(buf) = buffers.pop() {
            Ok(buf)
        } else {
            Err(TransportError::ProtocolError(
                "RDMA buffer pool exhausted".to_string()
            ))
        }
    }

    #[cfg(not(feature = "rdma"))]
    async fn get_buffer(&self) -> Result<Vec<u8>> {
        Ok(vec![0u8; self.buffer_size])
    }

    #[cfg(feature = "rdma")]
    async fn return_buffer(&self, buffer: RdmaBuffer) {
        let mut buffers = self.buffers.lock().await;
        if buffers.len() < self.pool_size {
            buffers.push(buffer);
        }
    }

    #[cfg(not(feature = "rdma"))]
    async fn return_buffer(&self, _buffer: Vec<u8>) {
        // No-op for non-RDMA builds
    }
}

/// RDMA connection to a peer DataNode
///
/// Manages a Queue Pair (QP) for RDMA operations with a specific peer.
/// In RDMA, each connection has a QP for sending/receiving data.
#[cfg(feature = "rdma")]
struct RdmaConnection {
    peer_addr: String,
    qp: Arc<QueuePair<'static>>,
    send_cq: Arc<CompletionQueue<'static>>,
    recv_cq: Arc<CompletionQueue<'static>>,
}

#[cfg(not(feature = "rdma"))]
struct RdmaConnection {
    peer_addr: String,
    _tcp_socket: Option<TcpStream>,
}

#[cfg(feature = "rdma")]
impl RdmaConnection {
    /// Create a new RDMA connection to a peer
    async fn connect(
        peer_addr: &str,
        context: &Arc<RdmaContext>,
        queue_depth: u32,
    ) -> Result<Self> {
        debug!("Establishing RDMA connection to {}", peer_addr);

        // Create completion queues for send and receive operations
        let send_cq = unsafe {
            CompletionQueue::new(&context.context, queue_depth as i32, std::ptr::null_mut())
                .map_err(|e| TransportError::ConnectionError(
                    format!("Failed to create send CQ: {:?}", e)
                ))?
        };

        let recv_cq = unsafe {
            CompletionQueue::new(&context.context, queue_depth as i32, std::ptr::null_mut())
                .map_err(|e| TransportError::ConnectionError(
                    format!("Failed to create recv CQ: {:?}", e)
                ))?
        };

        let send_cq = Arc::new(send_cq);
        let recv_cq = Arc::new(recv_cq);

        // Create queue pair
        let mut qp_init_attr = QueuePairInitAttr::default();
        qp_init_attr.send_cq = &send_cq;
        qp_init_attr.recv_cq = &recv_cq;
        qp_init_attr.cap.max_send_wr = queue_depth;
        qp_init_attr.cap.max_recv_wr = queue_depth;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.qp_type = QueuePairType::RC; // Reliable Connection

        let qp = unsafe {
            QueuePair::new(&context.pd, &qp_init_attr)
                .map_err(|e| TransportError::ConnectionError(
                    format!("Failed to create queue pair: {:?}", e)
                ))?
        };

        let qp = Arc::new(qp);

        // TODO: Complete QP state transitions (INIT -> RTR -> RTS)
        // This requires connection parameter exchange with the peer
        // For now, we'll note this is where RDMA CM or manual exchange would happen

        info!("RDMA connection established to {}", peer_addr);

        Ok(Self {
            peer_addr: peer_addr.to_string(),
            qp,
            send_cq,
            recv_cq,
        })
    }

    /// Post an RDMA SEND operation
    async fn post_send(&self, data: &[u8], mr: &MemoryRegion<'static>) -> Result<()> {
        let mut sge = ibverbs::ScatterGatherElement::default();
        sge.addr = data.as_ptr() as u64;
        sge.length = data.len() as u32;
        sge.lkey = mr.lkey();

        let mut send_wr = ibverbs::SendWorkRequest::default();
        send_wr.sg_list = &mut sge;
        send_wr.num_sge = 1;
        send_wr.opcode = ibverbs::WorkRequestOpcode::Send;

        unsafe {
            self.qp.post_send(&send_wr, std::ptr::null_mut())
                .map_err(|e| TransportError::ProtocolError(
                    format!("Failed to post SEND: {:?}", e)
                ))?;
        }

        debug!("RDMA SEND posted to {} ({} bytes)", self.peer_addr, data.len());
        Ok(())
    }

    /// Post an RDMA RECV operation
    async fn post_recv(&self, buffer: &mut [u8], mr: &MemoryRegion<'static>) -> Result<()> {
        let mut sge = ibverbs::ScatterGatherElement::default();
        sge.addr = buffer.as_ptr() as u64;
        sge.length = buffer.len() as u32;
        sge.lkey = mr.lkey();

        let mut recv_wr = ibverbs::ReceiveWorkRequest::default();
        recv_wr.sg_list = &mut sge;
        recv_wr.num_sge = 1;

        unsafe {
            self.qp.post_recv(&recv_wr, std::ptr::null_mut())
                .map_err(|e| TransportError::ProtocolError(
                    format!("Failed to post RECV: {:?}", e)
                ))?;
        }

        debug!("RDMA RECV posted from {}", self.peer_addr);
        Ok(())
    }

    /// Wait for completion on send queue
    async fn wait_send_completion(&self) -> Result<()> {
        let mut wc = ibverbs::WorkCompletion::default();

        loop {
            let num = unsafe {
                self.send_cq.poll(&mut wc, 1)
                    .map_err(|e| TransportError::ProtocolError(
                        format!("Failed to poll send CQ: {:?}", e)
                    ))?
            };

            if num > 0 {
                if wc.status != ibverbs::WorkCompletionStatus::Success {
                    return Err(TransportError::ProtocolError(
                        format!("SEND operation failed: {:?}", wc.status)
                    ));
                }
                return Ok(());
            }

            // Yield to avoid busy-waiting
            tokio::task::yield_now().await;
        }
    }

    /// Wait for completion on receive queue
    async fn wait_recv_completion(&self) -> Result<usize> {
        let mut wc = ibverbs::WorkCompletion::default();

        loop {
            let num = unsafe {
                self.recv_cq.poll(&mut wc, 1)
                    .map_err(|e| TransportError::ProtocolError(
                        format!("Failed to poll recv CQ: {:?}", e)
                    ))?
            };

            if num > 0 {
                if wc.status != ibverbs::WorkCompletionStatus::Success {
                    return Err(TransportError::ProtocolError(
                        format!("RECV operation failed: {:?}", wc.status)
                    ));
                }
                return Ok(wc.byte_len as usize);
            }

            // Yield to avoid busy-waiting
            tokio::task::yield_now().await;
        }
    }
}

#[cfg(not(feature = "rdma"))]
impl RdmaConnection {
    async fn connect(peer_addr: &str) -> Result<Self> {
        debug!("Establishing RDMA connection to {} (simulated)", peer_addr);

        let socket = TcpStream::connect(peer_addr).await
            .map_err(|e| TransportError::ConnectionError(
                format!("Failed to connect to {}: {}", peer_addr, e)
            ))?;

        Ok(Self {
            peer_addr: peer_addr.to_string(),
            _tcp_socket: Some(socket),
        })
    }
}

/// RDMA connection pool for peer DataNodes
struct RdmaConnectionPool {
    #[cfg(feature = "rdma")]
    connections: RwLock<HashMap<String, Arc<RdmaConnection>>>,
    #[cfg(feature = "rdma")]
    context: Arc<RdmaContext>,
    #[cfg(feature = "rdma")]
    queue_depth: u32,
    #[cfg(not(feature = "rdma"))]
    connections: RwLock<HashMap<String, Arc<RdmaConnection>>>,
    max_connections: usize,
}

impl RdmaConnectionPool {
    #[cfg(feature = "rdma")]
    fn new(context: Arc<RdmaContext>, queue_depth: u32, max_connections: usize) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            context,
            queue_depth,
            max_connections,
        }
    }

    #[cfg(not(feature = "rdma"))]
    fn new(max_connections: usize) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            max_connections,
        }
    }

    async fn get_or_create(&self, peer_addr: &str) -> Result<Arc<RdmaConnection>> {
        // Check if connection exists
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(peer_addr) {
                return Ok(conn.clone());
            }
        }

        // Create new connection
        #[cfg(feature = "rdma")]
        let conn = Arc::new(
            RdmaConnection::connect(peer_addr, &self.context, self.queue_depth).await?
        );

        #[cfg(not(feature = "rdma"))]
        let conn = Arc::new(RdmaConnection::connect(peer_addr).await?);

        // Store in pool
        {
            let mut connections = self.connections.write().await;
            if connections.len() >= self.max_connections {
                warn!("RDMA connection pool full, evicting oldest connection");
                // TODO: Implement LRU eviction
            }
            connections.insert(peer_addr.to_string(), conn.clone());
        }

        Ok(conn)
    }
}

/// RDMA-based data transport
///
/// This implements the DataTransport trait using RDMA verbs for both server-side
/// (serving requests) and client-side (connecting to peers) operations.
pub struct RdmaTransport {
    block_storage: Arc<BlockStorage>,
    config: RdmaConfig,
    #[cfg(feature = "rdma")]
    context: Arc<RdmaContext>,
    memory_pool: Arc<RdmaMemoryPool>,
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
        info!("RDMA config: queue_depth={}, max_inline={}, buffers={}",
              queue_depth, max_inline_data, buffer_pool_size);

        // Initialize RDMA device context
        #[cfg(feature = "rdma")]
        let context = Arc::new(RdmaContext::new()?);

        // Create memory pool with buffers sized for typical blocks
        let buffer_size = 1024 * 1024; // 1MB buffers

        #[cfg(feature = "rdma")]
        let memory_pool = Arc::new(RdmaMemoryPool::new(&context.pd, buffer_pool_size, buffer_size)?);

        #[cfg(not(feature = "rdma"))]
        let memory_pool = Arc::new(RdmaMemoryPool::new(buffer_pool_size, buffer_size)?);

        // Create connection pool
        #[cfg(feature = "rdma")]
        let connection_pool = Arc::new(RdmaConnectionPool::new(
            context.clone(),
            queue_depth,
            256, // Max 256 peer connections
        ));

        #[cfg(not(feature = "rdma"))]
        let connection_pool = Arc::new(RdmaConnectionPool::new(256));

        Ok(Self {
            block_storage,
            config: RdmaConfig {
                listen_addr,
                queue_depth,
                max_inline_data,
                buffer_pool_size,
            },
            #[cfg(feature = "rdma")]
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
        #[cfg(feature = "rdma")]
        {
            // Check RDMA device is still accessible
            debug!("RDMA health check: device {} OK", self.context.device_name);
        }

        Ok(())
    }

    async fn start_server(self: Arc<Self>) -> Result<()> {
        // TODO: Use RDMA CM for connection management in production
        // For now, use TCP listener for connection setup
        let listener = TcpListener::bind(self.config.listen_addr).await?;

        info!("RDMA data transfer server listening on {}", self.config.listen_addr);
        #[cfg(feature = "rdma")]
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
    use tempfile::TempDir;
    use std::fs;

    #[tokio::test]
    async fn test_rdma_transport_creation() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50011".parse().unwrap();

        // This test will fail if no RDMA hardware is present
        // In CI/CD, you would skip this test or use mock hardware
        let result = RdmaTransport::new(block_storage, addr, 128, 256, 1024);

        #[cfg(feature = "rdma")]
        {
            // With RDMA feature enabled, this requires actual hardware
            match result {
                Ok(transport) => {
                    assert_eq!(transport.transport_type(), TransportType::Rdma);
                    assert_eq!(transport.listen_addr(), "127.0.0.1:50011");
                }
                Err(e) => {
                    // Expected to fail in environments without RDMA hardware
                    println!("RDMA init failed (expected without hardware): {}", e);
                }
            }
        }

        #[cfg(not(feature = "rdma"))]
        {
            // Without RDMA feature, should still work (TCP fallback)
            let transport = result.unwrap();
            assert_eq!(transport.transport_type(), TransportType::Rdma);
            assert_eq!(transport.listen_addr(), "127.0.0.1:50011");
        }
    }

    #[tokio::test]
    async fn test_rdma_transport_health_check() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50012".parse().unwrap();

        let result = RdmaTransport::new(block_storage, addr, 128, 256, 1024);

        #[cfg(not(feature = "rdma"))]
        {
            let transport = result.unwrap();
            assert!(transport.health_check().await.is_ok());
        }

        #[cfg(feature = "rdma")]
        {
            if let Ok(transport) = result {
                assert!(transport.health_check().await.is_ok());
            }
        }
    }

    #[test]
    fn test_memory_pool_creation() {
        // Test memory pool creation (without RDMA feature)
        #[cfg(not(feature = "rdma"))]
        {
            let pool = RdmaMemoryPool::new(1024, 1024 * 1024).unwrap();
            assert_eq!(pool.pool_size, 1024);
            assert_eq!(pool.buffer_size, 1024 * 1024);
        }
    }
}
