//! TCP-based data transport implementation
//!
//! The wire format and the server/client logic live in [`super::wire`];
//! this type binds them to a TCP listener and a pool of peer connections.

use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::storage::block_storage::BlockStorage;
use super::wire::{self, ConnPool};
use super::{BlockChunks, DataTransport, Result, TransportType};

/// TCP-based data transport
///
/// Serves block reads and writes from local storage, and reads from or
/// streams blocks to peer DataNodes over pooled connections.
pub struct TcpTransport {
    block_storage: Arc<BlockStorage>,
    listen_addr: SocketAddr,
    pool: ConnPool,
}

impl TcpTransport {
    pub fn new(block_storage: Arc<BlockStorage>, listen_addr: SocketAddr) -> Self {
        Self {
            block_storage,
            listen_addr,
            pool: ConnPool::default(),
        }
    }
}

#[async_trait]
impl DataTransport for TcpTransport {
    async fn read_from_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let data = self.pool.read_block(peer_addr, block_id, gen_stamp, offset, length).await?;
        debug!("Read {} bytes of block {} from {} via TCP", data.len(), block_id, peer_addr);
        Ok(data)
    }

    async fn send_block_to_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        chunks: BlockChunks,
    ) -> Result<u64> {
        let stored = self.pool.send_block(peer_addr, block_id, gen_stamp, chunks).await?;
        debug!("Sent block {} ({} bytes) to {} via TCP", block_id, stored, peer_addr);
        Ok(stored)
    }

    fn transport_type(&self) -> TransportType {
        TransportType::Tcp
    }

    fn listen_addr(&self) -> String {
        self.listen_addr.to_string()
    }

    async fn health_check(&self) -> Result<()> {
        // TCP transport is always healthy if we can bind
        // More sophisticated checks could be added here
        Ok(())
    }

    async fn start_server(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;

        info!("TCP data transfer server listening on {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    debug!("Accepted TCP connection from {}", peer_addr);
                    wire::tune(&socket);
                    let storage = self.block_storage.clone();
                    tokio::spawn(async move {
                        if let Err(e) = wire::serve_connection(socket, storage, peer_addr).await {
                            warn!("Error handling TCP connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting TCP connection: {}", e);
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
    async fn test_tcp_transport_creation() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50010".parse().unwrap();

        let transport = TcpTransport::new(block_storage, addr);
        assert_eq!(transport.transport_type(), TransportType::Tcp);
        assert_eq!(transport.listen_addr(), "127.0.0.1:50010");
    }

    #[tokio::test]
    async fn test_tcp_transport_health_check() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50011".parse().unwrap();

        let transport = TcpTransport::new(block_storage, addr);
        assert!(transport.health_check().await.is_ok());
    }
}
