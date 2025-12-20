//! TCP-based data transport implementation

use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::storage::block_storage::{BlockStorage, BlockId};
use super::{DataTransport, DataTransferOp, Result, TransportError, TransportType};

/// TCP-based data transport
///
/// This implements the HDFS data transfer protocol over TCP sockets.
/// It provides both server-side (listening for connections) and client-side
/// (connecting to peers) functionality.
pub struct TcpTransport {
    block_storage: Arc<BlockStorage>,
    listen_addr: SocketAddr,
}

impl TcpTransport {
    pub fn new(block_storage: Arc<BlockStorage>, listen_addr: SocketAddr) -> Self {
        Self {
            block_storage,
            listen_addr,
        }
    }

    /// Handle a single TCP connection
    async fn handle_connection(&self, mut socket: TcpStream, peer_addr: SocketAddr) -> Result<()> {
        debug!("Handling TCP connection from {}", peer_addr);

        // Read operation code (1 byte)
        let mut op_code_buf = [0u8; 1];
        socket.read_exact(&mut op_code_buf).await?;

        let op = DataTransferOp::from_u8(op_code_buf[0])
            .ok_or_else(|| TransportError::ProtocolError(
                format!("Unknown operation code: {}", op_code_buf[0])
            ))?;

        debug!("TCP operation: {:?}", op);

        match op {
            DataTransferOp::ReadBlock => {
                self.handle_read_block(&mut socket).await?;
            }
            DataTransferOp::WriteBlock => {
                self.handle_write_block(&mut socket).await?;
            }
            DataTransferOp::ReplaceBlock => {
                warn!("ReplaceBlock operation not yet implemented");
            }
            DataTransferOp::CopyBlock => {
                warn!("CopyBlock operation not yet implemented");
            }
            DataTransferOp::BlockChecksum => {
                warn!("BlockChecksum operation not yet implemented");
            }
        }

        Ok(())
    }

    /// Handle read block operation over TCP
    async fn handle_read_block(&self, socket: &mut TcpStream) -> Result<()> {
        // Simplified protocol for MVP:
        // 1. Read block ID (8 bytes)
        // 2. Read generation stamp (8 bytes)
        // 3. Read offset (8 bytes)
        // 4. Read length (8 bytes)

        let mut buf = [0u8; 32];
        socket.read_exact(&mut buf).await?;

        let block_id = u64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]);
        let gen_stamp = u64::from_be_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]]);
        let offset = u64::from_be_bytes([buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23]]);
        let length = u64::from_be_bytes([buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31]]);

        debug!("TCP read request: block={}, offset={}, length={}", block_id, offset, length);

        // Use the trait method to read block
        let data = self.serve_read_request(block_id, gen_stamp, offset, length).await?;

        // Send success response (1 byte = 0)
        socket.write_u8(0).await?;

        // Send data length (8 bytes)
        socket.write_u64(data.len() as u64).await?;

        // Send data
        socket.write_all(&data).await?;
        socket.flush().await?;

        info!("Sent {} bytes for block {} via TCP", data.len(), block_id);

        Ok(())
    }

    /// Handle write block operation over TCP
    async fn handle_write_block(&self, socket: &mut TcpStream) -> Result<()> {
        // Simplified protocol for MVP:
        // 1. Read block ID (8 bytes)
        // 2. Read generation stamp (8 bytes)
        // 3. Read offset (8 bytes)
        // 4. Read length (8 bytes)
        // 5. Read data

        let mut buf = [0u8; 32];
        socket.read_exact(&mut buf).await?;

        let block_id = u64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]);
        let gen_stamp = u64::from_be_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]]);
        let offset = u64::from_be_bytes([buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23]]);
        let length = u64::from_be_bytes([buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31]]);

        debug!("TCP write request: block={}, offset={}, length={}", block_id, offset, length);

        // Read data
        let mut data = vec![0u8; length as usize];
        socket.read_exact(&mut data).await?;

        // Use the trait method to write block
        self.serve_write_request(block_id, gen_stamp, offset, &data).await?;

        // Send success response (1 byte = 0)
        socket.write_u8(0).await?;
        socket.flush().await?;

        info!("Received {} bytes for block {} via TCP", data.len(), block_id);

        Ok(())
    }

    /// Connect to a peer DataNode via TCP and perform an operation
    async fn connect_to_peer(&self, peer_addr: &str) -> Result<TcpStream> {
        let socket = TcpStream::connect(peer_addr).await
            .map_err(|e| TransportError::ConnectionError(
                format!("Failed to connect to {}: {}", peer_addr, e)
            ))?;

        debug!("Connected to peer {} via TCP", peer_addr);
        Ok(socket)
    }
}

#[async_trait]
impl DataTransport for TcpTransport {
    async fn serve_read_request(
        &self,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let block = BlockId::new(block_id, gen_stamp);

        // Read block data from storage
        let data = self.block_storage.read_block(&block, offset, length)
            .map_err(|_| TransportError::BlockNotFound(block_id))?;

        Ok(data)
    }

    async fn serve_write_request(
        &self,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let block = BlockId::new(block_id, gen_stamp);

        // Create block if it doesn't exist
        if self.block_storage.get_block_metadata(block_id).is_none() {
            self.block_storage.create_block(block)
                .map_err(|e| TransportError::ProtocolError(format!("Failed to create block: {}", e)))?;
        }

        // Write block data to storage
        self.block_storage.write_block(&block, offset, data)
            .map_err(|e| TransportError::ProtocolError(format!("Failed to write block: {}", e)))?;

        Ok(())
    }

    async fn read_from_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let mut socket = self.connect_to_peer(peer_addr).await?;

        // Send read block operation
        socket.write_u8(DataTransferOp::ReadBlock.to_u8()).await?;

        // Send block request (32 bytes: block_id + gen_stamp + offset + length)
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&block_id.to_be_bytes());
        buf[8..16].copy_from_slice(&gen_stamp.to_be_bytes());
        buf[16..24].copy_from_slice(&offset.to_be_bytes());
        buf[24..32].copy_from_slice(&length.to_be_bytes());
        socket.write_all(&buf).await?;
        socket.flush().await?;

        // Read response status (1 byte)
        let status = socket.read_u8().await?;
        if status != 0 {
            return Err(TransportError::ProtocolError(
                format!("Read failed with status: {}", status)
            ));
        }

        // Read data length (8 bytes)
        let data_len = socket.read_u64().await?;

        // Read data
        let mut data = vec![0u8; data_len as usize];
        socket.read_exact(&mut data).await?;

        debug!("Read {} bytes from peer {} via TCP", data.len(), peer_addr);

        Ok(Bytes::from(data))
    }

    async fn write_to_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<()> {
        let mut socket = self.connect_to_peer(peer_addr).await?;

        // Send write block operation
        socket.write_u8(DataTransferOp::WriteBlock.to_u8()).await?;

        // Send block request (32 bytes: block_id + gen_stamp + offset + length)
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&block_id.to_be_bytes());
        buf[8..16].copy_from_slice(&gen_stamp.to_be_bytes());
        buf[16..24].copy_from_slice(&offset.to_be_bytes());
        buf[24..32].copy_from_slice(&(data.len() as u64).to_be_bytes());
        socket.write_all(&buf).await?;

        // Send data
        socket.write_all(data).await?;
        socket.flush().await?;

        // Read response status (1 byte)
        let status = socket.read_u8().await?;
        if status != 0 {
            return Err(TransportError::ProtocolError(
                format!("Write failed with status: {}", status)
            ));
        }

        debug!("Wrote {} bytes to peer {} via TCP", data.len(), peer_addr);

        Ok(())
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

                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(socket, peer_addr).await {
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
