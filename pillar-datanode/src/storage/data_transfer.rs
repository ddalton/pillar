use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::Bytes;
use thiserror::Error;
use tracing::{info, warn, debug, error};

use super::block_storage::{BlockStorage, BlockId};

#[derive(Error, Debug)]
pub enum DataTransferError {
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Block not found: {0}")]
    BlockNotFound(u64),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, DataTransferError>;

/// HDFS Data Transfer Protocol operations
#[derive(Debug, Clone, Copy)]
pub enum DataTransferOp {
    ReadBlock = 81,
    WriteBlock = 80,
    ReplaceBlock = 82,
    CopyBlock = 83,
    BlockChecksum = 84,
}

impl DataTransferOp {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            80 => Some(Self::WriteBlock),
            81 => Some(Self::ReadBlock),
            82 => Some(Self::ReplaceBlock),
            83 => Some(Self::CopyBlock),
            84 => Some(Self::BlockChecksum),
            _ => None,
        }
    }
}

/// Data Transfer Protocol server
///
/// This handles the TCP-based block I/O protocol between HDFS clients/datanodes
/// Separate from the gRPC protocol used for control operations
pub struct DataTransferServer {
    block_storage: Arc<BlockStorage>,
    listen_addr: SocketAddr,
}

impl DataTransferServer {
    pub fn new(block_storage: Arc<BlockStorage>, listen_addr: SocketAddr) -> Self {
        Self {
            block_storage,
            listen_addr,
        }
    }

    /// Start the data transfer server
    pub async fn start(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;

        info!("Data transfer server listening on {}", self.listen_addr);

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    debug!("Accepted connection from {}", peer_addr);

                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(socket, peer_addr).await {
                            warn!("Error handling connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }
    }

    /// Handle a single connection
    async fn handle_connection(&self, mut socket: TcpStream, peer_addr: SocketAddr) -> Result<()> {
        debug!("Handling connection from {}", peer_addr);

        // Read operation code (1 byte)
        let mut op_code_buf = [0u8; 1];
        socket.read_exact(&mut op_code_buf).await?;

        let op = DataTransferOp::from_u8(op_code_buf[0])
            .ok_or_else(|| DataTransferError::ProtocolError(
                format!("Unknown operation code: {}", op_code_buf[0])
            ))?;

        debug!("Operation: {:?}", op);

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

    /// Handle read block operation
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

        debug!("Read request: block={}, offset={}, length={}", block_id, offset, length);

        let block = BlockId::new(block_id, gen_stamp);

        // Read block data
        let data = self.block_storage.read_block(&block, offset, length)
            .map_err(|_| DataTransferError::BlockNotFound(block_id))?;

        // Send success response (1 byte = 0)
        socket.write_u8(0).await?;

        // Send data length (8 bytes)
        socket.write_u64(data.len() as u64).await?;

        // Send data
        socket.write_all(&data).await?;
        socket.flush().await?;

        info!("Sent {} bytes for block {}", data.len(), block_id);

        Ok(())
    }

    /// Handle write block operation
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

        debug!("Write request: block={}, offset={}, length={}", block_id, offset, length);

        // Read data
        let mut data = vec![0u8; length as usize];
        socket.read_exact(&mut data).await?;

        let block = BlockId::new(block_id, gen_stamp);

        // Create block if it doesn't exist
        if self.block_storage.get_block_metadata(block_id).is_none() {
            self.block_storage.create_block(block)
                .map_err(|e| DataTransferError::ProtocolError(format!("Failed to create block: {}", e)))?;
        }

        // Write block data
        self.block_storage.write_block(&block, offset, &data)
            .map_err(|e| DataTransferError::ProtocolError(format!("Failed to write block: {}", e)))?;

        // Send success response (1 byte = 0)
        socket.write_u8(0).await?;
        socket.flush().await?;

        info!("Received {} bytes for block {}", data.len(), block_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_data_transfer_op_from_u8() {
        assert_eq!(DataTransferOp::from_u8(80).unwrap() as u8, DataTransferOp::WriteBlock as u8);
        assert_eq!(DataTransferOp::from_u8(81).unwrap() as u8, DataTransferOp::ReadBlock as u8);
        assert_eq!(DataTransferOp::from_u8(82).unwrap() as u8, DataTransferOp::ReplaceBlock as u8);
        assert_eq!(DataTransferOp::from_u8(83).unwrap() as u8, DataTransferOp::CopyBlock as u8);
        assert_eq!(DataTransferOp::from_u8(84).unwrap() as u8, DataTransferOp::BlockChecksum as u8);
        assert!(DataTransferOp::from_u8(99).is_none());
    }

    #[test]
    fn test_data_transfer_op_values() {
        assert_eq!(DataTransferOp::WriteBlock as u8, 80);
        assert_eq!(DataTransferOp::ReadBlock as u8, 81);
        assert_eq!(DataTransferOp::ReplaceBlock as u8, 82);
        assert_eq!(DataTransferOp::CopyBlock as u8, 83);
        assert_eq!(DataTransferOp::BlockChecksum as u8, 84);
    }

    #[tokio::test]
    async fn test_data_transfer_server_creation() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50010".parse().unwrap();

        let server = DataTransferServer::new(block_storage, addr);
        assert_eq!(server.listen_addr, addr);
    }
}
