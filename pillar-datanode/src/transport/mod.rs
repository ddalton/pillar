//! Data transport layer for block I/O operations
//!
//! This module provides an abstraction over different transport protocols (TCP, RDMA)
//! for block data transfers between HDFS clients and DataNodes, as well as between
//! DataNodes for replication.

use std::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod tcp;
pub mod selector;

#[cfg(feature = "rdma")]
pub mod rdma;

/// Errors that can occur during data transport operations
#[derive(Error, Debug)]
pub enum TransportError {
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Block not found: {0}")]
    BlockNotFound(u64),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Transport unavailable: {0}")]
    TransportUnavailable(String),

    #[cfg(feature = "rdma")]
    #[error("RDMA error: {0}")]
    RdmaError(String),
}

pub type Result<T> = std::result::Result<T, TransportError>;

/// Transport type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransportType {
    Tcp,
    #[cfg(feature = "rdma")]
    Rdma,
}

impl std::fmt::Display for TransportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportType::Tcp => write!(f, "tcp"),
            #[cfg(feature = "rdma")]
            TransportType::Rdma => write!(f, "rdma"),
        }
    }
}

impl TransportType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "tcp" => Some(TransportType::Tcp),
            #[cfg(feature = "rdma")]
            "rdma" => Some(TransportType::Rdma),
            _ => None,
        }
    }
}

/// Abstract data transport interface
///
/// This trait abstracts over different transport protocols (TCP, RDMA) for block I/O.
/// Each transport implementation provides both server-side (serving requests) and
/// client-side (connecting to peers) functionality.
#[async_trait]
pub trait DataTransport: Send + Sync {
    /// Serve a read request from a client or peer DataNode (server side)
    ///
    /// This is called by the transport server when it receives a read request.
    /// The implementation should read the block from storage and return it.
    async fn serve_read_request(
        &self,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes>;

    /// Serve a write request from a client or peer DataNode (server side)
    ///
    /// This is called by the transport server when it receives a write request.
    /// The implementation should write the data to storage.
    async fn serve_write_request(
        &self,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<()>;

    /// Read block data from a remote DataNode (client side)
    ///
    /// This is used for block replication or client reads. The implementation
    /// should connect to the peer DataNode and read the block.
    async fn read_from_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes>;

    /// Write block data to a remote DataNode (client side)
    ///
    /// This is used for block replication. The implementation should connect
    /// to the peer DataNode and write the block.
    async fn write_to_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<()>;

    /// Get the transport type identifier
    fn transport_type(&self) -> TransportType;

    /// Get the listening address for this transport
    fn listen_addr(&self) -> String;

    /// Check if the transport is healthy and available
    async fn health_check(&self) -> Result<()>;

    /// Start the transport server (listening for incoming connections)
    ///
    /// This spawns the server loop that accepts incoming connections and
    /// handles requests. It runs indefinitely until an error occurs.
    async fn start_server(self: Arc<Self>) -> Result<()>;
}

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

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}
