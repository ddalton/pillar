//! HDFS client utilities for transport selection and block access
//!
//! This module provides helper functions for HDFS clients to:
//! - Select optimal transport (RDMA vs TCP) based on capabilities
//! - Connect to DataNodes using the appropriate endpoint
//! - Handle heterogeneous clusters with mixed transport support

use std::net::SocketAddr;

/// Transport type for DataNode communication
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientTransportType {
    /// TCP transport (always available)
    Tcp,
    /// RDMA transport (requires RDMA hardware)
    Rdma,
}

/// DataNode endpoint information with transport capabilities
#[derive(Debug, Clone)]
pub struct DataNodeEndpoint {
    pub hostname: String,
    pub uuid: String,
    pub tcp_port: u16,
    pub rdma_port: Option<u16>,
    pub supported_transports: Vec<String>,
}

impl DataNodeEndpoint {
    /// Create endpoint from NameNode's DatanodeId
    pub fn from_datanode_id(
        hostname: String,
        uuid: String,
        tcp_port: u16,
        rdma_port: u16,
        supported_transports: Vec<String>,
    ) -> Self {
        Self {
            hostname,
            uuid,
            tcp_port,
            rdma_port: if rdma_port > 0 { Some(rdma_port) } else { None },
            supported_transports,
        }
    }

    /// Check if this DataNode supports RDMA
    pub fn supports_rdma(&self) -> bool {
        self.supported_transports.contains(&"rdma".to_string())
            && self.rdma_port.is_some()
    }

    /// Check if this DataNode supports TCP (always true)
    pub fn supports_tcp(&self) -> bool {
        self.supported_transports.is_empty()
            || self.supported_transports.contains(&"tcp".to_string())
    }

    /// Get TCP endpoint address
    pub fn tcp_addr(&self) -> String {
        format!("{}:{}", self.hostname, self.tcp_port)
    }

    /// Get RDMA endpoint address if available
    pub fn rdma_addr(&self) -> Option<String> {
        self.rdma_port.map(|port| format!("{}:{}", self.hostname, port))
    }

    /// Get TCP socket address
    pub fn tcp_socket_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.tcp_addr().parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid TCP address: {}", e),
            )
        })
    }

    /// Get RDMA socket address if available
    pub fn rdma_socket_addr(&self) -> Result<Option<SocketAddr>, std::io::Error> {
        if let Some(addr) = self.rdma_addr() {
            Ok(Some(addr.parse().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid RDMA address: {}", e),
                )
            })?))
        } else {
            Ok(None)
        }
    }
}

/// Transport selection strategy for clients
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSelectionStrategy {
    /// Prefer RDMA if available, fallback to TCP
    PreferRdma,
    /// Force TCP only
    TcpOnly,
    /// Force RDMA only (fails if not available)
    RdmaOnly,
}

/// Client transport selector
pub struct ClientTransportSelector {
    /// Does this client support RDMA?
    pub client_supports_rdma: bool,
    /// Selection strategy
    pub strategy: TransportSelectionStrategy,
}

impl ClientTransportSelector {
    /// Create a new transport selector
    pub fn new(client_supports_rdma: bool, strategy: TransportSelectionStrategy) -> Self {
        Self {
            client_supports_rdma,
            strategy,
        }
    }

    /// Create a selector that prefers RDMA (default)
    pub fn prefer_rdma(client_supports_rdma: bool) -> Self {
        Self::new(client_supports_rdma, TransportSelectionStrategy::PreferRdma)
    }

    /// Create a TCP-only selector
    pub fn tcp_only() -> Self {
        Self::new(false, TransportSelectionStrategy::TcpOnly)
    }

    /// Create an RDMA-only selector
    pub fn rdma_only() -> Self {
        Self::new(true, TransportSelectionStrategy::RdmaOnly)
    }

    /// Select transport for a given DataNode endpoint
    ///
    /// Returns (transport_type, endpoint_address)
    pub fn select_transport(
        &self,
        endpoint: &DataNodeEndpoint,
    ) -> Result<(ClientTransportType, String), ClientTransportError> {
        match self.strategy {
            TransportSelectionStrategy::TcpOnly => {
                // Always use TCP
                Ok((ClientTransportType::Tcp, endpoint.tcp_addr()))
            }

            TransportSelectionStrategy::RdmaOnly => {
                // Require RDMA
                if !self.client_supports_rdma {
                    return Err(ClientTransportError::ClientRdmaNotAvailable);
                }

                if !endpoint.supports_rdma() {
                    return Err(ClientTransportError::DataNodeRdmaNotAvailable(
                        endpoint.hostname.clone(),
                    ));
                }

                let rdma_addr = endpoint.rdma_addr().ok_or_else(|| {
                    ClientTransportError::DataNodeRdmaNotAvailable(endpoint.hostname.clone())
                })?;

                Ok((ClientTransportType::Rdma, rdma_addr))
            }

            TransportSelectionStrategy::PreferRdma => {
                // Use RDMA if both client and server support it
                if self.client_supports_rdma && endpoint.supports_rdma() {
                    if let Some(rdma_addr) = endpoint.rdma_addr() {
                        return Ok((ClientTransportType::Rdma, rdma_addr));
                    }
                }

                // Fallback to TCP
                Ok((ClientTransportType::Tcp, endpoint.tcp_addr()))
            }
        }
    }

    /// Select best DataNode from a list of replicas
    ///
    /// Prioritizes:
    /// 1. DataNodes with RDMA (if client supports it)
    /// 2. DataNodes with lowest latency (future enhancement)
    /// 3. First available DataNode
    pub fn select_best_datanode<'a>(
        &self,
        endpoints: &'a [DataNodeEndpoint],
    ) -> Result<&'a DataNodeEndpoint, ClientTransportError> {
        if endpoints.is_empty() {
            return Err(ClientTransportError::NoDataNodesAvailable);
        }

        // If client supports RDMA, prefer RDMA-capable DataNodes
        if self.client_supports_rdma {
            if let Some(rdma_endpoint) = endpoints.iter().find(|e| e.supports_rdma()) {
                return Ok(rdma_endpoint);
            }
        }

        // Otherwise, return first available
        Ok(&endpoints[0])
    }

    /// Select transport and endpoint for reading a block
    ///
    /// Returns (selected_endpoint, transport_type, endpoint_address)
    pub fn select_for_read<'a>(
        &self,
        endpoints: &'a [DataNodeEndpoint],
    ) -> Result<(&'a DataNodeEndpoint, ClientTransportType, String), ClientTransportError> {
        let endpoint = self.select_best_datanode(endpoints)?;
        let (transport, addr) = self.select_transport(endpoint)?;
        Ok((endpoint, transport, addr))
    }

    /// Select transport and endpoint for writing a block
    ///
    /// Returns (selected_endpoint, transport_type, endpoint_address)
    pub fn select_for_write<'a>(
        &self,
        endpoints: &'a [DataNodeEndpoint],
    ) -> Result<(&'a DataNodeEndpoint, ClientTransportType, String), ClientTransportError> {
        // For writes, we need all replicas - return first endpoint
        // (actual write pipeline will handle all replicas)
        self.select_for_read(endpoints)
    }
}

/// Errors that can occur during transport selection
#[derive(Debug, thiserror::Error)]
pub enum ClientTransportError {
    #[error("Client does not support RDMA")]
    ClientRdmaNotAvailable,

    #[error("DataNode {0} does not support RDMA")]
    DataNodeRdmaNotAvailable(String),

    #[error("No DataNodes available for block")]
    NoDataNodesAvailable,

    #[error("Invalid endpoint address: {0}")]
    InvalidAddress(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tcp_endpoint() -> DataNodeEndpoint {
        DataNodeEndpoint {
            hostname: "dn1.example.com".to_string(),
            uuid: "dn-001".to_string(),
            tcp_port: 50010,
            rdma_port: None,
            supported_transports: vec!["tcp".to_string()],
        }
    }

    fn create_rdma_endpoint() -> DataNodeEndpoint {
        DataNodeEndpoint {
            hostname: "dn2.example.com".to_string(),
            uuid: "dn-002".to_string(),
            tcp_port: 50010,
            rdma_port: Some(50011),
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        }
    }

    #[test]
    fn test_endpoint_supports_rdma() {
        let tcp_endpoint = create_tcp_endpoint();
        let rdma_endpoint = create_rdma_endpoint();

        assert!(!tcp_endpoint.supports_rdma());
        assert!(rdma_endpoint.supports_rdma());
        assert!(tcp_endpoint.supports_tcp());
        assert!(rdma_endpoint.supports_tcp());
    }

    #[test]
    fn test_endpoint_addresses() {
        let rdma_endpoint = create_rdma_endpoint();

        assert_eq!(rdma_endpoint.tcp_addr(), "dn2.example.com:50010");
        assert_eq!(rdma_endpoint.rdma_addr(), Some("dn2.example.com:50011".to_string()));
    }

    #[test]
    fn test_transport_selection_prefer_rdma() {
        let selector = ClientTransportSelector::prefer_rdma(true);
        let rdma_endpoint = create_rdma_endpoint();

        let (transport, addr) = selector.select_transport(&rdma_endpoint).unwrap();

        assert_eq!(transport, ClientTransportType::Rdma);
        assert_eq!(addr, "dn2.example.com:50011");
    }

    #[test]
    fn test_transport_selection_tcp_fallback() {
        let selector = ClientTransportSelector::prefer_rdma(true);
        let tcp_endpoint = create_tcp_endpoint();

        let (transport, addr) = selector.select_transport(&tcp_endpoint).unwrap();

        assert_eq!(transport, ClientTransportType::Tcp);
        assert_eq!(addr, "dn1.example.com:50010");
    }

    #[test]
    fn test_transport_selection_tcp_only() {
        let selector = ClientTransportSelector::tcp_only();
        let rdma_endpoint = create_rdma_endpoint();

        let (transport, addr) = selector.select_transport(&rdma_endpoint).unwrap();

        assert_eq!(transport, ClientTransportType::Tcp);
        assert_eq!(addr, "dn2.example.com:50010");
    }

    #[test]
    fn test_transport_selection_rdma_only_success() {
        let selector = ClientTransportSelector::rdma_only();
        let rdma_endpoint = create_rdma_endpoint();

        let (transport, addr) = selector.select_transport(&rdma_endpoint).unwrap();

        assert_eq!(transport, ClientTransportType::Rdma);
        assert_eq!(addr, "dn2.example.com:50011");
    }

    #[test]
    fn test_transport_selection_rdma_only_failure() {
        let selector = ClientTransportSelector::rdma_only();
        let tcp_endpoint = create_tcp_endpoint();

        let result = selector.select_transport(&tcp_endpoint);

        assert!(result.is_err());
        match result.unwrap_err() {
            ClientTransportError::DataNodeRdmaNotAvailable(hostname) => {
                assert_eq!(hostname, "dn1.example.com");
            }
            _ => panic!("Expected DataNodeRdmaNotAvailable error"),
        }
    }

    #[test]
    fn test_select_best_datanode_prefers_rdma() {
        let selector = ClientTransportSelector::prefer_rdma(true);
        let endpoints = vec![create_tcp_endpoint(), create_rdma_endpoint()];

        let best = selector.select_best_datanode(&endpoints).unwrap();

        assert_eq!(best.hostname, "dn2.example.com");
        assert!(best.supports_rdma());
    }

    #[test]
    fn test_select_best_datanode_tcp_only() {
        let selector = ClientTransportSelector::tcp_only();
        let endpoints = vec![create_tcp_endpoint(), create_rdma_endpoint()];

        let best = selector.select_best_datanode(&endpoints).unwrap();

        // Should return first available
        assert_eq!(best.hostname, "dn1.example.com");
    }

    #[test]
    fn test_select_for_read() {
        let selector = ClientTransportSelector::prefer_rdma(true);
        let endpoints = vec![create_tcp_endpoint(), create_rdma_endpoint()];

        let (endpoint, transport, addr) = selector.select_for_read(&endpoints).unwrap();

        assert_eq!(endpoint.hostname, "dn2.example.com");
        assert_eq!(transport, ClientTransportType::Rdma);
        assert_eq!(addr, "dn2.example.com:50011");
    }
}
