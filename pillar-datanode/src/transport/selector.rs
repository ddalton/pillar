//! Transport selection and management

use std::sync::Arc;
use tracing::{debug, info};

use super::{DataTransport, TransportType};
use super::tcp::TcpTransport;

#[cfg(feature = "rdma")]
use super::rdma::RdmaTransport;

/// Transport selector for choosing between available transports
///
/// This manages both TCP and RDMA transport instances and provides
/// methods to select the appropriate transport based on peer capabilities.
pub struct TransportSelector {
    /// TCP transport (always available)
    pub tcp_server: Arc<TcpTransport>,

    /// RDMA transport (optional, when feature enabled and hardware available)
    #[cfg(feature = "rdma")]
    pub rdma_server: Option<Arc<RdmaTransport>>,
}

impl TransportSelector {
    /// Create a new transport selector with only TCP
    pub fn new_tcp_only(tcp_server: Arc<TcpTransport>) -> Self {
        info!("Transport selector: TCP only");
        Self {
            tcp_server,
            #[cfg(feature = "rdma")]
            rdma_server: None,
        }
    }

    /// Create a new transport selector with both TCP and RDMA
    #[cfg(feature = "rdma")]
    pub fn new_with_rdma(
        tcp_server: Arc<TcpTransport>,
        rdma_server: Arc<RdmaTransport>,
    ) -> Self {
        info!("Transport selector: TCP + RDMA");
        Self {
            tcp_server,
            rdma_server: Some(rdma_server),
        }
    }

    /// Select transport for connecting to a peer based on their capabilities
    ///
    /// Returns a tuple of (transport, address) where address is formatted
    /// appropriately for the selected transport.
    pub fn select_for_peer(
        &self,
        peer_hostname: &str,
        peer_tcp_port: u16,
        #[allow(unused_variables)]
        peer_rdma_port: Option<u16>,
        #[allow(unused_variables)]
        peer_transports: &[String],
    ) -> (Arc<dyn DataTransport>, String) {
        #[cfg(feature = "rdma")]
        {
            // Check if both we and the peer support RDMA
            if let Some(rdma_server) = &self.rdma_server {
                if let Some(rdma_port) = peer_rdma_port {
                    if peer_transports.contains(&"rdma".to_string()) {
                        let rdma_addr = format!("{}:{}", peer_hostname, rdma_port);
                        debug!("Selected RDMA transport for peer {}", peer_hostname);
                        return (rdma_server.clone(), rdma_addr);
                    }
                }
            }
        }

        // Fallback to TCP
        let tcp_addr = format!("{}:{}", peer_hostname, peer_tcp_port);
        debug!("Selected TCP transport for peer {}", peer_hostname);
        (self.tcp_server.clone(), tcp_addr)
    }

    /// Get list of supported transports for this DataNode
    ///
    /// This is used when advertising capabilities to the NameNode
    pub fn supported_transports(&self) -> Vec<String> {
        let mut transports = vec!["tcp".to_string()];

        #[cfg(feature = "rdma")]
        if self.rdma_server.is_some() {
            transports.push("rdma".to_string());
        }

        transports
    }

    /// Get RDMA port if available
    pub fn rdma_port(&self) -> Option<u16> {
        #[cfg(feature = "rdma")]
        {
            if let Some(rdma_server) = &self.rdma_server {
                // Extract port from listen_addr
                let addr_str = rdma_server.listen_addr();
                if let Ok(addr) = addr_str.parse::<std::net::SocketAddr>() {
                    return Some(addr.port());
                }
            }
        }

        None
    }

    /// Get TCP port
    pub fn tcp_port(&self) -> u16 {
        if let Ok(addr) = self.tcp_server.listen_addr().parse::<std::net::SocketAddr>() {
            addr.port()
        } else {
            50010 // Default fallback
        }
    }

    /// Check if RDMA is available
    pub fn has_rdma(&self) -> bool {
        #[cfg(feature = "rdma")]
        {
            self.rdma_server.is_some()
        }

        #[cfg(not(feature = "rdma"))]
        {
            false
        }
    }

    /// Get TCP transport
    pub fn tcp_transport(&self) -> Arc<dyn DataTransport> {
        self.tcp_server.clone()
    }

    /// Get RDMA transport (if available)
    #[cfg(feature = "rdma")]
    pub fn rdma_transport(&self) -> Option<Arc<dyn DataTransport>> {
        self.rdma_server.as_ref().map(|s| s.clone() as Arc<dyn DataTransport>)
    }

    /// Start all available transport servers
    ///
    /// This spawns background tasks for each transport server
    pub async fn start_all_servers(&self) {
        // Start TCP server
        let tcp_server = self.tcp_server.clone();
        tokio::spawn(async move {
            if let Err(e) = tcp_server.start_server().await {
                tracing::error!("TCP server error: {}", e);
            }
        });

        // Start RDMA server if available
        #[cfg(feature = "rdma")]
        if let Some(rdma_server) = &self.rdma_server {
            let rdma_server = rdma_server.clone();
            tokio::spawn(async move {
                if let Err(e) = rdma_server.start_server().await {
                    tracing::error!("RDMA server error: {}", e);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::block_storage::BlockStorage;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_transport_selector_tcp_only() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50010".parse().unwrap();
        let tcp_transport = Arc::new(TcpTransport::new(block_storage, addr));

        let selector = TransportSelector::new_tcp_only(tcp_transport);

        assert_eq!(selector.supported_transports(), vec!["tcp"]);
        assert_eq!(selector.tcp_port(), 50010);
        assert_eq!(selector.rdma_port(), None);
        assert!(!selector.has_rdma());
    }

    #[test]
    fn test_transport_selection() {
        let temp_dir = TempDir::new().unwrap();
        let finalized_dir = temp_dir.path().join("finalized");
        let tmp_dir = temp_dir.path().join("tmp");

        fs::create_dir_all(&finalized_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let block_storage = Arc::new(BlockStorage::new(&finalized_dir, &tmp_dir).unwrap());
        let addr = "127.0.0.1:50010".parse().unwrap();
        let tcp_transport = Arc::new(TcpTransport::new(block_storage, addr));

        let selector = TransportSelector::new_tcp_only(tcp_transport);

        // Test peer selection with TCP-only
        let (transport, addr) = selector.select_for_peer(
            "datanode1.example.com",
            50010,
            None,
            &["tcp".to_string()],
        );

        assert_eq!(transport.transport_type(), TransportType::Tcp);
        assert_eq!(addr, "datanode1.example.com:50010");
    }
}
