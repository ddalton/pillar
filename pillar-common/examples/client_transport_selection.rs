//! Example: HDFS Client with Transport Selection (RDMA/TCP)
//!
//! This example demonstrates how an HDFS client uses the transport selection
//! utilities to choose the optimal DataNode endpoint based on capabilities.
//!
//! Run with:
//! ```bash
//! cargo run --example client_transport_selection
//! ```

use pillar_common::hdfs::{
    ClientTransportSelector, DataNodeEndpoint, ClientTransportType,
    TransportSelectionStrategy,
};

fn main() {
    println!("=== Pillar HDFS Client Transport Selection Example ===\n");

    // Scenario 1: Client with RDMA support connecting to mixed cluster
    example_rdma_capable_client();

    println!("\n{}\n", "=".repeat(60));

    // Scenario 2: TCP-only client
    example_tcp_only_client();

    println!("\n{}\n", "=".repeat(60));

    // Scenario 3: Force RDMA mode
    example_force_rdma();
}

fn example_rdma_capable_client() {
    println!("Scenario 1: RDMA-capable client connecting to mixed cluster");
    println!("{}", "-".repeat(60));

    // Simulate receiving block locations from NameNode
    let block_locations = vec![
        DataNodeEndpoint {
            hostname: "dn1.example.com".to_string(),
            uuid: "dn-001".to_string(),
            tcp_port: 50010,
            rdma_port: None,
            supported_transports: vec!["tcp".to_string()],
        },
        DataNodeEndpoint {
            hostname: "dn2.example.com".to_string(),
            uuid: "dn-002".to_string(),
            tcp_port: 50010,
            rdma_port: Some(50011),
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        },
        DataNodeEndpoint {
            hostname: "dn3.example.com".to_string(),
            uuid: "dn-003".to_string(),
            tcp_port: 50010,
            rdma_port: Some(50011),
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        },
    ];

    // Client has RDMA support and prefers RDMA
    let selector = ClientTransportSelector::prefer_rdma(true);

    // Select best DataNode for reading
    let (endpoint, transport, addr) = selector
        .select_for_read(&block_locations)
        .expect("Failed to select endpoint");

    println!("Client capability: RDMA-enabled");
    println!("Strategy: Prefer RDMA");
    println!("Available DataNodes:");
    for dn in &block_locations {
        println!(
            "  - {} (TCP:{}, RDMA:{}, Supports: {:?})",
            dn.hostname,
            dn.tcp_port,
            dn.rdma_port.map_or("N/A".to_string(), |p| p.to_string()),
            dn.supported_transports
        );
    }
    println!("\nSelected:");
    println!("  DataNode: {}", endpoint.hostname);
    println!("  Transport: {:?}", transport);
    println!("  Endpoint: {}", addr);
    println!("\n✅ Result: Connected via RDMA to dn2.example.com:50011");
    println!("   Expected performance: 3-5x faster than TCP");
}

fn example_tcp_only_client() {
    println!("Scenario 2: TCP-only client");
    println!("{}", "-".repeat(60));

    let block_locations = vec![
        DataNodeEndpoint {
            hostname: "dn1.example.com".to_string(),
            uuid: "dn-001".to_string(),
            tcp_port: 50010,
            rdma_port: Some(50011),
            supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
        },
    ];

    // Client without RDMA support
    let selector = ClientTransportSelector::tcp_only();

    let (endpoint, transport, addr) = selector
        .select_for_read(&block_locations)
        .expect("Failed to select endpoint");

    println!("Client capability: TCP-only (no RDMA hardware)");
    println!("Strategy: TCP only");
    println!("Available DataNode: {} (supports both TCP and RDMA)", endpoint.hostname);
    println!("\nSelected:");
    println!("  Transport: {:?}", transport);
    println!("  Endpoint: {}", addr);
    println!("\n✅ Result: Connected via TCP to {}:{}", endpoint.hostname, endpoint.tcp_port);
    println!("   Note: DataNode can serve both RDMA and TCP clients simultaneously");
}

fn example_force_rdma() {
    println!("Scenario 3: Force RDMA mode");
    println!("{}", "-".repeat(60));

    // RDMA-capable DataNode
    let rdma_datanode = vec![DataNodeEndpoint {
        hostname: "dn-rdma.example.com".to_string(),
        uuid: "dn-rdma-001".to_string(),
        tcp_port: 50010,
        rdma_port: Some(50011),
        supported_transports: vec!["tcp".to_string(), "rdma".to_string()],
    }];

    // TCP-only DataNode
    let tcp_datanode = vec![DataNodeEndpoint {
        hostname: "dn-tcp.example.com".to_string(),
        uuid: "dn-tcp-001".to_string(),
        tcp_port: 50010,
        rdma_port: None,
        supported_transports: vec!["tcp".to_string()],
    }];

    // Client requiring RDMA
    let selector = ClientTransportSelector::rdma_only();

    println!("Client capability: RDMA-only mode (strict requirement)");
    println!("Strategy: RDMA only\n");

    // Test 1: RDMA-capable DataNode
    println!("Test 1: Connecting to RDMA-capable DataNode");
    match selector.select_for_read(&rdma_datanode) {
        Ok((endpoint, transport, addr)) => {
            println!("  ✅ Success: Connected via {:?} to {}", transport, addr);
        }
        Err(e) => {
            println!("  ❌ Failed: {}", e);
        }
    }

    // Test 2: TCP-only DataNode
    println!("\nTest 2: Connecting to TCP-only DataNode");
    match selector.select_for_read(&tcp_datanode) {
        Ok((endpoint, transport, addr)) => {
            println!("  ✅ Success: Connected via {:?} to {}", transport, addr);
        }
        Err(e) => {
            println!("  ❌ Failed: {}", e);
            println!("     This is expected - client requires RDMA but DataNode doesn't support it");
        }
    }
}
