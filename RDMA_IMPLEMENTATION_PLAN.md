# RDMA Support Implementation Plan for Pillar HDFS

## Overview

This plan details the implementation of RDMA (Remote Direct Memory Access) support for both **client-to-DataNode** and **inter-DataNode** communication in Pillar HDFS, with heterogeneous cluster support allowing mixed RDMA/TCP deployments.

## Goals

1. **Client-to-DataNode RDMA**: Enable HDFS clients to read/write blocks via RDMA for maximum performance
2. **Inter-DataNode RDMA**: Enable block replication via RDMA for 3-5x throughput improvement
3. **Heterogeneous cluster support**: Enable clusters with both RDMA-capable and TCP-only DataNodes
4. **Per-connection transport negotiation**: Automatically use RDMA when both endpoints support it, fallback to TCP otherwise
5. **Smart block placement**: NameNode prefers RDMA-capable nodes for new block allocations
6. **Gradual migration**: Support incremental upgrade from TCP-only to RDMA-enabled clusters

## Architecture Design

### Transport Abstraction Layer

```rust
// pillar-datanode/src/transport/mod.rs
#[async_trait]
pub trait DataTransport: Send + Sync {
    /// Read block data from this DataNode (server side)
    async fn serve_read_request(
        &self,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes>;

    /// Write block data to this DataNode (server side)
    async fn serve_write_request(
        &self,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<()>;

    /// Read block data from remote DataNode (client side)
    async fn read_from_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes>;

    /// Write block data to remote DataNode (client side)
    async fn write_to_peer(
        &self,
        peer_addr: &str,
        block_id: u64,
        gen_stamp: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<()>;

    /// Get transport type identifier
    fn transport_type(&self) -> TransportType;

    /// Get listening address for this transport
    fn listen_addr(&self) -> String;

    /// Check if transport is available/healthy
    async fn health_check(&self) -> Result<()>;

    /// Start the transport server (listening for incoming connections)
    async fn start_server(self: Arc<Self>) -> Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransportType {
    Tcp,
    #[cfg(feature = "rdma")]
    Rdma,
}
```

### DataNode Transport Endpoints

Each DataNode exposes **both** TCP and RDMA endpoints (when RDMA enabled):

```
DataNode Pod:
  - TCP Data Transfer Server:  0.0.0.0:50010 (always available)
  - RDMA Data Transfer Server: 0.0.0.0:50011 (when RDMA enabled)
  - gRPC Server:                0.0.0.0:9867  (control plane)
  - HTTP Server:                0.0.0.0:50075 (web UI)
```

Clients and other DataNodes can connect to either endpoint based on their capabilities.

### Capability Advertisement

DataNodes advertise their supported transports AND endpoint addresses:

```protobuf
// pillar-proto/proto/hdfs.proto
message DatanodeInfoProto {
    required DatanodeIDProto id = 1;
    optional string ipAddr = 2;
    optional string hostName = 3;
    optional uint32 xferPort = 4;       // TCP data transfer port (50010)
    optional uint32 infoPort = 5;       // HTTP port
    optional uint32 ipcPort = 6;        // gRPC port
    optional uint32 infoSecurePort = 7;

    // NEW: RDMA endpoint information
    optional uint32 rdmaPort = 8;       // RDMA data transfer port (50011)
    repeated string supportedTransports = 9; // ["tcp", "rdma"]
}

message HeartbeatRequestProto {
    required DatanodeRegistrationProto registration = 1;
    repeated StorageReportProto reports = 2;
    optional uint64 cacheCapacity = 3;
    optional uint64 cacheUsed = 4;
    optional uint32 xmitsInProgress = 5;
    optional uint32 xceiverCount = 6;
    optional uint32 failedVolumes = 7;
    optional VolumeFailureSummaryProto volumeFailureSummary = 8;

    // NEW: Advertise supported transports
    repeated string supportedTransports = 9;
    optional uint32 rdmaPort = 10;
}
```

### Client Transport Discovery Flow

```
1. Client connects to NameNode (gRPC)
   Client: "I want to read block 12345"

2. NameNode returns DataNode locations with transport info
   NameNode: "Block 12345 is on DataNode1, DataNode2, DataNode3"

   LocatedBlockProto {
     block: { blockId: 12345, ... }
     locs: [
       DatanodeInfoProto {
         hostName: "dn1.example.com"
         xferPort: 50010              // TCP endpoint
         rdmaPort: 50011              // RDMA endpoint
         supportedTransports: ["tcp", "rdma"]
       },
       ...
     ]
   }

3. Client selects transport based on capabilities
   - If client supports RDMA AND DataNode supports RDMA:
     Connect to dn1.example.com:50011 via RDMA
   - Else:
     Connect to dn1.example.com:50010 via TCP

4. Client reads block using selected transport
```

### Transport Selection Algorithm

```rust
// pillar-datanode/src/transport/selector.rs
pub struct TransportSelector {
    tcp_server: Arc<TcpTransport>,
    #[cfg(feature = "rdma")]
    rdma_server: Option<Arc<RdmaTransport>>,
}

impl TransportSelector {
    /// Select transport for connecting to a peer DataNode
    pub fn select_for_peer(&self, peer_info: &DatanodeInfoProto) -> (Arc<dyn DataTransport>, String) {
        #[cfg(feature = "rdma")]
        if let Some(rdma) = &self.rdma_server {
            if peer_info.supported_transports.contains(&"rdma".to_string()) {
                let rdma_addr = format!("{}:{}", peer_info.ip_addr, peer_info.rdma_port.unwrap_or(50011));
                return (rdma.clone(), rdma_addr);
            }
        }

        // Fallback to TCP
        let tcp_addr = format!("{}:{}", peer_info.ip_addr, peer_info.xfer_port);
        (self.tcp_server.clone(), tcp_addr)
    }

    /// Get available transports for advertising
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
        if let Some(rdma) = &self.rdma_server {
            return Some(rdma.config.port);
        }

        None
    }
}
```

## Implementation Phases

### Phase 1: Transport Abstraction Layer

**Goal**: Create transport abstraction without changing existing TCP functionality

**Files to create:**
- `pillar-datanode/src/transport/mod.rs`
- `pillar-datanode/src/transport/tcp.rs`
- `pillar-datanode/src/transport/selector.rs`

**Changes:**
1. Define `DataTransport` trait with server and client methods
2. Refactor existing TCP code from `storage/data_transfer.rs`
3. Implement trait for `TcpTransport`
4. Create `TransportSelector` factory

**Acceptance criteria:**
- All existing tests pass
- TCP functionality unchanged
- Clean trait-based architecture

---

### Phase 2: Configuration and Compilation Features

**Goal**: Add RDMA feature flag and configuration

**Files to modify:**
- `Cargo.toml` (workspace root)
- `pillar-datanode/Cargo.toml`
- `pillar-datanode/src/config.rs`

**Configuration:**

```rust
pub struct DataNodeConfig {
    pub transport: String,  // "auto", "tcp", "rdma"
    pub data_addr: SocketAddr,  // TCP: 0.0.0.0:50010

    #[cfg(feature = "rdma")]
    pub rdma_addr: SocketAddr,  // RDMA: 0.0.0.0:50011

    #[cfg(feature = "rdma")]
    pub rdma_queue_depth: u32,  // Default: 128

    #[cfg(feature = "rdma")]
    pub rdma_buffer_pool_size: usize,  // Default: 1024
}
```

**Environment variables:**
- `DATANODE_TRANSPORT` - "auto", "tcp", "rdma"
- `DATANODE_DATA_PORT` - TCP port (50010)
- `DATANODE_RDMA_PORT` - RDMA port (50011)

---

### Phase 3: RDMA Transport Implementation

**Goal**: Implement RDMA transport using async-rdma

**Files to create:**
- `pillar-datanode/src/transport/rdma.rs`
- `pillar-datanode/src/transport/rdma_pool.rs`

**Key implementation details:**
- RDMA server listening on port 50011
- RDMA client for connecting to peers
- Memory pool for RDMA buffers (pre-allocated, registered)
- Connection pool for peer DataNodes
- Implement `DataTransport` trait

**Operations:**
- `serve_read_request()` - Server-side block reads
- `serve_write_request()` - Server-side block writes
- `read_from_peer()` - Client-side block reads
- `write_to_peer()` - Client-side block writes

---

### Phase 4: Capability Advertisement

**Goal**: DataNodes advertise capabilities, NameNode tracks them

**Files to modify:**
- `pillar-proto/proto/hdfs.proto`
- `pillar-datanode/src/grpc/namenode_client.rs`
- `pillar-namenode/src/datanode/manager.rs`

**Changes:**
1. Update protobuf with `rdmaPort` and `supportedTransports`
2. DataNode heartbeat includes transport capabilities
3. NameNode tracks per-DataNode transport support
4. NameNode returns transport info in block location responses
5. Smart placement: prefer RDMA-capable nodes

**NameNode DataNode tracking:**
```rust
pub struct DatanodeDescriptor {
    // ... existing fields ...
    pub supported_transports: HashSet<TransportType>,
    pub rdma_port: Option<u16>,
}
```

---

### Phase 5: Client-to-DataNode Transport Selection

**Goal**: Clients can discover and use RDMA

**Changes:**
1. DataNode runs both TCP and RDMA servers simultaneously
2. NameNode includes both endpoints in `DatanodeInfoProto`
3. Clients receive transport info in block locations
4. Clients connect to appropriate endpoint based on capabilities

**Flow:**
- RDMA-capable client → connects to `rdmaPort` (50011)
- TCP-only client → connects to `xferPort` (50010)
- DataNode serves request on connected transport

---

### Phase 6: Inter-DataNode Transport Selection

**Goal**: Block replication uses RDMA when available

**Files to create/modify:**
- `pillar-datanode/src/replication/pipeline.rs`
- `pillar-datanode/src/grpc/namenode_client.rs`

**Changes:**
1. Implement `handle_replicate_blocks()` command handler
2. Select transport based on target DataNode capabilities
3. Use RDMA when both source and target support it
4. Fallback to TCP otherwise

**Replication flow:**
```rust
async fn handle_replicate_blocks(blocks: Vec<u64>, targets: Vec<DatanodeInfoProto>) {
    for (block_id, target) in blocks.iter().zip(targets) {
        // Select transport
        let (transport, addr) = selector.select_for_peer(&target);

        // Read local block
        let data = block_storage.read_block(block_id, ...).await?;

        // Write to peer
        transport.write_to_peer(&addr, block_id, 0, &data).await?;
    }
}
```

---

### Phase 7: Testing and Validation

**Test scenarios:**

1. **Unit tests:**
   - Transport implementations
   - Configuration parsing
   - Transport selection logic

2. **Integration tests:**
   - TCP-only cluster
   - RDMA-only cluster
   - Mixed RDMA/TCP cluster
   - Client scenarios (all combinations)
   - Replication scenarios (all combinations)
   - Fallback behavior

3. **Performance benchmarks:**
   - Client read/write (TCP vs RDMA)
   - Block replication (TCP vs RDMA)
   - Concurrent operations

4. **Kubernetes tests (KUTTL):**
   - Mixed cluster deployment
   - Block placement verification
   - Replication verification

**Performance targets:**
- Client read: 3.5x improvement
- Client write: 3.6x improvement
- Replication: 3.3x improvement

---

### Phase 8: Documentation and Deployment

**Documentation:**
1. `RDMA.md` - Comprehensive RDMA guide
2. `CLIENT_RDMA.md` - Client integration guide
3. Update `README.md` with RDMA features
4. Migration guide for operators
5. Helm chart updates

**Helm chart changes:**
```yaml
datanode:
  dataPort: 50010  # TCP
  rdma:
    enabled: false
    port: 50011
    queueDepth: 128
  resources:
    limits:
      rdma/hca: 1  # When RDMA enabled
```

---

## Configuration Summary

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATANODE_TRANSPORT` | `auto` | Transport mode: `auto`, `tcp`, or `rdma` |
| `DATANODE_DATA_PORT` | `50010` | TCP listening port |
| `DATANODE_RDMA_PORT` | `50011` | RDMA listening port |
| `DATANODE_RDMA_QUEUE_DEPTH` | `128` | RDMA queue depth |
| `DATANODE_RDMA_BUFFER_POOL_SIZE` | `1024` | Pre-allocated buffers |

### Transport Selection Logic

**DataNode startup:**
1. Always start TCP server on port 50010
2. Try to start RDMA server on port 50011 (if enabled)
3. Advertise available transports in heartbeat

**Client connections:**
1. Query NameNode for block locations
2. Receive DatanodeInfoProto with transport info
3. Select transport based on capabilities
4. Connect to appropriate port

**Inter-DataNode replication:**
1. Receive ReplicateBlocks command from NameNode
2. Select transport for each target
3. Use RDMA if both nodes support it, else TCP
4. Transfer block using selected transport

## File Structure

```
pillar-datanode/src/
  transport/
    mod.rs          - Trait, types, TransportSelector
    tcp.rs          - TCP implementation
    rdma.rs         - RDMA implementation (cfg feature)
    rdma_pool.rs    - Memory pool (cfg feature)
  replication/
    pipeline.rs     - Block replication
  config.rs         - RDMA configuration

pillar-namenode/src/
  datanode/
    manager.rs      - Transport tracking, smart placement

pillar-proto/proto/
  hdfs.proto        - Updated with RDMA fields

docs/
  RDMA.md           - RDMA guide
  CLIENT_RDMA.md    - Client integration
```

## Dependencies

```toml
# pillar-datanode/Cargo.toml
[dependencies]
async-rdma = { version = "0.4", optional = true }

[features]
rdma = ["dep:async-rdma"]
```

**Build commands:**
```bash
# TCP-only
cargo build --release

# With RDMA
cargo build --release --features rdma
```

## Performance Targets

| Operation | TCP | RDMA | Improvement |
|-----------|-----|------|-------------|
| Client read (1GB) | 100 MB/s | 350 MB/s | 3.5x |
| Client write (1GB) | 90 MB/s | 320 MB/s | 3.6x |
| Replication (3-way) | 85 MB/s | 280 MB/s | 3.3x |
| Latency (4KB) | 500 μs | 150 μs | 3.3x |

## Migration Path

**Week 1-2:** Enable RDMA on 20% of DataNodes
- Build with `--features rdma`
- Set `DATANODE_TRANSPORT=auto`
- Monitor RDMA link usage

**Week 3-4:** Expand to 50% of cluster
- Verify smart placement
- Monitor performance improvements

**Week 5-6:** Complete rollout
- All nodes RDMA-capable
- Full cluster performance gains

**Rollback:** Set `DATANODE_TRANSPORT=tcp` on any node to disable RDMA

## Success Criteria

1. ✅ Client-to-DataNode RDMA functional
2. ✅ Inter-DataNode RDMA functional
3. ✅ Mixed clusters work correctly
4. ✅ 3-5x throughput improvement on RDMA links
5. ✅ Zero data corruption
6. ✅ Graceful TCP fallback
7. ✅ Complete documentation
8. ✅ Production deployment validated
9. ✅ Backward compatible with TCP-only deployments

## Timeline

- Phase 1: 1 week (abstraction)
- Phase 2: 1 week (configuration)
- Phase 3: 3 weeks (RDMA implementation)
- Phase 4: 1 week (capability advertisement)
- Phase 5: 1 week (client transport)
- Phase 6: 1 week (replication transport)
- Phase 7: 2-3 weeks (testing)
- Phase 8: 1-2 weeks (documentation)

**Total: 11-14 weeks**
