# RDMA Implementation - Next Steps

**Last Updated:** 2025-12-20
**Status:** Phase 5 Complete, Ready for Phase 6
**Current Branch:** `main`

---

## Quick Start for New Session

```bash
# Clone and navigate
cd /Users/ddalton/github/pillar

# Check current status
git log --oneline -5
git status

# Verify all tests pass
cargo test --workspace --lib

# Review implementation plan
cat RDMA_IMPLEMENTATION_PLAN.md

# Review this document
cat NEXT_STEPS.md
```

---

## Current Implementation Status

### ✅ Completed (Phases 1-5)

#### **Phase 1: Transport Abstraction Layer** ✅
- **Files Created:**
  - `pillar-datanode/src/transport/mod.rs` - Core trait and types
  - `pillar-datanode/src/transport/tcp.rs` - TCP implementation
  - `pillar-datanode/src/transport/selector.rs` - Transport selection factory

- **Key Achievement:** Clean abstraction allowing multiple transports without changing existing code
- **Tests:** 4/4 passing
- **Commit:** `d75631e`

#### **Phase 2: RDMA Configuration** ✅
- **Files Modified:**
  - `Cargo.toml` - Added `ibverbs = "0.9"` dependency
  - `pillar-datanode/Cargo.toml` - Added RDMA feature flag
  - `pillar-datanode/src/server.rs` - Configuration parsing
  - `pillar-datanode/src/main.rs` - CLI arguments

- **Configuration Options:**
  ```bash
  DATANODE_TRANSPORT=auto|tcp|rdma  # Default: auto
  DATANODE_DATA_PORT=50010          # TCP port
  DATANODE_RDMA_PORT=50011          # RDMA port
  DATANODE_RDMA_QUEUE_DEPTH=128     # RDMA queue depth
  DATANODE_RDMA_BUFFER_POOL_SIZE=1024
  ```

- **Build Commands:**
  ```bash
  cargo build --release              # TCP-only
  cargo build --release --features rdma  # With RDMA
  ```

#### **Phase 3: RDMA Transport Implementation** ✅
- **Files Created:**
  - `pillar-datanode/src/transport/rdma.rs` (950+ lines)

- **Key Components:**
  - `RdmaContext`: Device initialization with ibverbs
  - `RdmaBuffer`: Registered memory regions for DMA
  - `RdmaMemoryPool`: Pre-allocated 1MB buffers (1024 pool size)
  - `RdmaConnection`: Queue Pair management with completion queues
  - `RdmaConnectionPool`: Up to 256 peer connections
  - `RdmaTransport`: Full DataTransport implementation

- **RDMA Operations Implemented:**
  - `post_send()` / `post_recv()` - Two-sided operations
  - `wait_send_completion()` / `wait_recv_completion()` - Async polling
  - Server-side: `serve_read_request()`, `serve_write_request()`
  - Client-side: `read_from_peer()`, `write_to_peer()`

- **Library:** Switched from async-rdma to **ibverbs 0.9.2** (actively maintained, no tokio conflicts)

#### **Phase 4: Capability Advertisement** ✅
- **Files Modified:**
  - `pillar-proto/proto/hdfs.proto` - Added backward-compatible fields
  - `pillar-datanode/src/grpc/namenode_client.rs` - Advertises capabilities
  - `pillar-namenode/src/metadata/block_manager.rs` - Stores capabilities
  - `pillar-namenode/src/grpc/*.rs` - Updated registration/heartbeat

- **Protobuf Changes:**
  ```protobuf
  message DatanodeIDProto {
    // ... existing fields ...
    repeated string supportedTransports = 8;  // ["tcp", "rdma"]
    optional uint32 rdmaPort = 9 [default = 0];
  }
  ```

- **Capability Flow:**
  1. DataNode detects available transports at startup
  2. Advertises via heartbeat: `["tcp", "rdma"]` + RDMA port
  3. NameNode stores in `DatanodeId.supported_transports`
  4. NameNode exposes in block location responses

#### **Phase 5: Client-to-DataNode Transport Selection** ✅
- **Files Created:**
  - `pillar-common/src/hdfs/client.rs` (439 lines)
  - `pillar-common/examples/client_transport_selection.rs` (170 lines)

- **Key Types:**
  - `ClientTransportSelector` - Smart selection with 3 strategies
  - `DataNodeEndpoint` - Clean abstraction for connection info
  - `TransportSelectionStrategy` - PreferRdma | TcpOnly | RdmaOnly

- **Usage Pattern:**
  ```rust
  let selector = ClientTransportSelector::prefer_rdma(true);
  let (endpoint, transport, addr) = selector.select_for_read(&locations)?;
  // transport = Rdma, addr = "dn1.example.com:50011"
  ```

- **Tests:** 10/10 passing, example runs successfully
- **Commit:** `b946cb9`

---

## 📋 Remaining Work (Phases 6-8)

### Phase 6: Inter-DataNode Transport Selection & Replication 🔄

**Goal:** Implement block replication using RDMA with advanced strategies from OSU HiBD research

#### Sub-tasks:

##### 6.1: Basic Pipelined Replication (Baseline)
- **Create:** `pillar-datanode/src/replication/mod.rs`
- **Create:** `pillar-datanode/src/replication/pipeline.rs`

**Implementation:**
```rust
// Traditional HDFS pipeline: DN1 → DN2 → DN3
async fn replicate_block_pipeline(
    block_id: u64,
    targets: Vec<DatanodeId>,
    transport_selector: &TransportSelector,
) -> Result<()> {
    for target in targets {
        let (transport, addr) = transport_selector.select_for_peer(&target);
        let data = block_storage.read_block(block_id, ...).await?;
        transport.write_to_peer(&addr, block_id, 0, &data).await?;
    }
    Ok(())
}
```

##### 6.2: Parallel Replication (OSU HiBD)
- **Expected improvement:** 2.9x with RDMA, 1.3x with TCP
- **Strategy:** Write to all replicas concurrently

**Implementation:**
```rust
// Parallel: DN1 → [DN2, DN3] (concurrent)
async fn replicate_block_parallel(
    block_id: u64,
    targets: Vec<DatanodeId>,
    transport_selector: &TransportSelector,
) -> Result<()> {
    let data = block_storage.read_block(block_id, ...).await?;

    let tasks: Vec<_> = targets.iter().map(|target| {
        let (transport, addr) = transport_selector.select_for_peer(target);
        let data_clone = data.clone();

        tokio::spawn(async move {
            transport.write_to_peer(&addr, block_id, 0, &data_clone).await
        })
    }).collect();

    for task in tasks {
        task.await??;
    }
    Ok(())
}
```

##### 6.3: Overlapped Replication (SOR-HDFS)
- **Expected improvement:** 4.1x with RDMA, 1.5x with TCP
- **Strategy:** Pipeline disk reads, RDMA writes, and acknowledgments

**Implementation:**
```rust
// SEDA-based overlapping stages
async fn replicate_blocks_overlapped(
    blocks: Vec<u64>,
    targets: Vec<DatanodeId>,
) -> Result<()> {
    let (read_tx, read_rx) = mpsc::channel(16);
    let (write_tx, write_rx) = mpsc::channel(16);

    // Stage 1: Disk reads
    let read_task = tokio::spawn(async move {
        for block_id in blocks {
            let data = block_storage.read_block(block_id, ...).await?;
            read_tx.send((block_id, data)).await?;
        }
    });

    // Stage 2: RDMA writes
    let write_task = tokio::spawn(async move {
        while let Some((block_id, data)) = read_rx.recv().await {
            // Parallel write to all targets
            for target in &targets {
                transport.write_to_peer(target, block_id, 0, &data).await?;
            }
            write_tx.send(block_id).await?;
        }
    });

    // Stage 3: Acknowledgments
    let ack_task = tokio::spawn(async move {
        while let Some(block_id) = write_rx.recv().await {
            namenode_client.report_block_received(block_id).await?;
        }
    });

    tokio::try_join!(read_task, write_task, ack_task)?;
    Ok(())
}
```

##### 6.4: Adaptive Strategy Selection
- **Create:** `pillar-datanode/src/replication/strategy.rs`

**Implementation:**
```rust
pub enum ReplicationStrategy {
    Pipeline,    // Sequential (TCP or mixed)
    Parallel,    // Concurrent (RDMA preferred)
    Overlapped,  // SEDA stages (RDMA bulk transfers)
}

impl TransportSelector {
    pub fn select_replication_strategy(
        &self,
        targets: &[DatanodeId],
        block_size: u64,
    ) -> ReplicationStrategy {
        let all_rdma = targets.iter()
            .all(|t| t.supported_transports.contains(&"rdma".to_string()));

        if all_rdma && self.rdma_server.is_some() {
            if block_size > 64 * 1024 * 1024 {  // >64MB
                ReplicationStrategy::Overlapped  // 4.1x
            } else {
                ReplicationStrategy::Parallel    // 2.9x
            }
        } else if targets.len() >= 3 {
            ReplicationStrategy::Parallel  // 1.3x with TCP
        } else {
            ReplicationStrategy::Pipeline  // Baseline
        }
    }
}
```

##### 6.5: NameNode Command Handler
- **Modify:** `pillar-datanode/src/grpc/namenode_client.rs`
- **Implement:** `handle_replicate_blocks()` command

**Implementation:**
```rust
async fn handle_command(&self, command: DatanodeCommand) -> Result<()> {
    match command {
        DatanodeCommand::ReplicateBlocks { blocks, targets } => {
            let strategy = self.transport_selector
                .select_replication_strategy(&targets, block_size);

            match strategy {
                ReplicationStrategy::Pipeline =>
                    replicate_block_pipeline(blocks, targets).await?,
                ReplicationStrategy::Parallel =>
                    replicate_block_parallel(blocks, targets).await?,
                ReplicationStrategy::Overlapped =>
                    replicate_blocks_overlapped(blocks, targets).await?,
            }
        }
        // ... other commands
    }
}
```

##### 6.6: Testing Phase 6
```bash
# Unit tests
cargo test --package pillar-datanode replication::

# Integration test with mixed cluster
# 1. Start 3 DataNodes (1 TCP, 2 RDMA)
# 2. Write block to DN1
# 3. Trigger replication
# 4. Verify DN2/DN3 used RDMA
# 5. Verify DN1 used TCP fallback
```

**Success Criteria:**
- ✅ All three replication strategies implemented
- ✅ Adaptive selection based on capabilities
- ✅ Performance targets met (see RDMA_IMPLEMENTATION_PLAN.md)
- ✅ Tests covering all scenarios

---

### Phase 7: Testing and Benchmarks 🔄

#### 7.1: Unit Tests
**Expand existing test coverage:**

```bash
# Transport layer tests
cargo test --package pillar-datanode transport::

# Client selection tests (already done)
cargo test --package pillar-common hdfs::client::

# Replication tests (new)
cargo test --package pillar-datanode replication::

# NameNode capability tests
cargo test --package pillar-namenode grpc::datanode_protocol_service::
```

#### 7.2: Integration Tests
**Create:** `pillar-datanode/tests/integration/rdma_replication.rs`

```rust
#[tokio::test]
async fn test_heterogeneous_cluster_replication() {
    // Setup: 1 TCP DN + 2 RDMA DNs
    // Test: Block replication uses optimal transport for each peer
    // Verify: RDMA DNs use port 50011, TCP DN uses port 50010
}

#[tokio::test]
async fn test_rdma_fallback_on_failure() {
    // Setup: RDMA DN with simulated RDMA failure
    // Test: Automatic fallback to TCP
    // Verify: Data integrity maintained
}
```

#### 7.3: Performance Benchmarks
**Create:** `pillar-datanode/benches/rdma_throughput.rs`

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_client_read_tcp(c: &mut Criterion) {
    // Benchmark: Client read 1GB block via TCP
    // Target: ~100 MB/s
}

fn bench_client_read_rdma(c: &mut Criterion) {
    // Benchmark: Client read 1GB block via RDMA
    // Target: ~350 MB/s (3.5x improvement)
}

fn bench_replication_tcp_pipeline(c: &mut Criterion) {
    // Benchmark: 3-way replication via TCP
    // Target: ~85 MB/s
}

fn bench_replication_rdma_parallel(c: &mut Criterion) {
    // Benchmark: 3-way parallel replication via RDMA
    // Target: ~250 MB/s (2.9x improvement)
}

criterion_group!(
    benches,
    bench_client_read_tcp,
    bench_client_read_rdma,
    bench_replication_tcp_pipeline,
    bench_replication_rdma_parallel
);
criterion_main!(benches);
```

**Run benchmarks:**
```bash
cargo bench --package pillar-datanode --bench rdma_throughput
```

#### 7.4: Kubernetes/KUTTL Tests
**Create:** `tests/kuttl/rdma-cluster/`

```yaml
# 00-install.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: pillar-rdma-test
---
# Deploy NameNode + 3 DataNodes (2 RDMA, 1 TCP)
```

```yaml
# 01-assert.yaml
# Verify all DataNodes registered with correct capabilities
```

```yaml
# 02-test-write.yaml
# Write 1GB file, verify RDMA used for replication
```

**Run KUTTL tests:**
```bash
kubectl kuttl test --config tests/kuttl/kuttl-test.yaml
```

#### 7.5: Performance Targets Validation

| Operation | Target | Measurement Method |
|-----------|--------|-------------------|
| Client read (RDMA) | 350 MB/s | Benchmark + iperf3 baseline |
| Client write (RDMA) | 320 MB/s | Benchmark + iperf3 baseline |
| Replication parallel (RDMA) | 250 MB/s | Benchmark with 3-way replication |
| Replication overlapped (RDMA) | 350 MB/s | Benchmark with 64MB+ blocks |
| Latency (RDMA) | <200 μs | `rdma_lat` + application benchmark |

---

### Phase 8: Documentation 🔄

#### 8.1: RDMA Deployment Guide
**Create:** `docs/RDMA.md`

**Contents:**
```markdown
# RDMA Support in Pillar HDFS

## Prerequisites
- InfiniBand or RoCE network adapters
- RDMA drivers installed (rdma-core, libibverbs)
- Kubernetes with RDMA device plugin

## Building with RDMA
...

## Configuration
...

## Deployment (Helm)
...

## Troubleshooting
...

## Performance Tuning
...
```

#### 8.2: Client Integration Guide
**Create:** `docs/CLIENT_RDMA.md`

**Contents:**
```markdown
# HDFS Client RDMA Integration

## For Application Developers

### Quick Start
...

### Transport Selection API
...

### Example: Reading a File
...

### Example: Writing a File
...

## Configuration Options
...

## Troubleshooting
...
```

#### 8.3: Update README.md
**Modify:** `README.md`

Add comprehensive RDMA section:
```markdown
## 🚀 RDMA Support

Pillar HDFS supports RDMA (Remote Direct Memory Access) for high-performance
block transfers:

- **Client-to-DataNode:** 3.5x read, 3.6x write improvement
- **Inter-DataNode replication:** 2.9x-4.1x improvement
- **Heterogeneous clusters:** Mixed RDMA/TCP DataNodes
- **Automatic fallback:** Graceful TCP fallback

### Quick Start
...

### Performance
...

See [RDMA.md](docs/RDMA.md) for full documentation.
```

#### 8.4: Helm Chart Updates
**Modify:** `charts/pillar/values.yaml`

```yaml
datanode:
  # Existing configuration
  dataPort: 50010
  rpcPort: 9867
  httpPort: 50075

  # RDMA configuration
  rdma:
    enabled: false
    port: 50011
    queueDepth: 128
    maxInlineData: 256
    bufferPoolSize: 1024

  resources:
    requests:
      cpu: "2"
      memory: "4Gi"
    limits:
      cpu: "4"
      memory: "8Gi"
      # RDMA device (when enabled)
      rdma/hca: 1  # Request 1 RDMA device
```

**Modify:** `charts/pillar/templates/datanode-statefulset.yaml`

```yaml
{{- if .Values.datanode.rdma.enabled }}
env:
  - name: DATANODE_TRANSPORT
    value: "auto"
  - name: DATANODE_RDMA_PORT
    value: "{{ .Values.datanode.rdma.port }}"
resources:
  limits:
    rdma/hca: {{ .Values.datanode.rdma.devices | default 1 }}
{{- end }}
```

#### 8.5: Migration Guide
**Create:** `docs/RDMA_MIGRATION.md`

```markdown
# Migrating to RDMA-enabled Cluster

## Phased Rollout

### Week 1-2: Enable RDMA on 20% of DataNodes
...

### Week 3-4: Expand to 50%
...

### Week 5-6: Complete Rollout
...

## Rollback Procedure
...

## Monitoring
...
```

#### 8.6: Operator Guide
**Create:** `docs/RDMA_OPERATIONS.md`

```markdown
# RDMA Operations Guide

## Health Checks
...

## Metrics
...

## Common Issues
...

## Performance Tuning
...
```

---

## Key Decisions Made

### 1. **Library Choice: ibverbs over async-rdma**
- **Reason:** async-rdma has tokio version conflicts (pinned to 1.29.1)
- **Choice:** ibverbs 0.9.2 (actively maintained, March 2025)
- **Trade-off:** Lower-level API but more stable

### 2. **Transport Abstraction Design**
- **Pattern:** Trait-based with factory selector
- **Benefit:** Clean separation, testable, extensible
- **Location:** `pillar-datanode/src/transport/`

### 3. **Backward Compatibility**
- **Protobuf:** Optional fields only (no breaking changes)
- **Default:** TCP always available, RDMA optional
- **Mixed clusters:** Full support for heterogeneous deployments

### 4. **Configuration Strategy**
- **Default:** `transport=auto` (try RDMA, fallback TCP)
- **Options:** `tcp` (force), `rdma` (require)
- **Ports:** TCP 50010, RDMA 50011

### 5. **Replication Strategies (Phase 6)**
- **Based on:** OSU HiBD research
- **Adaptive:** Select strategy based on capabilities and block size
- **Fallback:** Always support TCP pipeline

---

## Important Context

### OSU HiBD Research Findings
We incorporated research from Ohio State University's High-Performance Big Data project:

1. **Parallel Replication:** 16% TeraGen improvement, 12% TestDFSIO gain
2. **SOR-HDFS (Overlapped):** 64% write throughput improvement
3. **Performance:**
   - RDMA parallel: 2.9x vs TCP
   - RDMA overlapped: 4.1x vs TCP
   - TCP parallel: 1.3x vs TCP pipeline (still beneficial!)

**Sources:**
- https://hibd.cse.ohio-state.edu/
- RDMA Hadoop 3.x User Guide
- IEEE HOTI 2013 research paper

### Architecture Decisions

**Dual Server Approach:**
```
DataNode:
  - TCP Server: 0.0.0.0:50010 (always running)
  - RDMA Server: 0.0.0.0:50011 (when RDMA enabled)
  - Both servers share BlockStorage
  - Clients/peers choose appropriate endpoint
```

**Memory Management:**
```
RDMA Memory Pool:
  - Buffer size: 1MB
  - Pool size: 1024 buffers
  - Pre-registered with RDMA device
  - Reused across connections
```

**Connection Pooling:**
```
RDMA Connection Pool:
  - Max connections: 256 peers
  - Per-connection: Queue Pair + CQs
  - Persistent connections (not per-request)
```

---

## Testing Strategy

### Current Test Coverage

```
pillar-common:      54/54 tests ✅
  - hdfs::client:   10/10 tests ✅ (Phase 5)

pillar-datanode:    4/4 tests ✅
  - transport:      4/4 tests ✅ (Phases 1-3)

pillar-namenode:    8/8 tests ✅
  - grpc:           8/8 tests ✅ (Phase 4)

Total:              66/66 tests passing
```

### Needed Test Coverage (Phase 7)

```
pillar-datanode:
  - replication::pipeline    (new)
  - replication::parallel    (new)
  - replication::overlapped  (new)
  - replication::strategy    (new)

Integration tests:
  - Heterogeneous cluster    (new)
  - RDMA fallback            (new)
  - Block replication        (new)

Benchmarks:
  - Client I/O               (new)
  - Replication throughput   (new)
  - Latency measurements     (new)
```

---

## Quick Commands Reference

### Build
```bash
# TCP-only
cargo build --release

# With RDMA
cargo build --release --features rdma

# Check compilation
cargo check --workspace
```

### Test
```bash
# All tests
cargo test --workspace --lib

# Specific modules
cargo test --package pillar-common hdfs::client::
cargo test --package pillar-datanode transport::
cargo test --package pillar-namenode grpc::

# With RDMA feature
cargo test --package pillar-datanode --features rdma
```

### Examples
```bash
# Client transport selection
cargo run --package pillar-common --example client_transport_selection
```

### Git
```bash
# View recent commits
git log --oneline --graph -10

# Check status
git status

# View specific commit
git show b946cb9  # Phase 5
git show d75631e  # Phases 1-4
git show da249fa  # Updated plan with OSU HiBD
```

---

## Next Session Checklist

### Before Starting
- [ ] Pull latest changes: `git pull origin main`
- [ ] Review this document: `cat NEXT_STEPS.md`
- [ ] Review implementation plan: `cat RDMA_IMPLEMENTATION_PLAN.md`
- [ ] Check all tests pass: `cargo test --workspace --lib`
- [ ] Review Phase 6 strategy in plan (lines 339-563)

### Start Phase 6
- [ ] Create `pillar-datanode/src/replication/mod.rs`
- [ ] Create `pillar-datanode/src/replication/pipeline.rs`
- [ ] Create `pillar-datanode/src/replication/parallel.rs`
- [ ] Create `pillar-datanode/src/replication/overlapped.rs`
- [ ] Create `pillar-datanode/src/replication/strategy.rs`
- [ ] Update `pillar-datanode/src/lib.rs` to include replication module
- [ ] Implement command handler in `namenode_client.rs`
- [ ] Write unit tests for each strategy
- [ ] Write integration tests

### Useful Context
- Main implementation plan: `RDMA_IMPLEMENTATION_PLAN.md`
- OSU HiBD findings: See section "OSU HiBD Research Findings" above
- Current code structure: See section "Completed (Phases 1-5)" above
- Performance targets: See `RDMA_IMPLEMENTATION_PLAN.md` lines 565-584

---

## Contact & Resources

### Documentation
- Implementation Plan: `RDMA_IMPLEMENTATION_PLAN.md`
- This Document: `NEXT_STEPS.md`
- README: `README.md`

### Key Files
- Transport abstraction: `pillar-datanode/src/transport/mod.rs`
- RDMA implementation: `pillar-datanode/src/transport/rdma.rs`
- Client selection: `pillar-common/src/hdfs/client.rs`
- Protobuf definitions: `pillar-proto/proto/hdfs.proto`

### External Resources
- OSU HiBD: https://hibd.cse.ohio-state.edu/
- ibverbs docs: https://docs.rs/ibverbs/0.9.2/
- RDMA programming: https://www.rdmamojo.com/

---

## Summary

**You are here:** Phase 5 complete, ready to start Phase 6

**What's done:**
- ✅ Complete transport abstraction (TCP + RDMA)
- ✅ ibverbs-based RDMA implementation
- ✅ DataNode → NameNode capability advertisement
- ✅ Client transport selection with 3 strategies
- ✅ 66/66 tests passing
- ✅ Working example program

**What's next:**
- 🔄 Phase 6: Inter-DataNode replication (3 strategies: pipeline, parallel, overlapped)
- 🔄 Phase 7: Comprehensive testing and benchmarks
- 🔄 Phase 8: Documentation and deployment guides

**Estimated time:**
- Phase 6: 1-2 weeks (replication implementation)
- Phase 7: 2-3 weeks (testing and benchmarks)
- Phase 8: 1-2 weeks (documentation)
- **Total remaining: 4-7 weeks**

**Key insight for Phase 6:**
The OSU HiBD research shows that parallel and overlapped replication strategies provide significant benefits even with TCP (1.3x and 1.5x respectively), but truly shine with RDMA (2.9x and 4.1x). Implement all three strategies with adaptive selection.

---

Good luck with Phase 6! 🚀
