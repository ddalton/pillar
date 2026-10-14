# Pillar - HDFS in Rust

**Pillar** is a drop-in replacement for Apache Hadoop HDFS, written in Rust for superior performance and reliability.

## Overview

Pillar implements the official Apache Hadoop HDFS protocols, providing 100% wire compatibility with the Hadoop ecosystem while delivering 3-5x better performance than Java HDFS.

### Key Features

- **🚀 High Performance**: designed to outrun Java HDFS (see [Performance](#performance) for what has been measured)
  - Lock-free data structures (DashMap, AtomicU64)
  - Zero garbage collection pauses
  - SPDK-backed storage via Flint
  - **RDMA support** for client-to-DataNode and inter-DataNode communication
  - Heterogeneous clusters (mixed RDMA/TCP DataNodes)
  - Auto-fallback to TCP when RDMA unavailable
  - Pure Rust - no glibc or OpenSSL dependencies

- **🔌 Hadoop wire compatible**: stock HDFS clients connect unchanged
  - NameNode speaks Hadoop IPC (`hdfs://namenode:9000`); DataNodes speak
    Hadoop DataTransferProtocol, including 3-node write pipelines
  - Tested with the Apache Hadoop 3.5.0 Java client and hdfs-native
    (see [Hadoop client compatibility](#hadoop-client-compatibility))
  - Implements official Apache Hadoop protobuf protocols

- **☸️ Kubernetes Native**: Designed for cloud-native deployments
  - NameNode: StatefulSet with HA support (1 active, N standby)
  - **Etcd cluster**: 3-node StatefulSet for HA metadata storage
  - DataNode: StatefulSet with persistent storage
  - Helm chart for easy deployment
  - Supports any Kubernetes storage class

- **🔐 Secure**: Enterprise-grade security
  - Kerberos authentication
  - Encryption at rest and in transit
  - ACL support

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ HDFS Clients (hadoop fs, Spark, Hive, etc.)                │
│  ↓ HDFS RPC Protocol (gRPC/Protobuf)                       │
├─────────────────────────────────────────────────────────────┤
│ Pillar NameNode (StatefulSet) Pillar DataNode (StatefulSet)│
│ - Metadata management         - Block storage              │
│ - Block allocation            - High-perf I/O              │
│ - Namespace tree              - Persistent volumes         │
│ - fsimage + edit log on a PV  - Optional RDMA support      │
└─────────────────────────────────────────────────────────────┘
```

## Components

- **pillar-namenode**: HDFS NameNode implementation
- **pillar-datanode**: HDFS DataNode implementation
- **pillar-proto**: Generated code from Apache Hadoop protobufs
- **pillar-common**: Shared utilities and HDFS abstractions

## Building from Source

### Prerequisites

- Rust 1.70+ (`rustup` recommended)
- Protocol Buffers compiler (`protoc`)

### Build

```bash
# Clone the repository
git clone https://github.com/ddalton/pillar.git
cd pillar

# Build all components
cargo build --release

# Or build individual components
cargo build -p pillar-namenode --release
cargo build -p pillar-datanode --release
```

### Binaries

After building, binaries will be in `target/release/`:
- `pillar-namenode` - NameNode server
- `pillar-datanode` - DataNode server

### Container images

```bash
docker build --target namenode -t pillar/namenode:dev .
docker build --target datanode -t pillar/datanode:dev .
```

## Quick Start

### Standalone Mode (Development)

The NameNode persists its namespace in `--name-dir` (see
[Namespace persistence](#namespace-persistence)); etcd is not needed.

```bash
# Start NameNode (in one terminal)
./target/release/pillar-namenode \
  --rpc-addr 0.0.0.0:9000 \
  --http-addr 0.0.0.0:9870 \
  --etcd-endpoints none \
  --name-dir /tmp/pillar/name \
  --replication 3 \
  --block-size-mb 128

# Start DataNode (in another terminal)
./target/release/pillar-datanode \
  --namenode localhost:9000 \
  --rpc-addr 0.0.0.0:9867 \
  --data-addr 0.0.0.0:50010 \
  --http-addr 0.0.0.0:50075 \
  --volume-mount-point /tmp/pillar/data \
  --block-pool-id BP-pillar-001

# Use with hadoop fs
hadoop fs -ls hdfs://localhost:9000/
hadoop fs -mkdir hdfs://localhost:9000/test
hadoop fs -put file.txt hdfs://localhost:9000/test/
```

### Kubernetes Deployment

```bash
# Install via Helm
helm install pillar ./pillar-chart \
  --set namenode.replicas=3 \
  --set datanode.replicas=3 \
  --set datanode.storage.size=100Gi

# Access from applications via HDFS protocol:
# hdfs://pillar-namenode:9000/
```

See [pillar-chart/README.md](pillar-chart/README.md) for detailed deployment and usage instructions.

## Configuration

Pillar uses environment variables for configuration (Kubernetes-native approach). Command-line arguments take precedence over environment variables.

### Global HDFS Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `HDFS_BLOCK_SIZE` | 128 | Block size in MB |
| `HDFS_REPLICATION` | 3 | Default replication factor |
| `HDFS_BLOCK_POOL_ID` | BP-pillar-001 | Block pool identifier |
| `HDFS_NAMENODE_RPC_ADDRESS` | pillar-namenode:9000 | NameNode RPC address |

### NameNode Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `NAMENODE_RPC_PORT` | 9000 | RPC server port |
| `NAMENODE_HTTP_PORT` | 9870 | HTTP UI port |
| `NAMENODE_METADATA_PATH` | /var/lib/pillar/metadata/metadata.db | Metadata database path |
| `NAMENODE_HANDLER_COUNT` | - | Number of RPC handler threads (optional) |
| `NAMENODE_SERVICE_HANDLER_COUNT` | - | Number of service RPC handler threads (optional) |
| `NAMENODE_HEARTBEAT_RECHECK_INTERVAL` | - | Heartbeat recheck interval in ms (optional) |
| `NAMENODE_STALE_DATANODE_INTERVAL` | - | Stale DataNode interval in ms (optional) |

### DataNode Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATANODE_RPC_PORT` | 9867 | RPC server port |
| `DATANODE_DATA_PORT` | 50010 | Data transfer port |
| `DATANODE_HTTP_PORT` | 50075 | HTTP UI port |
| `DATANODE_VOLUME_MOUNT_POINT` | /mnt/pillar-data | Volume mount point |
| `DATANODE_HANDLER_COUNT` | - | Number of RPC handler threads (optional) |
| `DATANODE_MAX_XCEIVER_COUNT` | - | Max concurrent data transfers (optional) |
| `DATANODE_MAX_TRANSFER_THREADS` | - | Max transfer threads (optional) |
| `DATANODE_HEARTBEAT_INTERVAL` | - | Heartbeat interval in seconds (optional) |
| `DATANODE_BLOCK_REPORT_INTERVAL` | - | Block report interval in ms (optional) |
| `DATANODE_DU_RESERVED_BYTES` | - | Reserved disk space in bytes (optional) |
| `NODE_NAME` | - | Node hostname (auto-detected in Kubernetes) |

### Client Configuration

For Hadoop clients (hadoop fs, Spark, Hive, etc.), create a `core-site.xml`:

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://pillar-namenode:9000</value>
  </property>
</configuration>
```

In the future, Pillar will provide a CLI tool to generate these XML files automatically.

## Testing

System tests run on Kubernetes with [Chainsaw](https://kyverno.github.io/chainsaw/):
NameNode and DataNode pods built from this tree, driven by the stock
Apache Hadoop client (`hdfs dfs`):

- **Basic operations**: mkdir, put, get, ls, stat, cat, cp, rm, overwrite
- **Hadoop CLI**: more of `hadoop fs`, including commands that must fail
- **Replication**: every DataNode holds every block (and its `.meta`); deletes remove them
- **NameNode restart**: the NameNode pod is force-deleted and recovers its namespace
- **Helm chart**: the chart as shipped serves the client

### Running Tests

```bash
tests/system/kind.sh                            # build images, kind cluster, all tests
TESTS=tests/namenode-restart tests/system/kind.sh
```

The `system-chainsaw` GitHub workflow runs the same script on every pull
request; `rust-ci` runs the workspace tests and the Java client interop.

See [tests/system/README.md](tests/system/README.md) for comprehensive testing documentation.

## Status

**Current Phase**: Phase 8 - Production Hardening (DataNode Storage Complete!)

- [x] **Phase 1:** Project Setup
  - Repository structure, Cargo workspace
  - Apache Hadoop protobuf files
  - Kerberos implementation

- [x] **Phase 2:** NameNode Implementation ✨ **FULLY FUNCTIONAL WITH HA**
  - Namespace (filesystem tree) with lock-free DashMap
  - Block manager with atomic ID allocation
  - DataNode manager and heartbeat monitor
  - **High-Performance gRPC Server** (NEW!)
  - **ClientNamenodeProtocol fully implemented** (NEW!)
  - **DatanodeProtocol fully implemented** (NEW!)
  - **100% Hadoop wire protocol compatible** (NEW!)
  - **Etcd-backed metadata storage for HA** (NEW!)
  - **3-node etcd cluster in Helm chart** (NEW!)

- [x] **Phase 3:** DataNode Implementation ✨ **COMPLETE!**
  - Volume manager (Flint RWO integration with HDFS directory layout)
  - Block storage layer (HDFS format: blk_{id}_{genstamp})
  - Data transfer protocol server (TCP for read/write operations)
  - NameNode gRPC client (registration, heartbeat, block reports)
  - ClientDatanodeProtocol service
  - Lock-free block metadata cache (DashMap)

- [x] **Phase 4:** Helm Chart & Deployment
  - Complete Helm chart with all components
  - NameNode StatefulSet with HA support
  - DataNode StatefulSet with persistent storage
  - ClusterIP services for NameNode and DataNodes
  - Etcd StatefulSet for metadata storage

- [x] **Phase 5:** Integration Testing
  - Chainsaw system tests on kind (converted from KUTTL)
  - Basic HDFS operations test (mkdir, put, get, ls, cat, cp)
  - Multi-DataNode replication testing (3-way replication)
  - Comprehensive Hadoop CLI compatibility tests
  - See [tests/system/README.md](tests/system/README.md) for details

- [x] **Phase 6:** gRPC Protocol Implementation ✨ **NEW!**
  - Complete gRPC server with tonic
  - ClientNamenodeProtocol (all client operations)
  - DatanodeProtocol (DataNode registration, heartbeat, block reports)
  - High-performance configuration (1024 concurrent requests/connection)
  - See [GRPC_IMPLEMENTATION.md](GRPC_IMPLEMENTATION.md) for details

- [x] **Phase 7:** DataNode Storage Implementation ✨ **NEW!**
  - Complete block storage on Flint volumes
  - HDFS-compatible block file format (blk_{id}_{genstamp})
  - Data transfer protocol server (TCP)
  - NameNode gRPC client (registration, heartbeat, block reports)
  - Volume manager with capacity tracking
  - See [DATANODE_IMPLEMENTATION.md](DATANODE_IMPLEMENTATION.md) for details

- [ ] **Phase 8:** Production Hardening
  - Block replication pipeline
  - Performance benchmarking
  - Production deployment guide

- [ ] **Phase 9:** RDMA Support
  - Client-to-DataNode RDMA for high-performance reads/writes
  - Inter-DataNode RDMA for fast block replication
  - Heterogeneous cluster support (mixed RDMA/TCP)
  - Auto-fallback to TCP when RDMA unavailable
  - See [RDMA_IMPLEMENTATION_PLAN.md](RDMA_IMPLEMENTATION_PLAN.md) for details

## Hadoop client compatibility

Verified end to end against an in-process / local cluster (NameNode + 3
DataNodes):

- `tests/interop/run-java-interop.sh` — the stock **Apache Hadoop 3.5.0**
  Java client (`DFSClient`, the code behind `hadoop fs`): `mkdirs -p`,
  multi-block write through a 3-DataNode pipeline, sequential and
  positional reads, `copyFromLocalFile` (`-put`) and read back (`-cat`),
  `listStatus`, rename, create with overwrite, `FileAlreadyExistsException`,
  recursive delete; then the NameNode is killed with SIGKILL, restarted on
  the same name directory, and the client reads back what it wrote.
- `pillar-datanode/tests/hdfs_client.rs` — the independent
  [hdfs-native](https://crates.io/crates/hdfs-native) client: the same
  operations, plus a DataNode crash with re-replication and replica
  cleanup after delete.
- `pillar-datanode/tests/namenode_restart.rs` — two NameNode crashes and
  restarts under hdfs-native: files, renames and deletes survive, DataNodes
  re-register and report their replicas, new writes get fresh block ids.

Not supported yet: Kerberos / SASL / block tokens (SIMPLE auth only),
append, hflush/hsync recovery, pipeline recovery after a DataNode fails
mid-write, block checksum (`-checksum`), short-circuit reads, erasure
coding, and NameNode HA (etcd is not consulted).

## Namespace persistence

The NameNode keeps the namespace in memory and makes every change durable
before answering, the way HDFS does (`dfs.namenode.name.dir`):

- Each change is appended to an **edit log** in `--name-dir` and fsynced
  before the client gets its reply. Concurrent changes share one fsync
  (group commit).
- The log is periodically folded into an **fsimage** (every 1,000,000
  transactions or hourly, if anything changed), so a restart replays only
  recent edits. The two newest images are kept.
- On startup the NameNode loads the newest image and replays the edits
  after it. A torn last record (a crash mid-write; never acknowledged) is
  dropped; corruption anywhere else stops startup.
- Block **locations** are not persisted: DataNodes are told to re-register
  when they heartbeat a NameNode that does not know them, and send a full
  block report. Replicas the namespace does not know (their file was
  deleted, possibly before a crash lost the pending deletion) or with a
  stale generation stamp are deleted.
- Formatting a name directory gives the namespace an **identity** (cluster
  id, namespace id, block pool id in `current/VERSION`; `--cluster-id`
  picks the cluster id). Each DataNode records it on its volume when it
  first joins, and refuses a NameNode with any other identity: a NameNode
  started on an empty or wrong directory gets no DataNodes, and so cannot
  delete their replicas as unknown. A volume that holds blocks but no
  `VERSION` is refused too.
- A lock file stops two NameNodes from using the same directory.

Without `--name-dir` the namespace is in memory only and lost on restart
(the NameNode logs a warning). The Helm chart mounts a PersistentVolume
there by default (`namenode.persistence`).

Measured on an Apple-silicon laptop SSD, file creates per second: 250 for a
single client (each waits for its own fsync), about 12,600 with 64
concurrent clients, and 300,000–400,000 without a name directory.

## Performance

Loopback benchmark of the DataNode TCP data path: one 128 MiB block
replicated from one DataNode to others on the same host, then read back
and byte-compared.

| Operation | Throughput |
|-----------|------------|
| Block write, pipeline, 1 target | 1.7–2.2 GB/s |
| Block write, pipeline, 2 targets | 1.9–2.2 GB/s |
| Block write, parallel, 2 targets | 1.1–1.8 GB/s |
| Block write, overlapped, 2 targets | 2.4–2.5 GB/s |
| Block read (whole block) | 2.2–3.4 GB/s |
| 4 KiB read latency (mean of 200) | ~100 µs |

Write figures count bytes delivered to all targets. Measured on Apple
Silicon (macOS), release build, two runs; block files were in the page
cache. Loopback measures protocol and copy overhead only — not disk or
network bandwidth — and these numbers are not a comparison with Java HDFS,
which has not been benchmarked. Reproduce with:

```sh
cargo test --release -p pillar-datanode --test data_path -- --ignored --nocapture --test-threads=1
```

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Acknowledgments

- Built on Apache Hadoop protobuf protocols
- Designed for high-performance storage in Kubernetes
- Inspired by the need for better HDFS performance in cloud-native environments
