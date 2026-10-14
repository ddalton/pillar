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

Metadata calls take effect and are persisted: `setReplication` (the
NameNode then adds or deletes replicas), `setPermission`, `setOwner`,
`setTimes`, `getContentSummary`; files are owned by the calling user.
Permissions are recorded but not enforced (like HDFS with
`dfs.permissions.enabled=false`). `reportBadBlocks` replaces corrupt
replicas.

`append` extends a partially filled last block in place (the DataNodes
reopen their replicas; the first packet completes a partial checksum
chunk), or starts a new block when the last one is full or the client asks
for `CreateFlag.NEW_BLOCK`.

**Pipeline recovery.** When a DataNode in a write pipeline dies, the
DataNode before it reports the failure in its acknowledgement, and the
client rebuilds the pipeline the way HDFS does: a new generation stamp
(`updateBlockForPipeline`), the survivors reopen their replicas being
written and cut them to the acknowledged length, unacknowledged packets
are replayed, and `updatePipeline` records the new pipeline. When the
client's policy asks for a replacement DataNode (`getAdditionalDatanode`),
a survivor copies its replica to it first (`TRANSFER_BLOCK`). `addBlock`
honours the client's excluded DataNodes. DataNodes report finished
replicas to the NameNode at once, so `complete()` does not wait for a
heartbeat.

**Leases.** A file is open for writing from `create` (or `append`) until
`complete`, by one client at a time: other writers get
`AlreadyBeingCreatedException`, and write calls from a client that lost
its lease get `LeaseExpiredException`. Readers see an open file's finished
blocks. Clients renew their lease (`renewLease`); after the soft limit
(60 s) another client may take the file over, and after the hard limit
(20 min) the NameNode recovers it. `recoverLease`, `isFileClosed` and
`listOpenFiles` work. Open files survive a NameNode restart, and their
writers get a fresh lease. A last block no DataNode finished goes through
**block recovery**: the NameNode has one DataNode of its pipeline ask the
others what they hold, agree on a length (a finished replica's, else the
shortest replica being written), cut, re-stamp and finalize every replica
at it, and report back (`commitBlockSynchronization`), which closes the
file; `recoverLease` answers false until then. If the whole pipeline is
down the file stays open until a DataNode returns. DataNodes report their
replicas being written in block reports, so a restarted NameNode can
recover a block too. Recovery stamps are logged before use, and a
finished replica with a newer stamp than the NameNode knows (a recovery
cut short by a NameNode crash) is kept for the next recovery.

**hflush / hsync.** A DataNode acknowledges a packet only once it is on
its disk (fsynced first for hsync), and the acknowledged length of a block
being written is what readers may see: another client reading a file that
is still open gets its complete blocks plus, from the last block's
DataNodes (`getReplicaVisibleLength` on the DataNode IPC port), everything
the writer hflushed. `hsync` with `UPDATE_LENGTH` records the length on the
NameNode, so the file's status shows it.

**Security.** By default clients authenticate with SIMPLE (the client says
who it is). With Kerberos, `--kerberos-keytab` and `--kerberos-principal`
(`nn/<host>@<REALM>` on the NameNode, `dn/<host>@<REALM>` on DataNodes)
make the Hadoop IPC ports require SASL GSSAPI, as HDFS with
`hadoop.security.authentication=kerberos`: SIMPLE clients are refused, a
caller is its authenticated principal mapped by Hadoop's DEFAULT
`auth_to_local` rule (`alice@REALM` and `alice/host@REALM` are `alice`),
and naming another user is refused unless proxy-user rules allow it
(below). The NameNode then serves its client protocol over Hadoop IPC
only, not gRPC. `tests/interop/run-java-kerberos.sh` runs the stock Java
client against a throwaway MIT KDC (`tests/kerberos/kdc.sh`).

**Block access tokens** are on with Kerberos (or alone with
`--block-access-tokens`), as HDFS's `dfs.block.access.token.enable`. The
NameNode gives every block it returns to a client a token for it (READ,
WRITE, COPY), signed with a secret block key it rolls every 10 hours and
sends to the DataNodes at registration and in heartbeats. DataNodes refuse
any data transfer without a valid token for the block and the access it
needs (`ERROR_ACCESS_TOKEN`), on Hadoop's DataTransferProtocol and on
Pillar's own data protocol alike, and sign their own for the copies they
make (re-replication, pipeline recovery, block recovery). On the DataNode
IPC port a client authenticates with the block's token (SASL DIGEST-MD5,
Hadoop's TOKEN method), so a Kerberized client can read a file still being
written.

**DataNodes authenticate to the NameNode** too, with Kerberos on. HDFS
runs DatanodeProtocol over Kerberized Hadoop IPC; Pillar's is gRPC, so
every call carries its own Kerberos AP-REQ (`x-kerberos-ap-req`) and every
reply the NameNode's AP-REP (`x-kerberos-ap-rep`). The NameNode only
accepts DataNode principals (`--datanode-principal`, default
`dn/_HOST@<its realm>`: any host) and refuses an authenticator it has seen
before (a replay cache over the clock-skew window). The DataNode checks the
AP-REP on every reply, so it takes commands only from the real NameNode. It
logs in from its keytab (`--kerberos-keytab`, `--kerberos-principal`; KDCs
from krb5.conf, `KRB5_CONFIG`) and renews its ticket for the NameNode
(`--namenode-principal`, default `nn/<NameNode host>@<realm>`) in the
background.

**RPC integrity and privacy** (`hadoop.rpc.protection`): `--rpc-protection`
on the NameNode and DataNodes takes `authentication` (the default),
`integrity`, `privacy`, or several comma separated, and SASL callers
negotiate one of them. With `integrity` every call and reply on the Hadoop
IPC ports is signed, with `privacy` signed and encrypted, after
authentication: Kerberos callers through the GSSAPI security layer (RFC
4121 Wrap tokens), block-token callers through DIGEST-MD5's (`auth-int`,
`auth-conf` with RC4, RFC 2831). Messages out of sequence are refused, and
so is a client that asks for less than the server requires ("No common
protection layer"). The Java client sets the same `hadoop.rpc.protection`.

The DataNode-to-NameNode gRPC calls follow the same `--rpc-protection`:
with `integrity` or `privacy` each call's body is an RFC 4121 Wrap token
under the call's own Kerberos subkey (from its AP-REQ), sealed for privacy,
and so is the reply, gRPC status included. Only the NameNode can read the
subkey, so a reply that unwraps proves it came from the NameNode. A
DataNode asks for the strongest level of its own `--rpc-protection`; the
NameNode refuses a level it does not allow.

**Permissions** are enforced as in HDFS (`dfs.permissions.enabled`, on by
default; `--permissions false` turns them off). A caller is checked
against an inode's owner, group or other bits; every operation needs
EXECUTE on the directories above its path, and each its own access:

| Operation | Needs |
|---|---|
| getFileInfo, isFileClosed | traversal |
| getListing | READ+EXECUTE on the directory |
| open (getBlockLocations) | READ |
| create, mkdirs | WRITE on the nearest existing ancestor (and on a file `create` overwrites) |
| append, truncate, setReplication, setTimes, recoverLease | WRITE |
| delete | WRITE on the parent, ALL on every non-empty directory below, the sticky bit |
| rename | WRITE on both parents, the sticky bit on source and replaced target |
| setPermission | ownership |
| setOwner | ownership; only the superuser changes the owner, and a group must be one of the caller's |
| getContentSummary | READ+EXECUTE on every directory below |
| concat | WRITE on the target, READ on each source and WRITE on its parent |
| listOpenFiles, datanode reports | the superuser |

In a sticky directory (mode `1xxx`, like `/tmp`) only an entry's owner, the
directory's owner or the superuser may delete or rename it. The superuser
(`--superuser`, default `hdfs`, the owner of `/`) and members of the
supergroup (`--superusergroup`, default `supergroup`) pass every check.
Groups come from `--user-groups "alice=staff,admins;bob=staff"` (as
Hadoop's static group mapping); users not listed have no groups. A new
file or directory takes its parent's group. Refusals are
`AccessControlException`s with HDFS's messages (`Permission denied:
user=bob, access=WRITE, inode="/data":alice:staff:drwxr-x---`). With
SIMPLE authentication (no Kerberos) callers name themselves, so
permissions guard against mistakes, not attackers, as in HDFS; gRPC
ClientProtocol callers (no Kerberos) act as `hdfs`.

**Proxy users** (Hadoop `hadoop.proxyuser.*`): a service that authenticates
as itself (Oozie, Livy, a gateway) may act as the users it serves if
`--proxy-users` allows it, e.g.
`"oozie.hosts=10.0.0.0/8,gw.example.com;oozie.groups=staff;livy.hosts=*;livy.users=alice,bob"`:
`users` and `groups` (of the effective user, from `--user-groups`) say whom,
`*` for anyone, and `hosts` (addresses, CIDR ranges, host names, `*`) from
where. Rules are keyed by the real user's short name. Under Kerberos the
real user is the authenticated principal; under SIMPLE, the real user the
client names. Anything else is refused with Hadoop's
`AuthorizationException` ("User: alice@REALM is not allowed to impersonate
bob", "Unauthorized connection for super-user: oozie from IP ..."), and
the effective user is who permission checks and file ownership see.

What security does **not** cover yet: ACLs and data transfer encryption (`dfs.encrypt.data.transfer`,
`dfs.data.transfer.protection`). Permission checks run before an
operation rather than under one namespace lock, so a concurrent chmod can
race an operation that already passed its check.

Not supported yet: block checksum (`-checksum`), short-circuit
reads, snapshots, ACLs, xattrs, quotas, erasure coding, and NameNode HA
(etcd is not consulted). Calls for unsupported features fail with
`UnsupportedOperationException` rather than pretending to succeed.

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
