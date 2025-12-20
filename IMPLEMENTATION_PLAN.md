# Pillar CSI Driver - Implementation Plan

## Overview

Extract pNFS code from Flint into a new CSI driver called "Pillar" that implements a **drop-in replacement for Apache Hadoop HDFS** in Rust.

**Key Changes:**
- Remove pNFS from Flint (keep only standalone NFS for RWX)
- Create new project "Pillar" at `/Users/ddalton/github/pillar`
- Implement official Apache Hadoop HDFS protobuf protocols
- NameNode and DataNode implement gRPC interfaces from Apache Hadoop
- Compatible with hadoop fs, Spark, Hive, and all Hadoop ecosystem tools
- DataNodes use Flint CSI RWO volumes for high-performance storage
- NameNode persists metadata to etcd or PVC

**Performance Goal:** Provide 3-5x performance improvement over Java HDFS by leveraging Rust's zero-cost abstractions and Flint's SPDK storage backend.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ HDFS Clients (hadoop fs, Spark, Hive, etc.)                │
│  ↓ HDFS RPC Protocol (gRPC/Protobuf)                       │
├─────────────────────────────────────────────────────────────┤
│ Pillar NameNode               Pillar DataNode (DaemonSet)  │
│ ┌──────────────────────┐      ┌──────────────────────────┐ │
│ │ClientNamenodeProtocol│      │ClientDatanodeProtocol    │ │
│ │DatanodeProtocol      │      │DatanodeProtocol          │ │
│ │- File operations     │      │- Block I/O               │ │
│ │- Block allocation    │      │- Block reporting         │ │
│ │- Metadata mgmt       │◄─────┤- Heartbeats              │ │
│ │- Namespace tree      │ gRPC │- Uses Flint RWO volume   │ │
│ │- Persists to etcd    │      │- High-perf SPDK backend  │ │
│ └──────────────────────┘      └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                                    ↓
                           Flint CSI RWO Volume
                                    ↓
                              SPDK Storage (373 MB/s)

Note: Also includes Pillar CSI driver for Kubernetes integration
```

**Protocol Stack:**
- **Client → NameNode:** Apache Hadoop `ClientNamenodeProtocol.proto` (gRPC)
- **Client → DataNode:** Apache Hadoop `ClientDatanodeProtocol.proto` (gRPC)
- **DataNode → NameNode:** Apache Hadoop `DatanodeProtocol.proto` (gRPC)
- **All protocols:** Official Apache Hadoop 3.4+ protobuf definitions

## Project Structure

```
/Users/ddalton/github/pillar/
├── Cargo.toml                    # Workspace root
├── pillar-csi-driver/            # CSI driver (new)
│   └── src/
│       ├── main.rs
│       ├── controller.rs         # CreateVolume, DeleteVolume
│       ├── node.rs               # NodePublishVolume (mount HDFS)
│       └── identity.rs
├── pillar-namenode/              # HDFS NameNode
│   └── src/
│       ├── main.rs
│       ├── server.rs             # Main NameNode server
│       ├── grpc/
│       │   ├── client_namenode_service.rs  # ClientNamenodeProtocol
│       │   └── datanode_protocol_service.rs # DatanodeProtocol
│       ├── metadata/
│       │   ├── namespace.rs      # HDFS namespace (INode tree)
│       │   ├── block_manager.rs  # Block allocation & tracking
│       │   └── persistence.rs    # Metadata persistence (etcd/SQLite)
│       └── datanode/
│           ├── manager.rs        # DataNode tracking
│           ├── heartbeat.rs      # Heartbeat monitor
│           └── placement.rs      # Block placement policy
├── pillar-datanode/              # HDFS DataNode
│   └── src/
│       ├── main.rs
│       ├── server.rs             # Main DataNode server
│       ├── grpc/
│       │   ├── client_datanode_service.rs  # ClientDatanodeProtocol
│       │   └── namenode_client.rs          # Client for NameNode
│       ├── storage/
│       │   ├── volume_manager.rs           # Manage Flint RWO volume
│       │   ├── block_storage.rs            # HDFS block file storage
│       │   └── data_transfer.rs            # HDFS data transfer protocol
│       └── heartbeat.rs          # Heartbeat loop
├── pillar-common/                # Shared utilities
│   └── src/
│       ├── security/
│       │   └── kerberos.rs       # Kerberos auth (from Flint)
│       └── hdfs/
│           ├── block.rs          # Block abstractions
│           ├── inode.rs          # INode utilities
│           ├── namespace.rs      # Namespace helpers
│           ├── replica.rs        # Replication logic
│           ├── placement.rs      # Placement algorithms
│           └── checksums.rs      # Block checksums
└── pillar-chart/                 # Helm chart
    └── templates/
        ├── namenode-deployment.yaml
        ├── datanode-daemonset.yaml
        └── csidriver.yaml
```

## Code Migration Map

### From Flint → Pillar

**What to Migrate:**

| Flint Source | Pillar Destination | Purpose |
|--------------|-------------------|---------|
| `src/nfs/kerberos.rs` | `pillar-common/src/security/kerberos.rs` | Kerberos authentication for HDFS |
| `src/pnfs/mds/device.rs` | `pillar-namenode/src/datanode/registry.rs` | DataNode tracking pattern |
| `src/pnfs/grpc.rs` | Reference for gRPC patterns | Pattern only, not direct copy |

**What to Build from Scratch:**

Everything else is new HDFS-specific code:
- HDFS namespace (INode tree)
- HDFS block management
- HDFS protobuf services (ClientNamenodeProtocol, DatanodeProtocol)
- HDFS data transfer protocol (TCP-based block I/O)
- HDFS block storage on filesystem

**What NOT to Migrate:**

❌ No NFS protocol code:
- `src/nfs/rpc.rs` - Not needed (HDFS uses gRPC)
- `src/nfs/xdr.rs` - Not needed (HDFS uses protobuf)
- `src/nfs/v4/*` - Not needed (HDFS has its own protocol)
- `src/pnfs/mds/layout.rs` - pNFS-specific, not HDFS
- `src/pnfs/protocol.rs` - pNFS-specific

### What Stays in Flint

- `src/nfs/server_v4.rs` - Standalone NFS server for RWX
- `src/nfs/kerberos.rs` - Keep in Flint, also copy to Pillar
- `src/pnfs/*` - Will be removed in separate task
- `src/driver.rs` - CSI driver
- `src/spdk_native.rs` - SPDK integration
- All SPDK/NVMe-oF code

**Note:** Flint pNFS removal is a **separate task** not covered in this plan.

## Apache Hadoop Protocol Implementation

### Official Protobuf Files to Import

**Source Repository:** [Apache Hadoop GitHub](https://github.com/apache/hadoop)

**Location:** `hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto/`

**Files to Copy:**

1. **ClientNamenodeProtocol.proto** - Client-to-NameNode RPC interface
   - File operations (create, delete, rename, mkdir)
   - Block management (getBlockLocations, addBlock, complete)
   - Metadata operations (setPermission, setOwner, setTimes)
   - Replication control (setReplication)
   - Snapshot management
   - Cache directives
   - Encryption zones
   - ~50+ RPC methods to implement

2. **ClientDatanodeProtocol.proto** - Client-to-DataNode RPC interface
   - Block operations (readBlock, writeBlock)
   - Block recovery
   - Volume information

3. **DatanodeProtocol.proto** - DataNode-to-NameNode RPC interface
   - registerDatanode
   - sendHeartbeat (with storage reports)
   - blockReport
   - cacheReport
   - blockReceivedAndDeleted
   - errorReport
   - commitBlockSynchronization

4. **hdfs.proto** - Core data structures
   - ExtendedBlockProto
   - DatanodeIDProto
   - DatanodeInfoProto
   - LocatedBlockProto
   - HdfsFileStatusProto
   - StorageReportProto

5. **Supporting protocols:**
   - `acl.proto` - Access control lists
   - `encryption.proto` - Encryption definitions
   - `erasurecoding.proto` - Erasure coding
   - `xattr.proto` - Extended attributes

### NameNode Service Implementation

```rust
// pillar-namenode/src/grpc/client_namenode_service.rs
use pillar_proto::hadoop::hdfs::protocol::*;

#[tonic::async_trait]
impl ClientNamenodeProtocol for NameNodeService {
    // File operations
    async fn create(&self, request: CreateRequestProto) -> Result<CreateResponseProto>;
    async fn delete(&self, request: DeleteRequestProto) -> Result<DeleteResponseProto>;
    async fn rename(&self, request: RenameRequestProto) -> Result<RenameResponseProto>;
    async fn mkdirs(&self, request: MkdirsRequestProto) -> Result<MkdirsResponseProto>;

    // Block management
    async fn get_block_locations(&self, request: GetBlockLocationsRequestProto)
        -> Result<GetBlockLocationsResponseProto>;
    async fn add_block(&self, request: AddBlockRequestProto) -> Result<AddBlockResponseProto>;
    async fn complete(&self, request: CompleteRequestProto) -> Result<CompleteResponseProto>;

    // Metadata operations
    async fn set_permission(&self, request: SetPermissionRequestProto)
        -> Result<SetPermissionResponseProto>;
    async fn set_owner(&self, request: SetOwnerRequestProto) -> Result<SetOwnerResponseProto>;
    async fn get_file_info(&self, request: GetFileInfoRequestProto)
        -> Result<GetFileInfoResponseProto>;

    // ... 40+ more methods
}
```

### DataNode Service Implementation

```rust
// pillar-datanode/src/grpc/datanode_protocol_service.rs
use pillar_proto::hadoop::hdfs::protocol::*;

#[tonic::async_trait]
impl DatanodeProtocol for DataNodeService {
    async fn register_datanode(&self, request: RegisterDatanodeRequestProto)
        -> Result<RegisterDatanodeResponseProto>;

    async fn send_heartbeat(&self, request: HeartbeatRequestProto)
        -> Result<HeartbeatResponseProto>;

    async fn block_report(&self, request: BlockReportRequestProto)
        -> Result<BlockReportResponseProto>;

    async fn cache_report(&self, request: CacheReportRequestProto)
        -> Result<CacheReportResponseProto>;

    async fn block_received_and_deleted(&self, request: BlockReceivedDeletedRequestProto)
        -> Result<BlockReceivedDeletedResponseProto>;
}
```

### Build Integration

```toml
# pillar-namenode/Cargo.toml
[dependencies]
tonic = "0.12"
prost = "0.13"
pillar-proto = { path = "../pillar-proto" }

[build-dependencies]
tonic-build = "0.12"
```

```rust
// pillar-namenode/build.rs
fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "proto/ClientNamenodeProtocol.proto",
                "proto/DatanodeProtocol.proto",
                "proto/hdfs.proto",
            ],
            &["proto/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
```

### Protocol Compatibility

**Hadoop Version Target:** 3.4+ (latest stable as of 2025)

**Wire Compatibility:**
- 100% compatible with official Hadoop clients
- Works with `hadoop fs -ls hdfs://pillar-namenode:9000/`
- Works with Spark, Hive, Flink, etc.
- Drop-in replacement for Java HDFS

**Performance Advantages:**
- Rust: Zero-cost abstractions, no GC pauses
- SPDK: Direct NVMe access (373 MB/s vs 97 MB/s)
- Async I/O: tokio for high concurrency
- Lock-free data structures: DashMap for metadata

**Expected Performance:** 3-5x faster than Java HDFS for:
- Metadata operations (no JVM overhead)
- Block I/O (SPDK vs kernel block layer)
- Memory efficiency (no GC, precise allocations)

## Implementation Phases

### Phase 1: Project Setup (3-4 days)

1. Create `/Users/ddalton/github/pillar` repository

2. Set up Cargo workspace with 5 crates:
   - `pillar-csi-driver` - Kubernetes CSI driver
   - `pillar-namenode` - HDFS NameNode implementation
   - `pillar-datanode` - HDFS DataNode implementation
   - `pillar-proto` - Generated protobuf code (shared)
   - `pillar-common` - Shared utilities and HDFS core logic

3. **Download Apache Hadoop protobuf files:**
   ```bash
   mkdir -p pillar-proto/proto
   cd pillar-proto/proto

   # Download from Apache Hadoop GitHub
   BASE_URL="https://raw.githubusercontent.com/apache/hadoop/trunk"
   PROTO_PATH="hadoop-hdfs-project/hadoop-hdfs-client/src/main/proto"

   wget "$BASE_URL/$PROTO_PATH/ClientNamenodeProtocol.proto"
   wget "$BASE_URL/$PROTO_PATH/ClientDatanodeProtocol.proto"
   wget "$BASE_URL/$PROTO_PATH/DatanodeProtocol.proto"
   wget "$BASE_URL/$PROTO_PATH/hdfs.proto"
   wget "$BASE_URL/$PROTO_PATH/acl.proto"
   wget "$BASE_URL/$PROTO_PATH/encryption.proto"
   wget "$BASE_URL/$PROTO_PATH/erasurecoding.proto"
   wget "$BASE_URL/$PROTO_PATH/xattr.proto"
   wget "$BASE_URL/$PROTO_PATH/datatransfer.proto"
   wget "$BASE_URL/$PROTO_PATH/inotify.proto"
   wget "$BASE_URL/$PROTO_PATH/ReconfigurationProtocol.proto"
   ```

4. Set up protobuf build pipeline:
   ```rust
   // pillar-proto/build.rs
   fn main() {
       tonic_build::configure()
           .build_server(true)
           .build_client(true)
           .compile(
               &[
                   "proto/ClientNamenodeProtocol.proto",
                   "proto/ClientDatanodeProtocol.proto",
                   "proto/DatanodeProtocol.proto",
                   "proto/hdfs.proto",
                   "proto/acl.proto",
                   "proto/encryption.proto",
                   "proto/erasurecoding.proto",
                   "proto/xattr.proto",
                   "proto/datatransfer.proto",
                   "proto/inotify.proto",
               ],
               &["proto/"],
           )
           .expect("Failed to compile Hadoop protos");
   }
   ```

5. Create basic `main.rs` entry points for each binary

6. Verify protobuf compilation works: `cargo build -p pillar-proto`

7. **Copy Kerberos implementation from Flint:**
   ```bash
   cp /Users/ddalton/github/flint/spdk-csi-driver/src/nfs/kerberos.rs \
      /Users/ddalton/github/pillar/pillar-common/src/security/kerberos.rs
   ```

**Key Files to Create:**
- `/Users/ddalton/github/pillar/Cargo.toml` (workspace)
- `/Users/ddalton/github/pillar/pillar-proto/Cargo.toml`
- `/Users/ddalton/github/pillar/pillar-proto/build.rs`
- `/Users/ddalton/github/pillar/pillar-proto/proto/` (with downloaded .proto files)
- `/Users/ddalton/github/pillar/pillar-common/src/security/kerberos.rs` (from Flint)
- `/Users/ddalton/github/pillar/pillar-common/src/hdfs/mod.rs`

### Phase 2: NameNode Implementation (6-8 days)

This is the most complex phase as it implements the full HDFS NameNode protocol.

1. **Implement gRPC service stubs:**
   ```rust
   // pillar-namenode/src/grpc/client_namenode_service.rs
   use pillar_proto::hadoop::hdfs::*;

   pub struct ClientNamenodeService {
       namespace: Arc<Namespace>,
       block_manager: Arc<BlockManager>,
       datanode_manager: Arc<DatanodeManager>,
   }

   #[tonic::async_trait]
   impl client_namenode_protocol_server::ClientNamenodeProtocol for ClientNamenodeService {
       // Start with essential operations:
       async fn get_listing(...) -> Result<GetListingResponseProto>;
       async fn mkdirs(...) -> Result<MkdirsResponseProto>;
       async fn create(...) -> Result<CreateResponseProto>;
       async fn get_block_locations(...) -> Result<GetBlockLocationsResponseProto>;
       async fn add_block(...) -> Result<AddBlockResponseProto>;
       async fn complete(...) -> Result<CompleteResponseProto>;
       // ... gradually add all 50+ methods
   }
   ```

2. **Implement namespace (filesystem tree):**
   ```rust
   // pillar-namenode/src/metadata/namespace.rs
   pub struct Namespace {
       root: Arc<RwLock<INode>>,
       inodes: DashMap<u64, Arc<INode>>,  // inode_id -> INode
       path_cache: DashMap<String, u64>,   // path -> inode_id
   }

   pub struct INode {
       id: u64,
       name: String,
       is_dir: bool,
       permissions: FsPermission,
       owner: String,
       group: String,
       mtime: u64,
       atime: u64,
       children: Option<DashMap<String, u64>>,  // For directories
       blocks: Option<Vec<BlockInfo>>,          // For files
   }

   impl Namespace {
       pub fn lookup(&self, path: &str) -> Option<Arc<INode>>;
       pub fn mkdir(&self, path: &str, perms: FsPermission) -> Result<u64>;
       pub fn create_file(&self, path: &str) -> Result<u64>;
       pub fn delete(&self, path: &str, recursive: bool) -> Result<()>;
   }
   ```

3. **Implement block manager:**
   ```rust
   // pillar-namenode/src/metadata/block_manager.rs
   pub struct BlockManager {
       blocks: DashMap<BlockId, BlockInfo>,
       file_blocks: DashMap<u64, Vec<BlockId>>,  // inode_id -> blocks
       block_locations: DashMap<BlockId, Vec<DatanodeId>>,
   }

   pub struct BlockInfo {
       block_id: BlockId,
       size: u64,
       generation_stamp: u64,
       num_bytes: u64,
   }

   impl BlockManager {
       pub fn allocate_block(&self, inode_id: u64) -> Result<BlockId>;
       pub fn get_block_locations(&self, block_id: &BlockId) -> Vec<DatanodeInfo>;
       pub fn report_block(&self, datanode_id: DatanodeId, blocks: Vec<BlockId>);
   }
   ```

4. **Implement DataNode manager:**
   ```rust
   // pillar-namenode/src/datanode/manager.rs
   pub struct DatanodeManager {
       datanodes: DashMap<DatanodeId, DatanodeDescriptor>,
       heartbeat_monitor: Arc<HeartbeatMonitor>,
   }

   pub struct DatanodeDescriptor {
       id: DatanodeId,
       hostname: String,
       rpc_port: u16,
       http_port: u16,
       capacity: u64,
       used: u64,
       remaining: u64,
       last_update: Instant,
       blocks: HashSet<BlockId>,
   }

   impl DatanodeManager {
       pub fn register(&self, req: RegisterDatanodeRequestProto) -> Result<()>;
       pub fn heartbeat(&self, req: HeartbeatRequestProto) -> Result<()>;
       pub fn select_datanodes(&self, num: usize) -> Vec<DatanodeId>;
   }
   ```

5. **Implement persistence (etcd or SQLite):**
   ```rust
   // pillar-namenode/src/metadata/persistence.rs
   pub trait MetadataStore: Send + Sync {
       async fn save_inode(&self, inode: &INode) -> Result<()>;
       async fn load_namespace(&self) -> Result<Namespace>;
       async fn save_block_map(&self, inode_id: u64, blocks: &[BlockInfo]) -> Result<()>;
   }
   ```

6. **Implement DatanodeProtocol service:**
   ```rust
   // pillar-namenode/src/grpc/datanode_protocol_service.rs
   #[tonic::async_trait]
   impl datanode_protocol_server::DatanodeProtocol for DatanodeProtocolService {
       async fn register_datanode(...) -> Result<RegisterDatanodeResponseProto>;
       async fn send_heartbeat(...) -> Result<HeartbeatResponseProto>;
       async fn block_report(...) -> Result<BlockReportResponseProto>;
   }
   ```

**Critical Files:**
- `/Users/ddalton/github/pillar/pillar-namenode/src/main.rs`
- `/Users/ddalton/github/pillar/pillar-namenode/src/grpc/client_namenode_service.rs`
- `/Users/ddalton/github/pillar/pillar-namenode/src/grpc/datanode_protocol_service.rs`
- `/Users/ddalton/github/pillar/pillar-namenode/src/metadata/namespace.rs`
- `/Users/ddalton/github/pillar/pillar-namenode/src/metadata/block_manager.rs`
- `/Users/ddalton/github/pillar/pillar-namenode/src/datanode/manager.rs`

**Incremental Implementation Strategy:**
1. Start with read-only operations (getListing, getBlockLocations)
2. Add write operations (create, mkdir, addBlock, complete)
3. Add metadata operations (setPermission, setOwner)
4. Add advanced features (snapshots, encryption, erasure coding)

### Phase 3: DataNode Implementation (4-5 days)

1. **Implement gRPC services:**
   ```rust
   // pillar-datanode/src/grpc/client_datanode_service.rs
   use pillar_proto::hadoop::hdfs::*;

   #[tonic::async_trait]
   impl client_datanode_protocol_server::ClientDatanodeProtocol for ClientDatanodeService {
       async fn get_replica_visible_length(...) -> Result<GetReplicaVisibleLengthResponseProto>;
       async fn refresh_namenodes(...) -> Result<RefreshNamenodesResponseProto>;
       async fn delete_block_pool(...) -> Result<DeleteBlockPoolResponseProto>;
       async fn get_block_local_path_info(...) -> Result<GetBlockLocalPathInfoResponseProto>;
       async fn get_hdfs_block_locations(...) -> Result<GetHdfsBlockLocationsResponseProto>;
       async fn shutdown_datanode(...) -> Result<ShutdownDatanodeResponseProto>;
   }
   ```

2. **Implement Flint volume mounting:**
   ```rust
   // pillar-datanode/src/storage/volume_manager.rs
   pub struct VolumeManager {
       mount_point: PathBuf,  // /mnt/pillar-data
       capacity_bytes: u64,
       used_bytes: u64,
       remaining_bytes: u64,
       block_pool_id: String,
   }

   impl VolumeManager {
       pub fn init(mount_point: PathBuf) -> Result<Self> {
           // Verify Flint volume is mounted at mount_point
           // Create directory structure: /mnt/pillar-data/current/BP-<pool-id>/
           // Report capacity using statvfs
       }

       pub fn get_storage_report(&self) -> StorageReportProto;
   }
   ```

3. **Implement HDFS block storage layer:**
   ```rust
   // pillar-datanode/src/storage/block_storage.rs
   pub struct BlockStorage {
       base_dir: PathBuf,  // /mnt/pillar-data/current/BP-xxx/current/finalized
       blocks: DashMap<BlockId, BlockMetadata>,
   }

   pub struct BlockMetadata {
       block_id: BlockId,
       generation_stamp: u64,
       num_bytes: u64,
       file_path: PathBuf,  // Actual block file
       meta_path: PathBuf,  // Metadata file
   }

   impl BlockStorage {
       pub async fn read_block(&self, block: &ExtendedBlockProto, offset: u64, length: u64)
           -> Result<Bytes>;

       pub async fn write_block(&self, block: &ExtendedBlockProto, offset: u64, data: Bytes)
           -> Result<u64>;

       pub async fn finalize_block(&self, block: &ExtendedBlockProto) -> Result<()>;

       pub fn get_all_blocks(&self) -> Vec<BlockReportReplicaProto>;
   }
   ```

4. **Implement data transfer protocol:**
   ```rust
   // pillar-datanode/src/storage/data_transfer.rs
   // This handles the actual block read/write protocol between clients and DataNode
   pub struct DataTransferServer {
       block_storage: Arc<BlockStorage>,
       listen_port: u16,
   }

   impl DataTransferServer {
       pub async fn handle_read_block(&self, header: OpReadBlockProto) -> Result<()>;
       pub async fn handle_write_block(&self, header: OpWriteBlockProto) -> Result<()>;
       pub async fn handle_transfer_block(&self, header: OpTransferBlockProto) -> Result<()>;
   }
   ```

5. **Implement registration and heartbeat:**
   ```rust
   // pillar-datanode/src/grpc/namenode_client.rs
   pub struct NamenodeClient {
       client: DatanodeProtocolClient<Channel>,
       datanode_id: DatanodeIdProto,
       storage_info: StorageInfo,
   }

   impl NamenodeClient {
       pub async fn register(&self) -> Result<NamespaceInfo>;

       pub async fn send_heartbeat(&self) -> Result<HeartbeatResponseProto> {
           let storage_reports = self.volume_manager.get_storage_report();
           let request = HeartbeatRequestProto {
               registration: self.datanode_id.clone(),
               reports: vec![storage_reports],
               cache_capacity: 0,
               cache_used: 0,
               xmits_in_progress: 0,
               xceivers_count: 0,
               failed_volumes: 0,
               request_full_block_report_lease: false,
           };
           self.client.send_heartbeat(request).await
       }

       pub async fn block_report(&self) -> Result<()> {
           let blocks = self.block_storage.get_all_blocks();
           // Send block report to NameNode
       }
   }
   ```

6. **Implement heartbeat loop:**
   ```rust
   // pillar-datanode/src/heartbeat.rs
   pub async fn heartbeat_loop(namenode_client: Arc<NamenodeClient>) {
       let mut interval = tokio::time::interval(Duration::from_secs(3));
       loop {
           interval.tick().await;

           match namenode_client.send_heartbeat().await {
               Ok(response) => {
                   // Process commands from NameNode
                   for cmd in response.cmds {
                       match cmd.action {
                           DatanodeCommandProto::DNA_TRANSFER => // replicate blocks
                           DatanodeCommandProto::DNA_INVALIDATE => // delete blocks
                           DatanodeCommandProto::DNA_SHUTDOWN => // shutdown
                           // ... handle commands
                       }
                   }
               }
               Err(e) => error!("Heartbeat failed: {}", e),
           }
       }
   }
   ```

**Critical Files:**
- `/Users/ddalton/github/pillar/pillar-datanode/src/main.rs`
- `/Users/ddalton/github/pillar/pillar-datanode/src/grpc/client_datanode_service.rs`
- `/Users/ddalton/github/pillar/pillar-datanode/src/grpc/namenode_client.rs`
- `/Users/ddalton/github/pillar/pillar-datanode/src/storage/volume_manager.rs`
- `/Users/ddalton/github/pillar/pillar-datanode/src/storage/block_storage.rs`
- `/Users/ddalton/github/pillar/pillar-datanode/src/storage/data_transfer.rs`
- `/Users/ddalton/github/pillar/pillar-datanode/src/heartbeat.rs`

**Implementation Priority:**
1. Volume manager (detect Flint mount)
2. Block storage (read/write blocks to files)
3. Registration with NameNode
4. Heartbeat loop
5. Block reporting
6. Data transfer protocol (client I/O)

### Phase 4: CSI Driver Implementation (3-4 days)

1. Implement CSI Controller:
   ```rust
   // pillar-csi-driver/src/controller.rs
   async fn create_volume(&self, req: CreateVolumeRequest) -> Result<CreateVolumeResponse> {
       // Call NameNode gRPC: CreateVolume
       // NameNode allocates blocks across DataNodes
       // Return volume_id
   }
   ```

2. Implement CSI Node:
   ```rust
   // pillar-csi-driver/src/node.rs
   async fn node_publish_volume(&self, req: NodePublishVolumeRequest) -> Result<NodePublishVolumeResponse> {
       // Mount NFS from NameNode:
       // mount -t nfs4 namenode-service:2049:/pillar/volumes/{volume_id} {target_path}
   }
   ```

3. Create protobuf definitions:
   - Copy `proto/csi.proto` from Flint
   - Create `proto/pillar_control.proto` (evolved from `pnfs_control.proto`)

**Critical Files:**
- `/Users/ddalton/github/pillar/pillar-csi-driver/src/main.rs`
- `/Users/ddalton/github/pillar/pillar-csi-driver/src/controller.rs`
- `/Users/ddalton/github/pillar/pillar-csi-driver/src/node.rs`
- `/Users/ddalton/github/pillar/proto/pillar_control.proto`

### Phase 5: Helm Chart & Deployment (3-4 days)

1. Create Helm chart structure:
   ```
   pillar-chart/
   ├── Chart.yaml
   ├── values.yaml
   └── templates/
       ├── namenode-deployment.yaml
       ├── namenode-service.yaml
       ├── datanode-daemonset.yaml
       ├── csidriver.yaml
       ├── controller.yaml (CSI controller)
       └── node.yaml (CSI node DaemonSet)
   ```

2. NameNode deployment:
   ```yaml
   # Stateful deployment with PVC for metadata
   apiVersion: apps/v1
   kind: StatefulSet
   metadata:
     name: pillar-namenode
   spec:
     replicas: 1
     template:
       spec:
         containers:
         - name: namenode
           image: pillar/namenode:v0.1.0
           volumeMounts:
           - name: metadata
             mountPath: /var/lib/pillar/metadata
     volumeClaimTemplates:
     - metadata:
         name: metadata
       spec:
         storageClassName: flint
         accessModes: [ReadWriteOnce]
         resources:
           requests:
             storage: 10Gi
   ```

3. DataNode DaemonSet:
   ```yaml
   # Runs on every node, mounts Flint RWO volume
   apiVersion: apps/v1
   kind: DaemonSet
   metadata:
     name: pillar-datanode
   spec:
     template:
       spec:
         containers:
         - name: datanode
           image: pillar/datanode:v0.1.0
           volumeMounts:
           - name: storage
             mountPath: /mnt/pillar-data
         volumes:
         - name: storage
           persistentVolumeClaim:
             # Pre-created PVC per node
             claimName: pillar-datanode-{{ .Values.nodeName }}
   ```

4. Create example deployments for testing

**Critical Files:**
- `/Users/ddalton/github/pillar/pillar-chart/values.yaml`
- `/Users/ddalton/github/pillar/pillar-chart/templates/namenode-deployment.yaml`
- `/Users/ddalton/github/pillar/pillar-chart/templates/datanode-daemonset.yaml`

### Phase 6: Integration Testing (2-3 days)

1. Deploy Flint CSI in test cluster
2. Deploy Pillar CSI in same cluster
3. Test workflow:
   - Create Pillar PVC (RWX)
   - Verify NameNode creates volume
   - Verify DataNodes register
   - Mount volume in pod
   - Read/write test
   - Verify data distributed across DataNodes

### Phase 7: Documentation & Finalization (1-2 days)

1. **Create comprehensive documentation:**
   - README.md with architecture overview
   - INSTALLATION.md with deployment guide
   - API_COMPATIBILITY.md listing supported HDFS operations
   - PERFORMANCE.md with benchmarks vs Java HDFS

2. **Create example configurations:**
   - Example core-site.xml for Hadoop clients
   - Example Spark configuration
   - Example Hive configuration

3. **Create troubleshooting guide:**
   - Common issues and solutions
   - Debugging NameNode/DataNode issues
   - Performance tuning guide

**Note:** Removing pNFS from Flint is a **separate task** and not part of this implementation plan.

## Key Technical Decisions

### 1. DataNode Storage: Flint RWO Volumes

**Approach:** DataNodes mount pre-created Flint RWO PVCs (one per node)

**Pros:**
- Leverages Flint's high performance (373 MB/s)
- Simple integration (just mount a PVC)
- Flint handles SPDK complexity

**Implementation:**
```yaml
# Pre-create PVC per node
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: pillar-datanode-storage-node1
spec:
  storageClassName: flint
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 100Gi
```

### 2. NameNode Metadata: PVC with SQLite (MVP)

**Approach:** Start with SQLite on PVC, migrate to etcd for HA later

**Pros:**
- Simpler initial implementation
- No external dependencies
- Easy to backup/restore

**Future:** Migrate to etcd for multi-replica HA

### 3. Protocol: Pure HDFS Implementation

**Approach:** Implement native HDFS protocols (not NFS)

**Protocols:**
- **gRPC-based RPC:** ClientNamenodeProtocol, DatanodeProtocol, ClientDatanodeProtocol
- **TCP data transfer:** HDFS data transfer protocol for block I/O
- **Protobuf serialization:** Apache Hadoop protobuf definitions

**Pros:**
- 100% compatible with Hadoop ecosystem (hadoop fs, Spark, Hive, Flink)
- Drop-in replacement for Java HDFS
- No custom protocol implementation needed (use official protobufs)

**Implementation:**
- Use `tonic` for gRPC (Rust gRPC framework)
- Use `prost` for protobuf (Rust protobuf library)
- Implement HDFS data transfer protocol in pure Rust (TCP-based)

## HDFS Compatibility Testing

### Phase 6 Testing Strategy

**1. Basic HDFS CLI Compatibility:**
```bash
# Configure Hadoop client to use Pillar NameNode
export HADOOP_NAMENODE=pillar-namenode-service:9000

# Test basic operations
hadoop fs -ls hdfs://pillar-namenode-service:9000/
hadoop fs -mkdir hdfs://pillar-namenode-service:9000/test
hadoop fs -put /tmp/testfile.txt hdfs://pillar-namenode-service:9000/test/
hadoop fs -cat hdfs://pillar-namenode-service:9000/test/testfile.txt
hadoop fs -get hdfs://pillar-namenode-service:9000/test/testfile.txt /tmp/retrieved.txt
hadoop fs -rm hdfs://pillar-namenode-service:9000/test/testfile.txt
```

**2. Performance Benchmarking:**
```bash
# TestDFSIO benchmark
hadoop jar hadoop-mapreduce-client-jobclient-tests.jar TestDFSIO \
  -write -nrFiles 10 -fileSize 1GB

# Compare with Java HDFS
# Expected: 3-5x better performance for:
# - Metadata operations (ls, mkdir, stat)
# - Sequential read/write (due to SPDK)
# - Memory footprint (Rust vs JVM)
```

**3. Spark Integration:**
```scala
// Test Spark reading from Pillar HDFS
val df = spark.read.parquet("hdfs://pillar-namenode-service:9000/data/parquet")
df.count()

// Test Spark writing to Pillar HDFS
df.write.parquet("hdfs://pillar-namenode-service:9000/output/parquet")
```

**4. Hive Integration:**
```sql
-- Create external table on Pillar HDFS
CREATE EXTERNAL TABLE test_table (id INT, name STRING)
LOCATION 'hdfs://pillar-namenode-service:9000/warehouse/test_table';

INSERT INTO test_table VALUES (1, 'test');
SELECT * FROM test_table;
```

## Success Criteria

1. **Build & Deployment:**
   - Pillar project builds successfully
   - NameNode and DataNode binaries run standalone
   - Helm chart deploys all components

2. **HDFS Protocol Compliance:**
   - `hadoop fs` commands work (ls, mkdir, put, get, rm)
   - NameNode responds to all essential ClientNamenodeProtocol RPCs
   - DataNodes successfully register and send heartbeats
   - Block reports work correctly

3. **Data Operations:**
   - Files can be created and written
   - Files can be read back correctly
   - Multi-block files work (>128MB files split across blocks)
   - Data persists across NameNode/DataNode restarts

4. **Ecosystem Compatibility:**
   - Spark can read/write data
   - Hive can create tables and query data
   - Standard Hadoop tools work (distcp, fsck)

5. **Performance:**
   - 3-5x faster metadata operations than Java HDFS
   - 2-4x faster block I/O (due to SPDK)
   - Lower memory usage (no JVM)
   - Sub-second `hadoop fs -ls` on large directories

6. **Integration:**
   - DataNodes successfully use Flint CSI RWO volumes
   - NameNode metadata persists to etcd or PVC
   - Both Flint and Pillar work in same Kubernetes cluster

7. **Documentation:**
   - Comprehensive documentation complete
   - Example configurations for Hadoop/Spark/Hive
   - Troubleshooting guide available

## Estimated Timeline

- **Phase 1 (Setup):** 3-4 days (download proto files, copy Kerberos, set up build)
- **Phase 2 (NameNode):** 6-8 days (implement 50+ HDFS RPC methods)
- **Phase 3 (DataNode):** 4-5 days (block storage + data transfer protocol)
- **Phase 4 (CSI):** 3-4 days (CSI driver for Kubernetes integration)
- **Phase 5 (Helm):** 3-4 days (deployment manifests)
- **Phase 6 (Testing):** 3-5 days (HDFS compatibility testing with hadoop fs, Spark)
- **Phase 7 (Documentation):** 1-2 days (comprehensive docs and examples)

**Total:** 23-32 days (4.5-6.5 weeks)

**Note:** This timeline assumes implementing essential HDFS operations first (read/write/ls/mkdir). Advanced features like snapshots, encryption, and erasure coding can be added incrementally in later phases.

## Next Steps

Once approved:
1. Create Pillar repository at `/Users/ddalton/github/pillar`
2. Set up Cargo workspace with 5 crates
3. Download Apache Hadoop protobuf files
4. Copy Kerberos implementation from Flint
5. Begin Phase 1 (Project Setup)

## Related Tasks (Not in This Plan)

**Separate Task:** Remove pNFS from Flint
- Delete `/Users/ddalton/github/flint/spdk-csi-driver/src/pnfs/`
- Remove pNFS protobuf files
- Update Flint to focus on RWO + standalone NFS RWX only
- This is a cleanup task independent of Pillar development
