# Pillar DataNode - Block Storage Implementation

## Overview

The Pillar DataNode provides complete HDFS block storage functionality with native integration to Flint volumes for high-performance I/O. This implementation includes:

✅ **Block Storage Layer** - HDFS-compatible block files on Flint volumes
✅ **Volume Management** - Flint RWO volume integration with HDFS directory structure
✅ **Data Transfer Protocol** - TCP server for block read/write operations
✅ **NameNode Communication** - gRPC client for registration, heartbeats, and block reports
✅ **High Performance** - Lock-free data structures, zero-copy I/O

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ HDFS Clients / NameNode / Other DataNodes              │
└──────────┬─────────────────────┬────────────────────────┘
           │                     │
           │ gRPC (control)      │ TCP (data transfer)
           │                     │
┌──────────▼────────┐   ┌────────▼────────────────────────┐
│ NameNode Client   │   │ Data Transfer Server            │
│ - Registration    │   │ - Read blocks                   │
│ - Heartbeat       │   │ - Write blocks                  │
│ - Block reports   │   │ - Block replication pipeline    │
└──────────┬────────┘   └────────┬────────────────────────┘
           │                     │
           └─────────┬───────────┘
                     │
           ┌─────────▼──────────┐
           │ Block Storage      │
           │ - Create blocks    │
           │ - Read/Write data  │
           │ - Finalize blocks  │
           │ - Delete blocks    │
           └─────────┬──────────┘
                     │
           ┌─────────▼──────────┐
           │ Volume Manager     │
           │ - Flint RWO volume │
           │ - HDFS layout      │
           │ - Capacity tracking│
           └────────────────────┘
                     │
           ┌─────────▼──────────┐
           │ Flint CSI Volume   │
           │ /mnt/pillar-data   │
           │ SPDK backend       │
           └────────────────────┘
```

## Key Components

### 1. Block Storage (`pillar-datanode/src/storage/block_storage.rs`)

Manages HDFS block files on the local filesystem.

**Block File Format:**
```
/mnt/pillar-data/current/BP-pillar-001/current/finalized/
  ├─ blk_1000_1000        # Block data file
  ├─ blk_1000_1000.meta   # Block metadata (checksums)
  ├─ blk_1001_1001
  ├─ blk_1001_1001.meta
  ...

/mnt/pillar-data/current/BP-pillar-001/tmp/
  ├─ blk_1002_1002        # Blocks being written
  ├─ blk_1002_1002.meta
  ...
```

**Key Operations:**
```rust
impl BlockStorage {
    // Create a new block (in tmp directory)
    pub fn create_block(&self, block_id: BlockId) -> Result<()>

    // Write data to a block at specific offset
    pub fn write_block(&self, block_id: &BlockId, offset: u64, data: &[u8]) -> Result<u64>

    // Read data from a block
    pub fn read_block(&self, block_id: &BlockId, offset: u64, length: u64) -> Result<Bytes>

    // Finalize a block (move from tmp to finalized)
    pub fn finalize_block(&self, block_id: &BlockId) -> Result<u64>

    // Delete a block
    pub fn delete_block(&self, block_id: &BlockId) -> Result<()>

    // Get all blocks (for block reporting)
    pub fn get_all_blocks(&self) -> Vec<(BlockId, u64)>
}
```

**Features:**
- Lock-free block cache using `DashMap<u64, BlockMetadata>`
- Atomic block operations
- Automatic block scanning on startup
- HDFS-compatible block file naming: `blk_{blockid}_{genstamp}`

### 2. Volume Manager (`pillar-datanode/src/storage/volume_manager.rs`)

Manages Flint RWO volumes and HDFS directory structure.

**Directory Layout:**
```
/mnt/pillar-data/                    # Flint volume mount point
  └── current/                       # Current version
      └── BP-pillar-001/             # Block pool
          ├── current/
          │   └── finalized/         # Finalized blocks
          │       ├── blk_*
          │       └── blk_*.meta
          └── tmp/                   # Temporary blocks
              ├── blk_*
              └── blk_*.meta
```

**Key Operations:**
```rust
impl VolumeManager {
    // Initialize volume with HDFS directory structure
    pub fn new(mount_point: impl AsRef<Path>, block_pool_id: String) -> Result<Self>

    // Get volume statistics (capacity, used, remaining)
    pub fn get_volume_info(&self) -> Result<VolumeInfo>

    // Check if volume is healthy
    pub fn is_healthy(&self) -> bool

    // Get directories
    pub fn get_finalized_dir(&self) -> &Path
    pub fn get_tmp_dir(&self) -> &Path
}
```

**Features:**
- Automatic HDFS directory creation
- Real-time capacity tracking using `statvfs`
- Health checks with write tests
- Block pool isolation

### 3. Data Transfer (`pillar-datanode/src/transport/wire.rs`)

Pillar's own block-transfer protocol between DataNodes, used by the TCP
transport (`transport/tcp.rs`) and by the RDMA transport, whose data
currently moves over TCP. It is **not** Hadoop's DataTransferProtocol: a
Hadoop client cannot talk to it.

**Wire format** (integers big-endian; a connection carries any number of
requests, one after another):

```
request   [1: op] [8: block_id] [8: generation_stamp] [8: offset] [8: length]

READ  (op 81)
  reply   [1: status=0] [8: len] [len bytes]        len = min(length, size - offset)
          [1: status≠0] [2: msg_len] [msg]          2 = not found, 1 = other error

WRITE (op 80)  offset/length reserved (0); a complete block in one session:
  packets [4: len, 1..=4 MiB] [len bytes] ...
  end     [4: 0]
  reply   [1: status=0] [8: bytes stored]  |  [1: status≠0] [2: msg_len] [msg]
```

**Behaviour:**
- A WRITE streams the whole block over one connection: the receiver writes
  packets sequentially into one open temp file and finalizes the block at
  the end marker, so it is readable as soon as the sender gets `status=0`.
  There is one acknowledgement per block; TCP flow control paces the sender.
- A sender that fails part-way closes the connection without the end
  marker; the receiver deletes the partial block. A block that is already
  finalized is never overwritten.
- READ replies are clamped to the block, so the server's memory use is
  bounded by the block rather than by the requested length.
- Disk I/O runs on blocking threads connected to the socket by bounded
  channels, overlapping disk and network in both directions.
- TCP_NODELAY on all data sockets; clients pool connections per peer
  (reads retry once on a stale pooled connection).

**Measuring:** `pillar-datanode/tests/data_path.rs` checks every replicator
end to end (block replicated → read back → byte-compared); its `#[ignore]`d
`throughput_*` tests are a loopback benchmark:

```
cargo test --release -p pillar-datanode --test data_path -- --ignored --nocapture --test-threads=1
```

### 4. NameNode Client (`pillar-datanode/src/grpc/namenode_client.rs`)

gRPC client for DataNode → NameNode communication.

**Operations:**

**a) Registration:**
```rust
// Register DataNode with NameNode on startup
pub async fn register(&self) -> Result<()>
```

Sends:
- DataNode UUID, hostname, ports
- Storage capacity and block pool info
- Software version

**b) Heartbeat (every 3 seconds):**
```rust
// Send periodic heartbeat with storage reports
pub async fn send_heartbeat(&self) -> Result<Vec<DatanodeCommand>>
```

Sends:
- Storage reports (capacity, used, remaining)
- Transfer statistics
- Failed volume count

Receives:
- Commands from NameNode (replicate blocks, invalidate blocks, etc.)

**c) Block Report:**
```rust
// Send full block inventory to NameNode
pub async fn send_block_report(&self) -> Result<()>
```

Sends:
- List of all block IDs on this DataNode
- Block generation stamps
- Block sizes

**Features:**
- Automatic gRPC client reconnection
- Protobuf message construction
- Command parsing and execution
- Configurable heartbeat interval

## Configuration

DataNode configuration via environment variables:

### Core Settings

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `HDFS_NAMENODE_RPC_ADDRESS` | pillar-namenode:9000 | NameNode address |
| `HDFS_BLOCK_POOL_ID` | BP-pillar-001 | Block pool ID |
| `DATANODE_VOLUME_MOUNT_POINT` | /mnt/pillar-data | Flint volume mount point |

### Port Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATANODE_RPC_PORT` | 9867 | RPC server port |
| `DATANODE_DATA_PORT` | 50010 | Data transfer port |
| `DATANODE_HTTP_PORT` | 50075 | HTTP UI port |

### Performance Tuning

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATANODE_HANDLER_COUNT` | (auto) | RPC handler threads |
| `DATANODE_MAX_XCEIVER_COUNT` | (auto) | Max concurrent transfers |
| `DATANODE_MAX_TRANSFER_THREADS` | (auto) | Max transfer threads |
| `DATANODE_HEARTBEAT_INTERVAL` | 3 | Heartbeat interval (seconds) |
| `DATANODE_BLOCK_REPORT_INTERVAL` | 21600000 | Block report interval (ms) |
| `DATANODE_DU_RESERVED_BYTES` | 0 | Reserved disk space (bytes) |

## Deployment

### Standalone Mode (Development)

```bash
# Start etcd for NameNode
docker run -d --name etcd \
  -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.11 \
  etcd --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://localhost:2379

# Start NameNode
./target/release/pillar-namenode \
  --rpc-addr 0.0.0.0:9000 \
  --http-addr 0.0.0.0:9870 \
  --etcd-endpoints localhost:2379

# Start DataNode
./target/release/pillar-datanode \
  --namenode localhost:9000 \
  --rpc-addr 0.0.0.0:9867 \
  --data-addr 0.0.0.0:50010 \
  --http-addr 0.0.0.0:50075 \
  --volume-mount-point /tmp/pillar/data \
  --block-pool-id BP-pillar-001
```

### Kubernetes (Production)

Deployed as a DaemonSet with Flint RWO volumes:

```yaml
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
        env:
        - name: HDFS_NAMENODE_RPC_ADDRESS
          value: "pillar-namenode:9000"
        - name: HDFS_BLOCK_POOL_ID
          value: "BP-pillar-001"
        - name: DATANODE_VOLUME_MOUNT_POINT
          value: "/mnt/pillar-data"
        volumeMounts:
        - name: data
          mountPath: /mnt/pillar-data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: flint
      resources:
        requests:
          storage: 100Gi
```

## Performance

No end-to-end disk or network benchmarks have been run yet. The only
measurements are the loopback data-path numbers in the README's
Performance section (`tests/data_path.rs`), which measure protocol and
copy overhead, not storage or network bandwidth. The targets in
`NEXT_STEPS.md` (e.g. ~350 MB/s sequential I/O) remain targets.

## Data Flow Examples

### Write Pipeline

1. **Client → NameNode**: Request block allocation
   - NameNode returns block ID, generation stamp, and DataNode list

2. **Client → DataNode-1**: Write block with replication pipeline
   ```
   Client → DN1: WRITE_BLOCK(block_id, gen_stamp, data, [DN2, DN3])
     DN1 → DN2: WRITE_BLOCK(block_id, gen_stamp, data, [DN3])
       DN2 → DN3: WRITE_BLOCK(block_id, gen_stamp, data, [])
       DN3 → DN2: ACK
     DN2 → DN1: ACK
   DN1 → Client: ACK
   ```

3. **DataNode-1 → NameNode**: Block received notification
   - Sent in next heartbeat

### Read Pipeline

1. **Client → NameNode**: Get block locations for file
   - NameNode returns list of blocks and their DataNode locations

2. **Client → DataNode**: Read block
   ```
   Client → DN1: READ_BLOCK(block_id, offset, length)
   DN1 → Client: Block data
   ```

## Monitoring

### DataNode Metrics

```bash
# Check DataNode status
curl http://localhost:50075/metrics

# View block count
curl http://localhost:50075/blockCount

# View storage capacity
curl http://localhost:50075/storage
```

### Logs

```bash
# View DataNode logs
docker logs pillar-datanode

# Follow logs
docker logs -f pillar-datanode

# In Kubernetes
kubectl logs -f daemonset/pillar-datanode
```

## Troubleshooting

### Common Issues

**Issue**: DataNode fails to register with NameNode
```
Error: Failed to connect to NameNode: connection refused
```

**Solution**: Verify NameNode is running and accessible
```bash
nc -zv pillar-namenode 9000
```

---

**Issue**: "Volume not mounted" error
```
Error: Volume not mounted: /mnt/pillar-data
```

**Solution**: Verify Flint volume is mounted
```bash
df -h /mnt/pillar-data
ls -la /mnt/pillar-data
```

---

**Issue**: Block storage full
```
Error: No space left on device
```

**Solution**: Check volume capacity and clean up old blocks
```bash
du -sh /mnt/pillar-data
# Increase PVC size or delete unused data
```

## Testing

The DataNode implementation is tested using:

1. **Unit Tests**: In-memory testing of block operations
2. **Integration Tests**: Full DataNode + NameNode interaction
3. **KUTTL Tests**: Kubernetes-based end-to-end tests

```bash
# Run unit tests
cargo test -p pillar-datanode

# Run integration tests
cd tests/system
make test-replication
```

## Future Enhancements

- [ ] Block checksums and verification
- [ ] Block caching layer
- [ ] Erasure coding support
- [ ] Disk balancing across multiple volumes
- [ ] Volume hot-swapping
- [ ] Decommissioning support
- [ ] Block scanner for corruption detection

## References

- [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Flint CSI Driver](https://github.com/ddalton/flint)
- [Apache Hadoop HDFS Protocol](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
