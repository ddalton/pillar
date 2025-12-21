# HDFS Test Requirements for Pillar

Based on research of Apache Hadoop's official test suites and best practices, this document outlines the comprehensive testing strategy for Pillar.

## Research Sources

Key findings from:
- [Apache Hadoop FileSystem Contract Testing](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/testing.html)
- [HDFS Recovery Processes Part 1](https://blog.cloudera.com/understanding-hdfs-recovery-processes-part-1/)
- [HDFS Recovery Processes Part 2](https://www.cloudera.com/blog/technical/understanding-hdfs-recovery-processes-part-2.html)
- [HDFS Append and Truncate Operations](https://www.waitingforcode.com/hdfs/append-and-truncate-in-hdfs/read)
- [HDFS Recovery in Detail](https://www.waitingforcode.com/hdfs/recovery-hdfs/read)

---

## 1. FileSystem Contract Tests

Apache Hadoop organizes tests by operation, with one test class per operation. Pillar should implement similar coverage.

### Core File Operations (Currently Tested ✅)

- [x] **Create** - File creation with various flags
- [x] **Delete** - File/directory deletion
- [x] **Rename** - File/directory renaming
- [x] **Mkdir** - Directory creation
- [x] **Get/List** - File info and directory listing

### Missing Core Operations (NEEDED 🔧)

- [ ] **Seek** - Positioning within files for random access
  - Seek to beginning
  - Seek to end
  - Seek beyond file length (should fail)
  - Seek backwards
  - Seek with different buffer sizes

- [ ] **Append** - Adding data to existing files
  - Append to closed file
  - Append to file with partial last block
  - Append to file with full last block
  - Append not allowed to file under construction
  - Multiple sequential appends

- [ ] **Truncate** - Cutting data from end of file
  - Truncate to shorter length
  - Truncate to same length (no-op)
  - Truncate beyond file length (should fail)
  - Truncate last block partially
  - Truncate last block completely
  - Truncate with snapshots (advanced)

- [ ] **Concat** - Joining multiple files into one
  - Concat multiple files
  - Concat with file under construction (should fail)
  - Concat with different block sizes
  - Concat validation (source files deleted)

---

## 2. HDFS-Specific Compatibility Tests

Passing basic contract tests does **NOT** mean a filesystem is "compatible with HDFS". Critical areas:

### A. Atomicity Tests (CRITICAL 🚨)

Test that operations which HDFS guarantees to be atomic are equally so in Pillar:

- [ ] **Rename atomicity**
  - Concurrent rename operations
  - Rename + read race conditions
  - Rename + delete race conditions

- [ ] **Create atomicity**
  - Concurrent creates of same file
  - Create with O_EXCL flag
  - Create + read race conditions

- [ ] **Delete atomicity**
  - Concurrent delete operations
  - Delete while reading
  - Delete directory while creating files in it

### B. Idempotency Tests (HIGH PRIORITY ⚠️)

Verify operations are idempotent even with retry policies and concurrent clients:

- [ ] **Idempotent writes**
  - Retry create operation
  - Retry append operation
  - Retry delete operation

- [ ] **Concurrent operations**
  - Multiple clients writing to different files
  - Multiple clients reading same file
  - Mixed read/write/delete operations

### C. Scalability Tests (MEDIUM PRIORITY 📊)

- [ ] **Large files**
  - Files larger than 10GB
  - Files with thousands of blocks
  - Files approaching HDFS limits (petabytes)

- [ ] **Many files**
  - Directories with 10,000+ files
  - Directories with 100,000+ files
  - Deep directory hierarchies (100+ levels)

- [ ] **Block size variations**
  - Very small blocks (1MB)
  - Standard blocks (128MB)
  - Large blocks (1GB)

### D. Durability Tests (CRITICAL 🚨)

Test that data actually persists and recovers correctly:

- [ ] **Crash recovery**
  - NameNode restart during writes
  - DataNode restart during writes
  - Client crash during writes
  - Verify data integrity after recovery

- [ ] **Data persistence**
  - Verify checksums after write
  - Read after write consistency
  - Data survives pod restarts (Kubernetes)

---

## 3. Failure Scenario Tests

Based on real-world HDFS failure patterns and JIRA issues.

### A. DataNode Failures (CRITICAL 🚨)

- [ ] **Single DataNode failure**
  - DataNode crashes during write
  - DataNode network partition
  - DataNode disk failure
  - Verify automatic re-replication

- [ ] **Multiple DataNode failures**
  - 2 DataNodes fail simultaneously
  - All DataNodes in pipeline fail
  - Insufficient DataNodes for replication

- [ ] **DataNode slow/stale**
  - Slow DataNode detection
  - Exclude slow DataNodes from new writes
  - Replace slow DataNodes in pipeline

### B. Block Corruption (HIGH PRIORITY ⚠️)

- [ ] **Checksum validation**
  - Detect corrupted blocks on read
  - Report corruption to NameNode
  - Re-replicate from good replica

- [ ] **Corruption scenarios**
  - Bit flip in block data
  - Missing blocks
  - Blocks with corrupt replicas
  - All replicas corrupted

- [ ] **Recovery from corruption**
  - fsck detection of corrupt blocks
  - Automatic re-replication
  - Client fallback to alternate replica

### C. Lease Recovery (IMPLEMENTED ✅)

- [x] **Client crash recovery** - Already implemented!
- [ ] **Lease timeout scenarios**
  - Hard lease timeout (1 hour default)
  - Soft lease timeout (1 minute default)
  - Lease renewal during long operations

- [ ] **Concurrent lease recovery**
  - Multiple clients trying to recover same lease
  - Recovery while writer is still alive
  - Recovery racing with pipeline recovery (HDFS-1555)

### D. Pipeline Recovery (PARTIALLY IMPLEMENTED ⚠️)

Implemented:
- [x] get_additional_datanode (pipeline replacement)
- [x] update_block_for_pipeline (generation stamp updates)

Still needed:
- [ ] **Pipeline error recovery**
  - Network error during write
  - DataNode failure mid-pipeline
  - Pipeline recovery completion
  - Last packet pipeline recovery (HDFS-5557)

- [ ] **Pipeline + Lease recovery race**
  - Pipeline recovery before lease recovery
  - Incremental block reports during recovery
  - Generation stamp synchronization

---

## 4. Performance & Benchmark Tests

### A. I/O Performance

- [ ] **TestDFSIO equivalent**
  - Sequential read throughput
  - Sequential write throughput
  - Random read performance
  - Number of concurrent operations

### B. Metadata Performance

- [ ] **Namespace operations**
  - Create 10,000 files benchmark
  - List large directory benchmark
  - Rename directory with many files
  - Delete directory with many files

### C. Replication Performance

- [ ] **Block replication speed**
  - Time to re-replicate after DataNode failure
  - Replication under load
  - RDMA vs TCP replication speed

---

## 5. Root Directory Tests

Special tests for root filesystem operations (marked as "potentially very destructive" in Hadoop docs):

- [ ] **Root directory operations**
  - Cannot delete root
  - Cannot rename root
  - Can list root
  - Root permissions

---

## 6. HDFS-Specific Features

### A. Replication Management

- [ ] **Set replication factor**
  - Increase replication (triggers copies)
  - Decrease replication (triggers deletions)
  - Verify actual replica count matches setting

### B. Block Management

- [ ] **Block reporting**
  - Incremental block reports
  - Full block reports
  - Block report after restart

- [ ] **Block invalidation**
  - Over-replicated blocks removed
  - Blocks from decommissioned DataNodes
  - Stale blocks cleaned up

### C. Permissions & Security

- [ ] **POSIX permissions**
  - File/directory permissions
  - Owner/group permissions
  - Permission inheritance

- [ ] **Kerberos authentication** (if implemented)
  - Authenticated operations
  - Token renewal
  - Delegation tokens

---

## 7. MiniCluster Integration Tests

Apache Hadoop provides MiniDFSCluster for integration testing. Pillar equivalent:

### Current State
- ✅ KUTTL-based system tests in `tests/system/`
- ✅ Basic HDFS operations test
- ✅ Multi-DataNode replication test
- ✅ Hadoop CLI compatibility test

### Needed Additions

- [ ] **Cluster lifecycle tests**
  - NameNode restart
  - DataNode rolling restart
  - Cluster upgrade scenarios

- [ ] **Multi-client tests**
  - Concurrent Hadoop CLI operations
  - Concurrent Spark jobs
  - Mixed workload tests

- [ ] **Etcd HA tests**
  - Etcd node failure
  - Etcd network partition
  - NameNode failover scenarios

---

## 8. Compatibility Tests

Ensure Pillar works with Hadoop ecosystem:

### A. Hadoop CLI
- [x] Basic operations (implemented)
- [ ] Advanced operations (append, truncate)
- [ ] Admin commands (fsck, dfsadmin)

### B. Spark Integration
- [ ] Spark read from Pillar
- [ ] Spark write to Pillar
- [ ] Spark job failures/recovery

### C. Hive Integration
- [ ] Hive table creation
- [ ] Hive queries
- [ ] Hive with Pillar backend

### D. MapReduce
- [ ] MapReduce job execution
- [ ] Job recovery after failure
- [ ] Large-scale MapReduce benchmarks

---

## Implementation Priority

### Phase 1: Critical Gaps (Immediate)
1. Seek operation tests
2. Append operation tests
3. Atomicity tests (rename, create, delete)
4. DataNode failure scenarios
5. Block corruption detection and recovery

### Phase 2: High Priority (Next Sprint)
1. Truncate operation tests
2. Concat operation tests
3. Idempotency tests
4. Pipeline recovery edge cases
5. Scalability tests (large files, many files)

### Phase 3: Medium Priority (Future)
1. Performance benchmarks
2. Root directory tests
3. Advanced replication scenarios
4. Ecosystem integration tests (Spark, Hive)

### Phase 4: Nice to Have
1. Snapshot support (not currently implemented)
2. Encryption zones (not currently implemented)
3. ACLs (not currently implemented)
4. Extended attributes (not currently implemented)

---

## Testing Framework Recommendations

### For Rust Unit Tests
Continue using current approach:
- Tokio for async tests
- TempDir for filesystem isolation
- Mock DataNodes for controlled testing

### For Integration Tests
Enhance KUTTL tests with:
- Chaos engineering (kill pods randomly)
- Network partition simulation
- Disk failure simulation
- Load testing tools

### For Compatibility Tests
Create separate test suite:
- Docker containers with Hadoop ecosystem
- Automated Spark/Hive job execution
- Compatibility matrix testing (Hadoop 2.x, 3.x)

---

## Test Metrics & Coverage Goals

### Unit Test Coverage
- Target: 80%+ code coverage
- Current: ~70% (estimated based on recent additions)
- Focus: Edge cases, error paths, concurrent operations

### Integration Test Coverage
- Target: All critical user workflows
- Current: Basic read/write/list operations
- Add: Failure scenarios, recovery scenarios, multi-client tests

### Performance Benchmarks
- Establish baseline performance metrics
- Track regression over time
- Compare to Apache HDFS where applicable

---

## References

- [Apache Hadoop Testing Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/testing.html)
- [Spring Hadoop Testing Support](https://docs.spring.io/spring-hadoop/docs/current/reference/html/springandhadoop-testing.html)
- [HDFS MiniCluster Guide](https://github.com/sakserv/hadoop-mini-clusters)
- [HDFS Block Corruption Handling](https://drdroid.io/stack-diagnosis/hadoop-hdfs-datanode-block-corruption)
- [HDFS Recovery Processes](https://www.waitingforcode.com/hdfs/recovery-hdfs/read)
- HDFS JIRA issues: HDFS-5557, HDFS-1555, HDFS-7738, HDFS-10780, HDFS-14498

---

## Next Steps

1. Review this document with the team
2. Prioritize which tests to implement first
3. Create JIRA/GitHub issues for each test category
4. Implement Phase 1 tests
5. Set up continuous testing infrastructure
6. Establish performance baselines
7. Create compatibility testing matrix

This comprehensive test suite will ensure Pillar is truly production-ready and compatible with the broader Hadoop ecosystem.
