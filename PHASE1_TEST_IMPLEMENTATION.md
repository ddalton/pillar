# Phase 1: Critical Test Implementation Plan

**Status**: In Progress
**Priority**: Immediate
**Goal**: Implement critical missing tests for production HDFS compatibility

---

## Overview

Phase 1 addresses the most critical gaps in Pillar's test coverage, focusing on operations and scenarios that are essential for HDFS compatibility and production readiness.

---

## 1. Seek Operation Implementation ⏳

**Priority**: CRITICAL
**Complexity**: Medium
**Estimated Effort**: 2-3 hours

### What is Seek?

Seek allows random access within files by positioning the read pointer at a specific offset. This is essential for:
- Random file reads
- Reading specific portions of large files
- Efficient data access patterns (Spark, Parquet readers)

### Implementation Tasks

#### A. Add Seek Support to Read Operations

**Files to modify**:
- `pillar-datanode/src/storage/block_storage.rs` - Add seek to block reader
- `pillar-datanode/src/grpc/client_datanode_service.rs` - Add offset parameter to read operations

**Implementation**:
```rust
// Add to BlockReader
pub fn seek(&mut self, offset: u64) -> Result<u64> {
    // Seek within the current file
    self.file.seek(SeekFrom::Start(offset))?;
    Ok(offset)
}

// Support different seek modes
pub enum SeekMode {
    Start(u64),      // Absolute position
    Current(i64),    // Relative to current
    End(i64),        // Relative to end
}
```

#### B. Unit Tests

**Test file**: `pillar-datanode/src/storage/block_storage.rs`

Tests to add:
- [x] `test_seek_to_beginning` - Seek to offset 0
- [x] `test_seek_to_middle` - Seek to middle of file
- [x] `test_seek_to_end` - Seek to file length
- [x] `test_seek_beyond_end` - Seek past EOF (should fail)
- [x] `test_seek_backwards` - Seek to earlier position
- [x] `test_seek_and_read` - Seek then read specific bytes
- [x] `test_multiple_seeks` - Multiple sequential seeks

#### C. Integration Tests

**Test file**: `tests/system/seek-operations/`

KUTTL test scenarios:
- Read file with random seeks
- Concurrent seeks from multiple clients
- Seek with different block sizes

**Expected Outcome**:
- ✅ Random file access works correctly
- ✅ Reads return correct data after seek
- ✅ Boundary conditions handled properly

---

## 2. Append Operation Implementation ⏳

**Priority**: CRITICAL
**Complexity**: High
**Estimated Effort**: 4-6 hours

### What is Append?

Append adds new data to the end of an existing file. Critical for:
- Log file aggregation
- Incremental data updates
- Streaming writes

### Implementation Tasks

#### A. NameNode Append Support

**Files to modify**:
- `pillar-namenode/src/grpc/client_namenode_service.rs` - Already has `handle_append()` ✅

**Enhancement needed**:
```rust
// Current implementation returns last partial block
// Add validation:
- [x] File must be closed (not under construction)
- [x] Move file to "under construction" state
- [x] Check if last block is full or partial
- [x] If full, allocate new block
- [x] If partial, return existing block for append
```

#### B. DataNode Append Support

**Files to modify**:
- `pillar-datanode/src/storage/block_storage.rs` - Add append to block
- `pillar-datanode/src/storage/data_transfer.rs` - Handle append in pipeline

**Implementation**:
```rust
// Add to BlockStorage
pub async fn append_block(
    &self,
    block_id: BlockId,
    data: &[u8],
) -> Result<u64> {
    // Open block file in append mode
    // Write data
    // Update block size
    // Return new size
}
```

#### C. Unit Tests

**Test files**:
- `pillar-namenode/src/grpc/client_namenode_service.rs`
- `pillar-datanode/src/storage/block_storage.rs`

Tests to add:
- [x] `test_append_to_closed_file` - Normal append flow
- [x] `test_append_to_partial_block` - Uses existing block
- [x] `test_append_to_full_block` - Allocates new block
- [x] `test_append_under_construction` - Should fail
- [x] `test_multiple_appends` - Sequential appends
- [x] `test_append_large_data` - Data spanning multiple blocks
- [x] `test_append_and_read` - Verify appended data readable

#### D. Integration Tests

**Test file**: `tests/system/append-operations/`

KUTTL test scenarios:
- Append data using hadoop fs -appendToFile
- Multiple appends to same file
- Concurrent appends (should serialize)
- Append after client restart

**Expected Outcome**:
- ✅ Appends work correctly
- ✅ Data integrity maintained
- ✅ Compatible with Hadoop CLI

---

## 3. Atomicity Tests ⏳

**Priority**: CRITICAL
**Complexity**: Medium-High
**Estimated Effort**: 3-4 hours

### What are Atomicity Tests?

Tests ensuring concurrent operations don't interfere with each other. Critical for:
- Multi-client environments
- Concurrent Spark/Hive jobs
- Data consistency guarantees

### Implementation Tasks

#### A. Concurrent Rename Tests

**Test file**: `pillar-namenode/src/grpc/client_namenode_service.rs`

Tests to add:
- [x] `test_concurrent_rename_different_files` - Should both succeed
- [x] `test_concurrent_rename_same_file` - One succeeds, one fails
- [x] `test_rename_read_race` - Read during rename
- [x] `test_rename_delete_race` - Delete during rename
- [x] `test_rename_directory_concurrent` - Concurrent dir renames

#### B. Concurrent Create Tests

**Test file**: `pillar-namenode/src/grpc/client_namenode_service.rs`

Tests to add:
- [x] `test_concurrent_create_same_file` - One succeeds, one fails
- [x] `test_concurrent_create_different_files` - Both succeed
- [x] `test_create_excl_flag` - Exclusive create behavior
- [x] `test_create_read_race` - Read during create

#### C. Concurrent Delete Tests

**Test file**: `pillar-namenode/src/grpc/client_namenode_service.rs`

Tests to add:
- [x] `test_concurrent_delete_same_file` - One succeeds, one fails gracefully
- [x] `test_concurrent_delete_different_files` - Both succeed
- [x] `test_delete_while_reading` - Read operations handle deletion
- [x] `test_delete_directory_while_creating_files` - Proper locking

#### D. Integration Tests

**Test file**: `tests/system/atomicity/`

KUTTL test scenarios:
- Launch multiple hadoop fs operations concurrently
- Verify only correct operations succeed
- Check no corruption or partial states

**Expected Outcome**:
- ✅ Operations are atomic
- ✅ Concurrent operations don't corrupt state
- ✅ Proper error messages for conflicts

---

## 4. DataNode Failure Scenario Tests ⏳

**Priority**: CRITICAL
**Complexity**: High
**Estimated Effort**: 4-5 hours

### What are DataNode Failure Tests?

Tests simulating DataNode failures during operations. Critical for:
- Fault tolerance validation
- Recovery mechanism verification
- Production reliability

### Implementation Tasks

#### A. Unit Tests for Failure Handling

**Test file**: `pillar-namenode/src/datanode/manager.rs`

Tests to add:
- [x] `test_datanode_failure_detection` - Heartbeat timeout
- [x] `test_mark_datanode_dead` - State transition
- [x] `test_exclude_dead_datanode_from_placement` - Placement excludes dead nodes
- [x] `test_trigger_replication_on_failure` - Under-replicated blocks detected

**Test file**: `pillar-namenode/src/grpc/client_namenode_service.rs`

Tests to add:
- [x] `test_write_with_datanode_failure` - Uses get_additional_datanode ✅ (already implemented!)
- [x] `test_insufficient_datanodes` - Fails gracefully
- [x] `test_read_with_datanode_failure` - Falls back to alternate replica

#### B. Integration Tests

**Test file**: `tests/system/datanode-failures/`

KUTTL test scenarios:
```yaml
# Scenario 1: DataNode crash during write
- Create file
- Kill one DataNode pod (kubectl delete pod)
- Continue write (should use replacement DataNode)
- Verify file integrity

# Scenario 2: Multiple DataNode failures
- Start with 3 DataNodes
- Kill 2 DataNodes
- Verify reads still work (1 replica available)
- Verify writes get queued for re-replication

# Scenario 3: All DataNodes in pipeline fail
- Start write
- Kill all DataNodes in pipeline
- Verify proper error handling
- Verify no data corruption
```

#### C. Chaos Testing Setup

**New file**: `tests/chaos/datanode-chaos.yaml`

Use Kubernetes chaos engineering:
- Random pod kills
- Network partitions
- Disk failures

**Expected Outcome**:
- ✅ System detects failures quickly
- ✅ Automatic failover works
- ✅ Data integrity maintained
- ✅ No write pipeline hangs

---

## 5. Block Corruption Detection Tests ⏳

**Priority**: CRITICAL
**Complexity**: Medium
**Estimated Effort**: 3-4 hours

### What are Corruption Detection Tests?

Tests verifying checksum validation and corruption recovery. Critical for:
- Data integrity guarantees
- Silent corruption detection
- Automatic recovery

### Implementation Tasks

#### A. Checksum Validation Enhancement

**Files to check**:
- `pillar-common/src/hdfs/checksums.rs` - Already has compute_checksum() ✅
- `pillar-datanode/src/storage/block_storage.rs` - Add validation on read

**Implementation needed**:
```rust
// Add to BlockStorage
pub async fn read_block_with_validation(
    &self,
    block_id: BlockId,
) -> Result<Vec<u8>> {
    // Read block data
    let data = self.read_block(block_id)?;

    // Read stored checksum
    let stored_checksum = self.read_block_checksum(block_id)?;

    // Compute actual checksum
    let actual_checksum = compute_checksum(&data);

    // Validate
    if !verify_checksum(&data, &stored_checksum) {
        return Err(BlockCorruptionError(block_id));
    }

    Ok(data)
}
```

#### B. Unit Tests

**Test file**: `pillar-datanode/src/storage/block_storage.rs`

Tests to add:
- [x] `test_detect_corrupted_block` - Tampered block detected
- [x] `test_checksum_validation_on_read` - Automatic validation
- [x] `test_report_corruption_to_namenode` - gRPC call to NameNode
- [x] `test_fallback_to_alternate_replica` - Client tries another DataNode

**Test file**: `pillar-namenode/src/metadata/block_manager.rs`

Tests to add:
- [x] `test_mark_block_corrupt` - Track corrupt blocks
- [x] `test_schedule_replication` - Re-replicate from good replica
- [x] `test_remove_corrupt_replica` - Clean up bad replica
- [x] `test_all_replicas_corrupt` - Handle worst case

#### C. Integration Tests

**Test file**: `tests/system/corruption/`

KUTTL test scenarios:
```yaml
# Scenario 1: Corrupt single replica
- Write file with 3 replicas
- Manually corrupt one replica (exec into pod, modify block file)
- Read file (should use good replica)
- Verify corruption detected
- Verify re-replication triggered

# Scenario 2: All replicas corrupt
- Write file
- Corrupt all replicas
- Try to read (should fail with corruption error)
- Verify file marked as corrupt in NameNode
```

#### D. Corruption Simulation

**New file**: `tests/corruption/corrupt-block.sh`

Script to simulate corruption:
```bash
#!/bin/bash
# Find block file
BLOCK_FILE=$(kubectl exec datanode-0 -- find /mnt/pillar-data -name "blk_*" | head -1)

# Corrupt middle of file (flip some bits)
kubectl exec datanode-0 -- dd if=/dev/urandom of=$BLOCK_FILE bs=1 count=100 seek=1024 conv=notrunc
```

**Expected Outcome**:
- ✅ Corruption detected on read
- ✅ NameNode notified of corruption
- ✅ Automatic re-replication from good replica
- ✅ Corrupt replica removed
- ✅ Client gets correct data (falls back)

---

## Testing Infrastructure Enhancements

### A. Test Utilities

**New file**: `pillar-common/src/test_utils.rs`

```rust
pub mod test_utils {
    /// Simulate DataNode failure by dropping connections
    pub async fn kill_datanode(uuid: &str) {
        // Implementation
    }

    /// Corrupt a block file
    pub fn corrupt_block(block_id: BlockId, datanode: &str) {
        // Implementation
    }

    /// Wait for replication to complete
    pub async fn wait_for_replication(expected_count: usize) {
        // Implementation
    }
}
```

### B. Integration Test Framework

**New file**: `tests/integration/mod.rs`

```rust
// Integration test framework using testcontainers or similar
pub struct PillarCluster {
    namenode: NameNodeContainer,
    datanodes: Vec<DataNodeContainer>,
    etcd: EtcdContainer,
}

impl PillarCluster {
    pub fn new(num_datanodes: usize) -> Self { }
    pub fn kill_datanode(&mut self, index: usize) { }
    pub fn restart_datanode(&mut self, index: usize) { }
    pub fn corrupt_block(&mut self, block_id: BlockId) { }
}
```

---

## Success Criteria

### Phase 1 Complete When:

- [x] All Seek operation tests pass (7+ tests)
- [x] All Append operation tests pass (7+ tests)
- [x] All Atomicity tests pass (12+ tests)
- [x] All DataNode failure tests pass (8+ tests)
- [x] All Block corruption tests pass (8+ tests)
- [x] Integration tests added to KUTTL suite
- [x] Documentation updated
- [x] All existing tests still pass

**Total New Tests**: 40+ unit tests, 10+ integration tests

---

## Timeline

**Week 1**:
- Day 1-2: Seek operation (implementation + tests)
- Day 3-4: Append operation (implementation + tests)
- Day 5: Atomicity tests

**Week 2**:
- Day 1-2: DataNode failure tests
- Day 3-4: Block corruption tests
- Day 5: Integration tests + documentation

**Total Estimated Time**: 8-10 days

---

## Dependencies

### Required for Implementation:
- Existing NameNode/DataNode architecture ✅
- Block storage implementation ✅
- Checksum utilities ✅
- KUTTL test framework ✅

### Blockers:
- None identified

---

## Risks & Mitigation

### Risk 1: Append Implementation Complexity
**Mitigation**: Start with simple append to closed files, add "under construction" state later if needed

### Risk 2: Integration Test Flakiness
**Mitigation**: Add retries, proper waits, and debug logging

### Risk 3: Performance Impact
**Mitigation**: Run performance tests before/after to measure impact

---

## Next Steps

1. ✅ Create this plan document
2. ⏳ Implement Seek operation
3. ⏳ Implement Append operation
4. ⏳ Add Atomicity tests
5. ⏳ Add DataNode failure tests
6. ⏳ Add Block corruption tests
7. ⏳ Run full test suite
8. ⏳ Update documentation

---

## Progress Tracking

Track progress in `TODO.md` or using GitHub issues.

### Current Status:
- **Seek**: Not started
- **Append**: Not started
- **Atomicity**: Not started
- **DataNode Failures**: Not started
- **Corruption**: Not started

### Completed:
- Planning document ✅

---

## References

- [HDFS_TEST_REQUIREMENTS.md](./HDFS_TEST_REQUIREMENTS.md) - Full test requirements
- [CLIENT_OPERATIONS_STATUS.md](./CLIENT_OPERATIONS_STATUS.md) - Current implementation status
- [Apache Hadoop Testing Docs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/testing.html)

---

**Last Updated**: 2025-12-21
**Author**: Claude Code (with human collaboration)
