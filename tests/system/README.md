# Pillar HDFS System Test Framework

This is a declarative test framework for testing Pillar HDFS on Kubernetes using [KUTTL](https://kuttl.dev/).

## Overview

Pillar is a drop-in replacement for Apache Hadoop HDFS written in Rust. These system tests verify:
- **Protocol Compatibility**: 100% wire compatibility with Hadoop HDFS protocol
- **Core Operations**: All standard HDFS operations (mkdir, put, get, ls, etc.)
- **Replication**: Multi-DataNode replication and consistency
- **Hadoop CLI**: Compatibility with standard `hadoop fs` commands
- **Resilience**: DataNode failure and recovery scenarios

## Prerequisites

### 1. Install KUTTL

```bash
# Using kubectl krew plugin manager (recommended)
kubectl krew install kuttl

# Or install directly
make install-kuttl

# Or download binary manually from:
# https://github.com/kudobuilder/kuttl/releases
```

### 2. Kubernetes Cluster

- A running Kubernetes cluster (local or remote)
- `kubectl` configured to access the cluster
- Sufficient resources (at least 3 worker nodes recommended for replication tests)

### 3. Build Pillar Docker Images

```bash
# From the project root
cd /path/to/pillar

# Build NameNode image
docker build -f Dockerfile.namenode -t pillar/namenode:latest .

# Build DataNode image
docker build -f Dockerfile.datanode -t pillar/datanode:latest .

# If using remote cluster, push to registry
docker tag pillar/namenode:latest <your-registry>/pillar/namenode:latest
docker push <your-registry>/pillar/namenode:latest
docker tag pillar/datanode:latest <your-registry>/pillar/datanode:latest
docker push <your-registry>/pillar/datanode:latest
```

## Project Structure

```
tests/system/
├── kuttl-testsuite.yaml                    # Main test suite config
├── kuttl-testsuite-datanode-failure.yaml   # Separate config for failure tests
├── Makefile                                # Test automation
├── README.md                               # This file
├── tests-standard/                         # Tests that run in parallel
│   ├── basic-operations/                   # Basic HDFS operations
│   │   ├── 00-namenode.yaml                # Deploy NameNode
│   │   ├── 00-assert.yaml                  # Assert NameNode ready
│   │   ├── 01-datanodes.yaml               # Deploy DataNodes
│   │   ├── 01-assert.yaml                  # Assert DataNodes ready
│   │   ├── 02-hadoop-client.yaml           # Run basic operations
│   │   ├── 02-assert.yaml                  # Assert tests passed
│   │   ├── 03-verify-data.yaml             # Verify data persistence
│   │   └── 03-assert.yaml                  # Assert verification passed
│   ├── replication/                        # Multi-DataNode replication
│   │   ├── 00-setup.yaml                   # Setup with 3 DataNodes
│   │   ├── 00-assert.yaml                  # Assert setup ready
│   │   ├── 01-write-large-file.yaml        # Write 200MB file
│   │   ├── 01-assert.yaml                  # Assert write succeeded
│   │   ├── 02-verify-replication.yaml      # Verify 3-way replication
│   │   └── 02-assert.yaml                  # Assert replication verified
│   └── hadoop-cli-compat/                  # Hadoop CLI compatibility
│       ├── 00-setup.yaml                   # Setup HDFS cluster
│       ├── 00-assert.yaml                  # Assert cluster ready
│       ├── 01-hadoop-cli-tests.yaml        # Run comprehensive CLI tests
│       └── 01-assert.yaml                  # Assert all tests passed
└── tests/                                  # Tests that run in isolation
    └── datanode-failure/                   # DataNode failure scenarios
        └── (future implementation)
```

## Running Tests

### Run All Tests

```bash
# Run all standard tests (parallel execution)
make test

# Run all tests including isolation tests
make test-all
```

### Run Specific Tests

```bash
# Basic operations only
make test-basic

# Replication testing
make test-replication

# Hadoop CLI compatibility
make test-cli

# DataNode failure (runs separately)
make test-datanode-failure
```

### Advanced Options

```bash
# Verbose output (for debugging)
make test-verbose

# Debug mode (keep resources on failure)
make test-debug

# Custom timeout
kubectl kuttl test --config kuttl-testsuite.yaml --timeout 900
```

## Test Descriptions

### 1. Basic Operations Test (`basic-operations/`)

**Purpose**: Verify core HDFS operations work correctly with Pillar.

**What it tests**:
- NameNode and DataNode deployment
- Directory creation (`mkdir`)
- File upload (`put`)
- File download (`get`)
- Directory listing (`ls`)
- File reading (`cat`)
- File statistics (`stat`)
- File copying within HDFS (`cp`)
- Block replication
- Data persistence across pods

**Duration**: ~2-3 minutes

### 2. Replication Test (`replication/`)

**Purpose**: Verify multi-DataNode replication with 3-way replication.

**What it tests**:
- 3 DataNode cluster deployment
- Large file (200MB) upload creating multiple blocks
- 3-way block replication across DataNodes
- Block health verification via `hadoop fsck`
- Correct replication factor reporting

**Duration**: ~4-5 minutes

### 3. Hadoop CLI Compatibility Test (`hadoop-cli-compat/`)

**Purpose**: Comprehensive compatibility testing with standard Hadoop CLI.

**What it tests**:
- All common `hadoop fs` commands:
  - `ls`, `mkdir`, `put`, `get`, `cat`
  - `cp`, `mv`, `rm`, `rmdir`
  - `du`, `df`, `stat`
  - `test` (existence, directory, file checks)
  - `setrep`, `count`, `checksum`
- `hadoop fsck` for filesystem verification
- `hadoop dfsadmin` for cluster administration

**Duration**: ~3-4 minutes

### 4. DataNode Failure Test (`datanode-failure/`)

**Purpose**: Test resilience and recovery when DataNodes fail.

**What it tests** (future implementation):
- Data availability when a DataNode goes down
- Block re-replication after DataNode failure
- Cluster recovery when DataNode returns
- No data loss during failures

**Duration**: ~5-7 minutes

## Configuration

### Test Timeouts

Edit timeout values in `kuttl-testsuite.yaml`:

```yaml
timeout: 600  # Overall test timeout in seconds
```

### Parallel Execution

Adjust parallelism in `kuttl-testsuite.yaml`:

```yaml
parallel: 3  # Number of tests to run concurrently
```

### Custom HDFS Configuration

Modify environment variables in test YAML files:

```yaml
env:
- name: HDFS_BLOCK_SIZE
  value: "128"  # Block size in MB
- name: HDFS_REPLICATION
  value: "3"    # Replication factor
- name: HDFS_BLOCK_POOL_ID
  value: "BP-pillar-test"
```

## Debugging Failed Tests

### View Test Logs

```bash
# Find test pods
kubectl get pods -A | grep kuttl-test

# View logs
kubectl logs <pod-name> -n <namespace>

# View NameNode logs
kubectl logs pillar-namenode-0 -n <test-namespace>

# View DataNode logs
kubectl logs pillar-datanode-0 -n <test-namespace>
```

### Keep Resources After Failure

```bash
# Resources won't be deleted on test failure
make test-debug

# Or manually
kubectl kuttl test --config kuttl-testsuite.yaml --skip-delete

# Inspect resources
kubectl get all -n <test-namespace>
```

### Check Events

```bash
kubectl get events -n <test-namespace> --sort-by='.lastTimestamp'
```

### Manual Cleanup

```bash
# Clean up all test namespaces
make clean

# Or manually delete specific namespace
kubectl delete ns <kuttl-test-namespace>
```

## Best Practices

1. **Resource Limits**: Ensure your cluster has sufficient resources
   - Minimum 4 CPU cores and 8GB RAM recommended
   - At least 3 worker nodes for replication tests

2. **Image Availability**: Ensure Docker images are built and accessible
   - Use `imagePullPolicy: IfNotPresent` for local testing
   - Push to registry for remote cluster testing

3. **Test Isolation**: Each test creates its own namespace
   - Tests don't interfere with each other
   - Failed tests don't affect other tests

4. **Cleanup**: Always clean up after testing
   - Use `make clean` to remove test namespaces
   - Check for stuck PVCs that may need manual deletion

## CI/CD Integration

### GitHub Actions Example

```yaml
name: System Tests
on: [push, pull_request]

jobs:
  system-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Kubernetes
        uses: helm/kind-action@v1

      - name: Install KUTTL
        run: make install-kuttl

      - name: Build Docker images
        run: |
          docker build -f Dockerfile.namenode -t pillar/namenode:latest .
          docker build -f Dockerfile.datanode -t pillar/datanode:latest .
          kind load docker-image pillar/namenode:latest
          kind load docker-image pillar/datanode:latest

      - name: Run system tests
        run: make test-all
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `ImagePullBackOff` | Build images or update `imagePullPolicy` |
| Test timeout | Increase timeout in testsuite config |
| PVC binding failure | Check StorageClass exists and has provisioner |
| NameNode not ready | Check logs for errors, verify metadata volume |
| DataNode connection failure | Verify NameNode service DNS resolution |
| Hadoop client errors | Check `core-site.xml` configuration |

### Logs to Check

```bash
# NameNode startup and initialization
kubectl logs pillar-namenode-0 -n <namespace>

# DataNode registration and heartbeat
kubectl logs pillar-datanode-0 -n <namespace>

# Test pod execution
kubectl logs hadoop-client -n <namespace>
```

## Adding New Tests

1. Create a new directory under `tests-standard/` or `tests/`
2. Add numbered YAML files for each step:
   - `XX-*.yaml` - Resources to create
   - `XX-assert.yaml` - Assertions to verify

Example structure:

```
tests-standard/my-new-test/
├── 00-setup.yaml      # Initial resources
├── 00-assert.yaml     # Verify setup
├── 01-test-step.yaml  # Execute test
├── 01-assert.yaml     # Verify results
└── README.md          # Test documentation
```

## Additional Resources

- [KUTTL Documentation](https://kuttl.dev/)
- [Pillar HDFS Documentation](../../README.md)
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/stable/)
- [Kubernetes Testing Best Practices](https://kubernetes.io/docs/tasks/debug/)

## Contributing

When adding new tests:
1. Follow the existing naming and structure conventions
2. Add comprehensive assertions
3. Include test documentation in README
4. Test locally before submitting PR
5. Ensure tests are idempotent and can run in parallel
