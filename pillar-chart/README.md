# Pillar HDFS Helm Chart

Drop-in replacement for Apache Hadoop HDFS written in Rust with 3-5x performance improvements.

## Architecture

**Pillar HDFS** is a high-performance distributed filesystem that implements the Apache Hadoop HDFS protocol in Rust:

```
┌─────────────────────────────────────────────────────────────┐
│ HDFS Clients (Spark, Hive, hadoop fs, custom apps)         │
│  ↓ Native HDFS Protocol (gRPC/Protobuf)                    │
├─────────────────────────────────────────────────────────────┤
│ Pillar NameNode (ClusterIP)  Pillar DataNodes (Headless)   │
│ - Metadata management        - Block storage (DaemonSet)    │
│ - Block allocation           - Uses Flint RWO volumes       │
│ - Client coordination        - SPDK backend (373 MB/s)      │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

1. **NameNode** (StatefulSet, 1 replica)
   - Manages HDFS namespace
   - Coordinates block placement
   - ClusterIP service for client access

2. **DataNode** (DaemonSet)
   - Runs on every Kubernetes node
   - Stores HDFS blocks on Flint RWO volumes
   - Headless service for direct pod access

3. **CSI Driver** (optional)
   - Administrative namespace management
   - **NOT** for mounting into pods (apps use HDFS protocol)

## Prerequisites

- Kubernetes 1.20+
- Helm 3.0+
- **Flint CSI Driver** installed (for DataNode storage)
- Storage class `flint` available

## Installation

### 1. Install Flint CSI Driver First

```bash
# Pillar DataNodes use Flint for high-performance storage
helm install flint flint-chart/
```

### 2. Install Pillar HDFS

```bash
helm install pillar pillar-chart/ \
  --set datanode.storage.storageClass=flint \
  --set datanode.storage.size=100Gi
```

### 3. Verify Installation

```bash
# Check NameNode
kubectl get pods -l app=pillar-namenode
kubectl logs -l app=pillar-namenode

# Check DataNodes
kubectl get pods -l app=pillar-datanode
kubectl get svc pillar-datanode  # Should be Headless (ClusterIP: None)

# Check services
kubectl get svc pillar-namenode   # ClusterIP
```

## Configuration

### values.yaml

```yaml
namenode:
  replicas: 1
  resources:
    requests:
      memory: 2Gi
      cpu: 500m
  metadata:
    size: 10Gi
  hdfs:
    blockSize: 128     # MB
    replication: 3

datanode:
  storage:
    storageClass: flint
    size: 100Gi        # Per-node capacity
  resources:
    requests:
      memory: 4Gi
      cpu: 1000m
```

## Usage

### Applications Access HDFS Directly

**Important:** Applications do NOT use CSI volume mounts. They connect via **native HDFS protocol**.

### Example 1: Hadoop CLI

```bash
# Port-forward for local access (optional)
kubectl port-forward svc/pillar-namenode 9000:9000

# Use hadoop fs commands
hadoop fs -ls hdfs://pillar-namenode:9000/
hadoop fs -mkdir hdfs://pillar-namenode:9000/data
hadoop fs -put localfile.txt hdfs://pillar-namenode:9000/data/
hadoop fs -get hdfs://pillar-namenode:9000/data/file.txt
```

### Example 2: Spark Application

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: spark-example
spec:
  containers:
  - name: spark
    image: apache/spark:3.5.0
    env:
    - name: HADOOP_NAMENODE
      value: "pillar-namenode:9000"
    command:
    - spark-submit
    - --class
    - org.apache.spark.examples.SparkPi
    - --master
    - local[2]
    - --conf
    - spark.hadoop.fs.defaultFS=hdfs://pillar-namenode:9000
    - /opt/spark/examples/jars/spark-examples.jar
```

**Scala code:**
```scala
val spark = SparkSession.builder()
  .appName("Pillar HDFS Example")
  .config("spark.hadoop.fs.defaultFS", "hdfs://pillar-namenode:9000")
  .getOrCreate()

// Read from HDFS
val df = spark.read.parquet("hdfs://pillar-namenode:9000/data/input")

// Write to HDFS
df.write.parquet("hdfs://pillar-namenode:9000/data/output")
```

### Example 3: Hive

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: hive-site
data:
  hive-site.xml: |
    <configuration>
      <property>
        <name>fs.defaultFS</name>
        <value>hdfs://pillar-namenode:9000</value>
      </property>
      <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>hdfs://pillar-namenode:9000/warehouse</value>
      </property>
    </configuration>
```

### Example 4: Custom Application (Java)

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

Configuration conf = new Configuration();
conf.set("fs.defaultFS", "hdfs://pillar-namenode:9000");

FileSystem fs = FileSystem.get(conf);
fs.mkdirs(new Path("/myapp/data"));
fs.copyFromLocalFile(new Path("local.txt"), new Path("/myapp/data/file.txt"));
```

### Example 5: Python (hdfs3 library)

```python
from hdfs3 import HDFileSystem

hdfs = HDFileSystem(host='pillar-namenode', port=9000)

# List files
hdfs.ls('/data')

# Write file
with hdfs.open('/data/output.txt', 'wb') as f:
    f.write(b'Hello HDFS')

# Read file
with hdfs.open('/data/output.txt', 'rb') as f:
    data = f.read()
```

## HDFS Connection Details

### Service Endpoints

```bash
# NameNode (ClusterIP)
pillar-namenode:9000       # HDFS RPC protocol
pillar-namenode:9870       # Web UI

# DataNodes (Headless - direct pod access)
pillar-datanode-<pod>.pillar-datanode:50010  # Data transfer
pillar-datanode-<pod>.pillar-datanode:9867   # RPC
```

### core-site.xml Configuration

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://pillar-namenode:9000</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value> <!-- 128 MB -->
  </property>
</configuration>
```

## Performance

### Expected Improvements over Java HDFS

- **Metadata operations:** 3-5x faster (no GC, lock-free data structures)
- **Block I/O:** 2-4x faster (SPDK backend: 373 MB/s vs 97 MB/s)
- **Memory usage:** 2-3x more efficient (no JVM overhead)
- **Startup time:** 10x faster (native binary vs JVM)

### Benchmarking

```bash
# TestDFSIO write
hadoop jar hadoop-mapreduce-client-jobclient-tests.jar TestDFSIO \
  -write -nrFiles 10 -fileSize 1GB \
  -resFile /tmp/TestDFSIO-write.txt

# TestDFSIO read
hadoop jar hadoop-mapreduce-client-jobclient-tests.jar TestDFSIO \
  -read -nrFiles 10 -fileSize 1GB \
  -resFile /tmp/TestDFSIO-read.txt
```

## Monitoring

### NameNode Web UI

```bash
kubectl port-forward svc/pillar-namenode 9870:9870
# Open http://localhost:9870
```

### DataNode Web UI

```bash
kubectl port-forward pillar-datanode-<pod> 50075:50075
# Open http://localhost:50075
```

### Metrics

```bash
# Check NameNode metrics
curl http://pillar-namenode:9870/jmx

# Check DataNode metrics
curl http://pillar-datanode-<pod>:50075/jmx
```

## Troubleshooting

### Check NameNode logs
```bash
kubectl logs -l app=pillar-namenode --tail=100 -f
```

### Check DataNode logs
```bash
kubectl logs -l app=pillar-datanode --tail=100 -f
```

### Verify DataNode registration
```bash
# Check NameNode logs for "Registered datanode" messages
kubectl logs -l app=pillar-namenode | grep "Registered datanode"
```

### Test HDFS connectivity
```bash
# From within cluster
kubectl run -it --rm hdfs-test --image=apache/hadoop:3.3.6 -- \
  hadoop fs -ls hdfs://pillar-namenode:9000/
```

## Upgrade

```bash
helm upgrade pillar pillar-chart/ \
  --set namenode.image.tag=v0.2.0 \
  --set datanode.image.tag=v0.2.0
```

## Uninstall

```bash
helm uninstall pillar
```

## License

Apache 2.0

## Support

- GitHub: https://github.com/ddalton/pillar
- Issues: https://github.com/ddalton/pillar/issues
