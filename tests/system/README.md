# Pillar system tests

End-to-end tests on Kubernetes with [Chainsaw](https://kyverno.github.io/chainsaw/):
real NameNode and DataNode pods built from this tree, and the stock Apache
Hadoop client (`apache/hadoop:3.3.6`) running `hdfs dfs` against them.

```bash
tests/system/kind.sh                          # everything
TESTS=tests/namenode-restart tests/system/kind.sh
KEEP=1 tests/system/kind.sh                   # leave the cluster up
```

`kind.sh` builds the images from the repo `Dockerfile`, creates a kind
cluster with a private kubeconfig, loads the images into it, and runs
Chainsaw. It needs docker, kind, kubectl, helm and chainsaw. The GitHub
workflow `.github/workflows/system-chainsaw.yml` runs the same script.

## Tests

Each test deploys its own cluster (`common/pillar-cluster.yaml`: a NameNode
with its namespace on a PersistentVolume, three DataNodes) and client
(`common/hadoop-client.yaml`, 1 MiB blocks, replication 3) in its own
namespace, so tests run in parallel.

| Test | What it checks |
|------|----------------|
| `basic-operations` | `mkdir -p`, `put`, `ls`, `stat`, `cat`, `cp`, `get`, `put -f`, `rm`; reads compared byte for byte |
| `hadoop-cli` | more of `hadoop fs`, including commands that must fail (`mkdir` of an existing directory, `rmdir` of a non-empty one, `cat` of a missing file) |
| `replication` | every DataNode holds a block file and a `.meta` checksum file for each of the file's blocks; deleting the file removes them all |
| `namenode-restart` | the NameNode pod is force-deleted and comes back on its volume; files, renames and deletes survive; DataNodes re-register; new writes work |
| `datanode-failure` | a DataNode pod is force-deleted while the Hadoop client streams a file through it; pipeline recovery finishes the write with the right content |
| `lease-recovery` | a writer pod is force-killed mid-file; the file stays open (`dfsadmin -listOpenFiles`, overwrite refused) until `hdfs debug recoverLease`, which keeps exactly the finished blocks |
| `helm-chart` | the Helm chart, installed as shipped, serves the client |

Not covered, because Pillar does not implement them: `hdfs dfs -checksum`,
`hdfs fsck`, and the NameNode web UI.

Converted from KUTTL in October 2026. The KUTTL manifests had never been
run: the NameNode had no arguments and tried to reach etcd on localhost,
the DataNodes' settings were environment variables that their command-line
defaults overrode, and most steps only checked that a pod completed.
