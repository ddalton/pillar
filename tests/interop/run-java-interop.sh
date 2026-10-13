#!/usr/bin/env bash
# Run the stock Apache Hadoop Java client (DFSClient, the code behind
# `hadoop fs`) against a local Pillar cluster: a NameNode and three
# DataNodes on 127.0.0.1, no etcd.
#
#   tests/interop/run-java-interop.sh            # needs a JDK (11+) and curl
#
# Hadoop's shaded client jars are fetched from Maven Central into
# ~/.cache/pillar-interop and checked against Maven's SHA-1 files.
set -euo pipefail

HADOOP_VERSION=${HADOOP_VERSION:-3.5.0}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CACHE=${PILLAR_INTEROP_CACHE:-$HOME/.cache/pillar-interop}
WORK=$(mktemp -d)
trap 'kill $(cat "$WORK"/*.pid 2>/dev/null) 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"' EXIT

fetch() { # maven-path
    local f="$CACHE/$(basename "$1")" url="https://repo1.maven.org/maven2/$1"
    if [ ! -f "$f" ]; then
        curl -sSf -o "$f.tmp" "$url"
        local want; want=$(curl -sSf "$url.sha1" | cut -c1-40)
        echo "$want  $f.tmp" | shasum -a 1 -c - >/dev/null || { echo "checksum mismatch: $url" >&2; exit 1; }
        mv "$f.tmp" "$f"
    fi
}
mkdir -p "$CACHE"
fetch "org/apache/hadoop/hadoop-client-api/$HADOOP_VERSION/hadoop-client-api-$HADOOP_VERSION.jar"
fetch "org/apache/hadoop/hadoop-client-runtime/$HADOOP_VERSION/hadoop-client-runtime-$HADOOP_VERSION.jar"
fetch "org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
fetch "commons-logging/commons-logging/1.2/commons-logging-1.2.jar"

(cd "$ROOT" && cargo build -q -p pillar-namenode -p pillar-datanode --bins)
BIN=${CARGO_TARGET_DIR:-$ROOT/target}/debug

"$BIN/pillar-namenode" --rpc-addr 127.0.0.1:19000 --http-addr 127.0.0.1:19870 \
    --etcd-endpoints none --replication 3 --block-size-mb 1 >"$WORK/nn.log" 2>&1 &
echo $! >"$WORK/nn.pid"
for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/19000) 2>/dev/null && break; sleep 0.2; done
for i in 0 1 2; do
    mkdir -p "$WORK/dn$i"
    "$BIN/pillar-datanode" --namenode 127.0.0.1:19000 --uuid "dn$i" --hostname 127.0.0.1 \
        --rpc-addr "127.0.0.1:1986$i" --data-addr "127.0.0.1:5001$i" --http-addr "127.0.0.1:5007$i" \
        --volume-mount-point "$WORK/dn$i" --transport tcp >"$WORK/dn$i.log" 2>&1 &
    echo $! >"$WORK/dn$i.pid"
done
for _ in $(seq 50); do [ "$(grep -c 'Registered datanode' "$WORK/nn.log")" -ge 3 ] && break; sleep 0.2; done
sleep 1 # first heartbeats carry capacity

cd "$HERE"
if HADOOP_USER_NAME=hdfs java -cp "$CACHE/*" HdfsInterop.java hdfs://127.0.0.1:19000 2>&1 \
        | grep -vE '^(SLF4J|WARNING)|^$'; then
    echo "Java interop passed (Hadoop $HADOOP_VERSION)"
else
    echo "Java interop FAILED; logs:" >&2
    tail -n 30 "$WORK"/*.log >&2
    exit 1
fi
