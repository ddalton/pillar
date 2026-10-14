#!/usr/bin/env bash
# Run the stock Apache Hadoop Java client (DFSClient, the code behind
# `hadoop fs`) against a local Pillar cluster: a NameNode and three
# DataNodes on 127.0.0.1, no etcd. Midway the NameNode is killed with
# SIGKILL and restarted on the same name directory; the Java client then
# reads back what it wrote before the crash.
#
#   tests/interop/run-java-interop.sh            # needs a JDK (11+) and curl
#   BLOCK_TOKENS=1 tests/interop/run-java-interop.sh   # with block access tokens
#   ENCRYPT=aes tests/interop/run-java-interop.sh       # encrypted data transfer (AES/CTR)
#   ENCRYPT=rc4 tests/interop/run-java-interop.sh       # encrypted, SASL wrapping only
#
# Hadoop's shaded client jars are fetched from Maven Central into
# ~/.cache/pillar-interop and checked against Maven's SHA-1 files.
set -euo pipefail

HADOOP_VERSION=${HADOOP_VERSION:-3.5.0}
NN_FLAGS=""
[ -n "${BLOCK_TOKENS:-}" ] && NN_FLAGS="--block-access-tokens"
DN_FLAGS=""
# Encrypted data transfer: the NameNode tells clients (getServerDefaults)
# and hands out data encryption keys; DataNodes refuse anything else. With
# ENCRYPT=aes the client offers AES/CTR (the fast path).
if [ -n "${ENCRYPT:-}" ]; then
    NN_FLAGS="$NN_FLAGS --encrypt-data-transfer"
    DN_FLAGS="--encrypt-data-transfer"
    [ "$ENCRYPT" = aes ] && export DATA_CIPHER_SUITES=AES/CTR/NoPadding
    # The DataNodes log how each connection was secured (checked below).
    export RUST_LOG="info,krb5=warn,pillar_datanode::transport::sasl=debug"
fi
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

start_namenode() { # log file
    "$BIN/pillar-namenode" --rpc-addr 127.0.0.1:19000 --http-addr 127.0.0.1:19870 \
        --etcd-endpoints none --replication 3 --block-size-mb 1 --name-dir "$WORK/name" \
        --user-groups "alice=staff;bob=staff" --proxy-users "oozie.hosts=127.0.0.1;oozie.users=bob" $NN_FLAGS >"$1" 2>&1 &
    echo $! >"$WORK/nn.pid"
    for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/19000) 2>/dev/null && break; sleep 0.2; done
}
start_namenode "$WORK/nn.log"
for i in 0 1 2; do
    mkdir -p "$WORK/dn$i"
    "$BIN/pillar-datanode" --namenode 127.0.0.1:19000 --uuid "dn$i" --hostname 127.0.0.1 \
        --rpc-addr "127.0.0.1:1986$i" --data-addr "127.0.0.1:5001$i" --http-addr "127.0.0.1:5007$i" \
        --volume-mount-point "$WORK/dn$i" --transport tcp $DN_FLAGS >"$WORK/dn$i.log" 2>&1 &
    echo $! >"$WORK/dn$i.pid"
done
for _ in $(seq 50); do [ "$(grep -c 'Registered datanode' "$WORK/nn.log")" -ge 3 ] && break; sleep 0.2; done
sleep 1 # first heartbeats carry capacity

cd "$HERE"
java_client() { # [mode]
    HADOOP_USER_NAME=alice java -cp "$CACHE/*" HdfsInterop.java hdfs://127.0.0.1:19000 "$@" 2>&1 \
        | grep -vE '^(SLF4J|WARNING)|^$'
}
fail() {
    echo "Java interop FAILED ($1); logs:" >&2
    tail -n 30 "$WORK"/*.log >&2
    exit 1
}

java_client || fail "before restart"
if [ -n "${ENCRYPT:-}" ]; then
    # Every data connection was encrypted, the client's as configured:
    # AES/CTR when it offers it, else DIGEST-MD5 privacy (RC4). DataNodes
    # always offer AES to each other (their key names block pool "pillar").
    aes=$(cat "$WORK"/dn*.log | grep -c "AES/CTR for" || true)
    rc4=$(cat "$WORK"/dn*.log | grep -c "SASL privacy for" || true)
    aes_clients=$(cat "$WORK"/dn*.log | grep "AES/CTR for" | grep -vc " pillar " || true)
    if [ "$ENCRYPT" = aes ]; then
        [ "$aes_clients" -gt 0 ] && [ "$rc4" -eq 0 ] || fail "expected AES/CTR only (aes=$aes rc4=$rc4)"
    else
        [ "$rc4" -gt 0 ] && [ "$aes_clients" -eq 0 ] || fail "expected RC4 for the client (rc4=$rc4 aes from clients=$aes_clients)"
    fi
    echo "ok    every data connection encrypted: $aes AES/CTR ($aes_clients from the client), $rc4 SASL RC4"
fi

# Crash the NameNode and bring it back on the same name directory. The
# DataNodes keep running; their next heartbeat is answered with a register
# command, and they re-register and send full block reports.
kill -9 "$(cat "$WORK/nn.pid")"
wait "$(cat "$WORK/nn.pid")" 2>/dev/null || true
start_namenode "$WORK/nn-restarted.log"
for _ in $(seq 100); do
    [ "$(grep -c 'Received block report' "$WORK/nn-restarted.log")" -ge 3 ] && break
    sleep 0.2
done
grep -q 'Namespace loaded' "$WORK/nn-restarted.log" || fail "namespace not loaded"

java_client verify || fail "after restart"

# Pipeline recovery: kill a DataNode while the Java client is writing a
# block through it; the write must complete on the survivors.
java_client pipeline "$WORK/ready" "$WORK/go" &
java_pid=$!
for _ in $(seq 150); do [ -f "$WORK/ready" ] && break; sleep 0.2; done
[ -f "$WORK/ready" ] || fail "the pipeline writer never got going"
kill -9 "$(cat "$WORK/dn1.pid")"
touch "$WORK/go"
wait "$java_pid" || fail "write across a DataNode failure"
echo "Java interop passed (Hadoop $HADOOP_VERSION), including a NameNode crash and restart and a DataNode failure mid-write"
