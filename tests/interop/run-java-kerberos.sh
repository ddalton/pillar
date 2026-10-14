#!/usr/bin/env bash
# The stock Apache Hadoop Java client against a Kerberized Pillar: a
# throwaway MIT KDC (tests/kerberos/kdc.sh, realm PILLAR.TEST), a NameNode
# and three DataNodes with keytabs on 127.0.0.1. Users log in from keytabs;
# SIMPLE clients and impersonation must be refused.
#
#   tests/interop/run-java-kerberos.sh       # needs a JDK (11+), curl, MIT krb5
set -euo pipefail

HADOOP_VERSION=${HADOOP_VERSION:-3.5.0}
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CACHE=${PILLAR_INTEROP_CACHE:-$HOME/.cache/pillar-interop}
WORK=$(mktemp -d)
KDC=$WORK/kdc
trap 'kill $(cat "$WORK"/*.pid "$KDC/kdc.pid" 2>/dev/null) 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"' EXIT

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

"$ROOT/tests/kerberos/kdc.sh" "$KDC" 18891
(cd "$ROOT" && cargo build -q -p pillar-namenode -p pillar-datanode --bins)
BIN=${CARGO_TARGET_DIR:-$ROOT/target}/debug

"$BIN/pillar-namenode" --rpc-addr 127.0.0.1:19100 --http-addr 127.0.0.1:19170 \
    --etcd-endpoints none --replication 3 --block-size-mb 1 --name-dir "$WORK/name" \
    --kerberos-keytab "$KDC/nn.keytab" --kerberos-principal nn/localhost@PILLAR.TEST >"$WORK/nn.log" 2>&1 &
echo $! >"$WORK/nn.pid"
for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/19100) 2>/dev/null && break; sleep 0.2; done
for i in 0 1 2; do
    mkdir -p "$WORK/dn$i"
    "$BIN/pillar-datanode" --namenode 127.0.0.1:19100 --uuid "kdn$i" --hostname 127.0.0.1 \
        --rpc-addr "127.0.0.1:1996$i" --data-addr "127.0.0.1:5011$i" --http-addr "127.0.0.1:5017$i" \
        --volume-mount-point "$WORK/dn$i" --transport tcp \
        --kerberos-keytab "$KDC/dn.keytab" --kerberos-principal dn/localhost@PILLAR.TEST >"$WORK/dn$i.log" 2>&1 &
    echo $! >"$WORK/dn$i.pid"
done
for _ in $(seq 50); do [ "$(grep -c 'Registered datanode' "$WORK/nn.log")" -ge 3 ] && break; sleep 0.2; done
sleep 1

cd "$HERE"
java_client() {
    java -Djava.security.krb5.conf="$KDC/krb5.conf" -cp "$CACHE/*" KerberosInterop.java "$@" 2>&1 \
        | grep -vE '^(SLF4J|WARNING)|^$'
}
fail() {
    echo "Kerberos interop FAILED ($1); logs:" >&2
    tail -n 30 "$WORK"/*.log "$KDC/kdc.log" >&2
    exit 1
}
# hdfs://localhost: the client derives the NameNode's principal from the
# host it connects to (nn/_HOST -> nn/localhost).
java_client hdfs://localhost:19100 kerberos "$KDC" || fail "kerberos"
java_client hdfs://localhost:19100 simple || fail "simple"
echo "Kerberos interop passed (Hadoop $HADOOP_VERSION)"
