#!/usr/bin/env bash
# The stock Apache Hadoop Java client against a Kerberized Pillar: a
# throwaway MIT KDC (tests/kerberos/kdc.sh, realm PILLAR.TEST), a NameNode
# and three DataNodes with keytabs on 127.0.0.1. Users log in from keytabs;
# SIMPLE clients and impersonation must be refused, and only DataNodes with
# a dn/ principal may join the NameNode.
#
#   tests/interop/run-java-kerberos.sh       # needs a JDK (11+), curl, MIT krb5
#   RPC_PROTECTION=privacy tests/interop/run-java-kerberos.sh   # hadoop.rpc.protection
set -euo pipefail

HADOOP_VERSION=${HADOOP_VERSION:-3.5.0}
export RPC_PROTECTION=${RPC_PROTECTION:-authentication}
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
    --kerberos-keytab "$KDC/nn.keytab" --kerberos-principal nn/localhost@PILLAR.TEST \
    --rpc-protection "$RPC_PROTECTION" --user-groups "bob=staff" \
    --proxy-users "oozie.hosts=127.0.0.1;oozie.groups=staff" >"$WORK/nn.log" 2>&1 &
echo $! >"$WORK/nn.pid"
for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/19100) 2>/dev/null && break; sleep 0.2; done
# DataNodes log in from their keytab (KDCs from krb5.conf) to call the NameNode.
export KRB5_CONFIG=$KDC/krb5.conf
datanode() { # index uuid [kerberos flags...]; $DN_PROTECTION overrides the protection
    local i=$1 uuid=$2; shift 2
    mkdir -p "$WORK/dn$i"
    "$BIN/pillar-datanode" --namenode 127.0.0.1:19100 --uuid "$uuid" --hostname 127.0.0.1 \
        --rpc-addr "127.0.0.1:1996$i" --data-addr "127.0.0.1:5011$i" --http-addr "127.0.0.1:5017$i" \
        --volume-mount-point "$WORK/dn$i" --transport tcp --rpc-protection "${DN_PROTECTION:-$RPC_PROTECTION}" "$@" >"$WORK/dn$i.log" 2>&1 &
    echo $! >"$WORK/dn$i.pid"
}
for i in 0 1 2; do
    datanode $i "kdn$i" --kerberos-keytab "$KDC/dn.keytab" --kerberos-principal dn/localhost@PILLAR.TEST \
        --namenode-principal nn/localhost@PILLAR.TEST
done
# Impostors: no Kerberos at all, and a user's keytab. Neither may register.
datanode 3 rogue-simple
datanode 4 rogue-alice --kerberos-keytab "$KDC/alice.keytab" --kerberos-principal alice@PILLAR.TEST \
    --namenode-principal nn/localhost@PILLAR.TEST
# With RPC protection on, a real DataNode asking for less is refused too.
if [ "$RPC_PROTECTION" != authentication ]; then
    DN_PROTECTION=authentication datanode 5 weak-dn --kerberos-keytab "$KDC/dn.keytab" \
        --kerberos-principal dn/localhost@PILLAR.TEST --namenode-principal nn/localhost@PILLAR.TEST
fi
for _ in $(seq 50); do [ "$(grep -c 'Registered datanode' "$WORK/nn.log")" -ge 3 ] && break; sleep 0.2; done
sleep 3

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
if [ "$RPC_PROTECTION" != authentication ]; then
    java_client hdfs://localhost:19100 unprotected "$KDC" || fail "unprotected"
fi

# Only the three Kerberized DataNodes joined; both impostors were refused.
grep 'Registered datanode' "$WORK/nn.log" | grep -q rogue && fail "an impostor DataNode registered"
grep -q 'no Kerberos credentials' "$WORK/nn.log" || fail "the SIMPLE DataNode was not refused"
grep -q 'alice@PILLAR.TEST is not a dn principal' "$WORK/nn.log" || fail "alice's DataNode was not refused"
echo "ok    DataNodes without a dn/ Kerberos principal cannot join"
if [ "$RPC_PROTECTION" != authentication ]; then
    grep 'Registered datanode' "$WORK/nn.log" | grep -q weak-dn && fail "a DataNode without $RPC_PROTECTION registered"
    grep -q 'asks for RPC protection authentication' "$WORK/nn.log" || fail "the unprotected DataNode was not refused"
    echo "ok    DataNode calls are protected ($RPC_PROTECTION); a DataNode asking for less is refused"
fi

# The DataNode-to-NameNode authentication against this KDC: replays, users
# and forged NameNode replies are refused.
(cd "$ROOT" && PILLAR_KDC_DIR=$KDC cargo test -q -p pillar-common --lib grpc_auth 2>&1 | tail -3) || fail "grpc_auth tests"
echo "Kerberos interop passed (Hadoop $HADOOP_VERSION, RPC protection $RPC_PROTECTION)"
