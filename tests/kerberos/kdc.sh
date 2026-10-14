#!/usr/bin/env bash
# Start a throwaway MIT KDC for Pillar's Kerberos tests.
#
#   tests/kerberos/kdc.sh <dir> [port]       (default port 18891)
#
# Realm PILLAR.TEST, unprivileged: database, keytabs, krb5.conf and log all
# live in <dir>. Principals, each with a keytab in <dir>:
#   nn/localhost   the NameNode        (nn.keytab)
#   dn/localhost   every DataNode      (dn.keytab)
#   hdfs           the HDFS superuser  (hdfs.keytab)
#   alice, bob     users               (alice.keytab, bob.keytab)
# Needs MIT krb5's krb5kdc, kdb5_util and kadmin.local (Debian/Ubuntu:
# krb5-kdc krb5-admin-server; macOS: brew install krb5). Stop it with
#   kill $(cat <dir>/kdc.pid)
set -euo pipefail

DIR=$(mkdir -p "$1" && cd "$1" && pwd)
PORT=${2:-18891}
REALM=PILLAR.TEST
# MIT first: macOS ships Heimdal's kadmin.local in /usr/sbin.
PATH=/opt/homebrew/opt/krb5/sbin:/usr/local/opt/krb5/sbin:$PATH:/usr/sbin:/usr/local/sbin
for t in krb5kdc kdb5_util kadmin.local; do
    command -v "$t" >/dev/null || { echo "$t not found (install MIT krb5)" >&2; exit 1; }
done

if [ -f "$DIR/kdc.pid" ]; then kill "$(cat "$DIR/kdc.pid")" 2>/dev/null || true; fi
rm -rf "${DIR:?}"/*

ENCTYPES="aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96"
cat >"$DIR/krb5.conf" <<CONF
[libdefaults]
    default_realm = $REALM
    dns_lookup_kdc = false
    dns_lookup_realm = false
    rdns = false
    udp_preference_limit = 1
    permitted_enctypes = $ENCTYPES
    default_tkt_enctypes = $ENCTYPES
    default_tgs_enctypes = $ENCTYPES
[realms]
    $REALM = {
        kdc = 127.0.0.1:$PORT
    }
[domain_realm]
    localhost = $REALM
CONF
cat >"$DIR/kdc.conf" <<CONF
[kdcdefaults]
    kdc_ports = $PORT
    kdc_tcp_ports = $PORT
[realms]
    $REALM = {
        database_name = $DIR/principal
        key_stash_file = $DIR/stash
        acl_file = $DIR/kadm5.acl
        supported_enctypes = $(for e in $ENCTYPES; do printf '%s:normal ' "$e"; done)
        default_principal_flags = +preauth
    }
[logging]
    kdc = FILE:$DIR/kdc.log
CONF
: >"$DIR/kadm5.acl"
export KRB5_CONFIG=$DIR/krb5.conf KRB5_KDC_PROFILE=$DIR/kdc.conf
kdb5_util create -s -r "$REALM" -P masterpw >/dev/null
k() {
    local out
    out=$(kadmin.local -r "$REALM" -q "$*" 2>&1)
    if grep -qiE 'error|cannot|failed|usage' <<<"$out"; then
        echo "kadmin.local -q '$*' failed:" >&2; echo "$out" >&2; exit 1
    fi
}
for p in nn/localhost dn/localhost hdfs alice bob; do
    k addprinc -randkey "$p"
    k ktadd -k "$DIR/$(echo "$p" | cut -d/ -f1).keytab" "$p"
done
krb5kdc -n -P "$DIR/kdc.pid" >"$DIR/kdc.out" 2>&1 &
echo $! >"$DIR/kdc.pid"
for _ in $(seq 50); do (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
echo "KDC for $REALM on 127.0.0.1:$PORT (KRB5_CONFIG=$DIR/krb5.conf)"
