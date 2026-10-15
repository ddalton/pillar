#!/usr/bin/env bash
# kind.sh — run the Chainsaw system tests against images built from THIS
# tree, on a throwaway kind cluster. The GitHub workflow
# (.github/workflows/system-chainsaw.yml) runs exactly this script; run it
# on any host with docker to reproduce a CI result.
#
#   1. builds pillar/namenode:system-test and pillar/datanode:system-test
#      from the repo's Dockerfile (BUILD=0: use images already built);
#   2. creates a kind cluster (1 control plane + 2 workers) and loads those
#      images plus the Hadoop client image into it, so no pod pulls a
#      Pillar image from anywhere else (the manifests say pullPolicy Never);
#   3. installs krb5-operator (KRB5_OPERATOR_VERSION, from its release
#      manifests and ghcr images) for the tests with Kerberos;
#   4. runs chainsaw over tests/ (TESTS=tests/<name> for one test).
#
# Env: CLUSTER (pillar-chainsaw)  BUILD (1)  KEEP (0; 1 = leave the cluster)
#      KIND_NODE_IMAGE (kindest/node:v1.34.0)  TESTS (tests)
#      KRB5_OPERATOR_VERSION (0.2.0)
#      REPORT_DIR (unset; set = write a JUnit report there)
# Uses a PRIVATE kubeconfig: never touches ~/.kube/config.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
CLUSTER="${CLUSTER:-pillar-chainsaw}"
BUILD="${BUILD:-1}"
KEEP="${KEEP:-0}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.34.0}"
HADOOP_IMAGE=apache/hadoop:3.3.6
KRB5_OPERATOR_VERSION="${KRB5_OPERATOR_VERSION:-0.2.0}"
export KUBECONFIG="${KUBECONFIG_PRIVATE:-$(mktemp -d)/kubeconfig}"

step() { printf '\n▶ %s\n' "$*" >&2; }
fail() { printf '\n✗ %s\n' "$*" >&2; exit 1; }

for t in docker kind kubectl helm chainsaw; do
  command -v "$t" >/dev/null || fail "$t not found in PATH"
done

cleanup() {
  rc=$?
  if [ "$KEEP" = 1 ]; then
    echo "KEEP=1: cluster $CLUSTER left up; KUBECONFIG=$KUBECONFIG" >&2
  else
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

if [ "$BUILD" = 1 ]; then
  for target in namenode datanode; do
    step "build pillar/$target:system-test"
    DOCKER_BUILDKIT=1 docker build --target "$target" -t "pillar/$target:system-test" "$ROOT"
  done
fi
for target in namenode datanode; do
  docker image inspect "pillar/$target:system-test" >/dev/null || fail "image pillar/$target:system-test not found"
done
step "pull $HADOOP_IMAGE"
docker image inspect "$HADOOP_IMAGE" >/dev/null 2>&1 || docker pull -q "$HADOOP_IMAGE"

step "kind cluster $CLUSTER ($KIND_NODE_IMAGE)"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --image "$KIND_NODE_IMAGE" --kubeconfig "$KUBECONFIG" --config - <<KIND
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
KIND
step "load images into the cluster"
kind load docker-image --name "$CLUSTER" pillar/namenode:system-test pillar/datanode:system-test "$HADOOP_IMAGE"

step "krb5-operator $KRB5_OPERATOR_VERSION"
ops="https://raw.githubusercontent.com/ddalton/krb5-operator/v$KRB5_OPERATOR_VERSION/deploy"
kubectl apply --server-side -f "$ops/crds.yaml" -f "$ops/operator.yaml"
kubectl -n krb5-operator rollout status deploy/krb5-operator --timeout=5m

TESTS="${TESTS:-tests}"
step "chainsaw: $TESTS"
report=()
if [ -n "${REPORT_DIR:-}" ]; then
  mkdir -p "$REPORT_DIR"
  report=(--report-format JUNIT-TEST --report-path "$REPORT_DIR" --report-name pillar-system)
fi
cd "$HERE"
chainsaw test --config chainsaw.yaml --test-dir "$TESTS" "${report[@]}"
