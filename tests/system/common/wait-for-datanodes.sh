#!/usr/bin/env bash
# Wait until the NameNode has registered $1 (default three) DataNodes (a DataNode pod is
# Ready when its data port listens, which can be before it registers).
set -euo pipefail
want=${1:-3}
nn=$(kubectl -n "$NAMESPACE" get pod -l app=pillar-namenode -o jsonpath='{.items[0].metadata.name}')
for _ in $(seq 90); do
  n=$(kubectl -n "$NAMESPACE" logs "$nn" | grep -c 'Registered datanode' || true)
  [ "$n" -ge "$want" ] && { echo "$n DataNodes registered with $nn"; exit 0; }
  sleep 2
done
echo "FAIL: only $n DataNodes registered with $nn"
kubectl -n "$NAMESPACE" logs "$nn" | tail -50
exit 1
