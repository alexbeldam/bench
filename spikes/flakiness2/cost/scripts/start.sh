#!/usr/bin/env bash
# Start the search-enabled MongoDB container and wait for a stable engine (same readiness rule as write-path/scripts/ci-start-engine.sh).
# Environment: C (container), PORT, CPUS (0 = no limit), MEM (default 10g), CPUSET (for example 0,1; empty = none), IMAGE.
set -euo pipefail
C="${C:-hoodie-fl2-cost}"; PORT="${PORT:-29501}"; CPUS="${CPUS:-3}"; MEM="${MEM:-10g}"; CPUSET="${CPUSET:-}"
IMAGE="${IMAGE:-mongodb/mongodb-atlas-local:8.0}"
docker rm -f "$C" >/dev/null 2>&1 || true
args=(-d --name "$C" -p "$PORT:27017" --memory "$MEM")
[ "$CPUS" != "0" ] && args+=(--cpus "$CPUS")
[ -n "$CPUSET" ] && args+=(--cpuset-cpus "$CPUSET")
docker run "${args[@]}" "$IMAGE" >/dev/null
READY_JS='const d = db.getSiblingDB("ready_probe"); try { d.createCollection("p"); } catch (e) {} d.p.aggregate([{ $listSearchIndexes: {} }]).toArray(); print("READY_OK");'
ok=0
for i in $(seq 1 150); do
  if docker exec "$C" mongosh --quiet --eval "$READY_JS" 2>/dev/null | grep -q READY_OK; then ok=$((ok+1)); else ok=0; fi
  if [ "$ok" -ge 5 ]; then echo "engine ready and stable after ~$((i*2))s"; exit 0; fi
  sleep 2
done
echo "engine did not become ready" >&2; docker logs "$C" | tail -50 >&2; exit 1
