#!/usr/bin/env bash
# Start the search engine container with explicit limits and wait until it answers.
# usage: start-engine.sh NAME PORT CPUS MEM      e.g. start-engine.sh nbench-engine 27102 3 10g
set -euo pipefail
NAME="${1:?name}"; PORT="${2:?port}"; CPUS="${3:?cpus}"; MEM="${4:?mem}"
IMAGE="${NBENCH_IMAGE:-mongodb/mongodb-atlas-local:8.0}"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --hostname nbench -p "${PORT}:27017" --cpus "$CPUS" --memory "$MEM" "$IMAGE" >/dev/null
# The container restarts mongod once shortly after it first answers, so a single successful ping is not enough
# (writes then fail with "interrupted at shutdown"). Ready means: a write and a search-index-management call
# succeed five times in a row, two seconds apart. Any failure resets the count.
READY_JS='const d = db.getSiblingDB("ready_probe"); try { d.createCollection("p"); } catch (e) {} d.p.aggregate([{ $listSearchIndexes: {} }]).toArray(); print("READY_OK");'
ok=0
for i in $(seq 1 150); do
  if docker exec "$NAME" mongosh --quiet --eval "$READY_JS" 2>/dev/null | grep -q READY_OK; then ok=$((ok + 1)); else ok=0; fi
  if [ "$ok" -ge 5 ]; then
    echo "engine ready and stable after ~$((i * 2)) s"
    exit 0
  fi
  sleep 2
done
echo "engine did not become ready" >&2
docker logs "$NAME" | tail -n 30 >&2
exit 1
