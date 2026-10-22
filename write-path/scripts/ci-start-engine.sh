#!/usr/bin/env bash
# Start the search-enabled MongoDB container with explicit limits and wait for a writable primary.
# Environment: WP_CONTAINER (default hoodie-writepath), WP_PORT (27103), WP_CPUS (3), WP_MEM (10g).
set -euo pipefail
C="${WP_CONTAINER:-hoodie-writepath}"; PORT="${WP_PORT:-27103}"; CPUS="${WP_CPUS:-3}"; MEM="${WP_MEM:-10g}"
df -h; docker --version; nproc; free -m
docker rm -f "$C" >/dev/null 2>&1 || true
docker run -d --name "$C" -p "$PORT:27017" --memory "$MEM" --cpus "$CPUS" mongodb/mongodb-atlas-local:8.0 >/dev/null
# The container restarts mongod once shortly after it first answers, so one writable-primary answer is not enough
# (writes then fail with "interrupted at shutdown"). Ready means: a write and a search-index-management call
# succeed five times in a row, two seconds apart. Any failure resets the count.
READY_JS='const d = db.getSiblingDB("ready_probe"); try { d.createCollection("p"); } catch (e) {} d.p.aggregate([{ $listSearchIndexes: {} }]).toArray(); print("READY_OK");'
ok=0
for i in $(seq 1 150); do
  if docker exec "$C" mongosh --quiet --eval "$READY_JS" 2>/dev/null | grep -q READY_OK; then ok=$((ok+1)); else ok=0; fi
  if [ "$ok" -ge 5 ]; then
    echo "engine ready and stable after ~$((i*2))s"; docker image inspect mongodb/mongodb-atlas-local:8.0 --format '{{.Id}} {{json .RepoDigests}}'; exit 0
  fi
  sleep 2
done
echo "engine did not become ready" >&2; docker logs "$C" | tail -50 >&2; exit 1
