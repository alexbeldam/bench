#!/usr/bin/env bash
# Start the search-enabled MongoDB container with explicit limits and wait for a writable primary.
# Environment: WP_CONTAINER (default hoodie-writepath), WP_PORT (27103), WP_CPUS (3), WP_MEM (10g).
set -euo pipefail
C="${WP_CONTAINER:-hoodie-writepath}"; PORT="${WP_PORT:-27103}"; CPUS="${WP_CPUS:-3}"; MEM="${WP_MEM:-10g}"
df -h; docker --version; nproc; free -m
docker rm -f "$C" >/dev/null 2>&1 || true
docker run -d --name "$C" -p "$PORT:27017" --memory "$MEM" --cpus "$CPUS" mongodb/mongodb-atlas-local:8.0 >/dev/null
for i in $(seq 1 90); do
  if docker exec "$C" mongosh --quiet --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q true; then
    echo "engine ready after ${i}x2s"; docker image inspect mongodb/mongodb-atlas-local:8.0 --format '{{.Id}} {{json .RepoDigests}}'; exit 0
  fi
  sleep 2
done
echo "engine did not become ready" >&2; docker logs "$C" | tail -50 >&2; exit 1
