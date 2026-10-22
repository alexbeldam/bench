#!/usr/bin/env bash
# Start the search engine container with explicit limits and wait until it answers.
# usage: start-engine.sh NAME PORT CPUS MEM      e.g. start-engine.sh nbench-engine 27102 3 10g
set -euo pipefail
NAME="${1:?name}"; PORT="${2:?port}"; CPUS="${3:?cpus}"; MEM="${4:?mem}"
IMAGE="${NBENCH_IMAGE:-mongodb/mongodb-atlas-local:8.0}"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --hostname nbench -p "${PORT}:27017" --cpus "$CPUS" --memory "$MEM" "$IMAGE" >/dev/null
for i in $(seq 1 120); do
  if docker exec "$NAME" mongosh --quiet --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then
    echo "engine ready after ~$((i * 2)) s"
    exit 0
  fi
  sleep 2
done
echo "engine did not become ready" >&2
docker logs "$NAME" | tail -n 30 >&2
exit 1
