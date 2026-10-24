#!/usr/bin/env bash
# Start the search-enabled container from the derived image (see ../Dockerfile) with the same limits and readiness
# rule as write-path/scripts/ci-start-engine.sh. Environment: WP_CONTAINER, WP_PORT, WP_CPUS (3), WP_MEM (10g),
# FL_IMAGE (default fl2-engine:local), FL_RUN_ARGS (extra docker run arguments, word-split),
# FL_NETNS (join the network namespace of that container instead of publishing WP_PORT).
set -euo pipefail
C="${WP_CONTAINER:-hoodie-fl2-forcing}"; PORT="${WP_PORT:-29301}"; CPUS="${WP_CPUS:-3}"; MEM="${WP_MEM:-10g}"
IMG="${FL_IMAGE:-fl2-engine:local}"
docker rm -f "$C" >/dev/null 2>&1 || true
NET=(-p "$PORT:27017")
[ -n "${FL_NETNS:-}" ] && NET=(--network "container:$FL_NETNS")
# shellcheck disable=SC2086
docker run -d --name "$C" "${NET[@]}" --memory "$MEM" --cpus "$CPUS" ${FL_RUN_ARGS:-} "$IMG" >/dev/null
READY_JS='const d = db.getSiblingDB("ready_probe"); try { d.createCollection("p"); } catch (e) {} d.p.aggregate([{ $listSearchIndexes: {} }]).toArray(); print("READY_OK");'
ok=0
for i in $(seq 1 150); do
  if docker exec "$C" mongosh --quiet --eval "$READY_JS" 2>/dev/null | grep -q READY_OK; then ok=$((ok+1)); else ok=0; fi
  if [ "$ok" -ge 5 ]; then echo "engine ready and stable after ~$((i*2))s"; exit 0; fi
  sleep 2
done
echo "engine did not become ready" >&2; docker logs "$C" | tail -50 >&2; exit 1
