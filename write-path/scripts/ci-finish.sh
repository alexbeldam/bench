#!/usr/bin/env bash
# Save engine logs next to the results, print disk use, and remove the container.
set -uo pipefail
C="${WP_CONTAINER:-hoodie-writepath}"; OUT="${1:-results}"
mkdir -p "$OUT"; docker logs "$C" 2>&1 | tail -n 2000 > "$OUT/engine.log" || true
df -h | tee "$OUT/df-end.txt"; docker rm -f "$C" >/dev/null 2>&1 || true
