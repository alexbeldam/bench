# Spike runbook

Harnesses copied from local spikes. They were written for a laptop and are **untested on Actions**: run the smallest input of each group first (see below), look at the artifact, then run the real input. Ports and container names are fixed by the scripts (`hoodie-s2-engine` on 27212; `hoodie-s3-*` on 27213 to 27216). Dispatch one group at a time, only the owner does it. Compare arms within one run only; repeat on three occasions.

## spike-probe (`spikes/probe`, search engine image)

| group | what | smallest input | real input | expected time |
|---|---|---|---|---|
| recreate | delete then recreate under the same id and a new id; rules A (epoch), A2, B (tombstone window), C (nothing) | `reps=2` | `reps=10` | under 1 h |
| hot | three probe forms (P1 stored copy, P2 index-side range, P3 packed token) with 1 and 5 hot documents at 200 and 1000 writes/s | `reps=1 duration=10` | `reps=3 duration=30` | 1 to 3 h |
| size | index size and build time per index definition (V0 to V6) | `docs=100000` | `docs=1000000` | 1 to 4 h |

Artifact: `spike-probe-<occasion>-<group>`, raw `q*.json` and `q*.log` plus the engine log tail.

## spike-feed (`spikes/feed`, plain `mongo:8.0` replica set, Redis, standalone and auth containers)

| group | what | smallest input | real input | expected time |
|---|---|---|---|---|
| streams | one database-level stream per process against one per collection, P processes, E entities | default | default | under 1 h |
| lazy | open a stream after the write at the write's own operation time; negative control; quiet-server open latency | `trials=500` | `trials=10000` | under 1 h |
| share | own streams, leader through a capped collection, leader through Redis (with and without killing the leader), marker stream | default | default | 1 to 2 h |
| resume | kill the stream connection and resume (automatic and manual); exact server errors for a standalone server, a missing privilege and open-cursor limits | default | default | 1 to 2 h |
| memory | bytes per event-log entry; live consumer at 500 and 1000 writes/s | default | default | under 1 h |

Known gaps in the harness (reported by its author): the marker-stream and capped-collection runs were inconclusive or crashed locally; after a leader kill every surviving node missed one event, cause unknown. Treat those as open, not as results.

Not coverable here: hosted-cluster limits (five streams on a free cluster, no streams on Flex, operations per second), another server version (the search-index-on-a-view probe needs 8.1 or later). Those need the hosted-cluster job and, for views, a newer engine image.

## spike-flakiness (`spikes/flakiness`, search engine image)

| group | what | smallest input | real input | expected time |
|---|---|---|---|---|
| startup | image pull time, cold-start stages, workers sharing one engine (each with its own database and index) | `n=2 workers=1,2` | `n=15 workers=1,2,4,8` | under 20 min |
| lag | write-to-searchable lag (10 ms polling) and delete-to-total-zero lag, one document per cycle | `n=5` | `n=400` | about 15 min |
| stale | exact total right after a delete, plain and with the index process paused (SIGSTOP/SIGCONT) around the delete | `n=2` | `n=20` | under 30 min |
| ghost | drop a collection while its search index exists, then recreate; three orders, 20 s bound | `n=1` | `n=10` | under 40 min |
| ties | native tie order against sort {score, _id} over index builds | `n=2` | `n=4` | under 10 min |

Artifact: `flakiness-<occasion>-<group>`. Group `startup` times the start itself and does not use the shared start script. Smoke-tested locally: lag, stale, ties, ghost. Group `startup` was not run before the first dispatch.
