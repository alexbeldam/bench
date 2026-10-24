# Spike runbook

Harnesses copied from local spikes. They were written for a laptop and are **untested on Actions**: run the smallest input of each group first (see below), look at the artifact, then run the real input. Ports and container names are fixed by the scripts (`hoodie-s2-engine` on 27212; `hoodie-s3-*` on 27213 to 27216). Dispatch one group at a time, only the owner does it. Compare arms within one run only; repeat on three occasions.

## spike-probe (`spikes/probe`, search engine image)

| group | what | smallest input | real input | expected time |
|---|---|---|---|---|
| recreate | delete then recreate under the same id and a new id; rules A (epoch), A2, B (tombstone window), C (nothing) | `reps=2` | `reps=10` | under 1 h |
| hot | three probe forms (P1 stored copy, P2 index-side range, P3 packed token) with 1 and 5 hot documents at 200 and 1000 writes/s | `reps=1 duration=10` | `reps=3 duration=30` | 1 to 3 h |
| size | index size and build time per index definition (V0 to V6) | `docs=100000` | `docs=1000000` | 1 to 4 h |
| ids | the three probe forms per id type (ObjectId, string, UUID, int64 above 2^53) with the right and wrong `_id` mappings: false hits and misses, most documents per query, latency | default | default | under 30 min |

Artifact: `spike-probe-<occasion>-<group>`, raw `q*.json` and `q*.log` plus the engine log tail.

## spike-feed (`spikes/feed`, plain `mongo:8.0` replica set, Redis, standalone and auth containers)

| group | what | smallest input | real input | expected time |
|---|---|---|---|---|
| streams | one database-level stream per process against one per collection, P processes, E entities | default | default | under 1 h |
| lazy | open a stream after the write at the write's own operation time; negative control; quiet-server open latency | `trials=500` | `trials=10000` | under 1 h |
| share | own streams, leader through a capped collection, leader through Redis (with and without killing the leader), marker stream | default | default | 1 to 2 h |
| resume | kill the stream connection and resume (automatic and manual); exact server errors for a standalone server, a missing privilege and open-cursor limits | default | default | 1 to 2 h |
| diag | `outage` and `redis kill` with and without keeping the event the first read after a resume returns; lists the missing events | default | default | under 15 min |
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

## spike-fl2-cost (`spikes/flakiness2/cost`, scrubbed suite in `spikes/flakiness2/suite`, search engine image)

Dispatch `spike-fl2-cost` with `group` and the inputs below. Push check: `spike-fl2-cost-smoke` (every group, tiny scale).

| group | what | smallest input | real input | expected time |
|---|---|---|---|---|
| image | `docker save` tarball in `actions/cache` against `docker pull`, to engine ready. Dispatch twice with the same `key`: the first misses and saves (cold), the second restores (warm). A new `key` gives another cold run | `key=fl2-image-v1` | the same, then again; a third dispatch with a new key for a second cold sample | about 10 min each |
| suite | tests/test_parity.py and tests/test_stress.py once (backends A, B, C), one fresh engine per run, no xdist and `-n 1,2,4,8`, engine limited to 3 CPUs and unlimited, database prefix per worker, randomised order | `reps=1 workers=0,2` | `reps=3 workers=0,1,2,4,8` | 1 to 1.5 h |
| cpu2 | the same, engine `--cpus 2` and cpuset 0,1, test process under `taskset -c 0,1`; an EMULATION of a 2 vCPU runner (memory not limited) | `reps=1 workers=0,2` | `reps=3 workers=0,1,2,4,8` | 40 to 60 min |
| model | synthetic suite: S tests with one write-and-wait each, M engine-free tests (mongomock plus a fixed CPU cost); fit of wall time against workers | `s=8 m=20 workers=0,2 reps=1` | `s=420 m=1300 fast_ms=40 reps=2 workers=0,2,4,8` | 40 to 60 min |

Artifact: name `fl2-cost-<occasion>-<group>`: `suite.json`, `suite.json.tests.gz` (per-test durations), `engine-tail.txt`, `analysis/tables.md`. Analyze downloaded runs with `python spikes/flakiness2/cost/cost.py analyze <dirs> --out <dir>` (standard library only, needs no environment).

## spike-fl2-planted (`spikes/flakiness2/planted`, package `fl2p`, scrubbed suite as a path dependency)

Dispatch `spike-fl2-planted` with `group` (`inmem`, `real`, `both`), `scale`, `reps`, `duration`, `warm`, `mult`, `seed`, `occasion`. Push check: jobs `smoke-inmem` and `smoke-real` (every phase at tiny scale). Three occasions at different times; compare strategies within one run only. Analyze all downloaded directories together: `uv run fl2p analyze <dir1> <dir2> <dir3> --out <dir>` (in `spikes/flakiness2/planted`).

| job (matrix value) | what | smallest input | real input | expected time (estimate, not yet measured on a runner) |
|---|---|---|---|---|
| inmem pool | every in-memory test in isolated runs, shuffled interleaved order (S2 blocks, S7, S8, S9, truth check) | `scale=smoke` | `scale=full duration=45` (n up to 9000 per test) | 15 to 25 min |
| inmem suite | S0, S1, S5 (x3, x10, jitter x5) | `scale=smoke` | `reps=100` | 15 to 20 min |
| inmem forced | S3, deterministic injection | `scale=smoke` | K=30 | under 5 min |
| inmem noise | S4, five arms (base, cpus2, stress50, stress100, cpus2+stress100), pool plus suite passes each | `scale=smoke` | default | 20 to 35 min |
| inmem prop | S6, Hypothesis state machines, 4 machines | `scale=smoke` | K=50, 100 examples | under 10 min |
| inmem repeat | S2 function and session scope on the cheap subset, 300 repeats, K=30 | `scale=smoke` | default | 15 to 20 min |
| inmem meta | pytest-repeat against an in-process loop, pytest-randomly conformance, timing-race margin calibration | `scale=smoke` | default | 10 to 15 min |
| real settle | settle bound 1.00/1.01/1.02/4.00 and tie order, 4 workers, arms engine1 and stress100 on two tests | `scale=smoke` | `mult=1.0` | 30 to 40 min |
| real tx | scenario s14 with bounds 1.00/1.01/1.02/4.00, same arms | `scale=smoke` | `mult=1.0` | 35 to 50 min |
| real purge | stale total, purge probe, reset order, each with its fixed variant | `scale=smoke` | `mult=1.0` | 30 to 45 min |

Artifacts: `fl2-planted-<occasion>-inmem-<family>` and `fl2-planted-<occasion>-real-<shard>` (the matrix value is appended to the group name), 90 days: result JSON (pools gzipped), `machine.txt`, canary JSONL (gzip), `report/tables.md` of that job alone, engine log tail for real jobs. Each job needs its own runner, so each artifact holds one slice; `fl2p analyze` merges the directories. Download with `gh run download <id> -n <artifact> -D results/downloads/<name>` and pass the parent folder, or every artifact folder of one occasion, as one run directory (the analyzer reads all files below it).
