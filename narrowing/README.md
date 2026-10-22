# Narrowing follow-up benchmark

Benchmark code for a search library's narrowing strategy: how a search is restricted by an ordinary database filter
when the search engine (MongoDB Atlas Search through `mongodb-atlas-local`) can only take an ID list. It tests the
conditions an earlier, single-machine benchmark did not cover.

**Status: harness verified with smoke runs only. No result at scale has been measured.** The real measurements
must run on a GitHub Actions runner (see `RUNBOOK.md`). `REPORT-narrowing.md` is a skeleton with every number marked pending.

## The strategies under test (described in our own words)

- **Strategy A (chunked ID constraint).** Read the IDs matching the filter with a `find` on `_id` (stopping at a cap + 1),
  then search with an `in` clause on `_id`. One `in` clause accepts at most 1,023 values on this engine version, so the IDs are cut
  into chunks searched concurrently, and the chunks' top results are merged by score.
- **Strategy B (search first).** Search with a limit of *factor* times the page size, then filter in MongoDB.
- **Planner.** No matching IDs: empty result. Up to the cap: strategy A. Above the cap: strategy B if the filter
  matches at least a threshold share of the entity (decided with an early-exit count), else A. A short page from B falls back to A.
  The parameters under test are cap, threshold, overfetch factor, chunk size and chunks in flight.

The planner here is a small mimic (`src/nbench/planner.py`). The harness uses the official async MongoDB driver directly.

## Questions and phases

| Question | Command | Phase or step |
|---|---|---|
| Q1 concurrent load, open loop, up to and beyond 200 queries/s | `nbench load-test` | stateful, last, ascending rates |
| Q2 clustered ID layouts (contiguous, grouped by a field, evenly spread) | `nbench run --phases layouts` | `layouts` |
| Q3 cost of an exact total and of facets | `--phases count_facets` | `count_facets` |
| Q4 chunk size 250/500/1000 x chunks in flight 2/4/8/16 | `--phases tuning` | `tuning` |
| Q5 selectivity 0.25/0.5/1/2/5% x overfetch 50/100/200/500 | `--phases selectivity` | `selectivity` |
| Q6 early-exit prefilter against a full count, and the estimate | `--phases prefilter` | `prefilter` |
| Q7 noise: at least 100 repetitions of the cap-deciding cases | `--phases noise --noise-reps 200` | `noise` |
| Q8 hosted Atlas M0 (informational) | `nbench m0` | skipped without `HOODIE_BENCH_ATLAS_URL` |
| calibration against the first benchmark | `--phases calibration` | `calibration` |

## Method (what makes it trustworthy)

- **Synthetic, deterministic, compact data**, one collection per scale: `_id`, `seq`, `rank` (a permutation, so `rank < k` selects k documents
  spread evenly), `tenant` (groups of exact sizes, so `tenant == x` is a clustered filter), `status` (5 values) and `brand` (20,000 values) for facets,
  and short `title`/`body` text with planted terms. The compact shape is used at every scale so scales stay comparable with each other;
  it differs from the first benchmark's shape, so only trends are comparable with it. Contiguous layout: `_id < oid(k)`.
- **Randomized blocks.** Within a phase, a round runs every case once in a shuffled order, so every case has the same number of repetitions.
  A short warm-up (one unrecorded-in-statistics call) follows every switch to a new case; one full warm-up round precedes each phase. Warm-up samples are kept in the raw data (`warm: 1`).
  The seed of round r is derived from the logged seed, so a run can be reproduced and resumed (`progress.json`).
- **Not shuffled (stateful):** data load and index build; the load test (rates ascending, run last). Phases themselves run in a fixed order.
- **Canary.** A tiny search query and a fixed CPU loop run at 10 Hz throughout every phase and the load test, timestamped. Analysis flags
  5-second windows whose canary median is well above the run's canary median and reports every table with and without flagged samples. Nothing is dropped from the raw data.
- **Robust statistics:** medians and percentiles with bootstrap 95% intervals over repetitions. Repetition counts are printed in every table.
- **Open-loop load:** arrivals at a fixed rate; latency is counted from the scheduled arrival. Engine CPU and memory come from `docker stats`; the client's CPU and event-loop lag are recorded so a saturated client is visible.
- **Disk:** `df -h` at the start, after load and after phases; the loader stops below 1.5 GB free; peak disk use per scale is sampled every 2 s and used to project 10M (`nbench project`).

## Environment

All numbers come from one environment: a GitHub Actions workflow on a public repository, `ubuntu-latest`, the standard Linux
runner (GitHub documents 4 vCPU, 16 GB RAM, 14 GB SSD for public repositories, and `ubuntu-latest` as Ubuntu 24.04 at the time of writing; the run's
`environment.json` records what the runner actually reports). The engine runs in Docker with explicit limits (default 3 CPUs, 10 GB, inputs of the workflow);
the Python client runs on the host of the same runner. Python 3.12. GitHub documents a limit of 6 hours per job and 35 days per workflow run
(Actions limits page, looked up on 2026-10-22); each dispatch is designed to finish well inside it.
Absolute latencies from a shared virtual machine are relative evidence for comparing strategies and finding limits, not guarantees for any deployment.

## Prerequisites (local smoke run)

Docker, [`uv`](https://docs.astral.sh/uv/), Python 3.12 (uv installs it). From this directory: `uv sync --locked`.

## Commands

```
bash scripts/start-engine.sh NAME PORT CPUS MEM     # engine container with limits, waits for readiness
uv run nbench env   --label L --container NAME      # environment.json
uv run nbench load  --label L --scale smoke|100k|1m|10m[,..] --container NAME
uv run nbench run   --label L --scale S --phases all|a,b --reps 30 --noise-reps 200 --seed N --time-limit-min 15
uv run nbench load-test --label L --scale S --container NAME --rates 25,50,100,150,200,300,400 --duration 30
uv run nbench limits --label L --scale S            # 1,023 / 1,024 values in an `in` clause
uv run nbench m0    --label L                       # only with HOODIE_BENCH_ATLAS_URL; never prints it
uv run nbench project --runs results/L --out results/L/disk_projection.json
uv run nbench analyze --runs results/run1 results/run2 results/run3 --out results/tables.md
```

Environment variables: `NBENCH_MONGO_URI` (default `mongodb://127.0.0.1:27102/?directConnection=true`), `NBENCH_RESULTS` (default `./results`),
`HOODIE_BENCH_ATLAS_URL` (M0 only). Workflows: `narrowing-ci` (smoke on every push), `narrowing-bench` (the real runs), `narrowing-m0`.
Every action is pinned to a full commit SHA and the workflows request read-only permissions (`actions: read` only in the job that may download a previous run's artifact).

## Run times

Not measured on the runner yet. A smoke pass (5,000 documents, 3 repetitions, all phases and a short load test) took a few minutes on a
shared laptop and means nothing for timing. The first real Step A run of `RUNBOOK.md` gives the real figures; write them here.

## Files

`src/nbench/`: `common` (layouts, filters), `gen` (data, loader, index), `planner` (the mimic), `phases` (cases and the randomized-block runner),
`load` (open loop), `canary`, `env` (environment, disk), `m0`, `analyze` (tables, calibration, noise windows, disk projection), `stats`, `cli`.
`results/smoke-*`: smoke outputs that only prove the code runs; they are not results.

Synthetic data only. Logs and results contain no document field values and no connection string.
