# Write-path overhead benchmark

Benchmarks a **simulated write path** for a library that adds Atlas Search to MongoDB documents. The library does
not exist yet, so this is a small, self-contained simulation (`src/wpbench/sim.py`, about 250 lines, async PyMongo
only) that does what the design says a write does:

1. stamp the write with a new **revision token** (an ObjectId generated client-side, stored in `_hoodie_rev` and
   present in the search index definition);
2. upsert a **sync marker** (unique index on `{entity, doc_id}`, plus `{entity, state}`) in `_hoodie_sync`, in the
   same transaction as the data when there is one;
3. write the data;
4. in STRONG mode, **probe** the search index until the token is visible (a delete: until the document is absent);
   probes for many documents are batched into one wait;
5. delete the marker on success, mark it `failed` on timeout. EVENTUAL mode stops after step 3. A bulk write that
   skips synchronization writes one per-entity dirty marker and makes no search calls.

It is a measurement tool, not that library. Synthetic data only; logs and results contain no document field values.

## Questions and targets

| Target | What is measured | Phase |
|---|---|---|
| NFR-02: overhead <= 50 ms p95 in STRONG mode, engine latency excluded | write call time minus time to searchable, split by part | `latency` |
| NFR-03: added latency <= 15 ms p95 in EVENTUAL mode | token + marker over a plain write, with and without a transaction, at 0, 1 and 5 ms simulated round trip | `latency` |
| NFR-04: one combined search wait per transaction | driver command counts | `counts` |
| NFR-05: skipped bulk writes add no search round trips | driver command counts | `counts` |
| Probe design | clause limit per shape, time to verify 1 to 5,000 documents, chunking, polling schedules, coalescing | `clauses`, `shapes`, `coalesce` |
| Time to searchable at 10, 100, 500 writes/s | engine latency (context only) | `latency` (`tts_*` cases) |
| Marker cost at scale | size, index size, write amplification, hotspots, 500 writes/s for one hour | `marker` |
| Failure behaviour | STRONG timeout, ExceptionGroup content, `failed` markers, no field values in messages | `failure` |
| Token semantics | a stale token never matches; deletes probed as absent | `semantics` |
| Hosted Atlas free tier (informational) | probe limits and time to searchable at <= 10 writes/s | `m0` (needs `HOODIE_BENCH_ATLAS_URL`) |

## Environment

All real numbers come from one place: a GitHub Actions workflow on a public repository, the `ubuntu-latest`
runner (documented as 4 vCPU, 16 GB RAM, 14 GB disk for public repositories; each run records the real values in
`environment.json`), with the engine in Docker: `mongodb/mongodb-atlas-local:8.0` (mongod plus mongot, a replica set,
so transactions work), limited by `WP_CPUS` (default 3) and `WP_MEM` (default 10g) so the client and the proxies
have the rest. Python 3.12 through `uv`, PyMongo (async driver). Laptop numbers are never used in a report.

GitHub limits (checked in GitHub's documentation, "Actions limits", when this was written): a job can run for at most
**6 hours**, a workflow run for 35 days. Artifact storage is 500 MB on the Free plan: raw rows are gzip-compressed
JSON lines and the marker test is the biggest producer, so check usage before the three-run protocol.

## Layout

```
src/wpbench/
  sim.py        the simulated write path (WritePath, run_transaction, SyncFailure)
  probe.py      probe query shapes, chunked Prober, polling Schedule, IndependentWaiter, Coalescer
  loadgen.py    open-loop load generator, sharded over processes, rows to .jsonl.gz
  netproxy.py   TCP proxy that adds a round-trip delay (own process)
  canary.py     fixed CPU loop and fixed database read, own process, timestamped
  cmdmon.py     driver command monitoring (counts commands and search queries)
  gen.py        deterministic synthetic data, resumable loader, search index builder
  phases.py     the phases
  envinfo.py    environment.json
  analyze.py    run directories -> report tables, verdicts, between-run variance
  cli.py        wpbench run | env | analyze
scripts/        ci-start-engine.sh, ci-finish.sh
RUNBOOK.md      how to dispatch, download and analyze
../.github/workflows/write-path-smoke.yml   on every push touching this folder (harness check)
../.github/workflows/write-path-full.yml    manual dispatch, one job group at a time
```

## Commands (local smoke, a few minutes)

Prerequisites: Docker, `uv`, Python 3.12.

```
docker run -d --name hoodie-writepath-smoke -p 27103:27017 --memory 3g --cpus 2 mongodb/mongodb-atlas-local:8.0
uv sync
uv run wpbench run --scale smoke --reps 1 --rates 5,10 --duration 6 --warm 2 --batch-sizes 1,100,1000,5000 \
    --marker-minutes 0.3 --failure-timeout 2 --failure-reps 3 --container hoodie-writepath-smoke
uv run wpbench analyze results/<run-id> --out results/analysis
docker rm -f hoodie-writepath-smoke
```

`WPBENCH_MONGO_URI` (default `mongodb://127.0.0.1:27103/?directConnection=true`) and `WPBENCH_RESULTS` override the
connection and the results folder. Smoke timings are not results; they only prove the harness runs. Real runs use
the workflows, see `RUNBOOK.md`.

## Method

- **Open loop.** Arrivals are on a fixed clock. Latency is measured from the scheduled slot, so a slow response
  shows up as queueing instead of hiding (`lat_ms`); the generator's own lateness is `lag_ms` and is reported, and a
  high `lag_ms` marks a saturated client. Rates above about 130 writes/s per process are sharded over processes.
- **Overhead is a range.** The engine's indexing time cannot be observed exactly. The new revision became visible
  after the last failed probe started and before the successful probe finished, which gives a lower and an upper
  bound on the engine time. Overhead = total minus engine time is therefore reported as a range; the conservative
  upper bound decides the verdict. It counts the wait between the revision becoming visible and the next poll as
  overhead (a cost of the polling schedule), so it is pessimistic by at most one polling interval plus one query. Cases named `tts_*` poll finely (2 to 10 ms) to tighten the engine-time bounds.
- **Network latency.** `netproxy.py` is a TCP proxy that holds each chunk for rtt/2 in each direction. `--rtts 0,1,5`
  starts one proxy per non-zero value; 0 connects directly. The proxy runs in its own process.
- **Randomization.** Within a phase, cases run in randomized blocks (every case once per repetition, shuffled per
  repetition) from a logged seed (`order.json`, `meta.json`); a different seed per workflow run. Warm-up samples
  (the first seconds of each case run) are kept in the raw files and excluded from statistics.
  **Not shuffled:** data load and index build (first), `marker` (long, changes the marker collection) and `failure`
  (pauses the engine), each run alone in its own job and last.
- **Canary.** Own process, 4 Hz, a fixed CPU loop and a primary-key read, timestamped. The analysis flags 5-second
  windows where the canary is well above its median and reports with and without them.
- **Robust statistics.** Medians and percentiles, bootstrap 95% confidence intervals over repetitions, no means.
- **Disk.** `df -h` at start and end of each job, free space checked at load, after index build and before each
  case; the run stops cleanly below 1.5 GB free (`stopped.json`).
- **Resumable.** Loading records progress; finished case runs are skipped when a run is repeated with the same
  `--run-id` and `--seed`.

## Document shape

Compact documents (`title` of 3 words, `body` of 7 words, `seq`, `touch`, `_hoodie_rev`) at every scale so scales stay
comparable. The search index maps `_id` and `_hoodie_rev` as `objectId`, and `title` and `body` as `string`.

## Not covered

10M documents are attempted only after the 100k and 1M results are written up and the measured size projection
leaves at least about 2 GB free (see `RUNBOOK.md`). Hosted Atlas runs only if the `HOODIE_BENCH_ATLAS_URL` secret
exists; the harness never prints it. Stored-source search results (`returnStoredSource`) are not tested as a probe
variant. The earlier laptop calibration subset is not part of this folder.
