# Runbook: producing the real numbers

Everything below happens on GitHub (Actions) and on the author's machine only for downloading and analysis. The
smoke workflow runs by itself on every push that touches `write-path/`; the full workflow is dispatched by hand.

Limits to keep in mind (GitHub documentation, "Actions limits"): a job runs for at most **6 hours**; artifact storage
is 500 MB on the Free plan. Each job below is sized well inside 6 hours (estimates are in the table; they come from
counting cases, not from a measured run, and should be replaced by the first real run's times).

## 0. Before the first dispatch

1. Push the repository. The `write-path-smoke` workflow must be green: it runs every phase at smoke scale on the
   runner. Open its job summary: the tables there must show the command-count checks passing. Its timings are not
   results.
2. Optional, hosted Atlas free tier: create the repository secret `HOODIE_BENCH_ATLAS_URL` (a connection string).
   The harness never prints it, never writes it to a file and skips the `m0` phase when it is not set.
3. Check artifact storage (Settings, Actions): the three-run protocol produces a few hundred MB at most (raw rows are
   gzip-compressed; the one-hour marker test is the largest). Delete old smoke artifacts if you are close to 500 MB.

## 1. What to dispatch (workflow `write-path-full`, Actions tab, "Run workflow")

The inputs are `group`, `scale`, `rtts`, `rates`, `reps`, `duration`, `warm`, `seed`, `marker_minutes`, `fail_pct`,
`occasion`. Leave `seed` empty so each dispatch draws its own seed (it is logged in `meta.json` and `order.json`;
pass the same seed to reproduce an order). Keep the other defaults unless stated.

Every group starts a fresh runner, starts the engine, loads its own data and uploads one artifact named
`write-path-<occasion>-<group>-<scale>`. Groups can run at the same time; they use separate runners.

| group | answers | reps x cases | estimated job time at 1m | notes |
|---|---|---|---|---|
| `probe` | clause limit, probe shapes (1 to 5,000 documents), token semantics, command counts (NFR-04, NFR-05), coalescing | 108 shape cases + 6 coalescing cases, 5 reps | 1.5 to 2.5 h | the command-count checks do not depend on timing |
| `latency` | time to searchable (10, 100, 500 writes/s; insert, update, delete), STRONG overhead (NFR-02), EVENTUAL overhead (NFR-03) at rtt 0, 1, 5 ms | 48 cases, 5 reps, 30 s plus 5 s warm-up each | 2 to 2.5 h | the cases that decide NFR-02 and NFR-03 |
| `marker` | marker growth, write amplification, hotspot check (500 writes/s) | 2 sub-runs of `marker_minutes` | about 2.3 h at 60 min | stateful; run alone |
| `failure` | timeout, ExceptionGroup, `failed` markers, paused engine | 5 timeouts of 10 s plus 3 paused | under 30 min | stateful; run alone, after the others |
| `m0` | hosted Atlas free tier, informational | small | under 15 min | skipped without the secret |

### Order and occasions

Run the whole protocol on **three occasions at different times of day** (for example morning, afternoon and the
next evening), so the runner's shared-machine noise shows up as between-run variance.

1. **Scale 100k, once** (scale trend only): `probe` and `latency` with `scale=100k`, `occasion=run1`.
2. **Write up 100k and 1m before 10m.** Scale 1m, three occasions. For each occasion `run1`, `run2`, `run3`:
   1. `group=probe`, `scale=1m`, `occasion=<runN>`
   2. `group=latency`, `scale=1m`, `occasion=<runN>`
   3. `group=marker`, `scale=1m`, `occasion=<runN>`, `marker_minutes=60`, `fail_pct=0.01`
   4. `group=failure`, `scale=1m`, `occasion=<runN>`
   5. `group=m0`, once only (occasion `run1`), if the secret is set.
   Steps 1 to 4 may be dispatched together; keep the occasion label identical within an occasion.
3. **Simulated network latency** is an input of the `latency` group: `rtts` defaults to `0,1,5` (milliseconds of
   added round trip, via the TCP proxy). To rerun only one value, dispatch `latency` with `rtts=5` (the 0 ms
   `tts_*` and `strong_*` cases still run; they need no proxy). All values are shuffled with the other cases in one
   randomized block design, in the same job.
4. **10M, only if it fits.** Look at `load.json` of the 1m runs (`free_gb` before load, after load, after index
   build) and `environment.json` (`df_h`): the data plus index cost per million documents is the drop in free space
   across load and index build. Project the peak at 10M as ten times that, plus the temporary space of an index
   build (use the larger of the two steps again as headroom). If the projection leaves less than about 2 GB of the
   14 GB, **do not dispatch 10M**; report it as not benchmarked with that projection. Otherwise dispatch `probe` and
   `latency` with `scale=10m` and `reps=3` once, label any comparison with 1m as scale trend, not a repeat. The
   harness itself stops cleanly (file `stopped.json`) when free disk falls below 1.5 GB.
5. For `rates`, use the default `10,100,500`. 500 writes/s is the design load; `rates=500` alone is a valid quick
   re-check of the load-sensitive cases. If the generator cannot keep up, `lag_ms` p95 in the overhead table
   shows it; do not trust a case whose generator lag p95 is above a few ms.

## 2. Download the artifacts

With the GitHub CLI (the author's own machine; this tool never contacts GitHub):

```
cd write-path
mkdir -p results/downloads
gh run list --workflow write-path-full --limit 30             # find run ids
gh run download <run-id> -n write-path-run1-latency-1m -D results/downloads/run1-latency-1m
# repeat for every group and occasion; or: gh run download <run-id> -D results/downloads/<run-id>  (all artifacts of a run)
```

Without the CLI: open each workflow run page, download the artifact zip from the Artifacts section and unzip it into
`results/downloads/<artifact-name>/`.

Each artifact contains `<occasion>-<group>-<scale>/` (the run directory: `meta.json`, `environment.json`, `load.json`,
one folder per phase with raw rows `*.jsonl.gz`, `cases.jsonl`, `order.json`, `canary.jsonl`), plus `engine.log` and
`df-end.txt`. Raw results are the files to keep and publish; the tables can always be rebuilt from them.

## 3. Run the analysis

One command per scale, listing every run directory of that scale (all groups, all occasions):

```
cd write-path
uv sync
uv run wpbench analyze results/downloads/*/*-1m --out results/analysis-1m
```

(`results/downloads/*/*-1m` matches the run directory inside each artifact folder; adjust if you unzipped elsewhere.
Runs of different scales in one call are refused.)

Outputs in `results/analysis-1m/`:

- `report-tables.md`: the verdict table first, then one section per question with its tables;
- `summary.json`: the verdicts and the per-case numbers behind NFR-02 and NFR-03, per run.

What the tables contain, and how each target is judged:

| Target | Table | Rule |
|---|---|---|
| NFR-02 | "Q2 overhead in STRONG mode" | p95 of the **upper-bound** overhead (total minus the lower bound of the engine time) <= 50 ms; the lower bound and the part-by-part split (token, marker upsert, data write, commit, cleanup, probe residual) are next to it; the p95 without noisy windows is shown |
| NFR-03 | "Q3 overhead in EVENTUAL mode" | p95 of the write with token and marker minus p95 of the plain write <= 15 ms, with and without a transaction, at each rtt |
| NFR-04 | "Q5 and Q6 command counts" | a transaction with N writes has `waits == 1` and `search queries <= poll attempts x chunks needed` |
| NFR-05 | same table | the skipped bulk has `search queries == 0` and one extra write (the dirty marker) |
| Variance | "Between-run variance" | per case, the range over runs of the per-run median; a verdict that differs between runs is marked `FLIPS between runs` and is not a conclusion |
| Noise | "Canary and noisy windows" | a 5-second window is noisy when the canary's median is above 1.5x (CPU loop) or 3x (database read) its own phase median; each table also gives the statistic without those windows, and the share of samples in them. Thresholds are the `cpu_factor` and `db_factor` arguments of `analyze()` |

Repetitions and sample counts are columns of every table. Percentile confidence intervals are bootstrap intervals
over repetitions (they need at least three repetitions; fewer shows n/a).

## 4. Writing the report

Copy the tables from `report-tables.md` into the written report, replacing each `pending` placeholder
marker, and fill the summary (at most 8 lines), the recommended probe shape, chunk size and polling schedule (decide from
the probe-shape and coalescing tables), and the proposed amendments. Quote `environment.json` for the environment
section, and state which conclusions are stable across the three occasions.
