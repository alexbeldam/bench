# Runbook: how to produce the real results

Nothing in this directory has been measured at scale. The code was verified with smoke runs only (5,000 documents,
a few repetitions, on a shared laptop). Every real number must come from the GitHub Actions runner, by following
this runbook. All commands below are for you (the author) to run; the harness never publishes anything.

## 0. One-time setup

1. Create the public repository, push the contents of this staging directory, and enable Actions.
2. The workflow `narrowing-ci` runs a smoke pass on every push. It must be green before any real run.
3. Optional, for the hosted Atlas M0 section: create the repository secret `HOODIE_BENCH_ATLAS_URL` (Settings, Secrets and
   variables, Actions). Without it, M0 is skipped and the report says so. Never paste the string anywhere else.

## 1. The dispatch plan: three occasions, each the same sequence

The between-run variance is measured by repeating the full sequence on **three different occasions** (different days
or at least different times of day, for example a weekday morning, a weekday evening, and a weekend). Each occasion
uses its own label so results do not overwrite each other. Do not dispatch two occasions back to back.

For occasion N (N = 1, 2, 3), use the label `runN`.

### Step A: 100k and 1M, all question groups, load test

Dispatch workflow **narrowing-bench** (Actions tab, Run workflow) with:

| input | value | why |
|---|---|---|
| label | `runN` | results directory and artifact name `narrowing-runN` |
| scales | `100k,1m` | both scales in one job; also gives the two disk measurements the 10M decision needs |
| phases | `all` | calibration, layouts, count_facets, tuning, selectivity, prefilter, noise (each shuffled inside itself) |
| reps | `30` | repetitions (rounds of the randomized block) per case |
| noise_reps | `200` | the cap-deciding cases need at least 100; 200 narrows the p99 interval |
| seed | empty | defaults to the workflow run number, so every occasion differs; the seed is logged |
| time_limit_min | `15` | per phase and scale: stops cleanly at a round boundary; the job limit is 6 hours |
| load_test | `true` | runs after the phases, for 1m only in this step |
| load_rates | `25,50,100,150,200,300,400` | queries per second, ascending |
| load_duration | `30` | seconds per rate |
| engine_cpus / engine_mem | `3` / `10g` | engine container limits; the runner has 4 vCPU and 16 GB, the client needs the rest |
| projection_* | empty | only for 10M |

When it finishes, open the run's **job summary**: it holds the analysis tables, the disk projection and the
environment. If a phase says "cut short" in "Repetitions and completeness", that phase has fewer repetitions than
planned: report that, and re-dispatch only that phase under a new label (for example `runN-b`, `phases` set to the
missing one) if you want more data. A continuation counts as a separate run in the variance table, so say so.

### Step B: the 10M decision

The workflow's step "Disk projection for 10m" wrote `results/runN/disk_projection.json` into the artifact and the job summary.
It extrapolates the measured peak disk use of the 100k and 1M loads (data, b-tree indexes and search index, including
the index build) to 10M with a 1.25 safety factor, and sets `"attempt": true` only if at least 2 GiB would remain
free on a fresh runner. The extrapolation is labelled as such in the file. Look at it after occasion 1:

- `"attempt": false`: do **not** run 10M. The report says "10M not benchmarked: projected peak disk use X GiB against Y GiB free".
  You may still repeat Step A on occasions 2 and 3. No 10M row appears in any table. The load test and the tuning, noise
  and selectivity questions then rest on 1M only; the report must say so.
- `"attempt": true`: run Step C on each occasion.

### Step C: 10M (only if the decision says so)

Dispatch **narrowing-bench** again, one job per occasion, with:

| input | value |
|---|---|
| label | `runN-10m` |
| scales | `10m` (never combine it with other scales) |
| phases | `all` |
| reps | `30` (the noise phase still uses `noise_reps`) |
| noise_reps | `200` |
| time_limit_min | `25` |
| load_test | `true` |
| projection_run_id | the numeric id of the Step A run of the same occasion (the number in its URL) |
| projection_label | `runN` |

The workflow downloads that run's artifact, reads `disk_projection.json`, and **ends cleanly without measuring** if the
attempt flag is not true. The loader also stops by itself if free space falls below 1.5 GB (it is resumable: dispatching
again with the same label starts from the recorded batch only if the engine volume survived, which it does not on a new
runner, so a stopped 10M load means 10M is reported as not benchmarked).

### Step D: hosted Atlas M0 (once, informational)

Dispatch **narrowing-m0** with label `m0` and docs `20000`. Skipped automatically without the secret. It runs
limits, tie behaviour, and a small strategy comparison, throttled to 40 operations per second, and drops its
collection and search index at the end. Repeat on another occasion if you want to see its variance.

### Expected order and count

Per occasion: Step A, then Step C if allowed. Three occasions: 3 x Step A and (if allowed) 3 x Step C, plus one Step D.
There is no measured run time yet. The first Step A run is the measurement of how long it takes; write that number into the README.

## 2. Download the artifacts

From the repository's Actions tab, open each run, scroll to Artifacts, download `narrowing-runN` (and `narrowing-runN-10m`,
`narrowing-m0`), and unzip each into `narrowing/results/<label>/` (the zip already holds the label's files at its top level):

```
narrowing/results/run1/            environment.json, raw_<scale>_<phase>.jsonl, canary_<scale>.jsonl, load_<scale>.jsonl, ...
narrowing/results/run1-10m/
narrowing/results/run2/ ...
narrowing/results/m0/m0.json
```

With the GitHub CLI installed, `gh run download <run-id> -n narrowing-run1 -D narrowing/results/run1` does the same.
Check that `environment.json` of each run names a GitHub run URL and a runner image: that is what makes a directory
a runner result. A directory without a run URL is a local run and must not be used for the report.

## 3. Generate the report tables

```
cd narrowing
uv sync --locked
uv run nbench analyze --out results/tables.md \
  --runs results/run1 results/run2 results/run3 results/run1-10m results/run2-10m results/run3-10m
```

(List only directories that exist. Add `--scale 1m` to restrict to one scale. `--noise-factor 2` is the canary
threshold: a 5-second window is noisy if its canary median is more than 2x the run's canary median; try 1.5 and 3 to
see whether a conclusion depends on it, and say which you used.)

`results/tables.md` contains, in this order:

1. **Environment per run** quoted from each `environment.json` (runner image, `nproc`, CPU model, container limits, engine and driver versions, run URL, the shuffle seeds).
2. **Noise windows**: per run and scale, the canary medians and percentiles and how many 5-second windows were flagged.
3. For each scale: the **calibration table** (runner column next to the laptop column of the first benchmark, environments named in the headings), then one section per question (Q2 layouts, Q3 totals and facets, Q4 tuning, Q5 selectivity, Q6 prefilter, Q7 noise, Q1 load).
   Every latency table gives pooled p50, p95 and p99 with 95% bootstrap intervals over repetitions, the number of repetitions,
   the same figures without samples in noisy windows, and the **per-run p50 and p95 with their spread**
   (that is the between-run variance). Sections that decide something end with a **stability** table: the
   conclusion recomputed per run, marked `stable` only if all runs agree.
4. **Disk use and the 10M decision** (peak disk per scale, and the projection).
5. **Repetitions and completeness** (rounds actually measured per phase, whether a time limit cut it short, the seed).

The load section is per run (a load test is a single time series, not pooled): rates, ok, errors, timeouts, dropped,
p50, p95 [interval], p99 [interval], p95 without noisy windows, engine CPU and memory from container stats,
client CPU, event-loop lag, which strategy served the queries, the knee, and whether the knee is stable across runs.

## 4. Fill the report

Open `REPORT-narrowing.md`. Replace every `pending: run on the GitHub Actions runner` with the table from `tables.md`
(copy the table, keep its row and column meaning). Write the summary last and only from tables that exist. The
amendment criteria in the report were written before any measurement; apply them as written and say where a criterion
was not met or was ambiguous. If 10M was not attempted, replace its sections with the projection numbers and say it is not benchmarked.
Re-check the rule in "What every report must say about the environment": all main numbers come from the runner,
the laptop appears only in the labelled calibration table, absolute latencies are relative evidence.

## 5. Check locally before the first real run (optional, no timings are valid)

```
cd narrowing
uv sync --locked
bash scripts/start-engine.sh nbench-local 27102 2 3g
export NBENCH_RESULTS=$PWD/results
uv run nbench env --label smoke-1 --container nbench-local
uv run nbench load --label smoke-1 --scale smoke --container nbench-local
uv run nbench run --label smoke-1 --scale smoke --phases all --reps 3 --noise-reps 5 --seed 11
uv run nbench load-test --label smoke-1 --scale smoke --container nbench-local --rates 10,20 --duration 4 --cooldown 1
uv run nbench analyze --runs results/smoke-1 --out results/smoke-1/tables.md
docker rm -f nbench-local
```

## 6. Things the first real run will tell you (unknown until then)

- Real run time per scale and whether 15 minutes per phase is enough for 30 repetitions at 1M (the output of `Repetitions and completeness` shows it).
- Whether `docker stats` and `du -sk /data/mongot` work on the runner (engine CPU and memory columns, and the search-index size column, are empty if not).
- Whether the artifact fits the free-plan storage (500 MB per GitHub's limits page) after three occasions; raw files are compressed in the zip.
- Whether the job summary (limited per step; the analysis is cut at 900,000 bytes) is complete. `results/<label>/tables.md` in the artifact always is.
- Whether a 3 CPU engine plus the Python client on the remaining CPU is enough at 400 queries per second: watch `loop lag` and `client CPU` in the load table. If the client saturates, the load result measures the client, and the report must say so.
