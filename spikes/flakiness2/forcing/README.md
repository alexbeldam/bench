# forcing: exact-lag and race-forcing experiments

Harness: `bench.py` (plain pymongo, synthetic data, JSON output). Target: `mongodb/mongodb-atlas-local:8.0` (mongod plus the search process), 3 CPUs and 10 GiB, started with `write-path/scripts/ci-start-engine.sh` (stock) or `scripts/start.sh` (derived image, same limits and readiness rule). Workflow: `.github/workflows/spike-fl2-forcing.yml` (push runs every group at smoke scale; dispatch runs one group).

## Groups and arms

| Group | Question | Arms (randomised block order, logged seed) |
|---|---|---|
| `failcmd` | Can the `failCommand` fail point be used on this image, and can it make write-to-searchable lag an exact value T? | Stock image: is `enableTestCommands` on, can it be set at runtime, what does restarting mongod in place do. Derived image (mongod started with `enableTestCommands`): block the search process's change-stream `getMore` (filtered by its application name) for T in {0.5, 1.5, 2, 4} s in three ways (`once` gated on seeing the read held, `once_ungated`, `always`) plus a no-fail-point control; block the application's own `aggregate` for the same T (single query, and write to first result). Reported: lag, lag minus T, n per arm. |
| `toxi` | Does a Toxiproxy latency toxic change index lag, on the client path and between the search process and mongod? Does `tc netem` work? | Client path: latency per direction in {0, 0.25, 1} s, polled through the proxy and through the direct port. Feed path (the search process is started with the proxy address): downstream latency in {0, 0.5, 1.5, 2, 4} s, both directions in {0.5, 2} s; a disabled-proxy check for bypass. netem from a sidecar sharing the network namespace: `lo` 0.25 and 0.75 s, `eth0` 0.25 s, none. |
| `pause` | How exact is a SIGSTOP pause of the search process as a lag control? | T in {0.5, 1.5, 2, 4} s. Reported: stopped time (lower and upper bound), time from resume to searchable, lag minus T, min/p50/p90/max. |
| `ghost` | Why did dropping a collection under its search index and recreating it at once leave the index unqueryable in 9 of 10 earlier repetitions? | Recreate after 0, 50, 200, 1000 ms; search process paused around the drop or not (warm engine, new database and index name per repetition); fresh arm: a new engine per repetition, the repetition is the first thing run. Per repetition: the state of the old index entry over time, whether it disappears and a new index then becomes queryable, engine uptime, index age at drop, filtered engine log lines. |

`ghost` outcome fields: `original_ok` (queryable within the bound with the old entry absent at once, or the old entry itself queryable) is comparable to the earlier criterion; `queryable_incl_recreate` also counts an index created under a new name after the old entry disappeared.

## Derived image

`Dockerfile` adds two wrappers to the stock image: `mongod` (adds `enableTestCommands` when `FL_TEST_COMMANDS=1`) and the search process launcher (replaces the mongod address when `FL_MONGOT_ADDR` is set). Nothing else changes. The stock image's mongod is started by the container runner from `PATH`; killing it makes the runner exit the container.

## Local smoke (one container, `hoodie-fl2-forcing`, port 29301)

```
uv sync
FL_SMOKE=1 uv run python bench.py pause 1 results/pause.json
FL_SMOKE=1 uv run python bench.py failcmd 1 results/failcmd.json
FL_SMOKE=1 uv run python bench.py toxi 1 results/toxi.json
FL_SMOKE=1 uv run python bench.py ghost 1 results/ghost.json.gz 20261023 1 8
docker rm -f hoodie-fl2-forcing hoodie-fl2-tp hoodie-fl2-tc
```

`failcmd`, `pause` and `ghost` expect the stock engine already running on the port. `toxi` starts its own containers. On Windows run the start scripts with Git Bash (`FL_BASH`) and a copy of `ci-start-engine.sh` without the `free` line (`FL_START_STD`).

## Real runs (GitHub Actions only)

Dispatch order and rough duration at the default `n=20` (smoke scale is minutes): `pause` about 15 min, `failcmd` about 40 min, `toxi` about 60 min, `ghost` up to about 2.5 h (fresh arm restarts the engine 80 times; each failing repetition waits the bound). Artifact `fl2-forcing-<occasion>-<group>`, well under 5 MB (ghost rows are gzipped). Three occasions at different times; compare arms within one run only.

```
gh workflow run spike-fl2-forcing.yml -f group=pause -f n=30 -f occasion=run1
gh workflow run spike-fl2-forcing.yml -f group=failcmd -f n=20 -f occasion=run1
gh workflow run spike-fl2-forcing.yml -f group=toxi -f n=20 -f occasion=run1
gh workflow run spike-fl2-forcing.yml -f group=ghost -f n=20 -f fresh_n=20 -f bound=20 -f occasion=run1
```

Results are summarised inside each JSON (`*_summary`, `summary`, `overall`, `passes_vs_failures`); raw rows are under `*rows`.
