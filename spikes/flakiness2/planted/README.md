# Planted flakes: which strategy finds which flake, at what cost

Package `fl2p`, one `uv sync` in this folder (the scrubbed suite in `../suite` is a path dependency). The test set, the
strategies and the analysis are in `src/fl2p/`. Run with `uv run fl2p <command>`.

## Question

Given the same set of tests with known failure behaviour, what does each detection strategy flag, at what false-positive rate,
and at what cost in wall time, billed minutes and runners? Detection rate = fraction of experiments (K independent
experiments per strategy and flake) in which the planted flake was flagged. K is printed in every cell, with a Wilson 95% interval.

## The planted set (registry labels: name, kind, truth, engine, variant)

In-memory engine with virtual time (`LagEngine` from the suite), natural mode draws the lag from a declared model, forced mode
injects a fixed value:

| name | what | truth per natural run |
|---|---|---|
| stale-total | exact total read right after a delete, no wait | 1 (the delete lag is 0.8 s) |
| purge-probe | purge that waits with a `$search` probe | 1 |
| reset-order | collections dropped before indexes (ghost index) | 0.9 (model input) |
| tie-order | tied pair at the page boundary, native order | 0.5 |
| settle-eq | settle bound 1.0 s, lag ~ N(1.003, 0.012) | 0.5987 (analytic) |
| tx-partial | scenario s14 shape, bound 1.0 s, two good documents with lag 0.5 + Exp, 0.005 each | 0.009975 (analytic) |
| delay-1pct | injected Exp(1) virtual delay against bound ln(100) | 0.01 exactly |
| rng-1pct, rng-0.1pct | fails iff an `os.urandom`-seeded draw is below 0.01 / 0.001 | 0.01, 0.001 exactly |
| timing-race | worker thread sets a flag after 2 ms, the loop checks after +margin (default 1 ms) | measured: baseline and under stress-ng |
| order-dep-hidden, order-dep-exposed | fail iff the polluter ran earlier; default order passes the first and fails the second | 0.5 per random-order run |

Each race also has a `-fixed` variant (bound 4.0 s, indexes-first reset, count probe, `(score, id)` order). There are 24
controls (`c-*`, truth 0, pure and in-memory-engine scenarios, wall-clock ones with a 100x margin) and the polluter helper. Fixed
variants and controls are the false-positive set.

Real engine (`mongodb/mongodb-atlas-local:8.0`, container `hoodie-fl2-planted`, port 29401): `r-settle-b{0.90,0.95,1.00,1.01,1.02,4.00}`,
`r-tx-partial-b{0.90,0.95,1.00,1.01,1.02,4.00}` (scenario s14 with its bound varied), `r-stale-total`, `r-purge-probe`, `r-reset-order`
(the index must also be usable after the reset), `r-tie-order`, and the fixed variants. Truth is `measured`: a large n, first
half of each test's sequence as the truth sample, blocks of the second half as the detection experiments.

## Strategies

| id | what | where computed |
|---|---|---|
| S0 | single run, default order | `suite --strategy s0` |
| S1 | single run, random order (seed logged) | `suite --strategy s1` |
| S2 | N repeats, N in {1,5,10,20,50,100,300}: function scope (pytest-repeat default), session scope (whole list N times); in-process loop and pytest-repeat are both measured (`pytestcost`) | `pool` (isolated runs, blocks), `repeat` (shared state, prefix of 300 repeats) |
| S3 | deterministic injection (forced mode); applies only where an injection point exists, so it measures "given the race is modeled" | `forced` |
| S4 | noise: stress-ng at 50 and 100%, `cpus2` (affinity to two cores: an emulation of a 2 vCPU runner), their combination; N=1 and N=5 | `noise` |
| S5 | random order x 3 and x 10 seeds, order-diff verdict, a GIL-hogging thread (scheduling jitter) | `suite` |
| S6 | Hypothesis `RuleBasedStateMachine` (insert, purge, reset, reindex, settle) with one bug switched on per machine | `prop` |
| S7 | history scoring (mixed window, flip count, Beta-Binomial) over the pool sequences as run history, detection and false positives against K | `analyze` |
| S8 | repeat only the tests a PR touches (k of M), two scenarios | `analyze` (resampled from the pool) |
| S9 | rerun x3 on failure (Meta and GitHub style): any failure seen, and classified flaky | `analyze` |

Canary: `canary` in its own process (1M-iteration CPU loop and a search query, every 10 s), started beside every job.

## Commands

```
uv run fl2p registry                      # the labels
uv run fl2p smoke --out-dir results/smoke # every in-memory phase at tiny scale, then analyze (about 3 min)
uv run fl2p pool|suite|forced|repeat|prop|noise|calib|pytestcost|pytestcheck ...   # see the workflow for the full-scale arguments
uv run fl2p real --shard settle|tx|purge --out-dir results ...                     # needs the engine on port 29401
uv run fl2p analyze <run dirs> --out <dir> # tables.md, one run directory per occasion
```

## What is modeled, not measured

Billed minutes and the wall time of S2, S7, S8 and S9 are computed from measured pass times and the measured set-up of the job;
nothing was billed. Lag models of the in-memory set are inputs of the experiment, not measurements of an engine. The pools of
isolated runs are independent by construction. `cpus2` is an emulation.

## Notes added after run1

- The settle bound counts the sleeps between probes, not wall time and not time from the commit: the real window is the bound plus the probe times and the work before the first probe (the `real` tables report it per run as `window`, with `seen`, the harness's own write-to-searchable time). Bounds 0.90 and 0.95 were added for that reason.
- The timing-race margin is calibrated inside the noise job, on its own machine (stress100 rate closest to 5%); the sweep and the chosen margin are in `calib-noise.json`. The `jitter` arm is now a GIL-hogging thread and is also an arm of the noise job; whether it fires is a result.
- Real arms of one shard run in the same job in a shuffled order, with a `ref` arm (no noise, same n) for comparison. Analysis keeps hardware and set-up per job file and warns when a table mixes CPU models.
