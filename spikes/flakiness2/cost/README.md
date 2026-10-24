# Cost of a search-backed test suite in CI

Question: what do the engine image, one shared engine with and without `pytest-xdist`, a 2-CPU runner and the number of
engine-bound tests cost in wall time? Groups (own runner each): `image`, `suite`, `cpu2`, `model`.

- `image`: `actions/cache` restore of a `docker save` tarball against `docker pull`; time to engine ready for both paths in the same job.
- `suite`: the suite in `../suite` (tests/test_parity.py, tests/test_stress.py, backends A, B, C) against one fresh engine per run. Arms: engine CPU limit (3, none) by xdist workers (none, 1, 2, 4, 8). One database prefix per run and worker (`KIT_TX_DB_PREFIX`).
- `cpu2`: as `suite`, engine `--cpus 2` and cpuset 0,1, test process under `taskset`. An emulation of a 2 vCPU runner on a 4 vCPU one; memory is not limited.
- `model`: `model/` has S tests that each insert one document and poll a text search until found, and M engine-free tests (mongomock plus `fast_ms` of CPU). Analysis fits `T = a + K/W` and `T = a + c*W + K/W`.

Order of arms and repetitions is shuffled with a logged seed. A CPU canary runs before each run. `cost.py analyze` writes `tables.md`.
Local smoke: `uv run python ../cost/cost.py suite ../cost/results/suite.json --workers 0,2 --cpus 3 --reps 1 -k "s01 or s20"` from `../suite`.
