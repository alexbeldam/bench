# GitHub Actions mechanics

Questions: how required checks behave with `paths` filters and with an always-run gate job, what `concurrency` with `cancel-in-progress` does on pull requests, whether a merge queue can be enabled on a personal-account repository and what the closest mechanism does, what pytest selection and repetition tools cost on a runner, and how long it takes to list the files a pull request changes.

Harness: `harness/orch.py` (setup, one occasion, teardown) opens the pull requests (`create`), takes one snapshot of their states (`collect`, repeatable) and tries the merges (`merge`), and never waits; `harness/harness.py` is the pytest measurement run by `spike-fl2-mech-pytest` (three phases, each dispatching the next); `harness/diff.py` times the changed-file lists. `proj/` is a 30-test dummy project. Branches are named `fl2-mech-*` and are deleted after the runs. Results are JSON files written outside the repository.
