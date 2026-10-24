# Refresh, visibility and ordering on Elasticsearch and OpenSearch

Harness only: code, workflow and measured tables. Plain `httpx` (0.28.1) against the REST API, synthetic documents, no security plugin. Python 3.12, `uv`.

Question: how long after a write or a delete does a search see it, can a stale answer be forced, is the order of equal-score hits stable, and can a given race be forced on the engine. The numbers are limits of these engines at these versions on one runner type, not general claims.

## Images (resolved 2026-10-23)

| id | image | manifest list digest | note |
|---|---|---|---|
| es-9.5.5 | `docker.elastic.co/elasticsearch/elasticsearch:9.5.5` | `sha256:13b4a40bb095774f5fda635e45fd5677cc6a26565e42b24997b0c72b88e75984` | latest GA, released 2026-10-06 |
| es-9.4.8 | `docker.elastic.co/elasticsearch/elasticsearch:9.4.8` | `sha256:fb7f2d0df199b3ea14a700e88a0b701d502cb44409ece3e1540b1dc1c5d5c403` | previous minor, released 2026-10-06 |
| os-3.9.0 | `opensearchproject/opensearch:3.9.0` | `sha256:adfa61f85025d06b4aeb562e7e74fde7e31c437039c93c3862c17e9acebd6c7c` | latest GA, released 2026-09-29 (amd64 manifest `sha256:13487e0953520edf6cc866dfc672fd67a70ab122aa7d84d4a6c97106f84d3f84`) |
| os-3.8.0 | `opensearchproject/opensearch:3.8.0` | `sha256:fafe3fc3587088674669235575aa166228c48bdb940294a8cdbbc1da75236a40` | previous minor, released 2026-08-05 |

Sources used:

- Elasticsearch releases: `https://api.github.com/repos/elastic/elasticsearch/releases` (9.5.5 and 9.4.8 both 2026-10-06, newest of each minor; 9.5.x is the highest minor, 8.19.23 is the older line and is not used).
- Elasticsearch digests: registry `https://docker.elastic.co/v2/elasticsearch/elasticsearch/manifests/<tag>` (token from `https://docker-auth.elastic.co/auth`), header `Docker-Content-Digest`. The local pull of 9.5.5 reported the same digest. Docker Hub `library/elasticsearch` carries different digests for the same tags and is not used.
- OpenSearch releases: `https://api.github.com/repos/opensearch-project/OpenSearch/releases` and the schedule at `https://opensearch.org/releases/` (3.9.0 released 2026-09-29, 3.8.0 on 2026-08-04 or 05 by the two sources; 2.19.7 is the 2.x maintenance line and is not used; 3.10.0 is scheduled for 2026-11-17).
- OpenSearch digests: Docker Hub API `https://registry.hub.docker.com/v2/repositories/opensearchproject/opensearch/tags/<tag>` and registry `registry-1.docker.io` manifest header. The local pull of 3.9.0 reported the manifest list digest above.

Every run records the image reference, image id, repo digests, engine version, CPU model, runner image, the container limits and the seed in the `meta` block of its JSON file. The digests in this table were resolved once; compare them with `meta.inspect` of a run (a re-pushed tag changes the digest; the Elasticsearch 9.x tags on Docker Hub were updated on the day of resolution).

## Engine container

`docker run -d --memory 10g --cpus 3 -p <port>:9200 -e discovery.type=single-node -e cluster.routing.allocation.disk.threshold_enabled=false`, heap `-Xms4g -Xmx4g`, Elasticsearch with `xpack.security.enabled=false`, OpenSearch with `DISABLE_SECURITY_PLUGIN=true`. The workflow raises `vm.max_map_count` to 262144 and records it. Indexes use one primary shard and no replica unless a test says otherwise. Defaults: names `hoodie-fl2-es` (port 29201) and `hoodie-fl2-os` (29202).

## Groups

`python bench.py GROUP N OUT` with `FL2_IMAGE` set to an id from the table. `python bench.py up` starts the container and waits for three consecutive yellow-or-green answers; `down` removes it. Each group writes one JSON file. Orders of arms are shuffled with the logged seed (`FL2_SEED`).

| group | what it does | N means |
|---|---|---|
| `start` | pull time, image size and digest, `docker run` to first HTTP answer, to yellow or green, to first successful index and search | cold starts, at most 5 |
| `lag` | write-to-searchable and delete-to-total-zero (`track_total_hits: true`), polling every 10 ms, under default `refresh_interval` (1s) without a refresh parameter, `refresh=wait_for`, `refresh=true`, `refresh_interval` 100ms, and `-1` with an explicit `_refresh` after each write and delete; min, p50, p90, p99, max plus raw values and timeouts (bound 30 s) | cycles per variant |
| `stale` | total right after a bulk delete: default settings, `refresh=true`, `wait_for`, `refresh_interval -1` (also `_count` and a realtime get), idle shard (`index.search.idle.after` set to 1s, 2.2 s pause before the delete, with and without an explicit `refresh_interval`) | repeats (idle arms at most 60) |
| `ties` | tie order of 120 equal-score documents; builds with a fixed insertion order, a shuffled one, and `_reindex` into a new index; 1 and 3 shards; after build and after `_forcemerge` to one segment; arms native, `dfs_query_then_fetch`, sort `[_score, ref]` (keyword copy of the id), `[_score, _id]`, `[_score, _doc]`, point in time with `_shard_doc`; distinct orders per arm | builds per config, at most 15 (at least 2) |
| `races` | for each race a forcing method and a success count: stale total after delete, purge by search and probe (and `_delete_by_query`) missing unrefreshed writes, index delete then recreate, late writer auto-creating the index, settle bound equal to the refresh interval, partial failure in one bulk, wait for a revision that was overwritten | repeats per race (the overwritten-revision race at most 30) |
| `stress` | write-then-wait with bound, `wait_for` writes, delete-then-total with and without `wait_for`, purge by search, reset (delete and recreate), plus two naive tests without any wait; several worker threads (`FL2_WORKERS`, default 4), per-test failures and messages | tenths of repeats: N=200 gives 20 repeats per worker |

Stress is latency and race behaviour over the REST API only; there is no engine emulation and no parity corpus. The naive tests are expected to fail and show the failure rate of not waiting.

## Workflows

- `.github/workflows/spike-fl2-es.yml` (`workflow_dispatch` and `workflow_call`): inputs `group`, `n`, `occasion`, `images` (JSON list, default all four). One job per group, a matrix cell per image, each on its own `ubuntu-latest` runner with its own container. Artifact `fl2-es-<occasion>-<group>-<image>`, 90 days, containing the JSON file, `machine.txt` and an engine log tail.
- `.github/workflows/spike-fl2-es-smoke.yml` (push): every group at its smallest input on `es-9.5.5` only. Its timings are not results.

Suggested dispatch: `gh workflow run spike-fl2-es.yml -f group=lag -f n=200 -f occasion=run1`, repeated for each group and for three occasions at different times. Compare arms within one run only. Expected duration per cell at the default N: start a few minutes, lag 15 to 30 minutes, stale 15 to 25, ties 10 to 20, races 20 to 40, stress 10 to 30 (estimates, not measured on a runner). Artifacts are small (tens of kilobytes).

## Not covered

- No canary process and no open-loop load: lag is measured by a single sequential client, stress by a few threads.
- Results from one runner type; the hosted runner CPU may differ between cells of one run.
- Replicas, multi-node clusters and security-enabled mode.
- OpenSearch behaviour of `search.idle.after` and of `_id` sort is measured, not assumed; the harness records errors per arm instead of failing.

## Local smoke status

Run on a laptop at N of 1 to 10 with a 512 MB heap and 2 CPUs, one container at a time, containers removed afterwards. Every group completed on both `es-9.5.5` and `os-3.9.0`; the numbers are not results. Observed in passing, not conclusions: Elasticsearch rejects sorting on `_id` ("Fielddata access on the _id field is disallowed") and the harness records that as an arm error.
