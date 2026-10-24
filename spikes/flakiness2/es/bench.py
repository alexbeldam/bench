"""Refresh, visibility and ordering experiments against Elasticsearch and OpenSearch containers. Plain httpx over the REST API, synthetic data.

  start   pull time, image size and digest, docker run to first HTTP answer, cluster status, first index+search
  lag     write-to-searchable and delete-to-total lag under five refresh settings (polling every 10 ms)
  stale   stale total after a delete: default settings, refresh=true, wait_for, refresh_interval -1, idle shard
  ties    tie order of equal-score hits over index builds, after force merge, against explicit tiebreakers
  races   can each race be forced on this engine, with a small test and a success count
  stress  write/delete/purge/reset tests with several workers, failures per test

Usage: python bench.py up | down | ref
       python bench.py start|lag|stale|ties|races|stress N OUT     (FL2_IMAGE selects the image id, see IMAGES)
Environment: FL2_IMAGE (es-9.5.5), FL2_CONTAINER, FL2_PORT, FL2_CPUS (3), FL2_MEM (10g), FL2_HEAP (4g), FL2_SEED, FL2_WORKERS (4)
"""
import hashlib
import json
import os
import platform
import random
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

IMAGES = {
    "es-9.5.5": "docker.elastic.co/elasticsearch/elasticsearch:9.5.5",
    "es-9.4.8": "docker.elastic.co/elasticsearch/elasticsearch:9.4.8",
    "os-3.9.0": "opensearchproject/opensearch:3.9.0",
    "os-3.8.0": "opensearchproject/opensearch:3.8.0",
}
ID = os.environ.get("FL2_IMAGE", "es-9.5.5")
REF = IMAGES[ID]
ES = ID.startswith("es")
C = os.environ.get("FL2_CONTAINER", "hoodie-fl2-es" if ES else "hoodie-fl2-os")
PORT = os.environ.get("FL2_PORT", "29201" if ES else "29202")
CPUS, MEM, HEAP = os.environ.get("FL2_CPUS", "3"), os.environ.get("FL2_MEM", "10g"), os.environ.get("FL2_HEAP", "4g")
SEED = int(os.environ.get("FL2_SEED", str(int(time.time()))))
WORKERS = int(os.environ.get("FL2_WORKERS", "4"))
MAP = {"properties": {"title": {"type": "text"}, "ref": {"type": "keyword"}, "tenant": {"type": "keyword"}, "rev": {"type": "integer"}}}
BOUND = 30.0


def q(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 4)


def summ(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "min": round(min(xs), 4), "p50": q(xs, .5), "p90": q(xs, .9), "p99": q(xs, .99), "max": round(max(xs), 4)}


def sh(*a):
    return subprocess.run(["docker", *a], capture_output=True, text=True)


# ---- client --------------------------------------------------------------------------------------------------------


class Eng:
    def __init__(self, timeout=60):
        self.c = httpx.Client(base_url=f"http://127.0.0.1:{PORT}", timeout=timeout, limits=httpx.Limits(max_connections=64))

    def __call__(self, method, path, body=None, **params):
        r = self.c.request(method, path, json=body, params={k: v for k, v in params.items() if v is not None})
        try:
            j = r.json()
        except ValueError:
            j = {"raw": r.text[:200]}
        return r.status_code, j

    def bulk(self, idx, ops, **params):
        body = "".join(json.dumps(o) + "\n" for o in ops)
        r = self.c.post(f"/{idx}/_bulk", content=body, headers={"content-type": "application/x-ndjson"}, params={k: v for k, v in params.items() if v is not None})
        return r.status_code, r.json()


def mk(e, name, shards=1, settings=None, mappings=MAP):
    st = {"number_of_shards": shards, "number_of_replicas": 0, **(settings or {})}
    s, j = e("PUT", f"/{name}", {"settings": {"index": st}, "mappings": mappings})
    if s != 200:
        raise RuntimeError(f"create {name}: {s} {j}")
    e("GET", f"/_cluster/health/{name}", wait_for_status="yellow", timeout="30s")


def rm(e, name):
    e("DELETE", f"/{name}", ignore_unavailable="true")


def count(e, idx, key):
    s, j = e("POST", f"/{idx}/_search", {"query": {"match": {"title": key}}, "size": 0, "track_total_hits": True})
    if s != 200:
        raise RuntimeError(f"search {idx}: {s} {str(j)[:200]}")
    return j["hits"]["total"]["value"]


def wait(e, idx, key, ok, bound=BOUND):
    """Poll every 10 ms until ok(total); seconds spent, or None after the bound."""
    t = time.perf_counter()
    while True:
        if ok(count(e, idx, key)):
            return time.perf_counter() - t
        if time.perf_counter() - t > bound:
            return None
        time.sleep(0.01)


def put_docs(e, idx, ids, title, refresh=None, **extra):
    ops = []
    for i in ids:
        ops += [{"index": {"_id": i}}, {"title": title, "ref": i, **extra}]
    s, j = e.bulk(idx, ops, refresh=refresh)
    assert s == 200 and not j["errors"], str(j)[:300]


def del_docs(e, idx, ids, refresh=None):
    s, j = e.bulk(idx, [{"delete": {"_id": i}} for i in ids], refresh=refresh)
    assert s == 200 and not j["errors"], str(j)[:300]


# ---- machine and engine --------------------------------------------------------------------------------------------


def meta():
    try:
        cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except (OSError, StopIteration):
        cpu = platform.processor()
    ins = sh("image", "inspect", REF, "--format", "{{.Id}} {{.Size}} {{json .RepoDigests}}").stdout.strip()
    m = {"image_id": ID, "image": REF, "inspect": ins, "cpu_model": cpu, "nproc": os.cpu_count(),
         "runner_image": f"{os.environ.get('ImageOS', '')} {os.environ.get('ImageVersion', '')}".strip(),
         "docker": sh("--version").stdout.strip(), "limits": {"cpus": CPUS, "mem": MEM, "heap": HEAP}, "seed": SEED, "python": platform.python_version()}
    try:
        m["max_map_count"] = open("/proc/sys/vm/max_map_count").read().strip()
    except OSError:
        pass
    try:
        s, j = Eng(5)("GET", "/")
        m["engine"] = {"version": j.get("version"), "tagline": j.get("tagline")}
    except httpx.HTTPError:
        pass
    return m


def run_args():
    a = ["run", "-d", "--name", C, "-p", f"{PORT}:9200", "--memory", MEM, "--cpus", CPUS, "-e", "discovery.type=single-node",
         "-e", "cluster.routing.allocation.disk.threshold_enabled=false"]
    if ES:
        a += ["-e", f"ES_JAVA_OPTS=-Xms{HEAP} -Xmx{HEAP}", "-e", "xpack.security.enabled=false"]
    else:
        a += ["-e", f"OPENSEARCH_JAVA_OPTS=-Xms{HEAP} -Xmx{HEAP}", "-e", "DISABLE_SECURITY_PLUGIN=true"]
    return a + [REF]


def health(e):
    try:
        s, j = e("GET", "/_cluster/health", wait_for_status="yellow", timeout="2s")
        return j.get("status") if s == 200 else None
    except httpx.HTTPError:
        return None


def up(stable=3):
    sh("rm", "-f", C)
    r = sh(*run_args())
    if r.returncode:
        sys.exit(r.stderr)
    e, ok = Eng(5), 0
    for _ in range(300):
        ok = ok + 1 if health(e) in ("green", "yellow") else 0
        if ok >= stable:
            print("engine ready", ID, flush=True)
            return
        time.sleep(1)
    print(sh("logs", "--tail", "50", C).stdout, file=sys.stderr)
    sys.exit("engine did not become ready")


def down():
    sh("rm", "-f", C)


# ---- start ---------------------------------------------------------------------------------------------------------


def start(n, out):
    r = {"meta": None, "cold_starts": []}
    present = sh("image", "inspect", REF).returncode == 0
    t = time.perf_counter()
    p = sh("pull", REF)
    r["pull"] = {"already_present": present, "pull_s": round(time.perf_counter() - t, 2), "rc": p.returncode}
    ins = sh("image", "inspect", REF, "--format", "{{.Size}}|{{json .RepoDigests}}|{{.Id}}").stdout.strip().split("|")
    r["image_size_bytes"], r["repo_digests"], r["image_config_id"] = ins[0], ins[1], ins[2]
    for k in range(min(n, 5)):
        sh("rm", "-f", C)
        st, t0 = {}, time.perf_counter()
        now = lambda: round(time.perf_counter() - t0, 2)  # noqa: E731
        run = sh(*run_args())
        st["docker_run_returned"] = now()
        if run.returncode:
            st["error"] = run.stderr[:300]
            r["cold_starts"].append(st)
            continue
        e = Eng(30)
        while "http_answer" not in st and time.perf_counter() - t0 < 300:
            try:
                if e("GET", "/")[0] == 200:
                    st["http_answer"] = now()
            except httpx.HTTPError:
                time.sleep(0.1)
        while "cluster_yellow_or_green" not in st and time.perf_counter() - t0 < 300:
            h = health(e)
            if h in ("green", "yellow"):
                st["cluster_yellow_or_green"] = now()
                st["cluster_status"] = h
            else:
                time.sleep(0.1)
        try:
            e("PUT", "/fl2-start", {"settings": {"index": {"number_of_replicas": 0}}, "mappings": MAP})
            e("PUT", "/fl2-start/_doc/1", {"title": "first doc"}, refresh="true")
            while count(e, "fl2-start", "first") != 1 and time.perf_counter() - t0 < 300:
                time.sleep(0.01)
            st["first_index_and_search"] = now()
        except (httpx.HTTPError, RuntimeError) as ex:
            st["error"] = f"{type(ex).__name__} {str(ex)[:200]}"
        if k == 0:
            r["meta"] = meta()
        r["cold_starts"].append(st)
        sh("rm", "-f", C)
    r["meta"] = r["meta"] or meta()
    for key in ("docker_run_returned", "http_answer", "cluster_yellow_or_green", "first_index_and_search"):
        r[f"{key}_s"] = summ([s.get(key) for s in r["cold_starts"]])
    return r


# ---- lag -----------------------------------------------------------------------------------------------------------

VARIANTS = [("default", {}, None), ("wait_for", {}, "wait_for"), ("true", {}, "true"),
            ("ri_100ms", {"refresh_interval": "100ms"}, None), ("ri_off_explicit_refresh", {"refresh_interval": "-1"}, "explicit")]


def lag(n, out):
    e, r = Eng(), {"meta": meta(), "n": n, "variants": {}}
    order = list(VARIANTS)
    random.Random(SEED).shuffle(order)
    r["order"] = [v[0] for v in order]
    for name, settings, mode in order:
        idx = f"fl2lag-{name}".replace("_", "-")
        rm(e, idx)
        mk(e, idx, settings=settings)
        par = mode if mode in ("true", "wait_for") else None
        e("PUT", f"/{idx}/_doc/seed", {"title": "seed"}, refresh="true")
        o = {k: [] for k in ("write_call", "write_to_searchable", "write_total", "delete_call", "delete_to_total_zero", "delete_total")}
        for k in range(1, n + 1):
            key = f"tok{k}x"
            t0 = time.perf_counter()
            s, j = e("PUT", f"/{idx}/_doc/{key}", {"title": f"{key} word"}, refresh=par)
            assert s in (200, 201), j
            t1 = time.perf_counter()
            if mode == "explicit":
                e("POST", f"/{idx}/_refresh")
            w = wait(e, idx, key, lambda v: v >= 1)
            t2 = time.perf_counter()
            o["write_call"].append(t1 - t0)
            o["write_to_searchable"].append(None if w is None else t2 - t1)
            o["write_total"].append(None if w is None else t2 - t0)
            t0 = time.perf_counter()
            s, j = e("DELETE", f"/{idx}/_doc/{key}", refresh=par)
            assert s == 200, j
            t1 = time.perf_counter()
            if mode == "explicit":
                e("POST", f"/{idx}/_refresh")
            w = wait(e, idx, key, lambda v: v == 0)
            t2 = time.perf_counter()
            o["delete_call"].append(t1 - t0)
            o["delete_to_total_zero"].append(None if w is None else t2 - t1)
            o["delete_total"].append(None if w is None else t2 - t0)
        r["variants"][name] = {"settings": settings, "mode": mode, "summary": {k: summ(v) for k, v in o.items()},
                               "timeouts": {k: sum(x is None for x in v) for k, v in o.items()},
                               "raw": {k: [None if x is None else round(x, 4) for x in v] for k, v in o.items()}}
        rm(e, idx)
    return r


# ---- stale ---------------------------------------------------------------------------------------------------------


def stale(n, out):
    e, r = Eng(), {"meta": meta(), "n": n, "arms": {}}
    ids = [f"s{i:03d}" for i in range(50)]
    arms = [("plain_default", {}, None, 0), ("refresh_true", {}, "true", 0), ("wait_for", {}, "wait_for", 0),
            ("ri_off", {"refresh_interval": "-1"}, None, 0), ("idle_default_idle_after_1s", {"search.idle.after": "1s"}, None, 2.2),
            ("idle_explicit_ri_1s", {"search.idle.after": "1s", "refresh_interval": "1s"}, None, 2.2)]
    random.Random(SEED).shuffle(arms)
    r["order"] = [a[0] for a in arms]
    for name, settings, par, idle in arms:
        reps = min(n, 60) if idle else n
        idx = "fl2stale-" + name.replace("_", "-")
        rm(e, idx)
        mk(e, idx, settings=settings)
        runs = []
        for _ in range(reps):
            put_docs(e, idx, ids, "tok common", refresh="true")
            assert wait(e, idx, "common", lambda v: v == 50) is not None
            if idle:
                time.sleep(idle)
            t = time.perf_counter()
            del_docs(e, idx, ids, refresh=par)
            first = count(e, idx, "common")
            run = {"total_right_after": first}
            if name == "ri_off":
                run["count_api"] = e("POST", f"/{idx}/_count", {"query": {"match": {"title": "common"}}})[1].get("count")
                run["get_by_id_status"] = e("GET", f"/{idx}/_doc/{ids[0]}")[0]
                time.sleep(1.5)
                run["total_after_1_5s"] = count(e, idx, "common")
                e("POST", f"/{idx}/_refresh")
            else:
                run["second_total"] = count(e, idx, "common")
                run["converge_s"] = None
                if first:
                    w = wait(e, idx, "common", lambda v: v == 0)
                    run["converge_s"] = None if w is None else round(time.perf_counter() - t, 4)
            assert wait(e, idx, "common", lambda v: v == 0) is not None
            runs.append(run)
        st = sum(x["total_right_after"] != 0 for x in runs)
        a = {"settings": settings, "refresh_param": par, "idle_wait_s": idle, "repeats": reps, "stale_right_after": f"{st}/{reps}",
             "totals_seen": sorted({x["total_right_after"] for x in runs})}
        if name == "ri_off":
            a["still_stale_after_1_5s"] = f"{sum(x['total_after_1_5s'] != 0 for x in runs)}/{reps}"
            a["count_api_stale"] = f"{sum(x['count_api'] != 0 for x in runs)}/{reps}"
            a["get_by_id_statuses"] = sorted({x["get_by_id_status"] for x in runs})
        else:
            a["still_stale_second_query"] = f"{sum(x['second_total'] != 0 for x in runs)}/{reps}"
            a["converge_s"] = summ([x["converge_s"] for x in runs])
        a["runs_head"] = runs[:3]
        r["arms"][name] = a
        rm(e, idx)
    return r


# ---- ties ----------------------------------------------------------------------------------------------------------

DOCS = 120


def ties(n, out):
    e, r = Eng(), {"meta": meta(), "docs": DOCS, "builds_per_config": max(2, min(n, 15)), "configs": {}}
    rnd = random.Random(SEED)
    ids = [f"d{i:03d}" for i in range(DOCS)]
    sorted_hash = hashlib.md5(",".join(ids).encode()).hexdigest()[:10]
    base = {"query": {"match": {"title": "alpha"}}, "size": DOCS, "track_total_hits": True}
    arms = {"native": {}, "dfs_native": {"_params": {"search_type": "dfs_query_then_fetch"}}, "score_ref": {"sort": [{"_score": "desc"}, {"ref": "asc"}]},
            "score_id": {"sort": [{"_score": "desc"}, {"_id": "asc"}]}, "score_doc": {"sort": [{"_score": "desc"}, {"_doc": "asc"}]}}

    def fill(idx, shards, order, src):
        mk(e, idx, shards)
        if order == "reindex":
            s, j = e("POST", "/_reindex", {"source": {"index": src}, "dest": {"index": idx}}, refresh="true")
            assert s == 200 and not j.get("failures"), j
            return
        seq = list(ids)
        if order == "shuffled":
            rnd.shuffle(seq)
        for i in range(0, DOCS, 30):
            put_docs(e, idx, seq[i:i + 30], "alpha beta", refresh="true")

    def hits(s, j):
        if s != 200:
            return None, f"{s} {str(j.get('error', j))[:160]}"
        return [h["_id"] for h in j["hits"]["hits"]], len({h["_score"] for h in j["hits"]["hits"]})

    def search(idx, body):
        body = dict(body)
        params = body.pop("_params", {})
        return hits(*e("POST", f"/{idx}/_search", {**base, **body}, **params))

    def pit(idx):
        s, j = e("POST", f"/{idx}/_pit", keep_alive="1m") if ES else e("POST", f"/{idx}/_search/point_in_time", keep_alive="1m")
        if s != 200:
            return None, f"pit {s} {str(j.get('error', j))[:120]}"
        pid = j.get("id") or j.get("pit_id")
        res = hits(*e("POST", "/_search", {**base, "sort": [{"_score": "desc"}, {"_shard_doc": "asc"}], "pit": {"id": pid, "keep_alive": "1m"}}))
        e("DELETE", "/_pit", {"id": pid}) if ES else e("DELETE", "/_search/point_in_time", {"pit_id": [pid]})
        return res

    for shards in (1, 3):
        for order in ("fixed", "shuffled", "reindex"):
            cfg = f"shards{shards}_{order}"
            res = {p: {a: {"hashes": [], "errors": set(), "scores": set()} for a in [*arms, "pit_shard_doc"]} for p in ("after_build", "after_forcemerge")}
            changed, src = 0, f"fl2ties-src-{shards}"
            if order == "reindex":
                rm(e, src)
                fill(src, shards, "shuffled", None)
            for b in range(r["builds_per_config"]):
                idx = f"fl2ties-{cfg}-{b}".replace("_", "-")
                fill(idx, shards, order, src)
                native = {}
                for phase in ("after_build", "after_forcemerge"):
                    if phase == "after_forcemerge":
                        e("POST", f"/{idx}/_forcemerge", max_num_segments=1)
                        e("POST", f"/{idx}/_refresh")
                    for a, body in [*arms.items(), ("pit_shard_doc", None)]:
                        got, info = pit(idx) if body is None else search(idx, body)
                        cell = res[phase][a]
                        if got is None:
                            cell["errors"].add(info)
                            continue
                        cell["hashes"].append(hashlib.md5(",".join(got).encode()).hexdigest()[:10])
                        cell["scores"].add(info)
                    native[phase] = res[phase]["native"]["hashes"][-1:]
                changed += native["after_build"] != native["after_forcemerge"]
                rm(e, idx)
            rm(e, src)
            r["configs"][cfg] = {"native_changed_by_forcemerge": f"{changed}/{r['builds_per_config']}", "sorted_ids_hash": sorted_hash, "phases": {
                p: {a: {"builds_ok": len(c["hashes"]), "distinct_orders": len(set(c["hashes"])), "equals_sorted_ids": sum(h == sorted_hash for h in c["hashes"]),
                        "distinct_score_counts_per_search": sorted(map(str, c["scores"])), "errors": sorted(c["errors"])} for a, c in cells.items()}
                for p, cells in res.items()}}
            print(cfg, r["configs"][cfg]["native_changed_by_forcemerge"], flush=True)
    return r


# ---- races ---------------------------------------------------------------------------------------------------------


def races(n, out):
    e, r = Eng(), {"meta": meta(), "n": n, "races": []}

    def add(race, method, ok, note="", arms=None, total=None):
        r["races"].append({"race": race, "forcing_method": method, "forced": f"{ok}/{total or n}", "can_be_forced": ok > 0, "note": note, **({"arms": arms} if arms else {})})
        print(race, f"{ok}/{total or n}", flush=True)

    ids = [f"r{i:02d}" for i in range(20)]
    # 1 stale total after delete
    for name, settings, par in (("default_refresh", {}, None), ("refresh_interval_off", {"refresh_interval": "-1"}, None), ("refresh_true", {}, "true")):
        idx = f"fl2r1-{name}".replace("_", "-")
        mk(e, idx, settings=settings)
        ok = 0
        for _ in range(n):
            put_docs(e, idx, ids, "tok common", refresh="true")
            assert wait(e, idx, "common", lambda v: v == 20) is not None
            del_docs(e, idx, ids, refresh=par)
            ok += count(e, idx, "common") != 0
            e("POST", f"/{idx}/_refresh")
        add(f"stale total after delete ({name})", f"bulk delete, refresh param {par}, total at once", ok)
        rm(e, idx)
    # 2 purge by probe: documents written but not yet refreshed escape a purge that finds its targets by search
    idx = "fl2r2"
    mk(e, idx)
    tq = {"query": {"term": {"tenant": "t"}}}
    for name, mode in (("purge_by_search_ids_and_probe", "ids"), ("delete_by_query_at_once", "dbq"), ("control_writes_wait_for", "ctl")):
        ok, probe_zero = 0, 0
        for _ in range(n):
            put_docs(e, idx, ids[:10], "tok purge", refresh="wait_for" if mode == "ctl" else None, tenant="t")
            if mode == "dbq":
                e("POST", f"/{idx}/_delete_by_query", tq, refresh="true", conflicts="proceed")
            else:
                s, j = e("POST", f"/{idx}/_search", {**tq, "size": 100, "_source": False})
                hit = [h["_id"] for h in j["hits"]["hits"]]
                if hit:
                    del_docs(e, idx, hit, refresh="true")
            probe_zero += e("POST", f"/{idx}/_search", {**tq, "size": 0, "track_total_hits": True})[1]["hits"]["total"]["value"] == 0
            e("POST", f"/{idx}/_refresh")
            left = e("POST", f"/{idx}/_count", tq)[1]["count"]
            ok += left > 0
            if left:
                e("POST", f"/{idx}/_delete_by_query", tq, refresh="true", conflicts="proceed")
        add(f"purge-by-probe survivors ({name})", "write without refresh, purge at once, refresh, count survivors", ok, f"probe saw zero in {probe_zero}/{n} trials")
    rm(e, idx)
    # 3 reset order
    ok_a = ok_b = 0
    body = {"mappings": MAP, "settings": {"index": {"number_of_replicas": 0}}}
    for k in range(n):
        idx = f"fl2r3-{k}"
        mk(e, idx)
        put_docs(e, idx, ids, "tok old", refresh="true")
        e("DELETE", f"/{idx}")
        s, j = e("PUT", f"/{idx}", body)
        e("POST", f"/{idx}/_refresh")
        left = e("POST", f"/{idx}/_count")[1].get("count", -1)
        ok_a += s != 200 or left != 0
        rm(e, idx)
        e("PUT", f"/{idx}", body)
        e("DELETE", f"/{idx}")
        late = e("PUT", f"/{idx}/_doc/late", {"title": "late write", "ref": "late"})  # a writer still in flight recreates the index
        s, j = e("PUT", f"/{idx}", body)
        mp = e("GET", f"/{idx}/_mapping")[1].get(idx, {}).get("mappings", {}).get("properties", {})
        ok_b += late[0] in (200, 201) and (s != 200 or mp.get("ref", {}).get("type") != "keyword")
        rm(e, idx)
    add("reset order: delete then recreate at once (old documents survive or create fails)", "DELETE index, PUT same name at once, count", ok_a)
    add("reset order: late writer auto-creates the index with dynamic mapping", "DELETE index, single write by id, then PUT with the intended mapping", ok_b)
    # 4 settle bound equal to the latency
    arms = {}
    for ri, bound in (("1s", 0.5), ("1s", 1.0), ("1s", 1.2), ("100ms", 0.1), ("100ms", 0.15)):
        idx = f"fl2r4-{ri}"
        rm(e, idx)
        mk(e, idx, settings={"refresh_interval": ri})
        e("PUT", f"/{idx}/_doc/seed", {"title": "seed"}, refresh="true")
        miss = 0
        for k in range(n):
            key = f"set{k}x"
            e("PUT", f"/{idx}/_doc/{key}", {"title": key})
            miss += wait(e, idx, key, lambda v: v >= 1, bound) is None
        arms[f"refresh_interval {ri}, bound {bound}s"] = f"{miss}/{n} not settled"
        rm(e, idx)
    add("settle bound equal to the refresh interval", "write, poll every 10 ms with a bound at or below the interval",
        sum(int(v.split("/")[0]) for v in arms.values()), "arms give timeouts per bound; the count is summed over arms", arms, total=n * len(arms))
    # 5 partial failure
    idx = "fl2r5"
    mk(e, idx)
    ok = vis = 0
    for k in range(n):
        ops = []
        for i in range(10):
            ops += [{"index": {"_id": f"b{k}-{i}"}}, {"title": f"tok bulk{k}", "rev": "bad" if i == 4 else i}]
        s, j = e.bulk(idx, ops, refresh="wait_for")
        got = count(e, idx, f"bulk{k}")
        ok += s == 200 and j["errors"] and 0 < got < 10
        vis += got == 9
    add("partial failure in one bulk (HTTP 200, errors true, some items applied)", "bulk of 10 with one document of the wrong type", ok,
        f"successful items visible at once after wait_for in {vis}/{n}")
    rm(e, idx)
    # 6 wait for an exact revision that was overwritten
    m = min(n, 30)
    for name, par in (("overwrite_before_refresh", None), ("control_wait_for", "wait_for")):
        idx = "fl2r6"
        rm(e, idx)
        mk(e, idx)
        ok = 0
        for k in range(m):
            e("PUT", f"/{idx}/_doc/x{k}", {"title": f"tok rev{k}", "rev": 1}, refresh=par)
            e("PUT", f"/{idx}/_doc/x{k}", {"title": f"tok rev{k}", "rev": 2})
            t, seen = time.perf_counter(), False
            while time.perf_counter() - t < 3:
                cq = {"query": {"bool": {"must": [{"term": {"rev": 1}}, {"match": {"title": f"rev{k}"}}]}}}
                if e("POST", f"/{idx}/_count", cq)[1].get("count"):
                    seen = True
                    break
                time.sleep(0.01)
            ok += not seen
        add(f"exact-revision wait never completes ({name}, bound 3 s)", "write revision 1, overwrite with revision 2, poll for revision 1", ok, total=m)
        rm(e, idx)
    return r


# ---- stress --------------------------------------------------------------------------------------------------------

SB = 5.0


def t_write_wait(e, idx):
    mk(e, idx)
    e("PUT", f"/{idx}/_doc/1", {"title": "tok one"})
    assert wait(e, idx, "one", lambda v: v == 1, SB) is not None, "not visible within bound"


def t_write_wait_for(e, idx):
    mk(e, idx)
    e("PUT", f"/{idx}/_doc/1", {"title": "tok one"}, refresh="wait_for")
    assert count(e, idx, "one") == 1, "not visible after wait_for"


def t_delete_total(e, idx):
    mk(e, idx)
    put_docs(e, idx, [f"a{i}" for i in range(20)], "tok many", refresh="true")
    del_docs(e, idx, ["a0"])
    assert wait(e, idx, "many", lambda v: v == 19, SB) is not None, "total not 19 within bound"


def t_delete_wait_for(e, idx):
    mk(e, idx)
    put_docs(e, idx, [f"a{i}" for i in range(20)], "tok many", refresh="true")
    del_docs(e, idx, ["a0"], refresh="wait_for")
    assert count(e, idx, "many") == 19, "total not 19 after wait_for"


def t_purge(e, idx):
    mk(e, idx)
    ops = []
    for i in range(30):
        ops += [{"index": {"_id": f"p{i}"}}, {"title": "tok purge", "tenant": "a" if i % 2 else "b"}]
    e.bulk(idx, ops, refresh="wait_for")
    t = time.perf_counter()
    while True:
        s, j = e("POST", f"/{idx}/_search", {"query": {"term": {"tenant": "a"}}, "size": 100, "_source": False})
        hit = [h["_id"] for h in j["hits"]["hits"]]
        if not hit:
            break
        del_docs(e, idx, hit, refresh="wait_for")
        assert time.perf_counter() - t < SB, "purge did not finish within bound"
    e("POST", f"/{idx}/_refresh")
    left = e("POST", f"/{idx}/_count", {"query": {"term": {"tenant": "a"}}})[1]["count"]
    other = e("POST", f"/{idx}/_count", {"query": {"term": {"tenant": "b"}}})[1]["count"]
    assert left == 0 and other == 15, f"survivors {left}, other tenant {other}"


def t_reset(e, idx):
    mk(e, idx)
    put_docs(e, idx, [f"o{i}" for i in range(10)], "tok old", refresh="true")
    e("DELETE", f"/{idx}")
    mk(e, idx)
    put_docs(e, idx, [f"n{i}" for i in range(5)], "tok new", refresh="true")
    tot = e("POST", f"/{idx}/_count")[1]["count"]
    assert tot == 5, f"total {tot} after reset"


def t_naive_delete(e, idx):
    mk(e, idx)
    put_docs(e, idx, [f"a{i}" for i in range(20)], "tok many", refresh="true")
    del_docs(e, idx, ["a0"])
    assert count(e, idx, "many") == 19, "stale total at once (no wait)"


def t_naive_write(e, idx):
    mk(e, idx)
    e("PUT", f"/{idx}/_doc/1", {"title": "tok one"})
    assert count(e, idx, "one") == 1, "not visible at once (no wait)"


TESTS = [t_write_wait, t_write_wait_for, t_delete_total, t_delete_wait_for, t_purge, t_reset, t_naive_delete, t_naive_write]


def stress(n, out):
    reps = max(1, n // 10)
    e, r = Eng(120), {"meta": meta(), "repeats": reps, "workers": WORKERS, "bound_s": SB, "tests": {}}
    res = {t.__name__: {"runs": 0, "failures": 0, "messages": {}, "secs": []} for t in TESTS}

    def work(a):
        rep, w = a
        order = list(TESTS)
        random.Random(SEED + rep * 100 + w).shuffle(order)
        for t in order:
            idx = f"fl2s-{rep}-{w}-{t.__name__[2:]}".replace("_", "-")
            s, msg = time.perf_counter(), None
            try:
                t(e, idx)
            except AssertionError as ex:
                msg = str(ex)[:120]
            except (httpx.HTTPError, RuntimeError, KeyError) as ex:
                msg = f"{type(ex).__name__} {str(ex)[:100]}"
            c = res[t.__name__]
            c["runs"] += 1
            c["secs"].append(time.perf_counter() - s)
            if msg:
                c["failures"] += 1
                c["messages"][msg] = c["messages"].get(msg, 0) + 1
            rm(e, idx)

    with ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(work, [(rep, w) for rep in range(reps) for w in range(WORKERS)]))
    for k, c in res.items():
        r["tests"][k] = {"runs": c["runs"], "failures": c["failures"], "messages": c["messages"], "secs": summ(c["secs"])}
    return r


# ---- main ----------------------------------------------------------------------------------------------------------

GROUPS = {"start": start, "lag": lag, "stale": stale, "ties": ties, "races": races, "stress": stress}

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "up":
        up()
    elif cmd == "down":
        down()
    elif cmd == "ref":
        print(REF)
    else:
        res = GROUPS[cmd](int(sys.argv[2]), sys.argv[3])
        res["group"] = cmd
        json.dump(res, open(sys.argv[3], "w"), indent=1, default=lambda o: sorted(o) if isinstance(o, set) else str(o))
        brief = json.loads(json.dumps(res, default=str))
        for v in brief.get("variants", {}).values():
            v.pop("raw", None)
        for v in brief.get("arms", {}).values():
            v.pop("runs_head", None)
        brief.pop("meta", None)
        print(json.dumps(brief, indent=1)[:6000])
