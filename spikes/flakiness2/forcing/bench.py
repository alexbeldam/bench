"""Forcing experiments against the search-enabled MongoDB container. Plain pymongo, synthetic data, JSON output.

  failcmd  can the failCommand fail point be used; if so, block the change-stream read of the index process (and the
           application's own $search) for an exact time and measure write-to-searchable lag minus that time
  toxi     Toxiproxy on the client path and between the index process and mongod; tc netem from a sidecar
  pause    pause the index process with SIGSTOP for an exact time, measure lag minus that time
  ghost    drop a collection under its search index, recreate it after a delay; what decides whether it becomes queryable

Usage: python bench.py GROUP N OUT [SEED] [FRESH_N] [BOUND_S]
Environment: FL_CONTAINER (hoodie-fl2-forcing), FL_PORT (29301), FL_TP_API (29302), FL_TP_CLIENT (29303),
FL_START_STD (script that starts the stock container), FL_BASH (bash to run the start scripts), FL_SMOKE=1 (shorter waits).
"""
import gzip
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

from pymongo import MongoClient
from pymongo.operations import SearchIndexModel

HERE = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..")).replace("\\", "/")
IMAGE = "mongodb/mongodb-atlas-local:8.0"
DER = "fl2-engine:local"
TP_IMAGE = "ghcr.io/shopify/toxiproxy:2.13.0"
C = os.environ.get("FL_CONTAINER", "hoodie-fl2-forcing")
TP = "hoodie-fl2-tp"
TC = "hoodie-fl2-tc"
PORT = int(os.environ.get("FL_PORT", "29301"))
TP_API = int(os.environ.get("FL_TP_API", "29302"))
TP_CLIENT = int(os.environ.get("FL_TP_CLIENT", "29303"))
START_STD = os.environ.get("FL_START_STD", ROOT + "/write-path/scripts/ci-start-engine.sh")
START_DER = HERE + "/scripts/start.sh"
BASH = os.environ.get("FL_BASH", "bash")
SMOKE = os.environ.get("FL_SMOKE") == "1"
TS = (0.5, 1.5, 2.0, 4.0)
DEFN = {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}}
FIND_ENGINE = "for p in /proc/[0-9]*; do if grep -qa '^/opt/mongot/bin/jdk/bin/java' $p/cmdline; then echo ${p#/proc/}; fi; done"
FIND_MONGOD = "for p in /proc/[0-9]*; do if grep -qa '^/usr/bin/mongod' $p/cmdline || grep -qa '^mongod' $p/cmdline; then echo ${p#/proc/}; fi; done"


def now():
    return time.perf_counter()


def q(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 3)


def summ(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "min": round(min(xs), 3), "p50": q(xs, .5), "p90": q(xs, .9), "max": round(max(xs), 3)}


def run(args, env=None, timeout=None):
    e = {**os.environ, **(env or {})}
    return subprocess.run(args, capture_output=True, text=True, env=e, timeout=timeout)


def sh(cmd, c=None):
    return run(["docker", "exec", c or C, "sh", "-c", cmd])


def rm(*names):
    for n in names:
        run(["docker", "rm", "-f", n])


def pids(find=FIND_ENGINE):
    return sh(find).stdout.split()


def uri(port=PORT):
    return f"mongodb://127.0.0.1:{port}/?directConnection=true"


def cli(port=PORT, app=None):
    return MongoClient(uri(port), appName=app, socketTimeoutMS=60000, serverSelectionTimeoutMS=30000)


def tail(s, n=400):
    return (s or "").strip()[-n:]


def start_std():
    p = run([BASH, START_STD], env={"WP_CONTAINER": C, "WP_PORT": str(PORT)})
    if p.returncode:
        raise RuntimeError("stock start failed: " + tail(p.stderr + p.stdout))
    return tail(p.stdout, 200)


def start_der(args="", netns=None):
    env = {"WP_CONTAINER": C, "WP_PORT": str(PORT), "FL_IMAGE": DER, "FL_RUN_ARGS": args}
    if netns:
        env["FL_NETNS"] = netns
    p = run([BASH, START_DER], env=env)
    if p.returncode:
        raise RuntimeError("derived start failed: " + tail(p.stderr + p.stdout))
    return tail(p.stdout, 200)


def build_der():
    p = run(["docker", "build", "-q", "-t", DER, HERE])
    if p.returncode:
        raise RuntimeError("build failed: " + tail(p.stderr))
    return tail(p.stdout, 100)


def env_info():
    cpu = ""
    try:
        cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except Exception:  # noqa: BLE001
        pass
    dig = run(["docker", "image", "inspect", IMAGE, "--format", "{{json .RepoDigests}}"]).stdout.strip()
    return {"image": IMAGE, "digest": dig, "cpu_model": cpu, "nproc": os.cpu_count(), "runner_image": os.environ.get("ImageVersion", ""),
            "cpus_limit": os.environ.get("WP_CPUS", "3"), "mem_limit": os.environ.get("WP_MEM", "10g")}


def make_index(coll, name, defn=DEFN, wait=120):
    coll.create_search_index(SearchIndexModel(definition=defn, name=name))
    end = now() + wait
    while now() < end:
        try:
            if any(ix.get("queryable") for ix in coll.list_search_indexes(name)):
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.05)
    raise TimeoutError("index not queryable: " + name)


def text(name, key):
    return {"index": name, "text": {"query": key, "path": "title"}}


def found(coll, name, key):
    return bool(list(coll.aggregate([{"$search": text(name, key)}, {"$limit": 1}])))


def wait_visible(coll, name, key, timeout=60):
    """Perf-counter time at which the key was first found, or None."""
    end = now() + timeout
    while now() < end:
        if found(coll, name, key):
            return now()
        time.sleep(0.01)
    return None


def newkey():
    return "kk" + uuid.uuid4().hex[:12]


def blocks(rng, conds, n):
    order = []
    for _ in range(n):
        b = list(conds)
        rng.shuffle(b)
        order += b
    return order


def save(out, r):
    data = json.dumps(r, indent=1)
    if out.endswith(".gz"):
        with gzip.open(out, "wt") as f:
            f.write(data)
    else:
        open(out, "w").write(data)


def setup_coll(client, db, name="i"):
    coll = client[db]["c"]
    coll.insert_one({"title": "seed"})
    make_index(coll, name)
    return coll


# ---- failcmd -------------------------------------------------------------------------------------------------------


def fp_on(a, cmds, app, t, mode):
    a.command({"configureFailPoint": "failCommand", "mode": mode,
               "data": {"failCommands": cmds, "appName": app, "blockConnection": True, "blockTimeMS": int(t * 1000)}})


def fp_off(a):
    a.command({"configureFailPoint": "failCommand", "mode": "off"})


def err(e):
    return f"{type(e).__name__}: {str(e)[:300]}"


def param(a):
    try:
        return a.command({"getParameter": 1, "enableTestCommands": 1}).get("enableTestCommands")
    except Exception as e:  # noqa: BLE001
        return err(e)


def failcmd_default(r):
    """The stock container: is the fail point there, can it be turned on."""
    d = r["stock"] = {}
    c = cli()
    a = c.admin
    d["enableTestCommands"] = param(a)
    for k, cmd in (("configureFailPoint", {"configureFailPoint": "failCommand", "mode": "off"}),
                   ("setParameter_at_runtime", {"setParameter": 1, "enableTestCommands": 1})):
        try:
            d[k] = {"ok": a.command(cmd).get("ok")}
        except Exception as e:  # noqa: BLE001
            d[k] = {"error": err(e)}
    d["processes"] = [x for x in sh("for p in /proc/[0-9]*; do tr '\\0' ' ' < $p/cmdline | cut -c1-260; echo; done").stdout.splitlines() if x.strip() and "proc/" not in x and not x.startswith("sh -c")]
    d["uid"] = sh("id").stdout.strip()
    d["tools_in_container"] = sh("for t in tc ip iptables nc socat curl python3 perl bash; do command -v $t >/dev/null 2>&1 && echo $t; done").stdout.split()
    # restart mongod in place: signal it and watch what the runner (pid 1) does
    mp = pids(FIND_MONGOD)
    d["mongod_pids"] = mp
    t0 = now()
    if mp:
        sh("kill -TERM " + mp[0])
    status = {}
    for _ in range(80):
        time.sleep(2)
        st = run(["docker", "inspect", C, "--format", "{{.State.Status}} exit={{.State.ExitCode}}"]).stdout.strip()
        status = {"state": st, "after_s": round(now() - t0, 1)}
        if not st.startswith("running"):
            break
        if now() - t0 > 150:
            break
    d["after_killing_mongod"] = status
    d["container_logs_tail"] = [l[:200] for l in (run(["docker", "logs", "--tail", "4", C]).stdout + run(["docker", "logs", "--tail", "4", C]).stderr).splitlines()][-4:]
    st = run(["docker", "start", C])
    d["docker_start_after_exit"] = tail(st.stdout + st.stderr, 120)
    time.sleep(15)
    try:
        c2 = cli()
        d["enableTestCommands_after_docker_start"] = param(c2.admin)
        c2.close()
    except Exception as e:  # noqa: BLE001
        d["enableTestCommands_after_docker_start"] = err(e)
    c.close()
    rm(C)


def discover(c, r):
    """appName of the index process's connections, and which command reads the change stream."""
    d = r["discovery"] = {}
    a = c.admin
    app = {}
    for _ in range(20):
        for op in a.aggregate([{"$currentOp": {"allUsers": True, "idleConnections": True}}]):
            n = op.get("appName")
            if n and "mongot" in n:
                app[n] = app.get(n, 0) + 1
        time.sleep(0.1)
    d["mongot_appNames_in_currentOp"] = app
    coll = c["fl_disc"]["c"]
    coll.insert_one({"title": "seed"})
    make_index(coll, "i")
    t0 = time.time()
    a.command({"profile": 0, "slowms": 0})
    coll.insert_one({"title": newkey()})
    time.sleep(3)
    a.command({"profile": 0, "slowms": 100})
    lines = run(["docker", "logs", "--since", str(int(t0)), C])
    seen = {}
    for ln in (lines.stdout + lines.stderr).splitlines():
        if '"appName":"mongot' not in ln or "{" not in ln:
            continue
        try:
            at = json.loads(ln)["attr"]
        except Exception:  # noqa: BLE001
            continue
        cmd = at.get("command") or {}
        name = next(iter(cmd), "?")
        k = f"{at.get('appName')} | {name} | {at.get('ns')}"
        o = (at.get("originatingCommand") or {})
        if o:
            k += " | originating " + next(iter(o), "?") + " " + ",".join(next(iter(s)) for s in (o.get("pipeline") or []))
        seen[k] = seen.get(k, 0) + 1
    d["mongot_commands_in_log"] = seen
    c.drop_database("fl_disc")


def wait_block(a, ns, timeout=8):
    """Perf-counter time at which the index process's change-stream cursor on ns has been idle for 50 ms, or None.
    Normally its getMore is always in flight (awaitData); idle for that long means the read is held by the fail point."""
    end = now() + timeout
    since = None
    while now() < end:
        ops = list(a.aggregate([{"$currentOp": {"allUsers": True, "idleCursors": True}},
                                {"$match": {"ns": ns, "$or": [{"type": "idleCursor"}, {"op": "getmore"}]}}]))
        if ops and all(o.get("type") == "idleCursor" for o in ops):
            since = since or now()
            if now() - since >= 0.05:
                return since
        else:
            since = None
        time.sleep(0.01)
    return None


def failcmd_feed(c, r, rng, n):
    """Block the change-stream read of the index process. once: armed, then the write is issued when the read is seen
    held; once_ungated: armed and written at once (an in-flight read may already be past the fail point); always: random phase."""
    a = c.admin
    app = "mongot steady state sync"
    coll = setup_coll(c, "fl_fc_feed")
    rows, errors = [], []
    conds = [("none", 0.0)] + [(m, t) for m in ("once", "once_ungated", "always") for t in TS]
    for mode, t in blocks(rng, conds, n):
        row = {"mode": mode, "T": t}
        try:
            if mode in ("once", "once_ungated"):
                fp_on(a, ["getMore"], app, t, {"times": 1})
            if mode == "once":
                a0 = now()
                b = wait_block(a, "fl_fc_feed.c")
                row["block_seen"] = b is not None
                row["block_seen_after_s"] = None if b is None else round(b - a0, 3)
            elif mode == "always":
                fp_on(a, ["getMore"], app, t, "alwaysOn")
                time.sleep(t + 1.3)
            key = newkey()
            s0 = now()
            coll.insert_one({"title": key})
            s1 = now()
            v = wait_visible(coll, "i", key, 60)
            row["insert_s"] = round(s1 - s0, 3)
            if v is None:
                row["lag"] = None
                errors.append({"mode": mode, "T": t, "error": "not visible in 60 s"})
            else:
                row["lag"] = round(v - s1, 3)
                row["lag_minus_T"] = round(v - s1 - t, 3)
        except Exception as e:  # noqa: BLE001
            row["error"] = err(e)
            errors.append({"mode": mode, "T": t, "error": err(e)})
        finally:
            try:
                fp_off(a)
            except Exception:  # noqa: BLE001
                pass
        rows.append(row)
        time.sleep(1.5)
    r["feed_rows"] = rows
    r["feed_errors"] = errors
    r["feed_summary"] = {f"{m} T={t}": {"lag": summ([x.get("lag") for x in rows if x["mode"] == m and x["T"] == t]),
                                        "lag_minus_T": summ([x.get("lag_minus_T") for x in rows if x["mode"] == m and x["T"] == t]),
                                        "block_seen": sum(bool(x.get("block_seen")) for x in rows if x["mode"] == m and x["T"] == t),
                                        "under_T": sum((x.get("lag_minus_T") or 0) < 0 for x in rows if x["mode"] == m and x["T"] == t)}
                         for m, t in conds}
    c.drop_database("fl_fc_feed")


def failcmd_app(c, r, rng, n):
    """Block the application's own aggregate ($search) for T."""
    a = c.admin
    app = "fl-app-q"
    cq = cli(app=app)
    coll = setup_coll(c, "fl_fc_app")
    coll_q = cq["fl_fc_app"]["c"]
    seedkey = newkey()
    coll.insert_one({"title": seedkey})
    wait_visible(coll, "i", seedkey)
    rows, errors = [], []
    conds = [("query", 0.0)] + [(m, t) for m in ("query", "write") for t in TS]
    for mode, t in blocks(rng, conds, n):
        row = {"mode": mode, "T": t}
        try:
            if t:
                fp_on(a, ["aggregate"], app, t, {"times": 1})
            if mode == "query":
                s = now()
                found(coll_q, "i", seedkey)
                row["latency"] = round(now() - s, 3)
                row["latency_minus_T"] = round(now() - s - t, 3)
            else:
                key = newkey()
                coll.insert_one({"title": key})
                s = now()
                v = wait_visible(coll_q, "i", key, 60)
                row["lag"] = None if v is None else round(v - s, 3)
                row["lag_minus_T"] = None if v is None else round(v - s - t, 3)
        except Exception as e:  # noqa: BLE001
            row["error"] = err(e)
            errors.append({"mode": mode, "T": t, "error": err(e)})
        finally:
            try:
                fp_off(a)
            except Exception:  # noqa: BLE001
                pass
        rows.append(row)
        time.sleep(0.5)
    r["app_rows"] = rows
    r["app_errors"] = errors
    r["app_summary"] = {f"{m} T={t}": {"latency": summ([x.get("latency") for x in rows if x["mode"] == m and x["T"] == t]),
                                       "lag": summ([x.get("lag") for x in rows if x["mode"] == m and x["T"] == t]),
                                       "minus_T": summ([x.get("latency_minus_T", x.get("lag_minus_T")) for x in rows if x["mode"] == m and x["T"] == t])}
                        for m, t in conds}
    cq.close()
    c.drop_database("fl_fc_app")


def failcmd(n, out, seed):
    rng = random.Random(seed)
    r = {"group": "failcmd", "n": n, "seed": seed, "env": env_info(), "Ts": TS,
         "note": "waitForFailPoint timesEntered returned within milliseconds of arming for a fail point filtered to another application name, so it is not used to tell that the index process's read was held"}
    save(out, r)
    try:
        failcmd_default(r)
    except Exception as e:  # noqa: BLE001
        r["stock_error"] = err(e)
    save(out, r)
    try:
        r["build"] = build_der()
        t0 = now()
        r["start_derived"] = start_der("-e FL_TEST_COMMANDS=1")
        r["start_derived_s"] = round(now() - t0, 1)
        c = cli(app="fl-app")
        a = c.admin
        r["derived"] = {"enableTestCommands": param(a)}
        try:
            fp_off(a)
            r["derived"]["configureFailPoint_off"] = "ok"
        except Exception as e:  # noqa: BLE001
            r["derived"]["configureFailPoint_off"] = err(e)
        r["derived"]["mongod_cmdline"] = [x for x in sh("for p in /proc/[0-9]*; do tr '\\0' ' ' < $p/cmdline | cut -c1-420; echo; done").stdout.splitlines() if "mongod" in x and "--replSet" in x][:1]
        save(out, r)
        discover(c, r)
        save(out, r)
        failcmd_feed(c, r, rng, n)
        save(out, r)
        failcmd_app(c, r, rng, n)
        c.close()
    except Exception as e:  # noqa: BLE001
        r["error"] = err(e)
    save(out, r)
    print(json.dumps({k: v for k, v in r.items() if not k.endswith("rows")}, indent=1)[:6000])
    if "error" in r:
        sys.exit(1)


# ---- toxi ----------------------------------------------------------------------------------------------------------


def tp(method, path, body=None):
    req = urllib.request.Request(f"http://127.0.0.1:{TP_API}{path}", method=method,
                                 data=None if body is None else json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as f:
        d = f.read()
        return json.loads(d) if d else None


def toxic(proxy, name, stream, ms):
    body = {"name": name, "type": "latency", "stream": stream, "toxicity": 1.0, "attributes": {"latency": int(ms), "jitter": 0}}
    try:
        tp("POST", f"/proxies/{proxy}/toxics/{name}", body)  # update
    except Exception:  # noqa: BLE001
        tp("POST", f"/proxies/{proxy}/toxics", body)


def lag_pair(cw, cd, name, coll_w, coll_d):
    """Insert through cw, poll through cd in a thread and through cw here."""
    key = newkey()
    box = {}

    def poll():
        box["dv"] = wait_visible(coll_d, name, key, 60)

    th = threading.Thread(target=poll)
    th.start()
    s0 = now()
    coll_w.insert_one({"title": key})
    s1 = now()
    pv = wait_visible(coll_w, name, key, 60)
    th.join()
    dv = box.get("dv")
    return {"insert_s": round(s1 - s0, 3), "other_from_start": None if dv is None else round(dv - s0, 3),
            "other_from_return": None if dv is None else round(dv - s1, 3), "own_from_return": None if pv is None else round(pv - s1, 3)}


def toxi_client(r, rng, n):
    d = r["client_path"] = {"proxy": f"client 0.0.0.0:27118 -> 127.0.0.1:27017, host port {TP_CLIENT}"}
    cd = cli(PORT)
    cp = cli(TP_CLIENT)
    coll_d = setup_coll(cd, "fl_tc")
    coll_p = cp["fl_tc"]["c"]
    rows = []
    conds = [0.0, 0.25, 1.0]
    for L in blocks(rng, conds, n):
        row = {"latency_each_way_s": L}
        try:
            toxic("client", "dn", "downstream", L * 1000)
            toxic("client", "up", "upstream", L * 1000)
            time.sleep(0.3)
            s = now()
            cp.admin.command("ping")
            row["ping_s"] = round(now() - s, 3)
            row.update(lag_pair(cp, cd, "i", coll_p, coll_d))
        except Exception as e:  # noqa: BLE001
            row["error"] = err(e)
        rows.append(row)
        time.sleep(1.0)
    d["rows"] = rows
    d["summary"] = {str(L): {k: summ([x.get(k) for x in rows if x["latency_each_way_s"] == L]) for k in ("ping_s", "insert_s", "other_from_start", "other_from_return", "own_from_return")} for L in conds}
    d["reading"] = "other_* = poll through the direct port (not delayed); own_from_return = poll through the proxy. Latency on the client path should move ping_s, insert_s and own_from_return, not other_from_start."
    for t in ("dn", "up"):
        toxic("client", t, "downstream" if t == "dn" else "upstream", 0)
    cd.drop_database("fl_tc")
    cd.close()
    cp.close()


def toxi_feed(r, rng, n):
    d = r["feed_path"] = {"proxy": "feed 127.0.0.1:27117 -> 127.0.0.1:27017; the index process was started with that address"}
    cd = cli(PORT)
    coll = setup_coll(cd, "fl_tf")
    d["wrap_env"] = sh("tr '\\0' '\\n' < /proc/$(for p in /proc/[0-9]*; do if grep -qa '^/opt/mongot/bin/jdk/bin/java' $p/cmdline; then echo ${p#/proc/}; fi; done | head -1)/environ | grep FL_ || true").stdout.strip()
    d["mongot_mongod_arg"] = [x for x in sh("tr '\\0' '\\n' < /proc/$(for p in /proc/[0-9]*; do if grep -qa '^/opt/mongot/bin/jdk/bin/java' $p/cmdline; then echo ${p#/proc/}; fi; done | head -1)/cmdline | grep -A1 mongodHostAndPort || true").stdout.split()]
    d["replset_member_host"] = cd.admin.command("hello").get("me")
    rows = []
    conds = [("down", 0.0)] + [("down", t) for t in TS] + [("both", t) for t in (0.5, 2.0)]
    for kind, L in blocks(rng, conds, n):
        row = {"stream": kind, "latency_s": L}
        try:
            toxic("feed", "dn", "downstream", L * 1000)
            toxic("feed", "up", "upstream", (L * 1000) if kind == "both" else 0)
            time.sleep(0.3)
            key = newkey()
            s1 = None
            coll.insert_one({"title": key})
            s1 = now()
            v = wait_visible(coll, "i", key, 90)
            row["lag"] = None if v is None else round(v - s1, 3)
            row["lag_minus_L"] = None if v is None else round(v - s1 - L, 3)
        except Exception as e:  # noqa: BLE001
            row["error"] = err(e)
        rows.append(row)
        time.sleep(1.5 + 2 * L)
    d["rows"] = rows
    d["summary"] = {f"{k} L={L}": {"lag": summ([x.get("lag") for x in rows if x["stream"] == k and x["latency_s"] == L]),
                                   "lag_minus_L": summ([x.get("lag_minus_L") for x in rows if x["stream"] == k and x["latency_s"] == L])} for k, L in conds}
    toxic("feed", "dn", "downstream", 0)
    toxic("feed", "up", "upstream", 0)
    # does anything bypass the proxy: disable it and see whether a write still becomes searchable
    d["disabled_proxy_check"] = {}
    try:
        tp("POST", "/proxies/feed", {"enabled": False})
        time.sleep(1)
        key = newkey()
        coll.insert_one({"title": key})
        s1 = now()
        v = wait_visible(coll, "i", key, 12 if not SMOKE else 6)
        d["disabled_proxy_check"]["searchable_within_bound_while_disabled"] = v is not None
        d["disabled_proxy_check"]["bound_s"] = 12 if not SMOKE else 6
        tp("POST", "/proxies/feed", {"enabled": True})
        v2 = wait_visible(coll, "i", key, 90)
        d["disabled_proxy_check"]["searchable_after_enable_s"] = None if v2 is None else round(v2 - s1, 2)
    except Exception as e:  # noqa: BLE001
        d["disabled_proxy_check"]["error"] = err(e)
    cd.drop_database("fl_tf")
    cd.close()


def toxi_netem(r, rng, n):
    d = r["netem"] = {}
    d["engine_alone"] = {"tools": sh("for t in tc ip; do command -v $t; done").stdout.split(), "uid": sh("id").stdout.strip()}
    rm(TC)
    p = run(["docker", "run", "-d", "--name", TC, "--net", f"container:{C}", "--cap-add", "NET_ADMIN", "alpine:3.20", "sh", "-c",
             "apk add -q iproute2 && touch /tmp/ready && sleep 36000"])
    d["sidecar_start"] = tail(p.stdout + p.stderr, 200)
    ok = False
    for _ in range(60):
        if run(["docker", "exec", TC, "test", "-f", "/tmp/ready"]).returncode == 0:
            ok = True
            break
        time.sleep(1)
    d["sidecar_ready"] = ok
    if not ok:
        d["error"] = "sidecar not ready: " + tail(run(["docker", "logs", TC]).stdout + run(["docker", "logs", TC]).stderr)
        rm(TC)
        return
    d["kernel"] = run(["docker", "exec", TC, "uname", "-r"]).stdout.strip()
    t = run(["docker", "exec", TC, "tc", "qdisc", "replace", "dev", "lo", "root", "netem", "delay", "1ms"])
    d["netem_probe"] = tail(t.stdout + t.stderr, 200) or "ok"
    run(["docker", "exec", TC, "tc", "qdisc", "del", "dev", "lo", "root"])
    if t.returncode:
        d["error"] = "netem unavailable"
        rm(TC)
        return
    cd = cli(PORT)
    coll = setup_coll(cd, "fl_tn")
    rows = []
    conds = [("none", 0.0), ("lo", 0.25), ("lo", 0.75), ("eth0", 0.25)]
    for dev, L in blocks(rng, conds, n):
        row = {"dev": dev, "delay_each_way_s": L}
        try:
            for dv in ("lo", "eth0"):
                run(["docker", "exec", TC, "tc", "qdisc", "del", "dev", dv, "root"])
            if dev != "none":
                x = run(["docker", "exec", TC, "tc", "qdisc", "replace", "dev", dev, "root", "netem", "delay", f"{int(L * 1000)}ms"])
                if x.returncode:
                    row["error"] = tail(x.stderr, 200)
            time.sleep(1.0 + 2 * L)
            s = now()
            cd.admin.command("ping")
            row["ping_s"] = round(now() - s, 3)
            key = newkey()
            s0 = now()
            coll.insert_one({"title": key})
            s1 = now()
            v = wait_visible(coll, "i", key, 120)
            row["insert_s"] = round(s1 - s0, 3)
            row["lag"] = None if v is None else round(v - s1, 3)
        except Exception as e:  # noqa: BLE001
            row["error"] = err(e)
        rows.append(row)
    for dv in ("lo", "eth0"):
        run(["docker", "exec", TC, "tc", "qdisc", "del", "dev", dv, "root"])
    d["rows"] = rows
    d["summary"] = {f"{k} {L}": {m: summ([x.get(m) for x in rows if x["dev"] == k and x["delay_each_way_s"] == L]) for m in ("ping_s", "insert_s", "lag")} for k, L in conds}
    cd.drop_database("fl_tn")
    cd.close()
    rm(TC)


def toxi(n, out, seed):
    rng = random.Random(seed)
    r = {"group": "toxi", "n": n, "seed": seed, "env": env_info(), "toxiproxy_image": TP_IMAGE, "toxiproxy_release": "v2.13.0 (latest release on the releases page on 2026-10-23)"}
    save(out, r)
    rm(C, TP, TC)
    try:
        r["build"] = build_der()
        p = run(["docker", "run", "-d", "--name", TP, "-p", f"{PORT}:27017", "-p", f"{TP_API}:8474", "-p", f"{TP_CLIENT}:27118", TP_IMAGE])
        r["toxiproxy_start"] = tail(p.stdout + p.stderr, 200)
        if p.returncode:
            raise RuntimeError("toxiproxy did not start")
        for _ in range(30):
            try:
                tp("GET", "/version")
                break
            except Exception:  # noqa: BLE001
                time.sleep(1)
        r["toxiproxy_version"] = tp("GET", "/version")
        tp("POST", "/proxies", {"name": "client", "listen": "0.0.0.0:27118", "upstream": "127.0.0.1:27017", "enabled": True})
        tp("POST", "/proxies", {"name": "feed", "listen": "127.0.0.1:27117", "upstream": "127.0.0.1:27017", "enabled": True})
        t0 = now()
        r["start_engine"] = start_der("-e FL_MONGOT_ADDR=127.0.0.1:27117", netns=TP)
        r["start_engine_s"] = round(now() - t0, 1)
        save(out, r)
        toxi_client(r, rng, n)
        save(out, r)
        toxi_feed(r, rng, n)
        save(out, r)
    except Exception as e:  # noqa: BLE001
        r["error"] = err(e)
    rm(C, TP)
    try:
        start_std()
        toxi_netem(r, rng, n)
    except Exception as e:  # noqa: BLE001
        r["netem_error"] = err(e)
    rm(TC)
    save(out, r)
    print(json.dumps({k: (v if not isinstance(v, dict) else {a: b for a, b in v.items() if a != "rows"}) for k, v in r.items()}, indent=1)[:6000])
    if "error" in r:
        sys.exit(1)


# ---- pause ---------------------------------------------------------------------------------------------------------


class Ctl:
    """One long-lived shell inside the container, so a signal costs a pipe write instead of a docker exec."""

    def __init__(self):
        self.p = subprocess.Popen(["docker", "exec", "-i", C, "sh"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)

    def cmd(self, s):
        self.p.stdin.write(s + "; echo OK\n")
        self.p.stdin.flush()
        line = self.p.stdout.readline()
        if line.strip() != "OK":
            raise RuntimeError("control shell: " + line)

    def close(self):
        try:
            self.p.stdin.close()
            self.p.wait(timeout=10)
        except Exception:  # noqa: BLE001
            self.p.kill()


def pause(n, out, seed):
    rng = random.Random(seed)
    r = {"group": "pause", "n": n, "seed": seed, "env": env_info(), "Ts": TS}
    c = cli()
    coll = setup_coll(c, "fl_pz")
    ps = pids()
    r["engine_pids"] = ps
    ctl = Ctl()
    rows = []
    try:
        for t in blocks(rng, list(TS), n):
            row = {"T": t}
            try:
                s_send = now()
                ctl.cmd("kill -STOP " + " ".join(ps))
                s_ack = now()
                key = newkey()
                coll.insert_one({"title": key})
                s_ins = now()
                while now() < s_ack + t:
                    time.sleep(0.0005)
                c_send = now()
                ctl.cmd("kill -CONT " + " ".join(ps))
                c_ack = now()
                v = wait_visible(coll, "i", key, 60)
                row["T_stopped_lower"] = round(c_send - s_ack, 4)
                row["T_stopped_upper"] = round(c_ack - s_send, 4)
                row["after_resume"] = None if v is None else round(v - c_ack, 3)
                row["lag_minus_T"] = None if v is None else round(v - s_ins - t, 3)
                row["lag_from_stop_minus_T"] = None if v is None else round(v - s_ack - t, 3)
            except Exception as e:  # noqa: BLE001
                row["error"] = err(e)
                try:
                    ctl.cmd("kill -CONT " + " ".join(ps))
                except Exception:  # noqa: BLE001
                    sh("kill -CONT " + " ".join(ps))
            rows.append(row)
            time.sleep(1.5)
    finally:
        sh("kill -CONT " + " ".join(ps))
        ctl.close()
    r["rows"] = rows
    r["summary"] = {str(t): {k: summ([x.get(k) for x in rows if x["T"] == t]) for k in ("T_stopped_lower", "T_stopped_upper", "after_resume", "lag_minus_T", "lag_from_stop_minus_T")} for t in TS}
    r["overall"] = {k: summ([x.get(k) for x in rows]) for k in ("after_resume", "lag_minus_T", "lag_from_stop_minus_T")}
    r["errors"] = [x for x in rows if "error" in x]
    save(out, r)
    print(json.dumps({k: v for k, v in r.items() if k != "rows"}, indent=1)[:6000])
    c.drop_database("fl_pz")


# ---- ghost ---------------------------------------------------------------------------------------------------------


def wait_ready_index(coll, name, bound):
    end = now() + bound
    while now() < end:
        try:
            for ix in coll.list_search_indexes(name):
                if ix.get("queryable"):
                    return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.05)
    return False


def logs_between(t0, t1, pat):
    p = run(["docker", "logs", "--since", f"{t0 - 1:.3f}", "--until", f"{t1 + 1:.3f}", C])
    out = []
    for ln in (p.stdout + p.stderr).splitlines():
        if re.search(pat, ln):
            out.append(ln[:260])
    return out[:14]


def ghost_rep(c, tag, delay, paused, bound, first):
    """One drop-and-recreate. A new database and index name each time."""
    name = f"ix{tag}"
    row = {"tag": tag, "delay_ms": int(delay * 1000), "mongot_paused": paused, "first_after_fresh_engine": first}
    db = c[f"fl_g_{tag}"]
    coll = db["c"]
    coll.insert_many([{"title": "x common"} for _ in range(50)])
    ps = pids()
    st = sh("echo $(cut -d' ' -f1 /proc/uptime) $(cut -d' ' -f22 /proc/1/stat)").stdout.split()
    row["engine_uptime_s"] = round(float(st[0]) - int(st[1]) / 100, 1) if len(st) == 2 else None
    t_start = time.time()
    make_index(coll, name)
    tq = now()
    t_drop = None
    try:
        if paused:
            sh("kill -STOP " + " ".join(ps))
        t_drop = now()
        row["age_at_drop_s"] = round(t_drop - tq, 3)
        coll.drop()
        time.sleep(delay)
        db.create_collection("c")
        t_recreate = now()
    finally:
        if paused:
            sh("kill -CONT " + " ".join(ps))
    t_resume = now()
    coll = db["c"]
    # watch the old index entry; when it is gone and the new collection has no index, create one under a new name
    name2 = None
    trace = []
    ok = False
    via = None
    row["queryable_within_bound"] = False
    while now() < t_resume + bound:
        try:
            st = list(coll.list_search_indexes(name)) if name2 is None else []
        except Exception as e:  # noqa: BLE001
            row["list_error"] = err(e)
            st = None
        if st is not None:
            state = (st[0].get("status"), bool(st[0].get("queryable"))) if st else None
            if not trace or trace[-1][1] != state:
                trace.append([round(now() - t_resume, 2), state])
            if st and st[0].get("queryable"):
                ok, via = True, "old_name"
                break
            if not st and name2 is None:
                name2 = name + "b"
                coll.create_search_index(SearchIndexModel(definition=DEFN, name=name2))
                row["new_index_created_after_s"] = round(now() - t_resume, 2)
        if name2 is not None:
            try:
                if any(ix.get("queryable") for ix in coll.list_search_indexes(name2)):
                    ok, via = True, "new_name"
                    break
            except Exception:  # noqa: BLE001
                pass
        time.sleep(0.05)
    row["old_index_listed_at_resume"] = bool(trace and trace[0][1] is not None)
    row["old_index_trace"] = trace[:8]
    row["queryable_within_bound"] = ok
    row["via"] = via
    row["original_ok"] = ok and (via == "old_name" or not row["old_index_listed_at_resume"])
    row["time_to_queryable_s"] = round(now() - t_resume, 2) if ok else None
    row["bound_s"] = bound
    if ok:
        coll.insert_one({"title": "post common"})
        v = wait_visible(coll, name if via == "old_name" else name2, "post", 15)
        row["searchable_after_recreate"] = v is not None
    row["log"] = logs_between(t_start, time.time(), f"fl_g_{tag}|{name}")
    try:
        c.drop_database(db.name)
    except Exception:  # noqa: BLE001
        pass
    return row


def ghost(n, out, seed, fresh_n, bound):
    rng = random.Random(seed)
    r = {"group": "ghost", "n": n, "fresh_n": fresh_n, "bound_s": bound, "seed": seed, "env": env_info()}
    delays = (0.0, 0.05, 0.2, 1.0)
    warm = [(d, p) for d in delays for p in (False, True)]
    rows = []
    r["rows"] = rows
    c = cli()
    # fresh arm: a new engine for every repetition, the repetition is the first thing run after readiness
    fresh = blocks(rng, list(delays), fresh_n)
    r["fresh_order_ms"] = [int(d * 1000) for d in fresh]
    for i, d in enumerate(fresh):
        arm = f"fresh d={int(d * 1000)}"
        try:
            if i:
                rm(C)
                start_std()
            c.close()
            c = cli()
            row = ghost_rep(c, f"f{i}", d, False, bound, True)
        except Exception as e:  # noqa: BLE001
            row = {"error": err(e), "delay_ms": int(d * 1000), "first_after_fresh_engine": True}
        row["arm"] = arm
        rows.append(row)
        print(arm, row.get("original_ok"), row.get("via"), flush=True)
        save(out, {**r, "rows": rows})
    if fresh_n:
        rm(C)
        start_std()
        c.close()
        c = cli()
    # warm arm: one engine, randomised block order. A throwaway repetition first so that repetition 1 is not special.
    try:
        ghost_rep(c, "warmup", 0.0, False, 5, False)
    except Exception as e:  # noqa: BLE001
        r["warmup_error"] = err(e)
    for i, (d, p) in enumerate(blocks(rng, warm, n)):
        arm = f"warm d={int(d * 1000)} paused={p}"
        try:
            row = ghost_rep(c, f"w{i}", d, p, bound, False)
        except Exception as e:  # noqa: BLE001
            row = {"error": err(e), "delay_ms": int(d * 1000), "mongot_paused": p}
            sh("kill -CONT " + " ".join(pids()))
        row["arm"] = arm
        row["order"] = i
        rows.append(row)
        print(arm, row.get("original_ok"), row.get("via"), flush=True)
        save(out, {**r, "rows": rows})
    arms = sorted({x["arm"] for x in rows})
    r["summary"] = {a: {"n": sum(x["arm"] == a for x in rows), "queryable_incl_recreate": sum(bool(x.get("queryable_within_bound")) for x in rows if x["arm"] == a),
                        "original_ok": sum(bool(x.get("original_ok")) for x in rows if x["arm"] == a),
                        "errors": sum("error" in x for x in rows if x["arm"] == a),
                        "old_index_listed_at_resume": sum(bool(x.get("old_index_listed_at_resume")) for x in rows if x["arm"] == a),
                        "via_old_name": sum(x.get("via") == "old_name" for x in rows if x["arm"] == a),
                        "via_new_name": sum(x.get("via") == "new_name" for x in rows if x["arm"] == a),
                        "time_to_queryable_s": summ([x.get("time_to_queryable_s") for x in rows if x["arm"] == a])} for a in arms}
    # what separates the passes from the failures, across all repetitions
    ok = [x for x in rows if x.get("original_ok")]
    bad = [x for x in rows if x.get("original_ok") is False]
    r["passes_vs_failures"] = {"passes": len(ok), "failures": len(bad),
                               "age_at_drop_s_pass": summ([x.get("age_at_drop_s") for x in ok]), "age_at_drop_s_fail": summ([x.get("age_at_drop_s") for x in bad]),
                               "uptime_s_pass": summ([x.get("engine_uptime_s") for x in ok]), "uptime_s_fail": summ([x.get("engine_uptime_s") for x in bad])}
    save(out, r)
    print(json.dumps({k: v for k, v in r.items() if k != "rows"}, indent=1)[:6000])
    c.close()


if __name__ == "__main__":
    g, n, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    seed = int(sys.argv[4]) if len(sys.argv) > 4 else 20261023
    if g == "ghost":
        ghost(n, out, seed, int(sys.argv[5]) if len(sys.argv) > 5 else n, float(sys.argv[6]) if len(sys.argv) > 6 else 20.0)
    else:
        {"failcmd": failcmd, "toxi": toxi, "pause": pause}[g](n, out, seed)
