"""Cost of running a search-backed test suite in CI: image source, one shared engine with and without xdist, a
2-CPU emulation, and a synthetic suite model. Standard library only; the suite and model groups need the suite's
environment (pytest, xdist, pymongo, mongomock).

  cost.py mark FILE EVENT [k=v ...]            append a timestamped event
  cost.py image FILE --hit true|false          load/pull paths to engine ready (marks go to FILE)
  cost.py suite OUT --label suite|cpu2 ...     real suite against one shared container
  cost.py model OUT ...                        synthetic suite
  cost.py analyze DIR... --out DIR             markdown tables
"""
from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import platform
import random
import re
import shutil
import statistics as st
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUITE = HERE.parent / "suite"
START = HERE / "scripts" / "start.sh"
BASH = next((b for b in ("C:/Program Files/Git/bin/bash.exe",) if os.name == "nt" and os.path.exists(b)), "bash")
IMAGE = os.environ.get("IMAGE", "mongodb/mongodb-atlas-local:8.0")


def sh(cmd, **kw):
    return subprocess.run(cmd, text=True, capture_output=True, **kw)


def machine() -> dict:
    cpu = ""
    try:
        cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except Exception:  # noqa: BLE001
        cpu = platform.processor()
    mem = ""
    try:
        mem = next(l for l in open("/proc/meminfo") if l.startswith("MemTotal")).strip()
    except Exception:  # noqa: BLE001
        pass
    dig = sh(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}} {{json .RepoDigests}}"]).stdout.strip()
    return {"cpu": cpu, "nproc": os.cpu_count(), "mem": mem, "os": platform.platform(), "image": IMAGE, "image_id": dig,
            "runner_image": os.environ.get("ImageOS", "") + " " + os.environ.get("ImageVersion", ""),
            "docker": sh(["docker", "--version"]).stdout.strip(), "python": sys.version.split()[0]}


# ---- marks and image --------------------------------------------------------------------------------------------


def mark(path, event, **kv):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps({"event": event, "t": time.time(), **kv}) + "\n")


def timed(path, name, fn):
    mark(path, name + "_start")
    t = time.perf_counter()
    out = fn()
    d = time.perf_counter() - t
    mark(path, name + "_end", s=round(d, 3))
    return d, out


def engine_up(container, port, cpus, cpuset="", mem="10g") -> float:
    env = dict(os.environ, C=container, PORT=str(port), CPUS=str(cpus), CPUSET=cpuset, MEM=mem, IMAGE=IMAGE)
    t = time.perf_counter()
    r = sh([BASH, START.as_posix()], env=env)
    d = time.perf_counter() - t
    if r.returncode:
        raise RuntimeError("engine start failed: " + (r.stderr or r.stdout)[-400:])
    m = re.search(r"first_ok=([0-9.]+)", r.stdout)
    engine_up.first_ok = float(m.group(1)) if m else None
    return d


def engine_down(container):
    sh(["docker", "rm", "-f", container])


def cmd_image(a):
    f = a.file
    tar = Path(a.dir) / "image.tar"
    skip = os.environ.get("SKIP_ENGINE") == "1"
    mark(f, "info", key=a.key, hit=a.hit, image=IMAGE, **{"machine": machine()})

    def ready(label):
        t0 = time.perf_counter()
        if not skip:
            d = engine_up(a.container, a.port, 3)
            mark(f, label + "_engine_ready", s=round(d, 3))
            engine_down(a.container)
        return time.perf_counter() - t0

    def drop():
        sh(["docker", "image", "rm", "-f", IMAGE])

    if a.hit == "true" and tar.exists():
        mark(f, "tarball", bytes=tar.stat().st_size)
        drop()
        t0 = time.perf_counter()
        d, _ = timed(f, "load", lambda: sh(["docker", "load", "-i", str(tar)]))
        e = ready("load")
        mark(f, "path_load_total", s=round(time.perf_counter() - t0, 3))
        drop()
    t0 = time.perf_counter()
    d, r = timed(f, "pull", lambda: sh(["docker", "pull", IMAGE]))
    if r.returncode:
        raise RuntimeError("pull failed: " + r.stderr[-300:])
    mark(f, "image_local", inspect=sh(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}} {{.Size}} {{json .RepoDigests}}"]).stdout.strip())
    ready("pull")
    mark(f, "path_pull_total", s=round(time.perf_counter() - t0, 3))
    if a.hit != "true" or not tar.exists():
        tar.parent.mkdir(parents=True, exist_ok=True)
        d, r = timed(f, "save", lambda: sh(["docker", "save", "-o", str(tar), IMAGE]))
        if r.returncode:
            raise RuntimeError("save failed: " + r.stderr[-300:])
        mark(f, "tarball", bytes=tar.stat().st_size)


# ---- running pytest against one container -------------------------------------------------------------------------


def canary(pin=0) -> float:
    code = "import time\nt=time.perf_counter()\nx=0\nfor i in range(6000000): x+=i*i\nprint(time.perf_counter()-t)"
    return round(float(sh(pin_cmd(pin) + [sys.executable, "-c", code]).stdout.strip() or 0), 3)


def parse_junit(p):
    out = []
    try:
        root = ET.parse(p).getroot()
    except Exception:  # noqa: BLE001
        return out
    for tc in root.iter("testcase"):
        o = "passed"
        reason = ""
        for ch in tc:
            if ch.tag in ("failure", "error"):
                o = "failed"
                reason = (ch.get("message") or "")[:200]
            elif ch.tag == "skipped" and o == "passed":
                o = "xfailed" if "xfail" in (ch.get("type") or "") else "skipped"
                reason = (ch.get("message") or "")[:200]
        out.append({"id": f"{tc.get('classname')}::{tc.get('name')}", "s": float(tc.get("time") or 0), "o": o, "why": reason})
    return out


def pin_cmd(pin):
    if pin and shutil.which("taskset"):
        return ["taskset", "-c", ",".join(str(i) for i in range(pin))]
    return []


def run_pytest(a, w, prefix, extra, env_extra, cwd, junit):
    cmd = pin_cmd(a.pin) + [sys.executable, "-m", "pytest", "-q", "-p", "no:randomly", "-p", "no:cacheprovider",
                            f"--junitxml={junit}", "-o", "junit_family=xunit2"]
    cmd += ["-p", "no:xdist"] if w == 0 else ["-n", str(w)]
    cmd += extra
    env = dict(os.environ, **env_extra)
    t = time.perf_counter()
    r = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    return time.perf_counter() - t, r


def engine_tail(container, n=40):
    return sh(["docker", "logs", "--tail", str(n), container]).stdout[-3000:]


def schedule(arms, reps, seed):
    rng = random.Random(seed)
    jobs = [(arm, r) for r in range(reps) for arm in arms]
    rng.shuffle(jobs)
    return jobs


def finish(out, meta, rows, tests):
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    json.dump({"meta": meta, "runs": rows}, open(out, "w"), indent=1)
    if tests:
        with gzip.open(str(out) + ".tests.gz", "wt") as f:
            json.dump(tests, f)


def cmd_suite(a):
    workers = [int(x) for x in a.workers.split(",")]
    cpus = [int(x) for x in a.cpus.split(",")]
    pins = [int(x) for x in a.pins.split(",")]
    # an arm with pin P > 0 runs the whole job on P CPUs (engine limit P, cpuset, taskset); pin 0 uses the engine limits in --cpus
    arms = [(p, c if not p else p, w) for p in pins for c in (cpus if not p else [0]) for w in workers]
    jobs = schedule(arms, a.reps, a.seed)
    meta = {"group": a.label, "seed": a.seed, "reps": a.reps, "arms": arms, "order": [[p, c, w, r] for (p, c, w), r in jobs],
            "pins": pins, "taskset": bool(any(pins) and shutil.which("taskset")),
            "emulation": any(pins), "stress_repeats": a.stress_repeats, "backends": a.backends, "k": a.k,
            "machine": machine(), "started": time.time()}
    print("order:", meta["order"], flush=True)
    rows, tests = [], {}
    tmp = Path(a.out).resolve().parent / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    tails = []
    for idx, ((p, c, w), rep) in enumerate(jobs):
        rid = f"{idx:02d}"
        prefix = f"r{rid}"
        cpuset = ",".join(str(i) for i in range(p)) if p else ""
        a.pin = p
        row = {"id": rid, "pin": p, "cpus": c, "workers": w, "rep": rep, "order": idx, "error": ""}
        try:
            row["canary_s"] = canary(p)
            row["start_s"] = round(engine_up(a.container, a.port, c, cpuset, a.mem), 2)
            row["first_ok_s"] = engine_up.first_ok
            extra = ["tests/test_parity.py", "tests/test_stress.py", f"--stress-repeats={a.stress_repeats}", f"--backends={a.backends}"]
            if a.k:
                extra += ["-k", a.k]
            junit = str(tmp / f"{rid}.xml")
            d, r = run_pytest(a, w, prefix, extra, {
                "KIT_TX_MONGO_URI": f"mongodb://127.0.0.1:{a.port}/?directConnection=true", "KIT_TX_DB_PREFIX": prefix}, SUITE, junit)
            ts = parse_junit(junit)
            row.update(pytest_s=round(d, 2), rc=r.returncode, n=len(ts), passed=sum(t["o"] == "passed" for t in ts),
                       failed=sum(t["o"] == "failed" for t in ts), skipped=sum(t["o"] == "skipped" for t in ts),
                       xfailed=sum(t["o"] == "xfailed" for t in ts),
                       sum_test_s=round(sum(t["s"] for t in ts), 2), summary=(r.stdout.strip().splitlines() or [""])[-1])
            tests[rid] = [[t["id"], t["s"], t["o"], t["why"]] for t in ts]
            if r.returncode and not ts:
                row["error"] = (r.stdout + r.stderr)[-600:]
            tails.append(f"== run {rid} cpus={c} workers={w}\n" + engine_tail(a.container))
        except Exception as e:  # noqa: BLE001
            row["error"] = str(e)[:600]
        finally:
            engine_down(a.container)
        rows.append(row)
        print(json.dumps({k: v for k, v in row.items() if k != "summary"}), flush=True)
        finish(a.out, meta, rows, tests)
    meta["finished"] = time.time()
    finish(a.out, meta, rows, tests)
    Path(a.out).with_name("engine-tail.txt").write_text("\n".join(tails)[-60000:])
    shutil.rmtree(tmp, ignore_errors=True)
    if any(r["error"] for r in rows):
        sys.exit("some runs errored")


def cmd_model(a):
    workers = [int(x) for x in a.workers.split(",")]
    pins = [int(x) for x in a.pins.split(",")]
    jobs = schedule([(p, p or a.cpus, w) for p in pins for w in workers], a.reps, a.seed)
    meta = {"group": "model", "seed": a.seed, "reps": a.reps, "order": [[p, w, r] for (p, _, w), r in jobs], "S": a.s, "M": a.m,
            "fast_ms": a.fast_ms, "cpus": a.cpus, "pins": pins, "machine": machine(), "started": time.time()}
    print("order:", meta["order"], flush=True)
    rows = []
    tmp = Path(a.out).resolve().parent / "tmp"
    for idx, ((p, c, w), rep) in enumerate(jobs):
        rid = f"{idx:02d}"
        cpuset = ",".join(str(i) for i in range(p)) if p else ""
        a.pin = p
        row = {"id": rid, "pin": p, "cpus": c, "workers": w, "rep": rep, "order": idx, "error": ""}
        od = tmp / rid
        od.mkdir(parents=True, exist_ok=True)
        try:
            row["canary_s"] = canary(p)
            row["start_s"] = round(engine_up(a.container, a.port, c, cpuset, a.mem), 2)
            row["first_ok_s"] = engine_up.first_ok
            junit = str(od / "j.xml")
            d, r = run_pytest(a, w, f"r{rid}", ["-x"] if False else [], {
                "MODEL_S": str(a.s), "MODEL_M": str(a.m), "MODEL_FAST_MS": str(a.fast_ms), "MODEL_OUT": str(od),
                "MODEL_URI": f"mongodb://127.0.0.1:{a.port}/?directConnection=true", "MODEL_PREFIX": f"r{rid}"},
                HERE / "model", junit)
            ts = parse_junit(junit)
            fast = [t["s"] for t in ts if "test_fast" in t["id"]]
            sett = [t["s"] for t in ts if "test_settle" in t["id"]]
            lag = [json.loads(l)["s"] for p in glob.glob(str(od / "settle-*.jsonl")) for l in open(p)]
            row.update(pytest_s=round(d, 2), rc=r.returncode, n=len(ts), failed=sum(t["o"] == "failed" for t in ts),
                       fast_mean_s=round(st.mean(fast), 4) if fast else None, settle_test_mean_s=round(st.mean(sett), 4) if sett else None,
                       lag_p50=round(st.median(lag), 4) if lag else None, lag_max=max(lag) if lag else None,
                       lag_mean=round(st.mean(lag), 4) if lag else None, summary=(r.stdout.strip().splitlines() or [""])[-1])
            if r.returncode and not ts:
                row["error"] = (r.stdout + r.stderr)[-600:]
        except Exception as e:  # noqa: BLE001
            row["error"] = str(e)[:600]
        finally:
            engine_down(a.container)
        rows.append(row)
        print(json.dumps({k: v for k, v in row.items() if k != "summary"}), flush=True)
        finish(a.out, meta, rows, None)
    meta["finished"] = time.time()
    finish(a.out, meta, rows, None)
    shutil.rmtree(tmp, ignore_errors=True)
    if any(r["error"] or r["failed"] for r in rows):
        sys.exit("some runs errored or had failed tests")


# ---- analysis -----------------------------------------------------------------------------------------------------


def med(x):
    x = [v for v in x if v is not None]
    return st.median(x) if x else float("nan")


def rng(x):
    x = [v for v in x if v is not None]
    return f"{min(x):.1f}-{max(x):.1f}" if x else ""


def table(head, rows):
    o = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    o += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(o) + "\n"


def fit(ts, K):
    """T = a + c*W + K/W by least squares on (W, T) with K known; returns (a, c)."""
    xs = [(1.0, w) for w, _ in ts]
    ys = [t - K / w for w, t in ts]
    n = len(xs)
    sw = sum(w for _, w in xs)
    sww = sum(w * w for _, w in xs)
    sy = sum(ys)
    swy = sum(w * y for (_, w), y in zip(xs, ys))
    det = n * sww - sw * sw
    if abs(det) < 1e-9:
        return None
    return (sy * sww - sw * swy) / det, (n * swy - sw * sy) / det


def an_suite(d, name):
    j = json.load(open(d / f"{name}.json"))
    m, rows = j["meta"], j["runs"]
    o = [f"### {name}: real suite, one shared container", ""]
    if m.get("emulation"):
        o.append(f"Rows with pin P are an EMULATION of a P-vCPU runner on the 4-vCPU hosted runner (container --cpus and --cpuset-cpus, test and canary processes under taskset: {m['taskset']}). Memory is not emulated. All arms of this job ran on one runner (one CPU model)." + "\n")
    o.append(f"CPU: {m['machine']['cpu']}; nproc {m['machine']['nproc']}; seed {m['seed']}; reps {m['reps']}; stress repeats {m['stress_repeats']}; backends {m['backends']}; image {m['machine']['image_id'][:80]}\n")
    heads = ["pin", "engine cpus", "xdist workers", "runs", "engine start s", "first answer s", "pytest s (median)", "pytest s (range)", "total s (median)",
             "tests", "failed (max)", "xfailed", "skipped", "sum of test s (median)", "canary s (median)"]
    t = []
    for pn, c in sorted({(r.get("pin", 0), r["cpus"]) for r in rows}):
        for w in sorted({r["workers"] for r in rows}):
            g = [r for r in rows if r.get("pin", 0) == pn and r["cpus"] == c and r["workers"] == w and not r["error"]]
            if not g:
                continue
            t.append(["none (4 vCPU)" if not pn else f"{pn} CPUs (emulation)", "unlimited" if c == 0 else c, "none (-p no:xdist)" if w == 0 else w, f"{len(g)}", f"{med([r['start_s'] for r in g]):.1f}", f"{med([r.get('first_ok_s') for r in g]):.1f}",
                      f"{med([r['pytest_s'] for r in g]):.1f}", rng([r["pytest_s"] for r in g]),
                      f"{med([r['start_s'] + r['pytest_s'] for r in g]):.1f}", g[0]["n"], max(r["failed"] for r in g),
                      max(r.get("xfailed", 0) for r in g), max(r["skipped"] for r in g), f"{med([r['sum_test_s'] for r in g]):.1f}", f"{med([r['canary_s'] for r in g]):.2f}"])
    o.append(table(heads, t))
    bad = [r for r in rows if r["error"]]
    if bad:
        o.append("Errored runs: " + "; ".join(f"{r['id']} (cpus {r['cpus']}, w {r['workers']}): {r['error'][:120]}" for r in bad) + "\n")
    # failures by test
    tf = d / f"{name}.json.tests.gz"
    if tf.exists():
        tests = json.load(gzip.open(tf, "rt"))
        fails: dict[str, list] = {}
        for rid, ts in tests.items():
            r = next(x for x in rows if x["id"] == rid)
            for tid, s, oc, why in ts:
                if oc == "failed":
                    fails.setdefault(tid, []).append(f"{r['workers']}w/{r['cpus']}c/pin{r.get('pin', 0)}")
        if fails:
            o.append("Failed tests (id, arms where it failed):\n")
            o.append(table(["test", "count", "arms"], [[k, len(v), ", ".join(sorted(set(v)))[:120]] for k, v in sorted(fails.items(), key=lambda x: -len(x[1]))[:25]]))
        slow = {}
        for rid, ts in tests.items():
            r = next(x for x in rows if x["id"] == rid)
            if r["workers"] == 0 and r.get("pin", 0) == 0:
                for tid, s, oc, why in ts:
                    slow.setdefault(tid, []).append(s)
        nf = {}
        for rid, ts in tests.items():
            for tid, s_, oc, why in ts:
                if oc in ("skipped", "xfailed"):
                    nf[(tid, oc, why)] = nf.get((tid, oc, why), 0) + 1
        if nf:
            o.append("Skipped and expected-failure tests (reason from the report):" + "\n")
            o.append(table(["test", "kind", "reason", "runs"], [[k[0], k[1], k[2][:90], v] for k, v in sorted(nf.items())]))
        if slow:
            o.append("Slowest tests with no xdist, no pin (median over runs):\n")
            o.append(table(["test", "median s"], [[k, f"{med(v):.2f}"] for k, v in sorted(slow.items(), key=lambda x: -med(x[1]))[:10]]))
    return "\n".join(o)


def an_model(d):
    j = json.load(open(d / "model.json"))
    m, rows = j["meta"], j["runs"]
    S, M = m["S"], m["M"]
    o = ["### model: synthetic suite", "",
         f"S = {S} engine tests (one write-and-wait each), M = {M} engine-free tests (mongomock plus {m['fast_ms']} ms CPU each); engine limit {m['cpus']} CPUs on the unpinned arm; seed {m['seed']}; reps {m['reps']}; CPU: {m['machine']['cpu']} (one runner for all arms)" + "\n"]
    for pn in sorted({r.get("pin", 0) for r in rows}):
        rp = [r for r in rows if r.get("pin", 0) == pn]
        o.append(f"#### {'4 vCPU runner, engine limit ' + str(m['cpus']) + ' CPUs' if not pn else str(pn) + ' CPUs (EMULATION: container --cpus/--cpuset, taskset)'}" + "\n")
        ws = sorted({r["workers"] for r in rp})
        g = {w: [r for r in rp if r["workers"] == w and not r["error"]] for w in ws}
        s_ = med([r["settle_test_mean_s"] for r in g.get(0, [])])
        t_ = med([r["fast_mean_s"] for r in g.get(0, [])])
        K = S * s_ + M * t_ if g.get(0) else float("nan")
        t = []
        for w in ws:
            if not g[w]:
                continue
            T = med([r["pytest_s"] for r in g[w]])
            t.append(["none" if w == 0 else w, len(g[w]), f"{T:.1f}", rng([r["pytest_s"] for r in g[w]]), f"{med([r['start_s'] for r in g[w]]):.1f}",
                      f"{med([r['lag_p50'] for r in g[w]]):.3f}", f"{max(r['lag_max'] for r in g[w]):.2f}", max(r["failed"] for r in g[w])])
        o.append(table(["xdist workers", "runs", "pytest s (median)", "range", "engine start s", "settle p50 s", "settle max s", "failed"], t))
        if g.get(0):
            o.append(f"Measured at no xdist: settle test mean {s_:.3f} s, fast test mean {t_*1000:.1f} ms; work K = S*s + M*t = {K:.0f} s." + "\n")
            pts = [(w, med([r["pytest_s"] for r in g[w]])) for w in ws if w >= 1 and g[w]]
            a0 = med([r["pytest_s"] for r in g[0]]) - K
            o.append(f"Fixed part at no xdist (pytest s minus K): {a0:.1f} s." + "\n")
            o.append("Model T(W) = a + K/W with a from the no-xdist run, against measured:" + "\n")
            o.append(table(["W", "measured s", "model s", "residual s"], [[w, f"{T:.1f}", f"{a0 + K / w:.1f}", f"{T - (a0 + K / w):+.1f}"] for w, T in pts]))
            f2 = fit(pts, K) if len(pts) >= 3 else None
            if f2:
                a, c = f2
                lo, hi = min(w for w, _ in pts), max(w for w, _ in pts)
                if c > 0 and lo <= (K / c) ** 0.5 <= hi:
                    o.append(f"Least squares T = a + c*W + K/W over the tested W {lo} to {hi}: a = {a:.1f} s, c = {c:.2f} s per worker, optimum W = {(K / c) ** 0.5:.1f} (inside the tested range)." + "\n")
                else:
                    o.append(f"Least squares T = a + c*W + K/W over the tested W {lo} to {hi}: a = {a:.1f} s, c = {c:.2f} s per worker; the optimum is outside the tested range, so no optimum is reported." + "\n")
            mt = []
            prev = None
            for w, T in [(0, med([r["pytest_s"] for r in g[0]]))] + pts:
                if prev is not None:
                    mt.append([f"{prev[0]} to {w}", f"{prev[1] - T:.1f}", f"{100 * (prev[1] - T) / prev[1]:.0f}%"])
                prev = (w, T)
            o.append("Saving of each step (under 10% is where adding workers stops paying; a negative saving means slower):" + "\n")
            o.append(table(["step", "saved s", "saved %"], mt))
            o.append("Total wall including engine start: " + ", ".join(f"{'none' if w == 0 else w}: {med([r['start_s'] + r['pytest_s'] for r in g[w]]):.0f} s" for w in ws if g[w]) + "." + "\n")
    bad = [r for r in rows if r["error"] or r["failed"]]
    if bad:
        o.append("Errored or failing runs: " + "; ".join(f"{r['id']} pin={r.get('pin', 0)} w={r['workers']} failed={r.get('failed')} {r['error'][:100]}" for r in bad) + "\n")
    return "\n".join(o)


def an_image(d):
    ev = [json.loads(l) for l in open(d / "image.jsonl")]
    by = {}
    for e in ev:
        by.setdefault(e["event"], []).append(e)

    def g(n, k="s"):
        n = n if n in by else n + "_end"
        return by[n][-1].get(k) if n in by else None

    info = by["info"][0]
    ms = lambda a, b: round(by[b][0]["t"] - by[a][0]["t"], 2) if a in by and b in by else None
    rs, lt = ms("restore_start", "restore_end"), g("path_load_total")
    rows = [["cache key", info["key"]], ["restore result", "hit" if info["hit"] == "true" else "miss"],
            ["restore step (stamped around the action, includes step overhead)", ms("restore_start", "restore_end")],
            ["tarball bytes", g("tarball", "bytes")], ["docker load s", g("load")], ["load path: engine ready s", g("load_engine_ready")],
            ["load path total s (load + start)", g("path_load_total")], ["docker pull s", g("pull")],
            ["pull path: engine ready s", g("pull_engine_ready")], ["pull path total s (pull + start)", g("path_pull_total")],
            ["restore path total s (restore step + load + start)", round(rs + lt, 2) if rs is not None and lt is not None else None],
            ["docker save s (miss only)", g("save")], ["cache save step s (miss only)", ms("save_cache_start", "save_cache_end")]]
    li = by.get("image_local")
    m = info["machine"]
    img = (li[-1]["inspect"] if li else m["image_id"])[:110]
    o = ["### image: cache restore against pull", "", f"CPU: {m['cpu']}; runner {m['runner_image']}; image (id, size, digest after pull) {img}" + "\n", table(["item", "value"], rows)]
    o.append("Restore-path total to engine ready = restore step + load path total. Compare with the pull path total in the same job.\n")
    return "\n".join(o)


def cmd_analyze(a):
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    parts = ["# Cost model results", ""]
    for d in map(Path, a.dirs):
        parts.append(f"## {d}\n")
        for name in ("suite", "cpu2"):
            if (d / f"{name}.json").exists():
                parts.append(an_suite(d, name))
        if (d / "model.json").exists():
            parts.append(an_model(d))
        if (d / "image.jsonl").exists():
            parts.append(an_image(d))
    (out / "tables.md").write_text("\n".join(parts))
    print("\n".join(parts))


def main():
    p = argparse.ArgumentParser()
    s = p.add_subparsers(dest="c", required=True)
    m = s.add_parser("mark")
    m.add_argument("file")
    m.add_argument("event")
    m.add_argument("kv", nargs="*")
    i = s.add_parser("image")
    i.add_argument("file")
    i.add_argument("--hit", default="false")
    i.add_argument("--key", default="")
    i.add_argument("--dir", default="/tmp/fl2-image")
    i.add_argument("--container", default="hoodie-fl2-cost")
    i.add_argument("--port", type=int, default=29501)
    for n in ("suite", "model"):
        q = s.add_parser(n)
        q.add_argument("out")
        q.add_argument("--workers", default="0,1,2,4,8" if n == "suite" else "0,2,4,8,12,16")
        q.add_argument("--pins", default="0", help="comma list; P>0 runs that arm on P CPUs (emulation), 0 = the whole runner")
        q.add_argument("--reps", type=int, default=3 if n == "suite" else 2)
        q.add_argument("--seed", type=int, default=1)
        q.add_argument("--container", default="hoodie-fl2-cost")
        q.add_argument("--port", type=int, default=29501)
        q.add_argument("--mem", default="10g")
        if n == "suite":
            q.add_argument("--label", default="suite")
            q.add_argument("--cpus", default="3,0", help="engine CPU limits to compare, 0 = none")
            q.add_argument("--stress-repeats", type=int, default=1)
            q.add_argument("--backends", default="A,B,C")
            q.add_argument("-k", default="")
        else:
            q.add_argument("--cpus", type=int, default=3)
            q.add_argument("--s", type=int, default=420)
            q.add_argument("--m", type=int, default=1300)
            q.add_argument("--fast-ms", type=float, default=40)
    an = s.add_parser("analyze")
    an.add_argument("dirs", nargs="+")
    an.add_argument("--out", required=True)
    a = p.parse_args()
    if a.c == "mark":
        mark(a.file, a.event, **dict(x.split("=", 1) for x in a.kv))
    else:
        {"image": cmd_image, "suite": cmd_suite, "model": cmd_model, "analyze": cmd_analyze}[a.c](a)


if __name__ == "__main__":
    main()
