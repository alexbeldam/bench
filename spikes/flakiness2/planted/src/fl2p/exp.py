"""Experiments on the in-process runner: pools (per-test run sequences), suite passes, repeat loops, forced mode.

Every experiment draws its randomness from a logged seed (order, interleaving) and from os.urandom (the flakes themselves).
Output files are JSON; pools are gzip.
"""
from __future__ import annotations

import gzip
import json
import math
import os
import random
import statistics
import time
from pathlib import Path
from typing import Any

from .core import REG, T, Ctx, arm, loop, machine, run_one, selected

POOL_KINDS_EXCLUDE = {"order-dep", "helper"}


def dump(path: str | Path, obj: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if str(p).endswith(".gz"):
        with gzip.open(p, "wt", encoding="utf8") as f:
            json.dump(obj, f, separators=(",", ":"))
    else:
        p.write_text(json.dumps(obj, indent=1), encoding="utf8")


def seed_or_new(seed: int | None) -> int:
    return seed if seed is not None else int.from_bytes(os.urandom(4), "big")


def warm(tests: list[T]) -> dict[str, float]:
    """Untimed runs so imports and first-use costs are not charged to a test. Returns the mean duration of 8 further runs."""
    out = {}
    for t in tests:
        for _ in range(int(os.environ.get("FL2P_WARM", "3"))):
            run_one(t, Ctx())
        out[t.name] = statistics.fmean(run_one(t, Ctx()).dur for _ in range(8))
    return out


def pool_sizes(tests: list[T], n: int, budget_s: float, n_min: int = 100) -> dict[str, int]:
    """n runs per test, reduced where n runs would exceed the time budget for that test (never below n_min, never above n)."""
    est = warm(tests)
    return {t.name: max(min(n, n_min), min(n, int(budget_s / max(est[t.name], 1e-5)))) for t in tests}


def pool(tests: list[T], sizes: dict[str, int], seed: int) -> dict[str, Any]:
    """Run each test sizes[name] times in a shuffled interleaved order; each run is its own process-like state (fresh Ctx)."""
    order = [t.name for t in tests for _ in range(sizes[t.name])]
    random.Random(seed).shuffle(order)
    bits: dict[str, list[str]] = {t.name: [] for t in tests}
    durs: dict[str, list[float]] = {t.name: [] for t in tests}
    errs: dict[str, dict[str, int]] = {t.name: {} for t in tests}
    t0 = time.perf_counter()
    for nm in order:
        r = run_one(REG[nm], Ctx())
        bits[nm].append("0" if r.ok else "1")
        durs[nm].append(r.dur)
        if not r.ok:
            errs[nm][r.err[:60]] = errs[nm].get(r.err[:60], 0) + 1
    wall = time.perf_counter() - t0
    res = {}
    for t in tests:
        d = sorted(durs[t.name])
        res[t.name] = {"label": t.label(), "n": len(d), "fail": "".join(bits[t.name]), "errs": errs[t.name],
                       "dur_mean": statistics.fmean(d), "dur_p95": d[int(0.95 * (len(d) - 1))], "dur_sum": sum(d)}
    return {"order_seed": seed, "wall_s": wall, "tests": res}


def suite_pass(tests: list[T], order_seed: int | None, state: dict[str, Any], forced: bool = False) -> dict[str, Any]:
    names = [t.name for t in tests]
    if order_seed is not None:
        random.Random(order_seed).shuffle(names)
    ctx = Ctx(forced=forced, state=state)
    fails, durs = [], {}
    t0 = time.perf_counter()
    for nm in names:
        r = run_one(REG[nm], ctx)
        durs[nm] = r.dur
        if not r.ok:
            fails.append(nm)
    return {"order_seed": order_seed, "fail": fails, "wall_s": time.perf_counter() - t0, "order": names if len(names) < 400 else None}


def suite_exps(strategy: str, k: int, seed: int, passes: int = 1, tests: list[T] | None = None) -> list[dict[str, Any]]:
    """strategy: s0 (default order), s1 (random order, one seed per pass). One experiment = one CI invocation: a fresh process state."""
    tests = tests or selected("all")
    tests = [t for t in tests if t.engine in ("none", "inmem")]
    out = []
    for i in range(k):
        state: dict[str, Any] = {}
        ps = []
        for _ in range(passes):
            os_ = None if strategy == "s0" else int.from_bytes(os.urandom(4), "big")
            ps.append(suite_pass(tests, os_, state))
        out.append({"i": i, "passes": [{"order_seed": p["order_seed"], "fail": p["fail"], "wall_s": p["wall_s"]} for p in ps],
                    "wall_s": sum(p["wall_s"] for p in ps)})
    return out


def forced_exps(k: int) -> list[dict[str, Any]]:
    tests = selected("all")
    out = []
    for i in range(k):
        p = suite_pass([t for t in tests if t.forced], None, {}, forced=True)
        out.append({"i": i, "fail": p["fail"], "wall_s": p["wall_s"]})
    return out


def repeat_exps(scope: str, k: int, nmax: int, tests: list[T]) -> list[dict[str, Any]]:
    """One experiment = nmax repeats of the test list in one process state. scope 'function': each test nmax times in a row
    (pytest-repeat default); 'session': the whole list nmax times (pytest-repeat --repeat-scope=session, or an in-process loop).
    Records, per test, the index of the first failing repeat (-1: none). Prefixes give every smaller N."""
    out = []
    for i in range(k):
        ctx = Ctx()
        first = {t.name: -1 for t in tests}
        nfail = {t.name: 0 for t in tests}
        t0 = time.perf_counter()
        if scope == "function":
            for t in tests:
                for j in range(nmax):
                    if not run_one(t, ctx).ok:
                        nfail[t.name] += 1
                        if first[t.name] < 0:
                            first[t.name] = j
        else:
            for j in range(nmax):
                for t in tests:
                    if not run_one(t, ctx).ok:
                        nfail[t.name] += 1
                        if first[t.name] < 0:
                            first[t.name] = j
        out.append({"i": i, "first_fail": first, "n_fail": nfail, "wall_s": time.perf_counter() - t0})
    return out


def header(kind: str, seed: int, **kw: Any) -> dict[str, Any]:
    return {"schema": 1, "kind": kind, "seed": seed, "machine": machine(), "t_start": time.time(), **kw,
            "labels": {t.name: t.label() for t in REG.values()}}


def cmd_pool(a: Any) -> None:
    seed = seed_or_new(a.seed)
    tests = [t for t in selected(a.tests) if t.kind not in POOL_KINDS_EXCLUDE]
    extra = {}
    for spec in (a.extra or "").split(","):
        if spec:
            nm, n = spec.split("=")
            extra[nm] = int(n)
    with arm(a.arm):
        sizes = pool_sizes(tests, a.n, a.budget)
        sizes.update({k: v for k, v in extra.items() if k in sizes})
        print(f"pool arm={a.arm} seed={seed} tests={len(tests)} runs={sum(sizes.values())}", flush=True)
        out = pool(tests, sizes, seed)
    out.update(header("pool", seed, arm=a.arm, n=a.n, budget=a.budget))
    dump(a.out, out)
    print(f"pool done wall={out['wall_s']:.1f}s -> {a.out}")


def cmd_suite(a: Any) -> None:
    seed = seed_or_new(a.seed)
    with arm(a.arm):
        exps = suite_exps(a.strategy, a.k, seed, a.passes)
    dump(a.out, {**header("suite", seed, strategy=a.strategy, passes=a.passes, k=a.k, arm=a.arm), "exps": exps})
    print(f"suite {a.strategy} arm={a.arm} k={a.k} passes={a.passes} -> {a.out}")


def cmd_forced(a: Any) -> None:
    exps = forced_exps(a.k)
    dump(a.out, {**header("forced", 0, k=a.k), "exps": exps})
    print(f"forced k={a.k} -> {a.out}")


REPEAT_SUBSET = ("order-dep", "rate", "delay", "timing", "helper", "control")


def cmd_repeat(a: Any) -> None:
    tests = [t for t in selected("all") if t.kind in REPEAT_SUBSET and t.cost == "micro"]
    exps = repeat_exps(a.scope, a.k, a.nmax, tests)
    dump(a.out, {**header("repeat", 0, scope=a.scope, k=a.k, nmax=a.nmax), "tests": [t.name for t in tests], "exps": exps})
    print(f"repeat {a.scope} k={a.k} nmax={a.nmax} -> {a.out}")


def wilson(x: int, n: int, z: float = 1.959964) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = x / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - h) / d, (c + h) / d)
