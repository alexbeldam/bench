"""Measurement phases (one per question group) and the randomized-block runner.

Design: every phase builds a list of cases. A round runs every case once, in a shuffled order (a randomized block),
after `warm` unrecorded-in-statistics warm-up calls on each switch. Rounds repeat until `reps` is reached or the time
limit hits at a round boundary, so every case has the same number of repetitions. The seed of round r is
`Random(f"{seed}:{scale}:{phase}:{r}")`, so a run is reproducible and resumable from the recorded round.
"""

from __future__ import annotations

import json
import math
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tqdm import tqdm

from .common import (
    A_MAX_IDS, CAP, CHUNK, INFLIGHT, LAYOUTS, LIMIT, OVERFETCH, THRESHOLD, SEL_GRID, abs_counts, append_jsonl,
    make_filter, make_filter_or_none, redact, sel_counts,
)
from .planner import Opts, Planner

FACTORS = [50, 100, 200, 500]
CHUNKS = [250, 500, 1000]
INFLIGHTS = [2, 4, 8, 16]
OPTS = [Opts(), Opts(count=True), Opts(count=True, facet="low"), Opts(count=True, facet="high")]
PHASES = ["calibration", "layouts", "count_facets", "tuning", "selectivity", "prefilter", "noise"]


@dataclass
class Case:
    id: str
    params: dict[str, Any]
    fn: Callable[[], Awaitable[dict]]


def _ks(n: int, with_abs: bool = True, with_sel: bool = True) -> list[int]:
    ks = (abs_counts(n) if with_abs else []) + (sel_counts(n) if with_sel else []) + [max(1, round(0.10 * n))]
    return sorted({k for k in ks if k <= n // 2})


def _ac(p: Planner, flt, term: str, k: int, opts: Opts = Opts(), chunk: int = CHUNK, inflight: int = INFLIGHT):
    async def run() -> dict:
        t0 = time.perf_counter()
        ids = await p.prefilter(flt.flt, k + 1)
        t1 = time.perf_counter()
        res = await p.chunked(ids, term, opts, chunk, inflight)
        return {"results": len(res.hits), "total": res.total, "ids": len(ids), "prefilter_ms": (t1 - t0) * 1000,
                "facet": {a: len(b) for a, b in (res.facets or {}).items()} or None}

    return run


def _b(p: Planner, flt, term: str, factor: int, opts: Opts = Opts()):
    async def run() -> dict:
        res = await p.search_first(flt.flt, term, factor, opts)
        return {"results": len(res.hits), "total": res.total, "facet": {a: len(b) for a, b in (res.facets or {}).items()} or None}

    return run


def build(phase: str, p: Planner, n: int, terms: list[str]) -> list[Case]:
    cases: list[Case] = []

    def add(cid: str, params: dict, fn) -> None:
        cases.append(Case(cid, params, fn))

    if phase == "calibration":
        for k in sorted({k for k in (10_000, 25_000) if k <= n // 2}):
            f = make_filter("even", n, k)
            add(f"AC|even|{k}", {"strategy": "AC", "layout": "even", "k": k, "term": "frequent"}, _ac(p, f, "frequent", k))
        k1 = max(1, round(0.01 * n))
        f = make_filter("even", n, k1)
        add(f"B100|even|{k1}", {"strategy": "B100", "layout": "even", "k": k1, "factor": 100, "term": "frequent"}, _b(p, f, "frequent", 100))
        if k1 <= A_MAX_IDS:
            add(f"AC|even|{k1}", {"strategy": "AC", "layout": "even", "k": k1, "term": "frequent"}, _ac(p, f, "frequent", k1))
    elif phase == "layouts":
        for layout in LAYOUTS:
            for k in _ks(n):
                f = make_filter_or_none(layout, n, k)
                if f is None:
                    continue
                if k <= A_MAX_IDS:
                    add(f"AC|{layout}|{k}", {"strategy": "AC", "layout": layout, "k": k}, _ac(p, f, "frequent", k))
                    add(f"PRE|{layout}|{k}", {"strategy": "prefilter", "layout": layout, "k": k}, _pre(p, f, k + 1))
                add(f"B100|{layout}|{k}", {"strategy": "B100", "layout": layout, "k": k, "factor": 100}, _b(p, f, "frequent", 100))
    elif phase == "count_facets":
        ks = sorted({k for k in [1000, 10_000, 25_000, round(0.01 * n), round(0.02 * n), round(0.05 * n)] if 0 < k <= n // 2})
        for k in ks:
            f = make_filter("even", n, k)
            for o in OPTS:
                if k <= A_MAX_IDS:
                    add(f"AC|{k}|{o.label}", {"strategy": "AC", "k": k, "opts": o.label}, _ac(p, f, "frequent", k, o))
                add(f"B100|{k}|{o.label}", {"strategy": "B100", "k": k, "opts": o.label, "factor": 100}, _b(p, f, "frequent", 100, o))
    elif phase == "tuning":
        for k in sorted({k for k in (10_000, 25_000) if k <= n // 2} or {max(1, n // 5)}):
            f = make_filter("even", n, k)
            for ch in CHUNKS:
                for fl in INFLIGHTS:
                    add(f"AC|{k}|{ch}x{fl}", {"strategy": "AC", "k": k, "chunk": ch, "inflight": fl}, _ac(p, f, "frequent", k, Opts(), ch, fl))
    elif phase == "selectivity":
        for term in terms:
            for k in sel_counts(n):
                f = make_filter("even", n, k)
                if k <= A_MAX_IDS:
                    add(f"AC|{term}|{k}", {"strategy": "AC", "k": k, "term": term}, _ac(p, f, term, k))
                for fac in FACTORS:
                    add(f"B{fac}|{term}|{k}", {"strategy": f"B{fac}", "k": k, "factor": fac, "term": term}, _b(p, f, term, fac))
    elif phase == "prefilter":
        for layout in LAYOUTS:
            for k in _ks(n):
                f = make_filter_or_none(layout, n, k)
                if f is None:
                    continue
                need = math.ceil(THRESHOLD * n)
                ops = {
                    "early": _pre(p, f, CAP + 1),
                    "count_full": _count(p, f, None),
                    "count_cap": _count(p, f, CAP + 1),
                    "count_threshold": _count(p, f, need),
                }
                if k <= 1_000_000:
                    ops["full_ids"] = _pre(p, f, k + 1)
                for op, fn in ops.items():
                    add(f"{op}|{layout}|{k}", {"op": op, "layout": layout, "k": k}, fn)

        async def est() -> dict:
            return {"results": await p.coll.estimated_document_count()}

        add("estimate|-|0", {"op": "estimate", "layout": "-", "k": 0}, est)
    elif phase == "noise":
        for k in sorted({min(10_000, n // 5), min(25_000, n // 2)}):
            f = make_filter("even", n, k)
            add(f"AC|even|{k}", {"strategy": "AC", "layout": "even", "k": k}, _ac(p, f, "frequent", k))
    else:
        raise ValueError(f"unknown phase {phase}")
    return cases


def _pre(p: Planner, flt, limit: int):
    async def run() -> dict:
        return {"results": len(await p.prefilter(flt.flt, limit))}

    return run


def _count(p: Planner, flt, limit: int | None):
    async def run() -> dict:
        kw = {"limit": limit} if limit else {}
        return {"results": await p.coll.count_documents(flt.flt, **kw)}

    return run


# ---- runner ------------------------------------------------------------------------------------------


def _progress_load(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


async def run_phase(*, phase: str, cases: list[Case], scale: str, outdir: Path, reps: int, warm: int, seed: int,
                    time_limit_s: float | None, canary=None, prewarm_rounds: int = 1, secrets: list[str] = ()) -> dict:
    raw = outdir / f"raw_{scale}_{phase}.jsonl"
    prog_path = outdir / "progress.json"
    key = f"{scale}:{phase}"
    prog = _progress_load(prog_path)
    st = prog.get(key, {"seed": seed, "rounds_done": 0, "complete": False})
    if st["complete"] and st["rounds_done"] >= reps + prewarm_rounds:
        print(f"[{scale}/{phase}] already complete ({st['rounds_done']} rounds)")
        return st
    if st["rounds_done"] and st["seed"] != seed:
        print(f"[{scale}/{phase}] WARNING: resuming with recorded seed {st['seed']} (ignoring {seed})")
        seed = st["seed"]
    st["seed"] = seed
    (outdir / f"cases_{scale}_{phase}.json").write_text(json.dumps({c.id: c.params for c in cases}, indent=1))
    if canary:
        canary.phase = phase
    t_start = time.time()
    bar = tqdm(total=reps, initial=max(0, st["rounds_done"] - prewarm_rounds), desc=f"{scale}/{phase}", unit="round", mininterval=2.0)
    last_case = None
    rounds_target = reps + prewarm_rounds
    # round numbering: 0..prewarm-1 are warm-up rounds (flagged), the rest are measured
    r = st["rounds_done"]
    while r < rounds_target:
        if time_limit_s and time.time() - t_start > time_limit_s and r >= prewarm_rounds + 1:
            st["complete"] = False
            st["stopped_by_time_limit"] = True
            break
        order = list(cases)
        random.Random(f"{seed}:{scale}:{phase}:{r}").shuffle(order)
        round_is_warm = r < prewarm_rounds
        for seq, c in enumerate(order):
            n_calls = (warm if c.id != last_case else 0) + 1
            for call in range(n_calls):
                is_warm = round_is_warm or call < n_calls - 1
                t = time.time()
                t0 = time.perf_counter()
                row = {"phase": phase, "scale": scale, "case": c.id, "round": r - prewarm_rounds, "seq": seq, "warm": int(is_warm)}
                try:
                    extra = await c.fn()
                    row["ok"] = 1
                    row.update(extra)
                except Exception as e:  # noqa: BLE001  a failure is data, recorded with the error type
                    row["ok"] = 0
                    row["err"] = redact(f"{type(e).__name__}: {e}", list(secrets))[:200]
                row["ms"] = (time.perf_counter() - t0) * 1000
                row["t"] = t
                append_jsonl(raw, row)
            last_case = c.id
        r += 1
        if not round_is_warm:
            bar.update(1)
        st["rounds_done"] = r
        st["rounds_measured"] = max(0, r - prewarm_rounds)
        prog[key] = st
        prog_path.write_text(json.dumps(prog, indent=1))
    else:
        st["complete"] = True
    bar.close()
    prog[key] = st
    prog_path.write_text(json.dumps(prog, indent=1))
    return st
