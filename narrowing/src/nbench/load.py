"""Open-loop load test (Q1): queries arrive at a fixed rate whatever the latency, as in production.

Latency is measured from the SCHEDULED arrival time, so queueing delay inside the client counts (no coordinated
omission). The full planner runs for each query (prefilter, strategy choice, fallback). This phase changes the
state of the machine (heat, caches, connections), so it is never shuffled with the others: it runs last, rates in
ascending order, each rate in its own time slice.
"""

from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from pathlib import Path

import psutil
from pymongo import AsyncMongoClient

from .common import CAP, DB, LIMIT, append_jsonl, coll_name, make_filter_or_none, oid, redact
from .env import stats_once
from .planner import Planner

DEFAULT_RATES = [25, 50, 100, 150, 200, 300, 400]
TIMEOUT_S = 10.0
MAX_OUTSTANDING = 3000  # beyond this the client itself is overloaded: arrivals are counted as dropped

# (name, weight). The window size is derived from the entity size in `make_query`.
MIX = [("zero", 0.10), ("small", 0.25), ("mid", 0.25), ("nearcap", 0.15), ("b_case", 0.15), ("lowsel_above_cap", 0.10)]


def make_query(kind: str, n: int, rng: random.Random):
    """Return (filter dict, term). Windows are random so queries differ and caches do not just replay one ID set."""
    if kind == "zero":
        return {"tenant": "none"}, "frequent"
    k = {"small": 500, "mid": 5000, "nearcap": CAP,
         "b_case": max(CAP + 1000, round(0.02 * n)), "lowsel_above_cap": min(25_000, n // 2)}[kind]
    k = min(k, n // 2)
    a = rng.randrange(0, n - k)
    if kind in ("small", "mid", "nearcap"):  # contiguous ID window: cheap prefilter, a realistic range filter
        return {"_id": {"$gte": oid(a), "$lt": oid(a + k)}}, "frequent"
    return {"rank": {"$gte": a, "$lt": a + k}}, "frequent"  # spread evenly


class Sampler:
    """Engine container stats, client CPU and event-loop lag during a rate step."""

    def __init__(self, container: str, path: Path, tag: dict) -> None:
        self.container, self.path, self.tag = container, path, tag
        self._stop = threading.Event()
        self._threads = [threading.Thread(target=self._docker, daemon=True), threading.Thread(target=self._proc, daemon=True)]

    def start(self) -> None:
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=15)

    def _docker(self) -> None:
        while not self._stop.is_set():
            s = stats_once(self.container)
            if s:
                append_jsonl(self.path, {**self.tag, "kind": "engine", **s})

    def _proc(self) -> None:
        pr = psutil.Process()
        pr.cpu_percent(None)
        while not self._stop.wait(2.0):
            append_jsonl(self.path, {**self.tag, "kind": "client", "t": time.time(), "cpu_pct": pr.cpu_percent(None),
                                     "rss_mib": pr.memory_info().rss / 2**20, "system_cpu_pct": psutil.cpu_percent(None)})


async def loop_lag(path: Path, tag: dict, stop: asyncio.Event) -> None:
    while not stop.is_set():
        t0 = time.perf_counter()
        await asyncio.sleep(0.05)
        lag = (time.perf_counter() - t0 - 0.05) * 1000
        append_jsonl(path, {**tag, "kind": "loop_lag", "t": time.time(), "ms": lag})
        await asyncio.sleep(0.45)


async def run_rate(planner: Planner, n: int, rate: float, duration: float, seed: int, raw: Path, secrets: list[str], tag: dict) -> dict:
    rng = random.Random(f"{seed}:{rate}")
    kinds, weights = zip(*MIX)
    total = int(rate * duration)
    outstanding = 0
    counts = {"ok": 0, "error": 0, "timeout": 0, "dropped": 0}
    t_base = time.perf_counter() + 0.5
    wall_base = time.time() + 0.5
    tasks: list[asyncio.Task] = []

    async def one(i: int, kind: str, flt: dict, term: str, sched: float) -> None:
        nonlocal outstanding
        start = time.perf_counter()
        row = {**tag, "i": i, "kind": kind, "rate": rate, "t": wall_base + (sched - t_base)}
        try:
            res = await asyncio.wait_for(planner.plan(flt, term), TIMEOUT_S)
            row.update(ok=1, status="ok", results=len(res.hits), strategy=res.info.get("strategy"),
                       fallback=bool(res.info.get("fallback")))
            counts["ok"] += 1
        except TimeoutError:
            row.update(ok=0, status="timeout")
            counts["timeout"] += 1
        except Exception as e:  # noqa: BLE001
            row.update(ok=0, status="error", err=redact(f"{type(e).__name__}: {e}", secrets)[:160])
            counts["error"] += 1
        end = time.perf_counter()
        row["ms"] = (end - sched) * 1000  # from scheduled arrival: includes client-side queueing
        row["service_ms"] = (end - start) * 1000
        append_jsonl(raw, row)
        outstanding -= 1

    for i in range(total):
        sched = t_base + i / rate
        delay = sched - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        kind = rng.choices(kinds, weights)[0]
        if outstanding >= MAX_OUTSTANDING:
            counts["dropped"] += 1
            append_jsonl(raw, {**tag, "i": i, "kind": kind, "rate": rate, "ok": 0, "status": "dropped", "t": time.time(), "ms": None})
            continue
        flt, term = make_query(kind, n, rng)
        outstanding += 1
        tasks.append(asyncio.create_task(one(i, kind, flt, term, sched)))
    await asyncio.gather(*tasks)
    return {"rate": rate, "scheduled": total, **counts}


async def run_load(uri: str, label: str, n: int, outdir: Path, container: str, rates: list[float], duration: float,
                   seed: int, cooldown: float, secrets: list[str], canary=None, stop_on_collapse: bool = True) -> None:
    client = AsyncMongoClient(uri, maxPoolSize=1000)
    raw = outdir / f"load_{label}.jsonl"
    side = outdir / f"load_side_{label}.jsonl"
    summary: list[dict] = []
    try:
        planner = Planner(client[DB][coll_name(label)], label)
        await planner.estimate_n()
        # warm-up: 10 s at a low rate, recorded with warm=1 in a separate file
        if canary:
            canary.phase = "load_warm"
        await run_rate(planner, n, 20, 10.0 if n > 100_000 else 2.0, seed, outdir / f"load_warmup_{label}.jsonl", secrets, {"rate_step": -1})
        collapsed = 0
        for step, rate in enumerate(rates):
            if canary:
                canary.phase = f"load_{rate:g}"
            tag = {"rate_step": step}
            sampler = Sampler(container, side, {**tag, "rate": rate})
            stop = asyncio.Event()
            lag = asyncio.create_task(loop_lag(side, {**tag, "rate": rate}, stop))
            sampler.start()
            t0 = time.time()
            res = await run_rate(planner, n, rate, duration, seed, raw, secrets, tag)
            res.update(t_start=t0, t_end=time.time())
            stop.set()
            await lag
            await asyncio.to_thread(sampler.stop)
            summary.append(res)
            (outdir / f"load_summary_{label}.json").write_text(json.dumps(summary, indent=1))
            bad = (res["error"] + res["timeout"] + res["dropped"]) / max(1, res["scheduled"])
            print(f"[{label}] rate {rate:g}/s: {res}")
            collapsed = collapsed + 1 if bad > 0.2 else 0
            if stop_on_collapse and collapsed >= 2:
                print(f"[{label}] stopping: more than 20% failures at two consecutive rates")
                break
            await asyncio.sleep(cooldown)
    finally:
        await client.close()
