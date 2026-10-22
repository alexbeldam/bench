"""Open-loop load test (Q1): queries arrive at a fixed rate whatever the latency, as in production.

Latency is measured from the SCHEDULED arrival time, so queueing delay inside the client counts (no coordinated
omission). The full planner runs for each query (prefilter, strategy choice, fallback). This phase changes the
state of the machine (heat, caches, connections), so it is never shuffled with the others: it runs last, rates in
ascending order, each rate in its own time slice.

The arrivals are spread over several worker processes. A single Python event loop saturated at about 100 queries/s
in the first run (client CPU 95%, event-loop lag in the hundreds of milliseconds), so everything above that rate
measured the client and not the engine. Worker k handles the arrivals j with j % workers == k of one global
schedule (`t_base + j / rate`), so together they produce exactly the requested open-loop arrival stream. The
parent samples the engine container and the CPU of the whole client (parent plus workers).
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import random
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import psutil
from pymongo import AsyncMongoClient

from .common import CAP, DB, append_jsonl, coll_name, oid, redact
from .env import stats_once
from .planner import Planner

DEFAULT_RATES = [25, 50, 100, 150, 200, 300, 400]
DEFAULT_WORKERS = 2
TIMEOUT_S = 10.0
MAX_OUTSTANDING = 3000  # per worker; beyond this the client itself is overloaded: arrivals are counted as dropped
START_DELAY_S = 4.0  # workers connect first, then all start at the same wall-clock instant

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
    """Engine container stats, and the CPU of the whole client (parent plus every worker process)."""

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
        parent = psutil.Process()
        known: dict[int, psutil.Process] = {}

        def is_client(p: psutil.Process) -> bool:
            # The load workers are Python processes; helpers such as the `docker stats` calls that sample the
            # engine are not part of the client and must not count towards its CPU.
            try:
                return p.name().lower().startswith("python")
            except psutil.Error:
                return False

        def procs() -> list[psutil.Process]:
            alive = [parent, *[c for c in parent.children(recursive=True) if is_client(c)]]
            for p in alive:
                if p.pid not in known:
                    known[p.pid] = p
                    p.cpu_percent(None)  # the first call only primes the counter
            return [known[p.pid] for p in alive]

        procs()
        while not self._stop.wait(2.0):
            cpu = rss = 0.0
            for p in procs():
                try:
                    cpu += p.cpu_percent(None)
                    rss += p.memory_info().rss / 2**20
                except psutil.Error:
                    continue
            # cpu_pct is the total over all client processes, in percent of one core (it can exceed 100).
            append_jsonl(self.path, {**self.tag, "kind": "client", "t": time.time(), "cpu_pct": cpu, "rss_mib": rss,
                                     "workers": len(known) - 1, "system_cpu_pct": psutil.cpu_percent(None)})


async def loop_lag(path: Path, tag: dict, stop: asyncio.Event) -> None:
    while not stop.is_set():
        t0 = time.perf_counter()
        await asyncio.sleep(0.05)
        lag = (time.perf_counter() - t0 - 0.05) * 1000
        append_jsonl(path, {**tag, "kind": "loop_lag", "t": time.time(), "ms": lag})
        await asyncio.sleep(0.45)


async def _worker_async(a: dict) -> dict:
    """One worker's share of one rate step. Runs in its own process, with its own event loop and connection pool."""
    uri, label, n, rate = a["uri"], a["label"], a["n"], a["rate"]
    worker, workers, secrets, tag = a["worker"], a["workers"], a["secrets"], a["tag"]
    raw, side = Path(a["raw"]), Path(a["side"])
    client = AsyncMongoClient(uri, maxPoolSize=1000)
    stop = asyncio.Event()
    lag = None
    try:
        planner = Planner(client[DB][coll_name(label)], label)
        await planner.estimate_n()
        await client.admin.command("ping")  # connect before the shared start instant
        rng = random.Random(f"{a['seed']}:{rate}:{worker}")
        kinds, weights = zip(*MIX)
        total = int(rate * a["duration"])
        t_base = a["t_base_wall"]
        outstanding = 0
        counts = {"ok": 0, "error": 0, "timeout": 0, "dropped": 0}
        tasks: list[asyncio.Task] = []
        wtag = {**tag, "worker": worker}
        if a["with_lag"]:
            lag = asyncio.create_task(loop_lag(side, {**wtag, "rate": rate}, stop))

        async def one(i: int, kind: str, flt: dict, term: str, sched: float) -> None:
            nonlocal outstanding
            start = time.perf_counter()
            row = {**wtag, "i": i, "kind": kind, "rate": rate, "t": sched}
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
            row["ms"] = (time.time() - sched) * 1000  # from scheduled arrival: includes client-side queueing
            row["service_ms"] = (end - start) * 1000
            append_jsonl(raw, row)
            outstanding -= 1

        for j in range(worker, total, workers):
            sched = t_base + j / rate
            delay = sched - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            kind = rng.choices(kinds, weights)[0]
            if outstanding >= MAX_OUTSTANDING:
                counts["dropped"] += 1
                append_jsonl(raw, {**wtag, "i": j, "kind": kind, "rate": rate, "ok": 0, "status": "dropped", "t": time.time(), "ms": None})
                continue
            flt, term = make_query(kind, n, rng)
            outstanding += 1
            tasks.append(asyncio.create_task(one(j, kind, flt, term, sched)))
        await asyncio.gather(*tasks)
        return {"rate": rate, "scheduled": len(range(worker, total, workers)), **counts}
    finally:
        stop.set()
        if lag is not None:
            await lag
        await client.close()


def _worker_job(a: dict) -> dict:
    # Top-level so that it can be pickled for a spawned process.
    return asyncio.run(_worker_async(a))


def _merge(parts: list[Path], target: Path) -> None:
    """Append the per-worker files to the file the analysis reads, then remove them."""
    with open(target, "ab") as out:
        for p in parts:
            if p.exists():
                out.write(p.read_bytes())
                p.unlink()


async def _step(pool: ProcessPoolExecutor, workers: int, base: dict, raw: Path, side: Path | None, rate: float,
                duration: float, tag: dict) -> dict:
    """One rate over all workers: returns the summed counts."""
    t_base = time.time() + START_DELAY_S
    raw_parts = [raw.with_name(f"{raw.stem}.w{k}{raw.suffix}") for k in range(workers)]
    side_parts = [side.with_name(f"{side.stem}.w{k}{side.suffix}") for k in range(workers)] if side else []
    loop = asyncio.get_running_loop()
    futs = [loop.run_in_executor(pool, _worker_job, {
        **base, "rate": rate, "duration": duration, "worker": k, "workers": workers, "t_base_wall": t_base, "tag": tag,
        "raw": str(raw_parts[k]), "side": str(side_parts[k] if side else raw_parts[k]), "with_lag": side is not None})
        for k in range(workers)]
    results = await asyncio.gather(*futs)
    _merge(raw_parts, raw)
    if side:
        _merge(side_parts, side)
    total = {"rate": rate, "scheduled": sum(r["scheduled"] for r in results)}
    for key in ("ok", "error", "timeout", "dropped"):
        total[key] = sum(r[key] for r in results)
    return total


async def run_load(uri: str, label: str, n: int, outdir: Path, container: str, rates: list[float], duration: float,
                   seed: int, cooldown: float, secrets: list[str], canary=None, stop_on_collapse: bool = True,
                   workers: int = DEFAULT_WORKERS) -> None:
    workers = max(1, min(workers, os.cpu_count() or 1))
    raw = outdir / f"load_{label}.jsonl"
    side = outdir / f"load_side_{label}.jsonl"
    summary: list[dict] = []
    base = {"uri": uri, "label": label, "n": n, "seed": seed, "secrets": secrets}
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        print(f"[{label}] load test with {workers} worker process(es)")
        # warm-up: 10 s at a low rate, recorded in a separate file
        if canary:
            canary.phase = "load_warm"
        await _step(pool, workers, base, outdir / f"load_warmup_{label}.jsonl", None, 20, 10.0 if n > 100_000 else 2.0, {"rate_step": -1})
        collapsed = 0
        for step, rate in enumerate(rates):
            if canary:
                canary.phase = f"load_{rate:g}"
            tag = {"rate_step": step}
            sampler = Sampler(container, side, {**tag, "rate": rate})
            sampler.start()
            t0 = time.time()
            res = await _step(pool, workers, base, raw, side, rate, duration, tag)
            res.update(t_start=t0, t_end=time.time(), workers=workers)
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
