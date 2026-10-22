"""Open-loop load generator (fixed arrival rate), sharded over processes.

Arrivals are scheduled on a fixed clock. Every operation is started when its slot arrives whether or not earlier
ones have finished, and latency is measured from the **scheduled** slot, so queueing shows up instead of being hidden
(no coordinated omission). The generator's own lateness (`lag_ms`) is recorded for every operation.

Rows go to `<case>.<shard>.jsonl.gz`, one JSON object per operation, with epoch timestamps.
"""

import asyncio
import gzip
import json
import random
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

from bson import ObjectId
from pymongo import AsyncMongoClient

from .cmdmon import CommandCounter
from .common import DB, REV, index_name
from .probe import SCHEDULES, Coalescer, IndependentWaiter, Prober, Schedule
from .sim import Op, SyncFailure, WritePath

POOL_FLAG = 1 << 62


def seq_of(uid: int, shard: int, k: int, pool: bool = False) -> int:
    """Unique sequence number for documents created by one shard of one case run (always above any loaded document)."""
    return (POOL_FLAG if pool else 0) | (uid << 24) | (shard << 20) | k


def fast_doc(seq: int) -> dict:
    from .common import doc_oid

    r = random.Random(seq)
    w = [f"w{r.randrange(2000):04d}" for _ in range(10)]
    return {"_id": doc_oid(seq), "seq": seq, "touch": 0, "title": " ".join(w[:3]), "body": " ".join(w[3:])}


def make_waiter(coll, index: str, spec: dict):
    sched = SCHEDULES[spec.get("schedule", "default")]
    if spec.get("timeout_s"):
        sched = Schedule(sched.initial_ms, sched.factor, sched.max_ms, spec["timeout_s"])
    prober = Prober(coll, index, spec.get("shape", "rev"), spec.get("result", "count"), spec.get("chunk", 500),
                    spec.get("conc", 4))
    return (Coalescer if spec.get("coalesce") else IndependentWaiter)(prober, sched)


class RowSink:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.f = gzip.open(path, "wt")
        self.n = 0

    def add(self, row: dict) -> None:
        self.f.write(json.dumps(row, separators=(",", ":")) + "\n")
        self.n += 1
        if self.n % 5000 == 0:
            self.f.flush()

    def close(self) -> None:
        self.f.close()


def _round(d: dict) -> dict:
    return {k: (round(v, 3) if isinstance(v, float) else v) for k, v in d.items()}


async def shard_main(spec: dict) -> dict:
    """Run one shard of an open-loop case and return a summary. Executes in a worker process."""
    counter = CommandCounter()
    client = AsyncMongoClient(spec["uri"], event_listeners=[counter], maxPoolSize=400)
    sink = RowSink(Path(spec["out"]))
    shard, nshards = spec["shard"], spec["nshards"]
    entity = spec["entity"]
    coll = client[DB][entity]
    waiter = make_waiter(coll, index_name(entity), spec)
    wp = WritePath(client, entity, waiter, mode=spec["mode"], fail_pct=spec.get("fail_pct", 0.0),
                   seed=spec["seed"] * 1000 + shard)
    rng = random.Random(spec["seed"] * 1000 + shard)
    rate = spec["rate"] / nshards
    warm, dur = spec["warm_s"], spec["duration_s"]
    total = int((warm + dur) * rate)
    op_kind, variant, txn = spec["op"], spec["variant"], spec["txn"]
    pool = spec.get("pool_seqs", [])  # delete cases: documents already created and searchable
    await client.admin.command("ping")
    loop = asyncio.get_running_loop()
    base = loop.time() + max(0.0, spec["start_at"] - time.time())
    wall0 = spec["start_at"]
    rows_failed = 0

    def build(k: int) -> Op:
        from .common import doc_oid

        if op_kind == "insert":
            s = seq_of(spec["uid"], shard, k)
            return Op("insert", doc_oid(s), fast_doc(s))
        if op_kind == "update":
            s = rng.randrange(spec["n_docs"])
            return Op("update", doc_oid(s), touch=k + 1)
        return Op("delete", doc_oid(pool[k % len(pool)]))

    async def one(k: int, sched: float) -> None:
        nonlocal rows_failed
        op = build(k)
        start = loop.time()
        row = {"k": k, "shard": shard, "ts": wall0 + (sched - base), "warm": (sched - base) < warm,
               "lag_ms": round((start - sched) * 1000, 3)}
        try:
            res = await (wp.plain(op, txn) if variant == "plain" else wp.write(op, txn))
            row.update(_round(res))
        except SyncFailure as e:
            row.update(ok=False, err="SyncFailure", total_ms=round((loop.time() - start) * 1000, 3), msg=str(e)[:200])
            rows_failed += 1
        except Exception as e:  # noqa: BLE001
            row.update(ok=False, err=type(e).__name__, total_ms=round((loop.time() - start) * 1000, 3))
            rows_failed += 1
        row["lat_ms"] = round((loop.time() - sched) * 1000, 3)  # from the scheduled slot
        sink.add(row)

    tasks = []
    k = 0
    while k < total:
        now = loop.time()
        due = base + k / rate
        if due > now:
            await asyncio.sleep(due - now)
        while k < total and base + k / rate <= loop.time():
            tasks.append(asyncio.create_task(one(k, base + k / rate)))
            k += 1
    await asyncio.gather(*tasks)
    sink.close()
    snap = counter.snapshot()
    await client.close()
    return {"shard": shard, "ops": total, "failed": rows_failed, **snap}


def _worker(spec: dict) -> dict:
    return asyncio.run(shard_main(spec))


class ShardPool:
    """Persistent worker processes, shared by all cases of a phase."""

    def __init__(self, workers: int) -> None:
        self.ex = ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn"))

    async def run(self, specs: list[dict]) -> list[dict]:
        loop = asyncio.get_running_loop()
        return await asyncio.gather(*(loop.run_in_executor(self.ex, _worker, s) for s in specs))

    def close(self) -> None:
        self.ex.shutdown(wait=True)


def shards_for(rate: float, per_shard: float = 130.0, cap: int = 6) -> int:
    return max(1, min(cap, int(-(-rate // per_shard))))
