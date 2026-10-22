"""Canary: a tiny fixed workload in its own process, sampled at a steady rate with timestamps.

Two probes per tick, independent of the cases under test:
- `cpu`: a fixed CPU-bound loop (reveals CPU steal and contention on the runner);
- `db`: a primary-key `find_one` on a tiny collection, straight to the database (no proxy).
Samples go to `canary.jsonl` in the phase directory. The analysis flags windows where the canary is well above
its own median and reports results with and without them; nothing is dropped from the raw data.
"""

import json
import multiprocessing as mp
import time
from pathlib import Path

PERIOD_S = 0.25


def _cpu_unit() -> float:
    t = time.perf_counter()
    x = 0
    for i in range(150_000):
        x += i * i % 7
    return (time.perf_counter() - t) * 1000.0


def _run(uri: str, db: str, path: str, stop) -> None:
    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    coll = client[db]["_canary"]
    coll.replace_one({"_id": 1}, {"_id": 1, "v": 1}, upsert=True)
    with open(path, "a", buffering=1) as f:
        nxt = time.time()
        while not stop.is_set():
            ts = time.time()
            cpu = _cpu_unit()
            t = time.perf_counter()
            try:
                coll.find_one({"_id": 1})
                dbms = (time.perf_counter() - t) * 1000.0
            except Exception:  # noqa: BLE001
                dbms = float("nan")
            f.write(json.dumps({"ts": ts, "cpu_ms": round(cpu, 3), "db_ms": round(dbms, 3)}) + "\n")
            nxt += PERIOD_S
            time.sleep(max(0.0, nxt - time.time()))
    client.close()


class Canary:
    def __init__(self, uri: str, db: str, path: Path) -> None:
        self.args = (uri, db, str(path))
        self.stop = mp.get_context("spawn").Event()
        self.proc = mp.get_context("spawn").Process(target=_run, args=(*self.args, self.stop), daemon=True)

    def __enter__(self):
        Path(self.args[2]).parent.mkdir(parents=True, exist_ok=True)
        self.proc.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.proc.join(timeout=10)
        if self.proc.is_alive():
            self.proc.terminate()
