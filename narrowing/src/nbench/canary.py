"""Canary: a fixed tiny query and a fixed CPU loop, sampled at a steady rate throughout every measurement phase.

Every sample is timestamped and written to `canary_<scale>.jsonl` with the current phase tag. Analysis flags
noisy windows from these samples. Nothing is dropped from the raw data.
"""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path

from pymongo import AsyncMongoClient

from .common import DB, append_jsonl, coll_name, oid
from .planner import Opts, Planner, search_stage

PERIOD_S = 0.1


class Canary:
    def __init__(self, uri: str, label: str, path: Path) -> None:
        self.uri, self.label, self.path = uri, label, path
        self.phase = "idle"
        self._stop = threading.Event()
        self._task: asyncio.Task | None = None
        self._thread: threading.Thread | None = None
        self._client: AsyncMongoClient | None = None

    async def start(self) -> None:
        self._client = AsyncMongoClient(self.uri, maxPoolSize=4)  # own client: its own connections
        self._task = asyncio.create_task(self._search_loop())
        self._thread = threading.Thread(target=self._cpu_loop, daemon=True, name="cpu-canary")
        self._thread.start()

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._thread:
            self._thread.join(timeout=2)
        if self._client:
            await self._client.close()

    async def _search_loop(self) -> None:
        assert self._client is not None
        coll = self._client[DB][coll_name(self.label)]
        ids = [oid(i) for i in range(10)]
        pipe = [search_stage(self.label, "frequent", ids, Opts()), {"$limit": 1}]
        while not self._stop.is_set():
            t = time.time()
            t0 = time.perf_counter()
            try:
                await (await coll.aggregate(pipe)).to_list()
                ok = 1
            except Exception:  # noqa: BLE001  recorded as a failed canary sample
                ok = 0
            append_jsonl(self.path, {"t": t, "kind": "search", "ms": (time.perf_counter() - t0) * 1000, "ok": ok, "phase": self.phase})
            await asyncio.sleep(PERIOD_S)

    def _cpu_loop(self) -> None:
        while not self._stop.is_set():
            t = time.time()
            t0 = time.perf_counter()
            x = 0
            for i in range(60_000):  # a fixed pure-Python loop, about 3 ms on an idle core
                x += i * i
            append_jsonl(self.path, {"t": t, "kind": "cpu", "ms": (time.perf_counter() - t0) * 1000, "ok": 1, "phase": self.phase})
            self._stop.wait(PERIOD_S)
