"""Deterministic fault injection for the in-memory engine: a virtual clock, an engine wrapper with controllable index lag,
and a model of the ghost index (a collection dropped while its search index still exists).

Everything here is a stand-in written for these tests. Time is virtual: `asyncio.sleep` is replaced by a function
that advances a clock and yields once, so a 4 s wait costs microseconds and gives the same result on every machine. The lag
values are inputs; the ones used in the tests come from `results/visibility_lag-*.json` (measured on the real engine).
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from searchkit.contract import IndexDef, IndexableDoc, IndexNeverQueryableError, Query, SearchResult
from searchkit.memory_engine import InMemoryEngine

_REAL_SLEEP = asyncio.sleep


class VClock:
    """Virtual seconds. Install with the `vclock` fixture, which replaces asyncio.sleep for the duration of a test."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    async def sleep(self, delay: float, result: Any = None) -> Any:
        self.sleeps.append(delay)
        self.now += max(delay, 0.0)
        await _REAL_SLEEP(0)  # let other tasks run, never really wait
        return result


@dataclass
class _Op:
    due: float
    seq: int
    kind: str  # "write" | "delete"
    entity: str
    index: str
    payload: Any


class LagEngine:
    """Wraps an `InMemoryEngine`. Writes and deletes reach the index `lag` virtual seconds after they are synced, so the
    index can be behind the data, as the real engine is. `search` hydrates hits from the data (a deleted document is never
    a hit) but reports the total from the index, which is what the real `$search` / `$searchMeta` pair does."""

    def __init__(
        self,
        inner: InMemoryEngine,
        clock: VClock,
        live_ids: Callable[[str, Sequence[Any]], Awaitable[set[Any]]] | None = None,
    ) -> None:
        self.inner = inner
        self.clock = clock
        self.name = "memory+lag"
        self.live_ids = live_ids  # async (entity, ids) -> ids still present in the data (the hydration step)
        self.write_lag = 0.0
        self.delete_lag = 0.0
        self.write_lag_by_id: dict[Any, float] = {}
        self.never_index: set[Any] = set()
        self._q: list[_Op] = []
        self._seq = 0
        self.ghost: set[tuple[str, str]] = set()  # (entity, index) left behind by a collection drop

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    # ---- lag machinery -------------------------------------------------------------------------------------
    async def _advance(self) -> None:
        due = sorted((o for o in self._q if o.due <= self.clock.now), key=lambda o: (o.due, o.seq))
        for o in due:
            self._q.remove(o)
            if o.kind == "write":
                await self.inner.sync_write(o.entity, o.index, o.payload)
            else:
                await self.inner.sync_delete(o.entity, o.index, o.payload)

    def _enqueue(self, kind: str, entity: str, index: str, payload: Any, lag: float) -> None:
        self._seq += 1
        self._q.append(_Op(self.clock.now + lag, self._seq, kind, entity, index, payload))

    def release_all(self) -> None:
        """Make every pending change due now (the 'unpause index updates' control)."""
        for o in self._q:
            o.due = self.clock.now

    # ---- index lifecycle (ghost model) --------------------------------------------------------------------
    async def create_index(self, entity: str, index: IndexDef) -> None:
        if (entity, index.name) in self.ghost:
            return  # accepted and silently never materialises: the behaviour observed on the real engine
        await self.inner.create_index(entity, index)

    async def drop_index(self, entity: str, index: str) -> None:
        self.ghost.discard((entity, index))
        await self.inner.drop_index(entity, index)

    async def index_status(self, entity: str, index: str) -> str:
        if (entity, index) in self.ghost:
            return "missing"
        return await self.inner.index_status(entity, index)

    async def wait_queryable(self, entity: str, index: str, timeout: float) -> None:
        status = await self.index_status(entity, index)
        if status != "queryable":
            await self.clock.sleep(timeout)  # a real wait spends the whole bound before giving up
            raise IndexNeverQueryableError(entity, index, timeout, status)

    def collection_dropped(self, entity: str) -> None:
        """Called when the data collection is dropped. An index that still exists becomes a ghost."""
        for e, ix in list(self.inner._indexes):
            if e == entity:
                self.inner._indexes.pop((e, ix))
                self.ghost.add((e, ix))

    # ---- sync, probes, search ----------------------------------------------------------------------------------
    async def sync_write(self, entity: str, index: str, docs: Sequence[IndexableDoc]) -> None:
        for d in docs:
            if d.id in self.never_index:
                continue
            self._enqueue("write", entity, index, [d], self.write_lag_by_id.get(d.id, self.write_lag))
        await self._advance()

    async def sync_delete(self, entity: str, index: str, ids: Sequence[Any]) -> None:
        self._enqueue("delete", entity, index, list(ids), self.delete_lag)
        await self._advance()

    async def probe(self, entity: str, index: str, pairs: Sequence[tuple[Any, Any]]) -> set[Any]:
        await self._advance()
        return await self.inner.probe(entity, index, pairs)

    async def probe_absent(self, entity: str, index: str, ids: Sequence[Any]) -> set[Any]:
        """Count-style probe: sees index entries whether or not the data still has the document."""
        await self._advance()
        return await self.inner.probe_absent(entity, index, ids)

    async def probe_absent_by_hits(self, entity: str, index: str, ids: Sequence[Any]) -> set[Any]:
        """The OLD probe: asks `$search` for the ids. Hits are hydrated from the data, so a deleted document never appears,
        however long its index entry lingers earlier measurement."""
        await self._advance()
        in_index = await self.inner.probe_absent(entity, index, ids)
        live = await self.live_ids(entity, list(in_index)) if self.live_ids else in_index
        return set(in_index) & set(live)

    async def search(self, entity: str, index: str, query: Query) -> SearchResult:
        await self._advance()
        res = await self.inner.search(entity, index, query)
        if self.live_ids is not None and res.hits:
            live = await self.live_ids(entity, [h.id for h in res.hits])
            res = SearchResult([h for h in res.hits if h.id in live], res.total)  # hits hydrated, total from the index
        return res


class DropWatch:
    """A database whose collections report `drop()` to the engine, so the ghost model can see it."""

    def __init__(self, db: Any, engine: LagEngine) -> None:
        self._db, self._engine = db, engine

    def __getitem__(self, name: str) -> Any:
        coll = self._db[name]
        engine = self._engine

        class _Coll:
            def __getattr__(self, attr: str) -> Any:
                return getattr(coll, attr)

            async def drop(self) -> None:
                await coll.drop()
                engine.collection_dropped(name)

        return _Coll()

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._db, attr)
