"""Harness for the write path, `settle`, `purge` and `reset`.

A minimal stand-in with the pieces the experiments need. Mongo access is duck-typed async pymongo
(`AsyncDatabase`), so it runs on a real server or on a mongomock-motor database. No import of pymongo here: the core must not import an optional client, and the in-memory engine path needs none.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from .contract import (
    DocumentNeverSearchableError,
    DocumentNeverUnsearchableError,
    IndexDef,
    IndexNeverQueryableError,
    SearchEngine,
)

SYNC_COLLECTION = "_kit_sync"


@dataclass(frozen=True)
class EntitySpec:
    name: str  # also the collection name in the harness
    index: IndexDef


@dataclass(frozen=True)
class Timeouts:
    """Total timeout plus interval and backoff. Attempts are derived, never configured."""

    sync: float = 10.0
    index_build: float = 150.0
    initial_interval: float = 0.05
    backoff: float = 1.5
    max_interval: float = 1.0

    def intervals(self, total: float):  # noqa: ANN201
        waited, step = 0.0, self.initial_interval
        while waited < total:
            yield min(step, total - waited)
            waited += min(step, total - waited)
            step = min(step * self.backoff, self.max_interval)


def new_rev() -> Any:
    from bson import ObjectId  # bson ships with pymongo; mongomock depends on it too

    return ObjectId()


class Harness:
    def __init__(self, db: Any, engine: SearchEngine, entities: Sequence[EntitySpec], timeouts: Timeouts | None = None):
        self.db = db
        self.engine = engine
        self.entities = list(entities)
        self.t = timeouts or Timeouts()
        self.log: list[tuple[str, float]] = []  # (step, seconds) of the last lifecycle call

    # ---- init ---------------------------------------------------------------------------------------------
    async def init(self) -> None:
        await asyncio.gather(*(self._ensure_index(e) for e in self.entities))

    async def _create_collection(self, e: EntitySpec) -> None:
        try:
            await self.db.create_collection(e.name)
        except Exception as exc:  # noqa: BLE001  already exists
            if "exist" not in str(exc).lower():
                raise

    async def _ensure_index(self, e: EntitySpec) -> None:
        status = await self.engine.index_status(e.name, e.index.name)
        if status == "missing":
            await self.engine.create_index(e.name, e.index)
        await self.engine.wait_queryable(e.name, e.index.name, self.t.index_build)

    # ---- write path (markers, revision, sync, settle) --------------------------------------------------------
    async def insert(self, e: EntitySpec, docs: list[dict[str, Any]], *, wait: bool = True) -> None:
        for d in docs:
            d["_rev"] = new_rev()
        markers = self.db[SYNC_COLLECTION]
        await markers.bulk_write(  # marker first, data second 
            [_upsert({"entity": e.name, "doc_id": d["_id"]}, {"state": "pending"}) for d in docs]
        ) if docs else None
        await self.db[e.name].insert_many(docs)
        from .data import to_indexable

        await self.engine.sync_write(e.name, e.index.name, to_indexable(docs, e.index))
        if wait:
            await self.settle(e, [(d["_id"], d["_rev"]) for d in docs])
        await markers.delete_many({"entity": e.name, "doc_id": {"$in": [d["_id"] for d in docs]}})

    async def settle(self, e: EntitySpec, pairs: Sequence[tuple[Any, Any]]) -> float:
        """Wait until every (id, rev) is visible, or raise an ExceptionGroup of named per-document errors."""
        t0 = time.perf_counter()
        pending = {i: r for i, r in pairs}
        for step in self.t.intervals(self.t.sync):
            seen = await self.engine.probe(e.name, e.index.name, [(i, r) for i, r in pending.items()])
            for i in seen:
                pending.pop(i, None)
            if not pending:
                return time.perf_counter() - t0
            await asyncio.sleep(step)
        seen = await self.engine.probe(e.name, e.index.name, [(i, r) for i, r in pending.items()])
        for i in seen:
            pending.pop(i, None)
        if not pending:
            return time.perf_counter() - t0
        raise ExceptionGroup(
            "documents never became searchable",
            [DocumentNeverSearchableError(e.name, e.index.name, i, self.t.sync) for i in pending],
        )

    async def settle_absent(self, e: EntitySpec, ids: Sequence[Any]) -> float:
        t0 = time.perf_counter()
        pending = set(ids)
        for step in self.t.intervals(self.t.sync):
            pending = await self.engine.probe_absent(e.name, e.index.name, list(pending))
            if not pending:
                return time.perf_counter() - t0
            await asyncio.sleep(step)
        pending = await self.engine.probe_absent(e.name, e.index.name, list(pending))
        if not pending:
            return time.perf_counter() - t0
        raise ExceptionGroup(
            "documents never left the index",
            [DocumentNeverUnsearchableError(e.name, e.index.name, i, self.t.sync) for i in pending],
        )

    # ---- delete-all and purge ----------------------------------------------------------------------------------
    async def delete_all_unsafe(self, e: EntitySpec) -> None:
        """the legacy `delete_all_entities`: delete and return. No sync, no wait (failure 1)."""
        ids = [d["_id"] async for d in self.db[e.name].find({}, {"_id": 1})]
        await self.db[e.name].delete_many({})
        await self.engine.sync_delete(e.name, e.index.name, ids)  # a no-op on Atlas: the index follows the oplog

    async def purge(self) -> dict[str, float]:
        """Empty every registered entity, leave indexes in place, return when no purged document can be found."""
        t0 = time.perf_counter()
        timings: dict[str, float] = {}

        async def one(e: EntitySpec) -> None:
            ids = [d["_id"] async for d in self.db[e.name].find({}, {"_id": 1})]
            await self.db[e.name].delete_many({})
            await self.engine.sync_delete(e.name, e.index.name, ids)
            await self.db[SYNC_COLLECTION].delete_many({"entity": e.name})
            if ids:
                timings[e.name] = await self.settle_absent(e, ids)

        await asyncio.gather(*(one(e) for e in self.entities))
        timings["total"] = time.perf_counter() - t0
        return timings

    # ---- reset ---------------------------------------------------------------------------------------------
    async def reset(self, *, order: str = "fixed") -> dict[str, float]:
        """order='fixed'  : indexes, then collections, then sync markers, then recreate and wait.
        order='legacy-order' : collections first, indexes after (the 23-minute stall)."""
        steps: dict[str, float] = {}
        t = time.perf_counter()

        async def drop_indexes() -> None:
            await asyncio.gather(*(self.engine.drop_index(e.name, e.index.name) for e in self.entities))
            # a dropped index is gone only when it stops being listed; wait for that, bounded
            for e in self.entities:
                for step in self.t.intervals(self.t.index_build):
                    if await self.engine.index_status(e.name, e.index.name) == "missing":
                        break
                    await asyncio.sleep(step)
                else:
                    raise IndexNeverQueryableError(e.name, e.index.name, self.t.index_build, "drop-pending")

        async def drop_collections() -> None:
            await asyncio.gather(*(self.db[e.name].drop() for e in self.entities))

        if order == "fixed":
            await drop_indexes()
            steps["drop_indexes"] = time.perf_counter() - t
            t = time.perf_counter()
            await drop_collections()
            steps["drop_collections"] = time.perf_counter() - t
        else:  # the legacy original reset: collections dropped, indexes left in place, then init recreates
            await drop_collections()
            steps["drop_collections"] = time.perf_counter() - t
        t = time.perf_counter()
        await self.db[SYNC_COLLECTION].delete_many({})
        steps["clear_markers"] = time.perf_counter() - t
        t = time.perf_counter()
        await asyncio.gather(*(self._create_collection(e) for e in self.entities))  # what init_beanie does
        await self.init()  # create the index when it is missing, then wait until queryable (bounded)
        steps["recreate_and_wait"] = time.perf_counter() - t
        steps["total"] = sum(steps.values())
        return steps


def _upsert(key: dict[str, Any], fields: dict[str, Any]) -> Any:
    from pymongo import UpdateOne  # only the Mongo-backed path needs it; mongomock accepts the same operation

    return UpdateOne(key, {"$set": fields}, upsert=True)
