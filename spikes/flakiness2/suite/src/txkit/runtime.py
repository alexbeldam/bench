"""Runtime state, markers, and the single batched wait (`finalize`) used by `@synced`, `defer()` and `@transactional`.

Design points (all observed in the harness):
 * A write records the ids it *touched* (before the data write). It does not carry document contents.
 * `finalize` runs after commit (or right after a non-transactional write): it re-reads the touched ids from the
   committed data, applies the engine changes in a canonical order (entity name, then id), then waits ONCE for all
   of them, then clears settled markers and marks unsettled ones `failed`.
 * Atlas needs no engine call (`sync_write` is a no-op there); the probe is the same call. So the scope has one code
   path for every engine: only the engine object differs.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from searchkit.contract import DocumentNeverSearchableError, DocumentNeverUnsearchableError, SearchEngine
from searchkit.data import to_indexable
from searchkit.lifecycle import SYNC_COLLECTION, EntitySpec, Harness, Timeouts

from .errors import NotInitializedError


class Pending:
    """Ids touched by writes in one scope or batch. `log` keeps the append order (it depends on task scheduling)."""

    def __init__(self) -> None:
        self.ids: dict[str, set[Any]] = defaultdict(set)
        self.log: list[tuple[str, Any]] = []

    def add(self, entity: str, ids: list[Any]) -> None:
        for i in ids:
            self.ids[entity].add(i)
            self.log.append((entity, i))

    def clear(self) -> None:
        self.ids.clear()
        self.log.clear()

    def __bool__(self) -> bool:
        return any(self.ids.values())


@dataclass
class Stats:
    waits: int = 0  # settle loops started (one per finalize that had work)
    probes: int = 0
    scopes_opened: int = 0
    attempts: int = 0
    finalizes: int = 0


@dataclass
class Runtime:
    db: Any
    client: Any
    engine: SearchEngine
    specs: dict[str, EntitySpec]  # keyed by collection name
    backend: str  # "mongo" (real driver, sessions) or "mock" (mongomock: no sessions)
    timeouts: Timeouts = field(default_factory=lambda: Timeouts(sync=10.0, index_build=60.0))
    no_search: bool = False
    mock_tx: str = "snapshot"  # "snapshot" (design iii) or "refuse" (design i) when backend == "mock"
    stats: Stats = field(default_factory=Stats)
    mock_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    harness: Harness | None = None

    def spec_for_class(self, cls: type) -> EntitySpec:
        return self.specs[cls.get_collection_name()]  # type: ignore[attr-defined]


_RT: Runtime | None = None


def set_runtime(rt: Runtime | None) -> None:
    global _RT
    _RT = rt


def get_runtime() -> Runtime:
    if _RT is None:
        raise NotInitializedError("no runtime")
    return _RT


# ---- markers -----------------------------------------------------------------------------------------


async def mark_pending(rt: Runtime, entity: str, ids: list[Any], session: Any) -> None:
    """Upsert one `pending` marker per id, with the data's session, so the marker commits or rolls back with the data.
    Sequential on purpose: a transaction's first commands must not be fanned out."""
    if not ids:
        return
    coll = rt.db[SYNC_COLLECTION]
    now = datetime.now(UTC)
    if rt.backend == "mongo":
        from pymongo import UpdateOne

        await coll.bulk_write(
            [UpdateOne({"entity": entity, "doc_id": i}, {"$set": {"state": "pending", "ts": now}}, upsert=True) for i in ids],
            session=session,
        )
    else:  # mongomock: bulk upsert is broken there, so one update_one each (a labelled stand-in)
        for i in ids:
            await coll.update_one({"entity": entity, "doc_id": i}, {"$set": {"state": "pending", "ts": now}}, upsert=True)


async def mark_dirty(rt: Runtime, entity: str, session: Any) -> None:
    """Skip-sync bulk path: one per-entity dirty marker, D14)."""
    kw = {"session": session} if rt.backend == "mongo" else {}
    await rt.db[SYNC_COLLECTION].update_one(
        {"entity": entity, "kind": "entity"}, {"$set": {"state": "dirty", "ts": datetime.now(UTC)}}, upsert=True, **kw
    )


async def _clear_markers(rt: Runtime, entity: str, settled: list[Any], failed: list[Any]) -> None:
    coll = rt.db[SYNC_COLLECTION]
    if settled:
        await coll.delete_many({"entity": entity, "doc_id": {"$in": settled}})
    if failed:
        await coll.update_many(
            {"entity": entity, "doc_id": {"$in": failed}},
            {"$set": {"state": "failed", "error": "DocumentNeverSearchableError"}},
        )


# ---- the one wait ---------------------------------------------------------------------------------------------


async def settle_all(
    rt: Runtime, writes: dict[str, list[tuple[Any, Any]]], deletes: dict[str, list[Any]]
) -> tuple[dict[str, set[Any]], dict[str, set[Any]]]:
    """One bounded wait for every (id, rev) to be visible and every deleted id to be gone, over all entities.
    Returns the unsettled ids as (unsettled writes, unsettled deletes), by entity. Empty means all settled."""
    t = rt.timeouts
    engine = rt.engine
    pw = {e: dict(p) for e, p in writes.items() if p}
    pd = {e: set(i) for e, i in deletes.items() if i}
    if not pw and not pd:
        return {}, {}
    rt.stats.waits += 1

    async def probe_round() -> None:
        for e in sorted(pw):
            if pw[e]:
                rt.stats.probes += 1
                seen = await engine.probe(e, rt.specs[e].index.name, list(pw[e].items()))
                for i in seen:
                    pw[e].pop(i, None)
        for e in sorted(pd):
            if pd[e]:
                rt.stats.probes += 1
                still = await engine.probe_absent(e, rt.specs[e].index.name, list(pd[e]))
                pd[e] = set(still)

    def done() -> bool:
        return not any(pw.values()) and not any(pd.values())

    for step in t.intervals(t.sync):
        await probe_round()
        if done():
            return {}, {}
        await asyncio.sleep(step)
    await probe_round()
    if done():
        return {}, {}
    return {e: set(p) for e, p in pw.items() if p}, {e: set(i) for e, i in pd.items() if i}


async def finalize(rt: Runtime, pending: Pending) -> None:
    """Apply the engine changes for every touched id (canonical order), wait once, settle the markers.
    Raises an ExceptionGroup naming only the unsettled documents. Data is never touched here."""
    if not pending:
        return
    rt.stats.finalizes += 1
    ids_by_entity = {e: sorted(ids) for e, ids in pending.ids.items() if ids}
    if rt.no_search:  #: verification skipped for the whole process
        for e, ids in ids_by_entity.items():
            await _clear_markers(rt, e, ids, [])
        return
    writes: dict[str, list[tuple[Any, Any]]] = {}
    deletes: dict[str, list[Any]] = {}
    for e in sorted(ids_by_entity):
        spec = rt.specs[e]
        ids = ids_by_entity[e]
        docs = await rt.db[e].find({"_id": {"$in": ids}}).to_list(None)
        docs.sort(key=lambda d: d["_id"])
        present = {d["_id"] for d in docs}
        gone = [i for i in ids if i not in present]
        if docs:
            await rt.engine.sync_write(e, spec.index.name, to_indexable(docs, spec.index))
        if gone:
            await rt.engine.sync_delete(e, spec.index.name, gone)
        writes[e] = [(d["_id"], d.get("_rev")) for d in docs]
        deletes[e] = gone
    bad_w, bad_d = await settle_all(rt, writes, deletes)
    errors: list[Exception] = []
    for e in sorted(ids_by_entity):
        bad = bad_w.get(e, set()) | bad_d.get(e, set())
        ok = [i for i in ids_by_entity[e] if i not in bad]
        await _clear_markers(rt, e, ok, sorted(bad))
        idx = rt.specs[e].index.name
        errors += [DocumentNeverSearchableError(e, idx, i, rt.timeouts.sync) for i in sorted(bad_w.get(e, ()))]
        errors += [DocumentNeverUnsearchableError(e, idx, i, rt.timeouts.sync) for i in sorted(bad_d.get(e, ()))]
    if errors:
        raise ExceptionGroup("documents never became searchable", errors)
