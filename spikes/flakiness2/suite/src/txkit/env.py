"""Test environments for the three backends, entities, and engine wrappers used by the scenarios.

 A: real MongoDB replica set + real Atlas Search engine (`searchkit.atlas_engine.AtlasEngine`)
 B: real MongoDB replica set + in-memory engine
 C: mongomock-motor (shimmed, `searchkit.mongomock_compat`) + in-memory engine
"""
from __future__ import annotations

import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from beanie import init_beanie
from searchkit.atlas_engine import AtlasEngine
from searchkit.contract import FieldDef, IndexDef, IndexableDoc, SearchEngine
from searchkit.lifecycle import SYNC_COLLECTION, EntitySpec, Harness, Timeouts
from searchkit.memory_engine import InMemoryEngine

from .entity import Entity
from .runtime import Runtime, Stats, set_runtime

DEFAULT_URI = os.environ.get("KIT_TX_MONGO_URI", "mongodb://127.0.0.1:27152/?directConnection=true")
# Optional database-name prefix (plus the xdist worker id) so several workers can share one engine; unset keeps the old names.
DB_PREFIX = os.environ.get("KIT_TX_DB_PREFIX", "")


def _db_name(backend: str) -> str:
    if not DB_PREFIX:
        return f"tx_{backend.lower()}"
    return f"{DB_PREFIX}{os.environ.get('PYTEST_XDIST_WORKER', 'main')}_tx_{backend.lower()}"

ITEM_INDEX = IndexDef(
    "tx_item_idx",
    (
        FieldDef("title", "string"),
        FieldDef("body", "string"),
        FieldDef("category", "token"),
        FieldDef("price", "number"),
        FieldDef("in_stock", "boolean"),
        FieldDef("_rev", "objectId"),
        FieldDef("_id", "objectId"),
    ),
)
NOTE_INDEX = IndexDef(
    "tx_note_idx",
    (FieldDef("title", "string"), FieldDef("_rev", "objectId"), FieldDef("_id", "objectId")),
)


class Item(Entity):
    title: str
    body: str = ""
    category: str = "cat-0"
    price: int = 1
    in_stock: bool = True

    class Settings:
        name = "tx_item"


class Note(Entity):
    title: str

    class Settings:
        name = "tx_note"


SPECS = {"tx_item": EntitySpec("tx_item", ITEM_INDEX), "tx_note": EntitySpec("tx_note", NOTE_INDEX)}


# ---- engine wrappers ------------------------------------------------------------------------------------------


class SpyEngine:
    """Delegates to an engine and records calls: `calls` is the ordered (method, entity, ids) log."""

    def __init__(self, inner: SearchEngine) -> None:
        self.inner = inner
        self.name = inner.name
        self.calls: list[tuple[str, str, tuple[Any, ...]]] = []
        self.never_index: set[Any] = set()  # fault: sync_write silently skips these ids (never searchable)
        self.hide_probe: set[Any] = set()  # fault: probe never reports these ids (for engines that index on their own)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def capabilities(self) -> frozenset[str]:
        return self.inner.capabilities()

    async def sync_write(self, entity: str, index: str, docs: Sequence[IndexableDoc]) -> None:
        self.calls.append(("sync_write", entity, tuple(d.id for d in docs)))
        await self.inner.sync_write(entity, index, [d for d in docs if d.id not in self.never_index])

    async def sync_delete(self, entity: str, index: str, ids: Sequence[Any]) -> None:
        self.calls.append(("sync_delete", entity, tuple(ids)))
        await self.inner.sync_delete(entity, index, ids)

    async def probe(self, entity: str, index: str, pairs: Sequence[tuple[Any, Any]]) -> set[Any]:
        self.calls.append(("probe", entity, tuple(i for i, _ in pairs)))
        return {i for i in await self.inner.probe(entity, index, pairs) if i not in self.hide_probe}

    async def probe_absent(self, entity: str, index: str, ids: Sequence[Any]) -> set[Any]:
        self.calls.append(("probe_absent", entity, tuple(ids)))
        return await self.inner.probe_absent(entity, index, ids)

    def reset_log(self) -> None:
        self.calls.clear()
        self.never_index.clear()
        self.hide_probe.clear()

    def writes_applied(self) -> list[tuple[str, tuple[Any, ...]]]:
        return [(e, ids) for m, e, ids in self.calls if m in ("sync_write", "sync_delete")]


@dataclass
class Env:
    backend: str  # "A" | "B" | "C"
    rt: Runtime
    spy: SpyEngine
    harness: Harness
    client: Any
    models: list[type] = field(default_factory=lambda: [Item, Note])
    notes: dict[str, Any] = field(default_factory=dict)
    default_timeouts: Timeouts = field(default_factory=lambda: Timeouts(sync=15.0, index_build=120.0))

    async def activate(self) -> None:
        """Bind the entities to this backend's database (Beanie binds a class to one database at a time)."""
        await init_beanie(database=self.rt.db, document_models=self.models)
        set_runtime(self.rt)

    def fresh(self) -> None:
        """Reset the per-test state (stats, faults, switches)."""
        self.spy.reset_log()
        self.rt.no_search = False
        self.rt.mock_tx = "snapshot"
        self.rt.timeouts = self.default_timeouts
        self.rt.stats = Stats()
        self.notes = {}

    async def purge(self) -> None:
        self.fresh()
        await self.harness.purge()
        await self.rt.db[SYNC_COLLECTION].delete_many({})  # also dirty / failed markers (purge clears per entity)

    async def close(self) -> None:
        try:
            await self.client.close()
        except TypeError:
            pass  # mongomock-motor close is sync


async def make_env(backend: str, uri: str = DEFAULT_URI, *, db_name: str | None = None, sync_timeout: float = 15.0) -> Env:
    timeouts = Timeouts(sync=sync_timeout, index_build=120.0)
    if backend == "C":
        from searchkit.mongomock_compat import mock_client

        client = mock_client()
        db = client[db_name or _db_name("C")]
        engine: SearchEngine = InMemoryEngine()
        kind = "mock"
    else:
        from pymongo import AsyncMongoClient

        client = AsyncMongoClient(uri, serverSelectionTimeoutMS=10000)
        db = client[db_name or _db_name(backend)]
        engine = AtlasEngine(db) if backend == "A" else InMemoryEngine()
        kind = "mongo"
    spy = SpyEngine(engine)
    harness = Harness(db, spy, list(SPECS.values()), timeouts)
    rt = Runtime(db=db, client=client, engine=spy, specs=dict(SPECS), backend=kind, timeouts=timeouts, harness=harness)
    env = Env(backend, rt, spy, harness, client, default_timeouts=timeouts)
    await env.activate()
    # a clean start whatever an earlier run left behind: indexes, collections, markers, then recreate and wait
    await harness.reset(order="fixed")
    await db[SYNC_COLLECTION].create_index([("entity", 1), ("doc_id", 1)], unique=True)
    return env
