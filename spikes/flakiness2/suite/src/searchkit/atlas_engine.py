"""Adapter for Atlas Search over a pymongo async database (a real engine, not a mock).

This adapter is the built-in engine here. It exists so the in-memory engine can be
compared with the real one through the same `SearchEngine` contract.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from typing import Any

from pymongo.asynchronous.database import AsyncDatabase
from pymongo.operations import SearchIndexModel

from .contract import (
    COMMON_CLAUSES,
    Clause,
    Compound,
    Equals,
    Hit,
    In,
    IndexDef,
    IndexableDoc,
    IndexNeverQueryableError,
    IndexNotFoundError,
    Phrase,
    Query,
    Range,
    SortKey,
    SearchResult,
    Text,
)
from .data import atlas_definition


def compile_clause(c: Clause) -> dict[str, Any]:
    match c:
        case Text():
            t: dict[str, Any] = {"query": c.query, "path": list(c.paths) if len(c.paths) > 1 else c.paths[0]}
            if c.max_edits:
                t["fuzzy"] = {"maxEdits": c.max_edits}
            return {"text": t}
        case Phrase():
            return {"phrase": {"query": c.query, "path": c.path, "slop": c.slop}}
        case Equals():
            return {"equals": {"path": c.path, "value": c.value}}
        case In():
            return {"in": {"path": c.path, "value": list(c.values)}}
        case Range():
            r: dict[str, Any] = {"path": c.path}
            for k in ("gt", "gte", "lt", "lte"):
                v = getattr(c, k)
                if v is not None:
                    r[k] = v
            return {"range": r}
        case Compound():
            out: dict[str, Any] = {}
            for k, v in (("must", c.must), ("should", c.should), ("filter", c.filter), ("mustNot", c.must_not)):
                if v:
                    out[k] = [compile_clause(x) for x in v]
            if c.minimum_should_match:
                out["minimumShouldMatch"] = c.minimum_should_match
            return {"compound": out}
    raise TypeError(type(c))


class AtlasEngine:
    name = "atlas"

    def __init__(self, db: AsyncDatabase, *, poll: float = 0.25, order: str = "total") -> None:
        """`order`: how the total order is produced.
        'native'  : $search order only (score desc, engine tie order). Not a total order.
        'total'   : the $search `sort` option {keys, _id: 1} (default).
        'post'    : $sort {score desc, [sort keys], _id asc} after $search (exact, sorts every match).
        """
        self.db = db
        self.poll = poll
        self.order = order

    def capabilities(self) -> frozenset[str]:
        return COMMON_CLAUSES

    def _coll(self, entity: str):  # noqa: ANN202
        return self.db[entity]

    async def create_index(self, entity: str, index: IndexDef, definition: dict[str, Any] | None = None) -> None:
        await self._coll(entity).create_search_index(
            SearchIndexModel(definition=definition or atlas_definition(index), name=index.name)
        )

    async def update_index(self, entity: str, index: IndexDef) -> None:
        await self._coll(entity).update_search_index(index.name, atlas_definition(index))

    async def drop_index(self, entity: str, index: str) -> None:
        try:
            await self._coll(entity).drop_search_index(index)
        except Exception as exc:  # noqa: BLE001
            if "not found" in str(exc).lower() or "NamespaceNotFound" in str(exc) or "does not exist" in str(exc):
                return
            raise

    async def _describe(self, entity: str, index: str) -> dict[str, Any] | None:
        async for ix in await self._coll(entity).list_search_indexes(index):
            return ix
        return None

    async def index_status(self, entity: str, index: str) -> str:
        ix = await self._describe(entity, index)
        if ix is None:
            return "missing"
        if ix.get("queryable"):
            return "queryable"
        st = str(ix.get("status", "")).upper()
        return "failed" if st == "FAILED" else "building"

    async def wait_queryable(self, entity: str, index: str, timeout: float) -> None:
        t0 = time.perf_counter()
        status = "missing"
        while True:
            status = await self.index_status(entity, index)
            if status == "queryable":
                return
            if time.perf_counter() - t0 >= timeout:
                raise IndexNeverQueryableError(entity, index, timeout, status)
            await asyncio.sleep(self.poll)

    async def sync_write(self, entity: str, index: str, docs: Sequence[IndexableDoc]) -> None:
        return None  # the index follows MongoDB on its own

    async def sync_delete(self, entity: str, index: str, ids: Sequence[Any]) -> None:
        return None

    async def probe(self, entity: str, index: str, pairs: Sequence[tuple[Any, Any]]) -> set[Any]:
        """probe by (id, revision) together, batched <= 500 pairs. The earlier measurement probed by
        revision alone proposal), which cannot tell documents apart when one update stamped the same revision
        on many documents."""
        seen: set[Any] = set()
        want = dict(pairs)
        items = list(pairs)
        for k in range(0, len(items), 500):
            chunk = items[k : k + 500]
            pipeline = [
                {
                    "$search": {
                        "index": index,
                        "compound": {
                            "filter": [
                                {"in": {"path": "_id", "value": [i for i, _ in chunk]}},
                                {"in": {"path": "_rev", "value": [r for _, r in chunk]}},
                            ]
                        },
                    }
                },
                {"$project": {"_rev": 1}},
            ]
            async for d in await self._coll(entity).aggregate(pipeline):
                if want.get(d["_id"]) == d["_rev"]:
                    seen.add(d["_id"])
        return seen

    async def _meta_count(self, entity: str, index: str, ids: Sequence[Any]) -> int:
        pl = [{"$searchMeta": {"index": index, "in": {"path": "_id", "value": list(ids)}, "count": {"type": "total"}}}]
        async for d in await self._coll(entity).aggregate(pl):
            return d["count"]["total"]
        return 0

    async def probe_absent(self, entity: str, index: str, ids: Sequence[Any]) -> set[Any]:
        """Ids still present in the *index*. `$search` hydrates hits from MongoDB, so a deleted document never shows
        up as a hit while its index entry lingers; only `$searchMeta` counts see it (measured). So count, and
        bisect any non-empty batch down to the ids that are still indexed."""
        present: set[Any] = set()

        async def walk(batch: list[Any]) -> None:
            if not batch or await self._meta_count(entity, index, batch) == 0:
                return
            if len(batch) == 1:
                present.add(batch[0])
                return
            mid = len(batch) // 2
            await walk(batch[:mid])
            await walk(batch[mid:])

        ids = list(ids)
        for k in range(0, len(ids), 1000):
            await walk(ids[k : k + 1000])
        return present

    def pipeline(self, index: str, q: Query, *, with_score: bool = True) -> list[dict[str, Any]]:
        compound = compile_clause(q.root)
        if self.order == "total":  # the engine sorts: explicit keys, score, then _id (measured: same cost as no key)
            keys: dict[str, Any] = {}
            for s in q.sort or (SortKey("score", -1),):
                keys[s.path] = {"$meta": "searchScore"} if s.path == "score" else s.direction
                if s.path == "score" and s.direction > 0:
                    raise ValueError("ascending score sort is not supported by the engine sort option")
            keys["_id"] = 1
            pl = [{"$search": {"index": index, **compound, "sort": keys}}, {"$set": {"_score": {"$meta": "searchScore"}}}]
            if q.skip:
                pl.append({"$skip": q.skip})
            pl += [{"$limit": q.limit}, {"$project": {"_id": 1, "_score": 1}}]
            return pl
        pl = [{"$search": {"index": index, **compound}}]
        pl.append({"$set": {"_score": {"$meta": "searchScore"}}})
        if self.order == "native" and not q.sort:
            pl.append({"$skip": q.skip}) if q.skip else None
            pl.append({"$limit": q.limit})
        else:
            keys: dict[str, int] = {}
            for s in q.sort or ():
                keys["_score" if s.path == "score" else s.path] = s.direction
            if not keys:
                keys["_score"] = -1
            keys["_id"] = 1
            pl.append({"$sort": keys})
            if q.skip:
                pl.append({"$skip": q.skip})
            pl.append({"$limit": q.limit})
        pl.append({"$project": {"_id": 1, "_score": 1}})
        return pl

    async def search(self, entity: str, index: str, query: Query) -> SearchResult:
        if await self.index_status(entity, index) == "missing":
            raise IndexNotFoundError(index)
        coll = self._coll(entity)
        compound = compile_clause(query.root)
        hits = [Hit(d["_id"], d["_score"]) async for d in await coll.aggregate(self.pipeline(index, query))]
        meta = [
            d
            async for d in await coll.aggregate(
                [{"$searchMeta": {"index": index, **compound, "count": {"type": "total"}}}]
            )
        ]
        total = meta[0]["count"]["total"] if meta else 0
        return SearchResult(hits, total)
