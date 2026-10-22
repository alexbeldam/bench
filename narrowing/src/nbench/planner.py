"""Small mimic of a two-strategy narrowing planner, written for this benchmark only.

Strategy A  : read matching IDs with a `find` on `_id`, search with an ID constraint, in chunks run concurrently
              and merged by score.
Strategy B  : search first with a limit of `factor` times the page size, then a MongoDB post-filter.
Planner     : zero IDs -> empty; up to the cap -> chunked A; above the cap -> B when the filter matches at least
              the threshold share of the entity, else chunked A; a short B page falls back to chunked A.
The selectivity estimate is an early-exit count up to the number of documents the threshold needs.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Any

from pymongo.asynchronous.collection import AsyncCollection

from .common import CAP, CHUNK, INFLIGHT, LIMIT, OVERFETCH, THRESHOLD, TERMS, index_name

FACETS = {
    "low": {"status": {"type": "string", "path": "status", "numBuckets": 10}},
    "high": {"brand": {"type": "string", "path": "brand", "numBuckets": 10}},
}
SCORE = {"$set": {"_score": {"$meta": "searchScore"}}}
FALLBACK_MAX_IDS = 1_000_000


@dataclass(frozen=True)
class Opts:
    """Extra work a real paginate asks for: exact total and a facet ('' / 'low' / 'high')."""

    count: bool = False
    facet: str = ""

    @property
    def label(self) -> str:
        return ("count" if self.count else "plain") + (f"+facet_{self.facet}" if self.facet else "")

    @property
    def meta(self) -> bool:
        return self.count or bool(self.facet)


@dataclass
class Result:
    hits: list[tuple[Any, float]] = field(default_factory=list)
    total: int | None = None
    facets: dict[str, dict[str, int]] | None = None
    info: dict[str, Any] = field(default_factory=dict)


def search_stage(label: str, term: str, ids: list | None, opts: Opts) -> dict:
    compound: dict[str, Any] = {"must": [{"text": {"query": TERMS.get(term, term), "path": ["title", "body"]}}]}
    if ids is not None:
        compound["filter"] = [{"in": {"path": "_id", "value": ids}}]
    stage: dict[str, Any] = {"index": index_name(label)}
    if opts.facet:
        stage["facet"] = {"operator": {"compound": compound}, "facets": FACETS[opts.facet]}
    else:
        stage["compound"] = compound
    if opts.count:
        stage["count"] = {"type": "total"}
    return {"$search": stage}


def _wrap(items: list[dict], opts: Opts) -> list[dict]:
    """Without meta: the items pipeline. With meta: the $facet wrapper that reads $$SEARCH_META."""
    if not opts.meta:
        return items
    return [{"$facet": {"items": items, "meta": [{"$replaceWith": "$$SEARCH_META"}, {"$limit": 1}]}}]


def _read(docs: list[dict], opts: Opts) -> Result:
    if not opts.meta:
        return Result([(d["_id"], d["_score"]) for d in docs])
    top = docs[0] if docs else {"items": [], "meta": []}
    meta = top["meta"][0] if top["meta"] else {}
    res = Result([(d["_id"], d["_score"]) for d in top["items"]])
    if opts.count:
        res.total = meta.get("count", {}).get("total", 0)
    if opts.facet:
        res.facets = {
            name: {b["_id"]: b["count"] for b in f.get("buckets", [])} for name, f in meta.get("facet", {}).items()
        }
    return res


class Planner:
    def __init__(self, coll: AsyncCollection, label: str, n_est: int | None = None) -> None:
        self.coll = coll
        self.label = label
        self.n_est = n_est

    # ---- building blocks -------------------------------------------------------------------------
    async def prefilter(self, flt: dict, limit: int) -> list:
        """Matching IDs, stopping at `limit` (the early-exit read)."""
        return [d["_id"] async for d in self.coll.find(flt, {"_id": 1}).limit(limit)]

    async def estimate_n(self) -> int:
        if self.n_est is None:
            self.n_est = await self.coll.estimated_document_count()
        return self.n_est

    async def _one_chunk(self, term: str, chunk: list, opts: Opts) -> Result:
        pipe = [search_stage(self.label, term, chunk, opts)]
        pipe += _wrap([{"$limit": LIMIT}, SCORE], opts)
        return _read(await (await self.coll.aggregate(pipe)).to_list(), opts)

    async def chunked(self, ids: list, term: str, opts: Opts = Opts(), chunk: int = CHUNK, inflight: int = INFLIGHT) -> Result:
        sem = asyncio.Semaphore(inflight)

        async def one(part: list) -> Result:
            async with sem:
                return await self._one_chunk(term, part, opts)

        parts = await asyncio.gather(*(one(ids[k : k + chunk]) for k in range(0, len(ids), chunk)))
        merged = sorted((h for p in parts for h in p.hits), key=lambda h: -h[1])[:LIMIT]  # stable: ties keep chunk order
        res = Result(merged, info={"chunks": len(parts)})
        if opts.count:
            res.total = sum(p.total or 0 for p in parts)  # chunks are disjoint, so the sum is exact
        if opts.facet:
            acc: dict[str, dict[str, int]] = {}
            for p in parts:
                for name, buckets in (p.facets or {}).items():
                    tgt = acc.setdefault(name, {})
                    for k, v in buckets.items():
                        tgt[k] = tgt.get(k, 0) + v
            res.facets = {name: dict(sorted(b.items(), key=lambda kv: -kv[1])[:10]) for name, b in acc.items()}
        return res

    async def search_first(self, flt: dict, term: str, factor: int = OVERFETCH, opts: Opts = Opts()) -> Result:
        items = [{"$limit": LIMIT * factor}, {"$match": flt}, {"$limit": LIMIT}, SCORE]
        pipe = [search_stage(self.label, term, None, opts)] + _wrap(items, opts)
        res = _read(await (await self.coll.aggregate(pipe)).to_list(), opts)
        # NOTE: with meta, total and facets describe the unfiltered search matches, not the filtered set.
        return res

    # ---- the whole planner -----------------------------------------------------------------------
    async def plan(self, flt: dict, term: str, *, cap: int = CAP, threshold: float = THRESHOLD, factor: int = OVERFETCH,
                   chunk: int = CHUNK, inflight: int = INFLIGHT, opts: Opts = Opts()) -> Result:
        t0 = time.perf_counter()
        ids = await self.prefilter(flt, cap + 1)
        t1 = time.perf_counter()
        info: dict[str, Any] = {"prefilter_ms": (t1 - t0) * 1000}
        if not ids:
            return Result(info={**info, "strategy": "zero"})
        if len(ids) <= cap:
            res = await self.chunked(ids, term, opts, chunk, inflight)
            res.info.update(info, strategy="A", ids=len(ids))
            return res
        n = await self.estimate_n()
        need = math.ceil(threshold * n)
        if len(ids) >= need:
            sel_ok = True
        else:
            c0 = time.perf_counter()
            sel_ok = await self.coll.count_documents(flt, limit=need) >= need
            info["estimate_ms"] = (time.perf_counter() - c0) * 1000
        if sel_ok:
            res = await self.search_first(flt, term, factor, opts)
            if len(res.hits) >= LIMIT:
                res.info.update(info, strategy="B")
                return res
            info["fallback"] = True
        ids = await self.prefilter(flt, FALLBACK_MAX_IDS if sel_ok else need)
        res = await self.chunked(ids, term, opts, chunk, inflight)
        res.info.update(info, strategy="A_fallback" if sel_ok else "A_lowsel", ids=len(ids))
        return res
