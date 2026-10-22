"""Deterministic synthetic data (compact shape), resumable loader and search-index builder."""

import asyncio
import time

import numpy as np
from pymongo import AsyncMongoClient
from pymongo.errors import OperationFailure
from pymongo.operations import SearchIndexModel

from .common import DB, REV, SCALES, SEED, SYNC, check_disk, free_gb, doc_oid, entity_name, index_name, seed_rev

VOCAB = 2000
BATCH = 10_000
GROUP = 4
WORDS = [f"w{k:04d}" for k in range(VOCAB)]
_cdf = np.cumsum(1.0 / np.arange(1, VOCAB + 1))
CDF = _cdf / _cdf[-1]

# The index holds _id, the reserved revision field (objectId) and two text fields.
SEARCH_DEFINITION = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "_id": {"type": "objectId"},
            REV: {"type": "objectId"},
            "title": {"type": "string"},
            "body": {"type": "string"},
        },
    }
}
# Same index without the revision field: a probe can never succeed against it (failure tests).
BLIND_DEFINITION = {
    "mappings": {"dynamic": False, "fields": {"_id": {"type": "objectId"}, "title": {"type": "string"}}}
}


def make_docs(start: int, count: int, kind: int = 0) -> list[dict]:
    rng = np.random.default_rng([SEED, kind, start])
    idx = np.searchsorted(CDF, rng.random((count, 10))).tolist()
    return [
        {
            "_id": doc_oid(start + r),
            "seq": start + r,
            REV: seed_rev(start + r),
            "touch": 0,
            "title": " ".join(WORDS[k] for k in row[:3]),
            "body": " ".join(WORDS[k] for k in row[3:]),
        }
        for r, row in enumerate(idx)
    ]


async def load(client: AsyncMongoClient, coll_name: str, n: int, label: str) -> None:
    """Insert documents 0..n-1, recording progress so that an interrupted load resumes."""
    coll = client[DB][coll_name]
    meta = client[DB]["_wp_meta"]
    key = f"load:{coll_name}"
    state = await meta.find_one({"_id": key}) or {"loaded": 0, "done": False}
    if state["done"]:
        print(f"[{label}] {coll_name}: already loaded ({n:,})", flush=True)
        return
    start = state["loaded"]
    if start:
        await coll.delete_many({"seq": {"$gte": start, "$lt": 10**9}})
        print(f"[{label}] {coll_name}: resuming from {start:,}", flush=True)
    t0 = time.perf_counter()
    while start < n:
        starts = list(range(start, min(start + GROUP * BATCH, n), BATCH))
        batches = await asyncio.gather(*(asyncio.to_thread(make_docs, s, min(BATCH, n - s)) for s in starts))
        await asyncio.gather(*(coll.insert_many(b, ordered=False) for b in batches))
        start = starts[-1] + len(batches[-1])
        await meta.replace_one({"_id": key}, {"_id": key, "loaded": start, "done": False}, upsert=True)
    await meta.replace_one({"_id": key}, {"_id": key, "loaded": n, "done": True}, upsert=True)
    print(f"[{label}] loaded {n:,} docs in {time.perf_counter() - t0:.0f}s", flush=True)


def _search_service_starting(e: Exception) -> bool:
    # Right after the container answers, mongot may still be starting: the server cannot reach the search
    # index management service yet. It is transient, unlike any other error.
    return isinstance(e, OperationFailure) and "Search Index Management service" in str(e)


async def _index_states(coll, name: str) -> list[dict]:
    """The named index's state documents, waiting (up to about 3 minutes) for the search service to come up."""
    for attempt in range(90):
        try:
            return [ix async for ix in await coll.list_search_indexes(name)]
        except OperationFailure as e:
            if not _search_service_starting(e) or attempt == 89:
                raise
            await asyncio.sleep(2)
    return []


async def wait_queryable(coll, name: str, timeout_s: float = 6 * 3600) -> float:
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout_s:
        if any(ix.get("queryable") for ix in await _index_states(coll, name)):
            return time.perf_counter() - t0
        await asyncio.sleep(2)
    raise TimeoutError(f"search index {name} not queryable after {timeout_s:.0f}s")


async def ensure_index(coll, name: str, definition: dict) -> float | None:
    """Create the search index if absent and wait until queryable. Returns the wait in seconds when it had to wait."""
    states = await _index_states(coll, name)
    if any(ix.get("queryable") for ix in states):
        return None
    t0 = time.perf_counter()
    if not states:
        for attempt in range(90):
            try:
                await coll.create_search_index(SearchIndexModel(definition=definition, name=name))
                break
            except OperationFailure as e:
                if not _search_service_starting(e) or attempt == 89:
                    raise
                await asyncio.sleep(2)
    await wait_queryable(coll, name)
    return time.perf_counter() - t0


async def ensure_sync_collection(db) -> None:
    sync = db[SYNC]
    await sync.create_index([("entity", 1), ("doc_id", 1)], unique=True, name="entity_doc")
    await sync.create_index([("entity", 1), ("state", 1)], name="entity_state")


async def prepare(client: AsyncMongoClient, label: str, extra_blind: bool = True) -> dict:
    """Load the main entity, build its index, create the marker collection and (optionally) the blind entity."""
    n = SCALES[label]
    db = client[DB]
    name = entity_name(label)
    out: dict = {"scale": label, "documents": n}
    check_disk("before load")
    out["free_gb"] = {"before_load": round(free_gb(), 2)}
    await load(client, name, n, label)
    check_disk("after load")
    out["free_gb"]["after_load"] = round(free_gb(), 2)
    await ensure_sync_collection(db)
    secs = await ensure_index(db[name], index_name(name), SEARCH_DEFINITION)
    out["index_wait_s"] = secs
    check_disk("after index build")
    out["free_gb"]["after_index"] = round(free_gb(), 2)
    if extra_blind:
        blind = f"blind_{label}"
        await load(client, blind, min(n, 2000), label)
        await ensure_index(db[blind], index_name(blind), BLIND_DEFINITION)
    return out
