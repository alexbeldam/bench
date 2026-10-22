"""Deterministic synthetic data (compact shape), resumable loader and search-index builder."""

from __future__ import annotations

import asyncio
import json
import shutil
import time

import numpy as np
from pymongo import AsyncMongoClient
from pymongo.errors import OperationFailure
from pymongo.operations import SearchIndexModel
from tqdm import tqdm

from .common import (
    DB, SCALES, SEED_DATA, coll_name, index_name, oid, rank_of, run_dir, tenant_of, tenant_thresholds,
)

VOCAB = 5000
TITLE_WORDS = 3
BODY_WORDS = 10  # compact shape: shorter text than the first benchmark, used at every scale
BATCH = 10_000
GROUP = 4
WORDS = [f"w{k:04d}" for k in range(VOCAB)]
_cdf = np.cumsum(1.0 / np.arange(1, VOCAB + 1))
CDF = _cdf / _cdf[-1]
N_BRANDS = 20_000  # high-cardinality facet field
MIN_FREE_BYTES = int(1.5 * 1024**3)

SEARCH_DEFINITION = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "_id": {"type": "objectId"},
            "title": {"type": "string"},
            "body": {"type": "string"},
            "status": {"type": "stringFacet"},
            "brand": {"type": "stringFacet"},
        },
    }
}


def make_docs(n: int, start: int, count: int) -> list[dict]:
    rng = np.random.default_rng([SEED_DATA, start])
    idx = np.searchsorted(CDF, rng.random((count, TITLE_WORDS + BODY_WORDS))).tolist()
    thresholds = tenant_thresholds(n)
    out = []
    for r in range(count):
        i = start + r
        row = idx[r]
        body = " ".join(WORDS[k] for k in row[TITLE_WORDS:])
        if i % 50 == 7:
            body += " zzmid"
        if i % 1000 == 13:
            body += " zzrare"
        out.append(
            {
                "_id": oid(i),
                "seq": i,
                "rank": rank_of(i, n),
                "tenant": tenant_of(i, n, thresholds),
                "status": f"s{(i * 2654435761) % 5}",
                "brand": f"b{(i * 40503) % N_BRANDS:05d}",
                "title": " ".join(WORDS[k] for k in row[:TITLE_WORDS]),
                "body": body,
            }
        )
    return out


def free_bytes(path: str = "/") -> int:
    return shutil.disk_usage(path).free


async def _load(client: AsyncMongoClient, label: str, disk_path: str) -> None:
    n = SCALES[label]
    name = coll_name(label)
    coll = client[DB][name]
    meta = client[DB]["_meta"]
    key = f"load:{name}"
    state = await meta.find_one({"_id": key}) or {"loaded": 0, "done": False}
    if state["done"]:
        tqdm.write(f"[{label}] {name}: already loaded ({n:,})")
        return
    start = state["loaded"]
    if start:
        await coll.delete_many({"_id": {"$gte": oid(start)}})  # roll back a possibly half-written group
        tqdm.write(f"[{label}] {name}: resuming from {start:,}")
    bar = tqdm(mininterval=2.0, total=n, initial=start, desc=f"load {name}", unit="doc", unit_scale=True)
    while start < n:
        if free_bytes(disk_path) < MIN_FREE_BYTES:
            bar.close()
            raise SystemExit(f"STOP: free disk below 1.5 GB during load at {start:,} documents; load is resumable")
        starts = list(range(start, min(start + GROUP * BATCH, n), BATCH))
        batches = await asyncio.gather(*(asyncio.to_thread(make_docs, n, s, min(BATCH, n - s)) for s in starts))
        await asyncio.gather(*(coll.insert_many(b, ordered=False) for b in batches))
        start = starts[-1] + len(batches[-1])
        await meta.replace_one({"_id": key}, {"_id": key, "loaded": start, "done": False}, upsert=True)
        bar.n = start
        bar.refresh()
    bar.close()
    await meta.replace_one({"_id": key}, {"_id": key, "loaded": n, "done": True}, upsert=True)


async def wait_queryable(coll, name: str, timeout_s: float) -> None:
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout_s:
        async for ix in await coll.list_search_indexes(name):
            if ix.get("queryable"):
                return
        await asyncio.sleep(2)
    raise TimeoutError(f"search index {name} not queryable after {timeout_s:.0f}s")


async def prepare(uri: str, label: str, out: str, disk_path: str = "/") -> dict:
    client = AsyncMongoClient(uri)
    info: dict = {"scale": label, "documents": SCALES[label]}
    try:
        t0 = time.perf_counter()
        await _load(client, label, disk_path)
        info["load_s"] = round(time.perf_counter() - t0, 1)
        info["free_after_load_bytes"] = free_bytes(disk_path)
        coll = client[DB][coll_name(label)]
        t0 = time.perf_counter()
        await coll.create_index("rank")
        await coll.create_index("tenant")
        info["btree_index_s"] = round(time.perf_counter() - t0, 1)
        name = index_name(label)
        exists = False
        for attempt in range(90):  # mongot may still be starting right after the container answers
            try:
                async for _ in await coll.list_search_indexes(name):
                    exists = True
                break
            except OperationFailure as e:
                if e.code != 125:
                    raise
                await asyncio.sleep(2)
        t0 = time.perf_counter()
        if not exists:
            await coll.create_search_index(SearchIndexModel(definition=SEARCH_DEFINITION, name=name))
        await wait_queryable(coll, name, 6 * 3600)
        info["search_index_s"] = round(time.perf_counter() - t0, 1)
        info["free_after_index_bytes"] = free_bytes(disk_path)
        stats = await client[DB].command("dbStats")
        info["dbstats"] = {k: stats.get(k) for k in ("dataSize", "storageSize", "indexSize", "totalSize", "fsUsedSize", "fsTotalSize")}
        info["tenants"] = {nm: await coll.count_documents({"tenant": nm}) for nm, _ in tenant_thresholds(SCALES[label])}
        info["estimated_count"] = await coll.estimated_document_count()
        (run_dir(out) / f"load_{label}.json").write_text(json.dumps(info, indent=2))
    finally:
        await client.close()
    return info
