"""Canary in its own process: a fixed CPU loop and a search query, timed every few seconds and appended to a JSONL file.
Run it beside an experiment; samples taken while a noise arm is active show the load, they are not a defect.
`--uri` queries the real engine (own database); without it the query runs on the in-memory engine."""
from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any


def cpu_ms() -> float:
    t0 = time.perf_counter()
    x = 0
    for i in range(1_000_000):
        x += i * i
    return (time.perf_counter() - t0) * 1000


async def _mem_query():
    from searchkit.contract import Compound, IndexDef, FieldDef, IndexableDoc, Query, Text
    from searchkit.memory_engine import InMemoryEngine

    e = InMemoryEngine()
    ix = IndexDef("c", (FieldDef("title", "string"),))
    await e.create_index("c", ix)
    await e.sync_write("c", "c", [IndexableDoc(i, None, {"title": f"tok{i} common"}) for i in range(500)])
    q = Query(Compound(must=(Text("common", ("title",)),)), limit=20)

    async def one() -> float:
        t0 = time.perf_counter()
        await e.search("c", "c", q)
        return (time.perf_counter() - t0) * 1000

    return one


async def _real_query(uri: str):
    from pymongo import AsyncMongoClient
    from pymongo.operations import SearchIndexModel

    c = AsyncMongoClient(uri)
    coll = c["fl2p_canary"]["c"]
    await coll.drop()
    await coll.insert_many([{"title": f"tok{i} common"} for i in range(500)])
    await coll.create_search_index(SearchIndexModel(definition={"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}}, name="cn"))
    while True:
        ix = [i async for i in await coll.list_search_indexes("cn")]
        if ix and ix[0].get("queryable"):
            break
        await asyncio.sleep(0.2)
    while not [d async for d in await coll.aggregate([{"$search": {"index": "cn", "text": {"query": "common", "path": "title"}}}, {"$limit": 1}])]:
        await asyncio.sleep(0.1)

    async def one() -> float:
        t0 = time.perf_counter()
        [d async for d in await coll.aggregate([{"$search": {"index": "cn", "text": {"query": "common", "path": "title"}}}, {"$limit": 20}])]
        return (time.perf_counter() - t0) * 1000

    return one


async def main(out: str, interval: float, uri: str | None, count: int | None) -> None:
    one = await (_real_query(uri) if uri else _mem_query())
    k = 0
    while count is None or k < count:
        row: dict[str, Any] = {"t": time.time(), "cpu_ms": round(cpu_ms(), 2), "query_ms": round(await one(), 2), "engine": "real" if uri else "inmem"}
        with open(out, "a", encoding="utf8") as f:
            f.write(json.dumps(row) + "\n")
        k += 1
        await asyncio.sleep(interval)


def cmd_canary(a: Any) -> None:
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    asyncio.run(main(a.out, a.interval, a.uri, a.count))
