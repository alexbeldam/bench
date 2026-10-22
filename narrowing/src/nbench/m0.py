"""Hosted Atlas M0 (informational, a different environment). Runs only if HOODIE_BENCH_ATLAS_URL is set.

Never prints, logs or stores the connection string: every message is passed through `redact`. Throttled to 40 ops/s
(the free tier allows 100; the brief caps this at 50). Stops loading at 80% of 0.5 GB. Uses exactly one search index,
and drops the collection and the index when done. No load or scale test: only scale-independent questions.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import numpy as np
from pymongo import AsyncMongoClient
from pymongo.operations import SearchIndexModel

from .common import oid, redact
from .gen import make_docs
from .planner import Opts, Planner, search_stage

ENV = "HOODIE_BENCH_ATLAS_URL"
MAX_OPS = 40.0
STORAGE_LIMIT = int(0.5 * 1024**3 * 0.8)
COLL = "hb_narrowing_m0"
LABEL = "m0"
DEFINITION = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "_id": {"type": "objectId"}, "ref": {"type": "objectId"}, "title": {"type": "string"}, "body": {"type": "string"},
            "status": {"type": "stringFacet"}, "brand": {"type": "stringFacet"},
        },
    }
}


class Throttled:
    """Wraps a collection; every operation first waits for its slot (a blocking limiter is fine at 40 ops/s)."""

    def __init__(self, coll) -> None:
        self._c = coll
        self._next = 0.0

    def _wait(self) -> None:
        now = time.monotonic()
        if now < self._next:
            time.sleep(self._next - now)
        self._next = max(now, self._next) + 1.0 / MAX_OPS

    def find(self, *a, **k):
        self._wait()
        return self._c.find(*a, **k)

    async def aggregate(self, *a, **k):
        self._wait()
        return await self._c.aggregate(*a, **k)

    async def count_documents(self, *a, **k):
        self._wait()
        return await self._c.count_documents(*a, **k)

    async def estimated_document_count(self, *a, **k):
        self._wait()
        return await self._c.estimated_document_count(*a, **k)

    async def insert_many(self, *a, **k):
        self._wait()
        return await self._c.insert_many(*a, **k)

    def __getattr__(self, name):
        return getattr(self._c, name)


async def run(out: Path, docs: int, reps: int) -> bool:
    """Returns False when an M0 task or its cleanup failed (the caller turns that into a non-zero exit), True when
    everything ran or the tasks were skipped because the secret is not set."""
    raw_url = os.environ.get(ENV) or ""
    # A secret pasted with a trailing newline or spaces would end up inside the last URL option (for example
    # w=majority followed by a newline, which the server rejects), so surrounding whitespace is stripped.
    url = raw_url.strip()
    if not url:
        (out / "m0_skipped.json").write_text(json.dumps({"skipped": True, "reason": f"{ENV} is not set"}))
        print(f"M0: {ENV} is not set; skipping every M0 task.")
        return True
    p = urlparse(url)
    secrets = [raw_url, url, p.password or "", unquote(p.password or ""), p.username or "", p.hostname or ""]
    secrets = [s for s in secrets if s]
    result: dict = {"environment": "hosted Atlas M0 (informational)", "documents": docs, "limits_stated": {
        "storage_gb": 0.5, "ops_per_s": 100, "harness_ops_per_s": MAX_OPS, "search_indexes": 3}}
    client = AsyncMongoClient(url, serverSelectionTimeoutMS=20000)
    # The database comes from HOODIE_BENCH_ATLAS_DB, else from the database named in the URL, else "bench". Only
    # the collection named COLL (and its search index) is ever dropped.
    db = client[os.environ.get("HOODIE_BENCH_ATLAS_DB") or client.get_default_database(default="bench").name]
    coll = db[COLL]
    tc = Throttled(coll)
    try:
        await coll.drop()  # leftovers of a previous run
        # --- load, within the storage budget
        loaded = 0
        while loaded < docs:
            batch = make_docs(docs, loaded, min(1000, docs - loaded))
            for d in batch:
                d["ref"] = oid(loaded + 10_000_000)
            await tc.insert_many(batch, ordered=False)
            loaded += len(batch)
            if loaded % 5000 == 0:
                stats = await db.command("dbStats")
                if stats["dataSize"] + stats["indexSize"] > STORAGE_LIMIT:
                    print(f"M0: stopping load at {loaded} documents (80% of 0.5 GB)")
                    break
        await coll.create_index("rank")
        stats = await db.command("dbStats")
        result["loaded"] = loaded
        result["storage_bytes"] = stats["dataSize"] + stats["indexSize"]
        # --- one search index
        await coll.create_search_index(SearchIndexModel(definition=DEFINITION, name=f"docs_{LABEL}_idx"))
        t0 = time.time()
        while True:
            ready = False
            async for ix in await coll.list_search_indexes(f"docs_{LABEL}_idx"):
                ready = bool(ix.get("queryable"))
            if ready:
                break
            if time.time() - t0 > 900:
                raise TimeoutError("search index not queryable after 15 minutes")
            await asyncio.sleep(5)
        result["index_ready_s"] = round(time.time() - t0, 1)
        planner = Planner(tc, LABEL, loaded)  # type: ignore[arg-type]

        # --- limits: the `in` operator on _id and on an ObjectId field
        limits: dict = {}
        for path in ("_id", "ref"):
            for size in (500, 1000, 1023, 1024, 1025, 2048):
                ids = [oid(i) for i in range(size)] if path == "_id" else [oid(i + 10_000_000) for i in range(size)]
                stage = search_stage(LABEL, "frequent", ids, Opts())
                stage["$search"]["compound"]["filter"][0]["in"]["path"] = path
                try:
                    r = await (await tc.aggregate([stage, {"$limit": 1}])).to_list()
                    limits[f"{path}:{size}"] = "ok"
                except Exception as e:  # noqa: BLE001
                    limits[f"{path}:{size}"] = redact(f"{type(e).__name__}: {e}", secrets)[:160]
        result["in_limits"] = limits

        # --- tie behaviour: chunked A with two chunk sizes against search-first, on an evenly spread filter
        k = min(3000, loaded // 3)
        flt = {"rank": {"$lt": k}}
        ids = await planner.prefilter(flt, k + 1)
        runs = {
            "A_chunk1000": await planner.chunked(ids, "medium", chunk=1000, inflight=1),
            "A_chunk500": await planner.chunked(ids, "medium", chunk=500, inflight=1),
            "B100": await planner.search_first(flt, "medium", 100),
            "B500": await planner.search_first(flt, "medium", 500),
        }
        ref = runs["A_chunk1000"]
        result["ties"] = {name: {"results": len(r.hits), "same_score_sequence": [round(h[1], 5) for h in r.hits] == [round(h[1], 5) for h in ref.hits],
                                 "same_ids_as_A_chunk1000": [h[0] for h in r.hits] == [h[0] for h in ref.hits],
                                 "id_overlap": len({h[0] for h in r.hits} & {h[0] for h in ref.hits})}
                          for name, r in runs.items()}

        # --- one small strategy comparison
        cmp: dict = {}
        for kk in sorted({max(100, loaded // 20), max(200, loaded // 10), max(400, loaded // 4)}):
            f = {"rank": {"$lt": kk}}
            ids = await planner.prefilter(f, kk + 1)
            a, b = [], []
            for _ in range(reps):
                t0 = time.perf_counter()
                ra = await planner.chunked(ids, "frequent", chunk=1000, inflight=1)
                a.append((time.perf_counter() - t0) * 1000)
                t0 = time.perf_counter()
                rb = await planner.search_first(f, "frequent", 100)
                b.append((time.perf_counter() - t0) * 1000)
            cmp[str(kk)] = {"selectivity_pct": round(100 * kk / loaded, 2), "A_p50_ms": float(np.median(a)), "B100_p50_ms": float(np.median(b)),
                            "B100_results": len(rb.hits), "A_results": len(ra.hits), "reps": reps}
        result["strategy_comparison"] = cmp
    except Exception as e:  # noqa: BLE001
        result["error"] = redact(f"{type(e).__name__}: {e}", secrets)[:300]
    finally:
        try:
            async for ix in await coll.list_search_indexes():
                await coll.drop_search_index(ix["name"])
            await coll.drop()
            result["cleaned_up"] = True
        except Exception as e:  # noqa: BLE001
            result["cleaned_up"] = False
            result["cleanup_error"] = redact(f"{type(e).__name__}: {e}", secrets)[:200]
        await client.close()
        text = redact(json.dumps(result, indent=1), secrets)
        (out / "m0.json").write_text(text)
        print("M0 done; results written to m0.json (no connection details are stored)")
    ok = "error" not in result and result.get("cleaned_up") is not False
    if not ok:
        # Already redacted above, so it is safe to show in the CI log.
        print(f"M0 FAILED: {result.get('error') or result.get('cleanup_error')}", file=sys.stderr)
    return ok
