"""Planted tests on the real search engine (mongodb-atlas-local). Same races as the in-memory set, with no injected lag: the
failure rate of each buggy variant is whatever the engine does, and is measured with a large n (the truth is "measured").

A worker process owns one database. Every run uses its own token so runs do not see each other's documents.
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from functools import partial
from typing import Any

from searchkit.lifecycle import Timeouts
from searchkit.ordering import sort_hits

from .core import Ctx, reg
from .env import Item, mk, total

URI = os.environ.get("FL2P_URI", "mongodb://127.0.0.1:29401/?directConnection=true")
W: dict[str, Any] = {"env": None, "n": 0, "dirty": False, "post": None}


async def renv():
    if W["env"] is None:
        from txkit.env import make_env

        W["env"] = await make_env("A", URI, db_name=f"fl2p_{os.getpid()}")
    e = W["env"]
    e.fresh()
    return e


def nxt() -> int:
    W["n"] += 1
    return W["n"]


async def after_run() -> None:
    """Untimed cleanup between runs: recover a ghost index, purge when a test needs a clean database or every 150 runs."""
    e = W["env"]
    if e is None:
        return
    if W["dirty"]:
        W["dirty"] = False
        e.fresh()
        await e.harness.reset(order="fixed")
    elif W["post"] == "purge" or W["n"] % 150 == 0:
        e.fresh()
        await e.purge()
    W["post"] = None


async def r_settle(ctx: Ctx, bound: float) -> None:
    """One write, settle with the given bound. Records the harness's own timing in ctx.val (seconds, from the engine sync call
    that follows the data commit): `pre` first probe, `seen` probe that saw the document, `window` start of the last probe.
    The bound counts sleeps between probes, not wall time, and not from the commit, so `window` is larger than the bound."""
    env = await renv()
    n = nxt()
    env.rt.timeouts = Timeouts(sync=bound, index_build=60.0)
    t: dict[str, float] = {}
    spy = env.spy
    o_probe, o_sync = spy.probe, spy.sync_write

    async def probe(entity, index, pairs):
        now = time.perf_counter()
        t.setdefault("first", now)
        t["last"] = now
        out = await o_probe(entity, index, pairs)
        if out:
            t.setdefault("seen", time.perf_counter())
        return out

    async def sync_write(entity, index, docs):
        t["sync"] = time.perf_counter()
        return await o_sync(entity, index, docs)

    spy.probe, spy.sync_write = probe, sync_write
    try:
        await mk(Item, n * 10000, f"u{n} word").insert()
    finally:
        spy.__dict__.pop("probe", None)
        spy.__dict__.pop("sync_write", None)
        if "sync" in t:
            ctx.val["pre"] = t.get("first", t["sync"]) - t["sync"]
            ctx.val["window"] = t.get("last", t["sync"]) - t["sync"]
            if "seen" in t:
                ctx.val["seen"] = t["seen"] - t["sync"]


async def r_stale(ctx: Ctx, fixed: bool) -> None:
    env = await renv()
    n = nxt()
    u = f"u{n}"
    await Item.insert_many([mk(Item, n * 10000 + j, f"{u} common") for j in range(100)])
    assert await total(Item, u) == 100
    if fixed:
        await env.harness.purge()
    else:
        await env.harness.delete_all_unsafe(env.rt.specs["tx_item"])
    assert await total(Item, u) == 0


async def r_purge(ctx: Ctx, fixed: bool) -> None:
    env = await renv()
    n = nxt()
    u = f"u{n}"
    await Item.insert_many([mk(Item, n * 10000 + j, f"{u} common") for j in range(50)])

    async def by_hits(entity, index, ids):
        pl = [{"$search": {"index": index, "in": {"path": "_id", "value": list(ids)}}}, {"$project": {"_id": 1}}]
        return {d["_id"] async for d in await env.rt.db[entity].aggregate(pl)}

    if not fixed:
        env.spy.probe_absent = by_hits  # the old probe: hits are hydrated from the data, so a deleted document never shows
    try:
        await env.harness.purge()
    finally:
        env.spy.__dict__.pop("probe_absent", None)
    assert await total(Item, u) == 0


async def r_reset(ctx: Ctx, fixed: bool) -> None:
    """Reset, then the index must be usable: a document written afterwards becomes searchable within 8 s. The legacy order
    can fail at the reset (index never queryable) or later (an index that reports queryable but never indexes)."""
    env = await renv()
    W["dirty"] = True
    env.harness.t = Timeouts(sync=5.0, index_build=6.0)
    await env.harness.reset(order="fixed" if fixed else "legacy-order")
    env.rt.timeouts = Timeouts(sync=8.0, index_build=60.0)
    n = nxt()
    await mk(Item, n * 10000, f"u{n} word").insert()
    W["dirty"] = False


async def r_tx(ctx: Ctx, bound: float) -> None:
    import scenarios

    env = await renv()
    W["post"] = "purge"
    scenarios.SYNC_BOUND["A"] = bound
    await scenarios.s14_partial_failure_at_commit(env)


# ---- tie order on the real engine ---------------------------------------------------------------------------------

TIE_KEY = "w0000 w0001 w0002"
TIE_K = 20
_T: dict[str, Any] = {}


async def _tie_setup(env) -> None:
    from pymongo.operations import SearchIndexModel  # noqa: F401

    coll = env.rt.db["fl2p_ties"]
    rng = random.Random(20261022)
    docs = [{"title": " ".join(f"w{rng.choices(range(40), [1 / (j + 1) for j in range(40)])[0]:04d}" for _ in range(rng.randint(2, 4)))} for _ in range(2000)]
    await coll.drop()
    await coll.insert_many(docs)
    _T["want"] = sum(any(w in d["title"].split() for w in TIE_KEY.split()) for d in docs)
    _T["coll"] = coll


async def r_tie(ctx: Ctx, fixed: bool) -> None:
    from pymongo.operations import SearchIndexModel

    env = await renv()
    if "coll" not in _T:
        await _tie_setup(env)
    coll, n = _T["coll"], nxt()
    name = f"t{os.getpid()}_{n}"
    defn = {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}, "_id": {"type": "objectId"}}}}
    await coll.create_search_index(SearchIndexModel(definition=defn, name=name))
    while True:
        ix = [i async for i in await coll.list_search_indexes(name)]
        if ix and ix[0].get("queryable"):
            break
        await asyncio.sleep(0.1)
    txt = {"index": name, "text": {"query": TIE_KEY, "path": "title"}}
    while True:
        tot = [d async for d in await coll.aggregate([{"$searchMeta": {**txt, "count": {"type": "total"}}}])]
        if tot and tot[0]["count"]["total"] == _T["want"]:
            break
        await asyncio.sleep(0.1)
    pl = [{"$search": txt}, {"$set": {"_s": {"$meta": "searchScore"}}}, {"$project": {"_id": 1, "_s": 1}}]
    allhits = [(d["_id"], d["_s"]) async for d in await coll.aggregate(pl)]
    native = [i for i, _ in allhits][:TIE_K]
    if "golden" not in _T:
        _T["golden"] = [i for i, _ in sort_hits(allhits)][:TIE_K]
    got = [i for i, _ in sort_hits(allhits)][:TIE_K] if fixed else native
    W["tie_index"] = name
    assert got == _T["golden"], "page differs from the expected total order"


async def after_tie() -> None:
    nm = W.pop("tie_index", None)
    if nm and "coll" in _T:
        try:
            await _T["coll"].drop_search_index(nm)
        except Exception:  # noqa: BLE001
            pass


def _rr(name, kind, truth, fn, variant, **kw):
    reg(name, kind, truth, "real", variant, fn, cost="slow", **kw)


for _b in (0.9, 0.95, 1.0, 1.01, 1.02):
    _rr(f"r-settle-b{_b:.2f}", "real-race", "measured", partial(r_settle, bound=_b), "buggy", note=f"one write, settle bound {_b} s on the real engine")
_rr("r-settle-b4.00", "real-race", 0.0, partial(r_settle, bound=4.0), "fixed", note="bound 4.0 s; expected never to fail, rate measured")
for _b in (0.9, 0.95, 1.0, 1.01, 1.02):
    _rr(f"r-tx-partial-b{_b:.2f}", "real-race", "measured", partial(r_tx, bound=_b), "buggy", note=f"scenario s14 with bound {_b} s")
_rr("r-tx-partial-b4.00", "real-race", 0.0, partial(r_tx, bound=4.0), "fixed", note="scenario s14 with bound 4.0 s (the suite's own value)")
_rr("r-stale-total", "real-race", "measured", partial(r_stale, fixed=False), "buggy", note="delete then exact total at once")
_rr("r-stale-total-fixed", "real-race", 0.0, partial(r_stale, fixed=True), "fixed", note="purge waits by counting index entries")
_rr("r-purge-probe", "real-race", "measured", partial(r_purge, fixed=False), "buggy", note="purge that waits with a $search probe")
_rr("r-purge-probe-fixed", "real-race", 0.0, partial(r_purge, fixed=True), "fixed")
_rr("r-reset-order", "real-race", "measured", partial(r_reset, fixed=False), "buggy", note="collections dropped before indexes, 6 s bound")
_rr("r-reset-order-fixed", "real-race", 0.0, partial(r_reset, fixed=True), "fixed")
_rr("r-tie-order", "real-race", "measured", partial(r_tie, fixed=False), "buggy", note="native top 20 against score then id, one new index per run")
_rr("r-tie-order-fixed", "real-race", 0.0, partial(r_tie, fixed=True), "fixed")

POST = {"r-tie-order": after_tie, "r-tie-order-fixed": after_tie}
# default pool size per real test at full scale (runs); the slowest are smaller
FULL_N = {"r-settle-b0.90": 600, "r-settle-b0.95": 600, "r-tx-partial-b0.90": 300, "r-tx-partial-b0.95": 300, "r-settle-b1.00": 1000, "r-settle-b1.01": 1000, "r-settle-b1.02": 1000, "r-settle-b4.00": 600,
          "r-tx-partial-b1.00": 500, "r-tx-partial-b1.01": 500, "r-tx-partial-b1.02": 500, "r-tx-partial-b4.00": 300,
          "r-stale-total": 500, "r-stale-total-fixed": 300, "r-purge-probe": 500, "r-purge-probe-fixed": 300,
          "r-reset-order": 300, "r-reset-order-fixed": 200, "r-tie-order": 100, "r-tie-order-fixed": 60}
SHARDS = {
    "settle": ["r-settle-b0.90", "r-settle-b0.95", "r-settle-b1.00", "r-settle-b1.01", "r-settle-b1.02", "r-settle-b4.00", "r-tie-order", "r-tie-order-fixed"],
    "tx": ["r-tx-partial-b0.90", "r-tx-partial-b0.95", "r-tx-partial-b1.00", "r-tx-partial-b1.01", "r-tx-partial-b1.02", "r-tx-partial-b4.00"],
    "purge": ["r-stale-total", "r-stale-total-fixed", "r-purge-probe", "r-purge-probe-fixed", "r-reset-order", "r-reset-order-fixed"],
}
