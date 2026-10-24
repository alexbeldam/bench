"""An in-memory search environment with controllable index lag and a virtual clock, built outside pytest."""
from __future__ import annotations

import asyncio
import pathlib
import sys
from contextlib import asynccontextmanager

from searchkit.contract import Compound, Query, Text
from searchkit.data import oid
from searchkit.lifecycle import Harness, Timeouts
from searchkit.memory_engine import InMemoryEngine
from searchkit.mongomock_compat import mock_client
from txkit.env import SPECS, Env, Item, Note, SpyEngine
from txkit.runtime import Runtime, set_runtime
from txkit.scope import search

from vt.vtime import DropWatch, LagEngine, VClock

SUITE_TESTS = pathlib.Path(__file__).resolve().parents[3] / "suite" / "tests"
if str(SUITE_TESTS) not in sys.path:
    sys.path.insert(0, str(SUITE_TESTS))

_REAL_SLEEP = asyncio.sleep


@asynccontextmanager
async def lag_env():
    """Same construction as tests_vt/conftest.py: virtual clock in place of asyncio.sleep, engine with lag, ghost model."""
    clock = VClock()
    asyncio.sleep = clock.sleep  # type: ignore[assignment]
    try:
        db = mock_client()["fl2p_env"]
        engine = LagEngine(InMemoryEngine(), clock)

        async def live_ids(entity, ids):
            return {d["_id"] async for d in db[entity].find({"_id": {"$in": list(ids)}}, {"_id": 1})}

        engine.live_ids = live_ids
        spy = SpyEngine(engine)
        timeouts = Timeouts(sync=15.0, index_build=60.0)
        harness = Harness(DropWatch(db, engine), spy, list(SPECS.values()), timeouts)
        rt = Runtime(db=db, client=None, engine=spy, specs=dict(SPECS), backend="mock", timeouts=timeouts, harness=harness)
        env = Env("C", rt, spy, harness, None, default_timeouts=timeouts)
        env.lag = engine  # type: ignore[attr-defined]
        env.clock = clock  # type: ignore[attr-defined]
        await env.activate()
        await harness.reset(order="fixed")
        clock.now = 0.0
        clock.sleeps.clear()
        yield env
    finally:
        asyncio.sleep = _REAL_SLEEP  # type: ignore[assignment]
        set_runtime(None)


def mk(cls, n, title=None):
    ns = {"tx_item": 1, "tx_note": 2}[cls.get_collection_name()]
    return cls(id=oid(n, ns), title=title or f"tok{n} common")


def q(tok: str) -> Query:
    return Query(Compound(must=(Text(tok, ("title",)),)), limit=1000)


async def total(cls, tok: str) -> int:
    return (await search(cls, q(tok))).total


__all__ = ["lag_env", "mk", "q", "total", "Item", "Note", "Timeouts", "VClock"]
