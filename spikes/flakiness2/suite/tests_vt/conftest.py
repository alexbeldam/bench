"""Fixtures for the deterministic race tests: virtual clock, and a mongomock + in-memory-engine environment whose engine
has controllable index lag. No container, no real waiting. Rows for the report are collected in ROWS."""
from __future__ import annotations

import asyncio
import json
import os

import pytest
import pytest_asyncio
from searchkit.lifecycle import Harness, Timeouts
from searchkit.memory_engine import InMemoryEngine
from searchkit.mongomock_compat import mock_client
from txkit.env import SPECS, Env, SpyEngine
from txkit.runtime import Runtime, set_runtime

from vt.vtime import DropWatch, LagEngine, VClock

ROWS: list[dict] = []


def row(race: str, variant: str, outcome: str, **kw) -> None:
    ROWS.append({"race": race, "variant": variant, "outcome": outcome, **kw})


@pytest.fixture
def vclock(monkeypatch):
    c = VClock()
    monkeypatch.setattr(asyncio, "sleep", c.sleep)
    return c


@pytest_asyncio.fixture
async def lag_env(vclock):
    db = mock_client()["fs_lag_env"]
    engine = LagEngine(InMemoryEngine(), vclock)

    async def live_ids(entity, ids):
        return {d["_id"] async for d in db[entity].find({"_id": {"$in": list(ids)}}, {"_id": 1})}

    engine.live_ids = live_ids
    spy = SpyEngine(engine)
    timeouts = Timeouts(sync=15.0, index_build=60.0)
    harness = Harness(DropWatch(db, engine), spy, list(SPECS.values()), timeouts)
    rt = Runtime(db=db, client=None, engine=spy, specs=dict(SPECS), backend="mock", timeouts=timeouts, harness=harness)
    env = Env("C", rt, spy, harness, None, default_timeouts=timeouts)
    env.lag = engine  # type: ignore[attr-defined]
    env.clock = vclock  # type: ignore[attr-defined]
    await env.activate()
    await harness.reset(order="fixed")
    vclock.now = 0.0
    vclock.sleeps.clear()
    yield env
    set_runtime(None)


def pytest_sessionfinish(session, exitstatus):
    path = os.environ.get("FS_ROWS")
    if path and ROWS:
        with open(path, "w", encoding="utf8") as f:
            json.dump(ROWS, f, indent=1)
