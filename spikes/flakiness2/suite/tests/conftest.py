"""Fixtures: one environment per backend for the whole session (cold start paid once), purge between tests.
`--stress-repeats N` repeats the tests marked `stress` N times inside the session (no retry plugin: each run is a
separate parametrized test id, a failure is reported with its run number).
Results of every test are appended to the JSON file named by KIT_TX_RESULTS (nodeid, outcome, duration, reason)."""
from __future__ import annotations

import json
import os
import time

import pytest
import pytest_asyncio

from txkit.env import DEFAULT_URI, make_env

_ENVS: dict[str, object] = {}
_RESULTS: list[dict] = []


def pytest_addoption(parser):
    parser.addoption("--stress-repeats", type=int, default=1, help="repeat count for tests marked `stress`")
    parser.addoption("--backends", default="A,B,C", help="comma list of backends to run")


def pytest_generate_tests(metafunc):
    if "stress_rep" in metafunc.fixturenames:
        n = metafunc.config.getoption("--stress-repeats")
        metafunc.parametrize("stress_rep", range(n), ids=[f"r{k + 1:03d}" for k in range(n)])


@pytest.fixture(params=["A", "B", "C"])
def backend(request):
    if request.param not in request.config.getoption("--backends").split(","):
        pytest.skip(f"backend {request.param} not selected")
    return request.param


async def _get_env(backend: str):
    if backend not in _ENVS:
        try:
            _ENVS[backend] = await make_env(backend)
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"backend {backend} unavailable: {type(exc).__name__}")
    return _ENVS[backend]


@pytest_asyncio.fixture
async def env(backend):
    e = await _get_env(backend)
    await e.activate()
    e.fresh()
    yield e
    await e.purge()


@pytest_asyncio.fixture
async def env_a():
    e = await _get_env("A")
    await e.activate()
    e.fresh()
    yield e
    await e.purge()


@pytest_asyncio.fixture
async def env_b():
    e = await _get_env("B")
    await e.activate()
    e.fresh()
    yield e
    await e.purge()


@pytest_asyncio.fixture(scope="session")
async def real_session_client():
    from pymongo import AsyncMongoClient

    c = AsyncMongoClient(DEFAULT_URI, serverSelectionTimeoutMS=5000)
    yield c
    await c.close()


@pytest_asyncio.fixture
async def real_session(real_session_client):
    async with real_session_client.start_session() as s:
        yield s


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    out = yield
    rep = out.get_result()
    if rep.when == "call" or (rep.when == "setup" and rep.outcome != "passed"):
        reason = ""
        outcome = "xfailed" if (hasattr(rep, "wasxfail") and rep.outcome == "skipped") else rep.outcome
        if hasattr(rep, "wasxfail"):
            reason = str(rep.wasxfail)[:300]
        elif rep.outcome != "passed":
            reason = (str(rep.longrepr.reprcrash.message) if hasattr(rep.longrepr, "reprcrash") else str(rep.longrepr))[:300]
        _RESULTS.append({"nodeid": item.nodeid, "outcome": outcome, "duration": round(rep.duration, 3), "reason": reason,
                         "t": time.time(), "notes": getattr(item, "_kit_notes", None)})


def pytest_sessionfinish(session, exitstatus):
    path = os.environ.get("KIT_TX_RESULTS")
    if path:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf8") as f:
            json.dump(_RESULTS, f, indent=1)
