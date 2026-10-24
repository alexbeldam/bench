"""pytest fixtures (extra `kit[testing]`): `pytest_plugins = ["searchkit.fixtures"]`.

 - `memory_engine`    : fresh in-memory engine per test (no container, no I/O).
 - `atlas_uri`        : session-scoped URI of a real MongoDB + Atlas Search (container or KIT_TEST_MONGO_URI);
                        the tests that need it are skipped, with the named reason, when Docker is unavailable.
 - `real_db`          : an async database on that server, one fresh database per test module.
 - `kit_isolation` : factory that gives `Harness.purge()` after each test (bounded, no index rebuild).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from .containers import DockerUnavailableError, Started, start_atlas_local
from .memory_engine import InMemoryEngine


@pytest.fixture
def memory_engine() -> InMemoryEngine:
    return InMemoryEngine()


@pytest.fixture(scope="session")
def atlas_started() -> Iterator[Started]:
    try:
        started = start_atlas_local()
    except DockerUnavailableError as exc:
        pytest.skip(f"real-engine tests need Docker or KIT_TEST_MONGO_URI: {exc}")
    yield started
    started.stop()


@pytest.fixture(scope="session")
def atlas_uri(atlas_started: Started) -> str:
    return atlas_started.uri


@pytest_asyncio.fixture(scope="session")
async def real_client(atlas_uri: str):  # noqa: ANN201
    from pymongo import AsyncMongoClient

    client = AsyncMongoClient(atlas_uri)
    yield client
    await client.close()


@pytest_asyncio.fixture
async def real_db(real_client, request: pytest.FixtureRequest) -> AsyncIterator:  # noqa: ANN001
    name = "t_" + request.module.__name__.rsplit(".", 1)[-1][:30]
    yield real_client[name]


@pytest_asyncio.fixture
async def kit_isolation() -> AsyncIterator:
    """Usage: `iso = kit_isolation; iso.register(harness)`. After the test every registered harness is purged."""
    registered: list = []

    class _Iso:
        def register(self, harness):  # noqa: ANN001, ANN202
            registered.append(harness)
            return harness

    yield _Iso()
    await asyncio.gather(*(h.purge() for h in registered))
