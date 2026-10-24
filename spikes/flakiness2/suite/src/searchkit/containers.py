"""testcontainers adapter for a real MongoDB with Atlas Search (extra `kit[testing]`, imports testcontainers).

`KIT_TEST_MONGO_URI` points the fixtures at an already-running deployment instead (warm start, CI services).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field

IMAGE = "mongodb/mongodb-atlas-local:8.0"


class DockerUnavailableError(RuntimeError):
    """Docker is not installed or its daemon is not running. Named so a test run can report it."""


def docker_available() -> tuple[bool, str]:
    if shutil.which("docker") is None:
        return False, "docker executable not found on PATH"
    try:
        r = subprocess.run(["docker", "info", "--format", "{{.ServerVersion}}"], capture_output=True, text=True, timeout=20)  # noqa: S603, S607
    except Exception as exc:  # noqa: BLE001
        return False, f"docker info failed: {type(exc).__name__}"
    if r.returncode != 0:
        return False, "docker daemon not reachable"
    return True, r.stdout.strip()


@dataclass
class Started:
    uri: str
    container: object | None = None
    timings: dict[str, float] = field(default_factory=dict)

    def stop(self) -> None:
        if self.container is not None:
            self.container.stop()  # type: ignore[attr-defined]


async def _wait_search_ready(uri: str, timeout: float) -> float:
    """Ready means a search index on a scratch collection becomes queryable (not only that mongod answers)."""
    from pymongo import AsyncMongoClient
    from pymongo.operations import SearchIndexModel

    t0 = time.perf_counter()
    client = AsyncMongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        while True:
            try:
                await client.admin.command("ping")
                coll = client["kit_ready"]["probe"]
                await coll.replace_one({"_id": 1}, {"_id": 1, "t": "x"}, upsert=True)
                try:
                    await coll.create_search_index(SearchIndexModel(definition={"mappings": {"dynamic": True}}, name="p"))
                except Exception:  # noqa: BLE001  already exists on a warm start
                    pass
                while True:
                    async for ix in await coll.list_search_indexes("p"):
                        if ix.get("queryable"):
                            await client.drop_database("kit_ready")
                            return time.perf_counter() - t0
                    if time.perf_counter() - t0 > timeout:
                        raise TimeoutError("search index on scratch collection never queryable")
                    await asyncio.sleep(0.25)
            except TimeoutError:
                raise
            except Exception:  # noqa: BLE001  server not up yet
                if time.perf_counter() - t0 > timeout:
                    raise
                await asyncio.sleep(0.5)
    finally:
        await client.close()


def start_atlas_local(timeout: float = 240.0) -> Started:
    uri = os.environ.get("KIT_TEST_MONGO_URI")
    if uri:
        t0 = time.perf_counter()
        ready = asyncio.run(_wait_search_ready(uri, timeout))
        return Started(uri, None, {"container_start": 0.0, "search_ready": ready, "total": time.perf_counter() - t0})
    ok, why = docker_available()
    if not ok:
        raise DockerUnavailableError(why)
    from testcontainers.core.container import DockerContainer

    t0 = time.perf_counter()
    c = DockerContainer(IMAGE).with_exposed_ports(27017)
    c.start()
    started = time.perf_counter() - t0
    uri = f"mongodb://{c.get_container_host_ip()}:{c.get_exposed_port(27017)}/?directConnection=true"
    ready = asyncio.run(_wait_search_ready(uri, timeout))
    return Started(uri, c, {"container_start": started, "search_ready": ready, "total": time.perf_counter() - t0})
