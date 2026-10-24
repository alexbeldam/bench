"""Synthetic suite: M engine-free tests (mongomock plus a fixed CPU cost) and S tests that each write one document to the
shared engine and wait until a text search finds it. Sizes come from MODEL_S, MODEL_M and MODEL_FAST_MS.
One database and index per xdist worker (prefix MODEL_PREFIX); settle times are appended to MODEL_OUT/settle-<pid>.jsonl."""
import json
import os
import time

import pytest

S = int(os.environ.get("MODEL_S", "420"))
M = int(os.environ.get("MODEL_M", "1300"))
FAST_MS = float(os.environ.get("MODEL_FAST_MS", "40"))
URI = os.environ.get("MODEL_URI", "mongodb://127.0.0.1:29501/?directConnection=true")
PREFIX = os.environ.get("MODEL_PREFIX", "m")
OUT = os.environ.get("MODEL_OUT", "")


def pytest_generate_tests(metafunc):
    if "i" in metafunc.fixturenames:
        n = S if metafunc.module.__name__.endswith("test_settle") else M
        metafunc.parametrize("i", range(n))


@pytest.fixture(scope="session")
def coll():
    from pymongo import MongoClient
    from pymongo.operations import SearchIndexModel

    w = os.environ.get("PYTEST_XDIST_WORKER", "main")
    c = MongoClient(URI, serverSelectionTimeoutMS=15000)
    c.admin.command("ping")
    db = c[f"{PREFIX}{w}_m"]
    col = db["docs"]
    try:
        db.create_collection("docs")
    except Exception:  # noqa: BLE001
        pass
    name = f"{PREFIX}{w}_idx"
    col.create_search_index(SearchIndexModel(definition={"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}}, name=name))
    deadline = time.time() + 120
    while time.time() < deadline:
        ix = list(col.list_search_indexes(name))
        if ix and ix[0].get("queryable"):
            break
        time.sleep(0.2)
    else:
        raise RuntimeError("index not queryable")
    col.index_name = name  # type: ignore[attr-defined]
    yield col
    c.close()


def settle(col, token: str) -> float:
    t0 = time.perf_counter()
    step = 0.02
    while True:
        r = list(col.aggregate([{"$search": {"index": col.index_name, "text": {"query": token, "path": "title"}}}, {"$limit": 1}]))
        if r:
            return time.perf_counter() - t0
        if time.perf_counter() - t0 > 120:
            raise AssertionError("never searchable")
        time.sleep(step)
        step = min(step * 1.5, 0.5)


def record(w: float) -> None:
    if OUT:
        with open(os.path.join(OUT, f"settle-{os.getpid()}.jsonl"), "a") as f:
            f.write(json.dumps({"s": round(w, 4)}) + "\n")
