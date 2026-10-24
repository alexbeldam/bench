import os
import time

import mongomock

from conftest import FAST_MS


def burn(ms: float) -> None:
    end = time.process_time() + ms / 1000
    while time.process_time() < end:
        pass


def test_fast(i):
    c = mongomock.MongoClient()["t"]["c"]
    c.insert_many([{"k": j, "v": f"x{j}"} for j in range(20)])
    assert c.count_documents({"k": {"$gte": 10}}) == 10
    burn(FAST_MS)
