import os
import uuid

from conftest import record, settle


def test_settle(coll, i):
    token = "u" + uuid.uuid4().hex
    r = coll.insert_one({"title": f"item {token}"})
    record(settle(coll, token))
    coll.delete_one({"_id": r.inserted_id})
