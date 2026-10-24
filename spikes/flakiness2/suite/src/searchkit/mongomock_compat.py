"""Compatibility shim: run Beanie 2.x on mongomock-motor (extra `kit[testing]`, imports mongomock).

What it papers over (each item observed in experiments/mongomock_probe.py):
 - Beanie 2.2.0 calls `list_collection_names(authorizedCollections=True, nameOnly=True)`; mongomock rejects both.
 - pymongo 4.19 `UpdateOne` forwards a `sort` argument that mongomock's bulk builder does not accept.
"""
from __future__ import annotations

from typing import Any

from mongomock_motor import AsyncMongoMockClient, AsyncMongoMockDatabase


def patch() -> None:
    import mongomock.database as mdb
    import mongomock.collection as mcol

    orig = mdb.Database.list_collection_names
    if not getattr(orig, "_kit_patched", False):
        def list_collection_names(self: Any, filter: Any = None, session: Any = None, **_ignored: Any) -> Any:  # noqa: A002
            return orig(self, filter=filter, session=session)
        list_collection_names._kit_patched = True  # type: ignore[attr-defined]
        mdb.Database.list_collection_names = list_collection_names  # type: ignore[method-assign]
    ob = mcol.BulkOperationBuilder
    orig_up = ob.add_update
    if not getattr(orig_up, "_kit_patched", False):
        def add_update(self: Any, *args: Any, sort: Any = None, **kwargs: Any) -> Any:
            return orig_up(self, *args, **kwargs)
        add_update._kit_patched = True  # type: ignore[attr-defined]
        ob.add_update = add_update  # type: ignore[method-assign]


def mock_client() -> AsyncMongoMockClient:
    patch()
    return AsyncMongoMockClient()


def patch_bulk_comment() -> None:
    """let `Collection.bulk_write` accept the `comment` argument Beanie's
    BulkWriter passes (pymongo 4.19). Without it BulkWriter and `replace_many` fail on mongomock."""
    import mongomock.collection as mcol

    orig = mcol.Collection.bulk_write
    if getattr(orig, "_kit_patched", False):
        return

    def bulk_write(self: Any, requests: Any, ordered: bool = True, bypass_document_validation: bool = False, session: Any = None, comment: Any = None, **kw: Any) -> Any:  # noqa: ANN401
        return orig(self, requests, ordered=ordered, bypass_document_validation=bypass_document_validation, session=session)

    bulk_write._kit_patched = True  # type: ignore[attr-defined]
    mcol.Collection.bulk_write = bulk_write  # type: ignore[method-assign]
