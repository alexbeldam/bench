"""A small Entity: an ambient session mechanism (no global patch) plus the write path.

query-builder subclasses with a data descriptor for `session`,
and `Entity.bulk_writer()`. Every write: (1) guard explicit `session=` and a stock BulkWriter, (2) stamp `_rev`,
(3) upsert markers with the data's session, (4) record the touched ids, (5) write the data. `@synced` (implicit batch)
or `@transactional` then does the engine sync and the one wait (see runtime.finalize).
"""
from __future__ import annotations

from typing import Any, ClassVar, Self

from beanie import BulkWriter, Document
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.delete import DeleteMany, DeleteOne
from beanie.odm.queries.find import FindMany, FindOne
from beanie.odm.queries.update import UpdateMany, UpdateOne, UpdateQuery
from beanie import PydanticObjectId
from bson import ObjectId
from pydantic import Field
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.results import InsertManyResult

from .errors import InvalidSessionTypeError, StockBulkWriterError, UnmanagedSessionError
from .runtime import get_runtime, mark_dirty, mark_pending
from .scope import current_batch, current_scope, current_session, synced, track


class _AmbientSession:
    """Data descriptor for `session` on Beanie builders: an explicit value wins, else the ambient session is read when
    the call runs. The setter stores None when handed the ambient session itself (so a builder awaited after the
    commit does not pin the finished transaction), as legacy does."""

    def __set_name__(self, owner: type, name: str) -> None:
        self._slot = f"_explicit_{name}"

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            return self
        return obj.__dict__.get(self._slot) or current_session()

    def __set__(self, obj: Any, value: Any) -> None:
        obj.__dict__[self._slot] = None if value is current_session() else value


class _Ambient:
    session = _AmbientSession()


class HUpdateQuery(_Ambient, UpdateQuery): ...
class HUpdateMany(_Ambient, UpdateMany): ...
class HUpdateOne(_Ambient, UpdateOne): ...
class HDeleteMany(_Ambient, DeleteMany): ...
class HDeleteOne(_Ambient, DeleteOne): ...
class HAggregationQuery(_Ambient, AggregationQuery): ...


class HFindMany(_Ambient, FindMany):
    UpdateQueryType = HUpdateMany
    DeleteQueryType = HDeleteMany
    AggregationQueryType = HAggregationQuery


class HFindOne(_Ambient, FindOne):
    UpdateQueryType = HUpdateOne
    DeleteQueryType = HDeleteOne
    AggregationQueryType = HAggregationQuery


class HBulkWriter(BulkWriter):
    """the bulk writer of this layer: commit uses the ambient session when none was given."""

    session = _AmbientSession()


def _with_rev(ops: tuple[Any, ...]) -> dict[str, Any]:
    """Merge the user's update operators and the new `_rev` into ONE query. Appending a second
    `{"$set": ...}` operator would replace the caller's `$set`: Beanie merges operators with `dict.update` (observed)."""
    merged: dict[str, Any] = {}
    for op in ops:
        q = op.query if hasattr(op, "query") else op
        if not isinstance(q, dict):
            raise TypeError("pipeline updates are not supported by the prototype")
        for k, v in q.items():
            merged[k] = {**merged.get(k, {}), **v} if k == "$set" else v
    merged["$set"] = {**merged.get("$set", {}), "_rev": ObjectId()}
    return merged


def _guard(cls: type, session: Any, bulk_writer: Any) -> Any:
    """Explicit-session and stock-BulkWriter rules. Returns the session to use (ambient, else None)."""
    scope = current_scope()
    if session is not None:
        if not isinstance(session, AsyncClientSession):
            raise InvalidSessionTypeError(type(session))
        batch = current_batch()
        if scope is None and (batch is None or batch.implicit):
            raise UnmanagedSessionError(cls.__name__)
    if bulk_writer is not None and scope is not None and not isinstance(bulk_writer, HBulkWriter):
        raise StockBulkWriterError(cls.__name__)
    return session or current_session()


class Entity(Document):
    """Base of searchable entities (reduced). `id` is client-generated so markers can be written before the data."""

    id: PydanticObjectId = Field(default_factory=PydanticObjectId, alias="_id")  # type: ignore[assignment]
    rev: PydanticObjectId | None = Field(default=None, alias="_rev")

    _find_many_query_class: ClassVar[type] = HFindMany
    _find_one_query_class: ClassVar[type] = HFindOne

    @classmethod
    def _name(cls) -> str:
        return cls.get_collection_name()  # type: ignore[no-any-return]

    @classmethod
    async def _begin(cls, ids: list[Any], session: Any, *, bulk_writer: Any = None) -> bool:
        """Markers first, then record the touched ids. Returns False when the write skips sync (bulk writer)."""
        rt = get_runtime()
        if bulk_writer is not None:  #: a bulk writer skips verification, one dirty marker per entity
            await mark_dirty(rt, cls._name(), session)
            return False
        await mark_pending(rt, cls._name(), ids, session)
        track(cls._name(), ids)
        return True

    @classmethod
    async def _ids_matching(cls, flt: Any, session: Any) -> list[Any]:
        coll = get_runtime().db[cls._name()]
        kw = {"session": session} if session is not None else {}
        return [d["_id"] async for d in coll.find(flt, {"_id": 1}, **kw)]

    # ---- writes -----------------------------------------------------------------------------------------------

    @synced
    async def insert(self, *, session: Any = None, **kw: Any) -> Self:  # type: ignore[override]
        sess = _guard(type(self), session, None)
        self.rev = PydanticObjectId()
        await self._begin([self.id], sess)
        return await super().insert(session=sess, **kw)

    @classmethod
    @synced
    async def insert_many(cls, documents: Any, session: Any = None, **kw: Any) -> InsertManyResult:  # type: ignore[override]
        sess = _guard(cls, session, None)
        docs = list(documents)
        for d in docs:
            d.rev = PydanticObjectId()
        await cls._begin([d.id for d in docs], sess)
        return await super().insert_many(docs, session=sess, **kw)

    @synced
    async def replace(self, *, session: Any = None, bulk_writer: Any = None, **kw: Any) -> Self:  # type: ignore[override]
        sess = _guard(type(self), session, bulk_writer)
        self.rev = PydanticObjectId()
        await self._begin([self.id], sess, bulk_writer=bulk_writer)
        return await super().replace(session=sess, bulk_writer=bulk_writer, **kw)

    @synced
    async def update(self, *args: Any, session: Any = None, bulk_writer: Any = None, **kw: Any) -> Self:  # type: ignore[override]
        sess = _guard(type(self), session, bulk_writer)
        await self._begin([self.id], sess, bulk_writer=bulk_writer)
        return await super().update(_with_rev(args), session=sess, bulk_writer=bulk_writer, **kw)

    @classmethod
    @synced
    async def update_many(cls, flt: Any, *ops: Any, session: Any = None, bulk_writer: Any = None) -> Any:
        sess = _guard(cls, session, bulk_writer)
        ids = await cls._ids_matching(flt, sess)
        await cls._begin(ids, sess, bulk_writer=bulk_writer)
        return await cls.find({"_id": {"$in": ids}}).update(_with_rev(ops), bulk_writer=bulk_writer)  # ambient session

    @synced
    async def delete(self, *, session: Any = None, bulk_writer: Any = None, **kw: Any) -> Any:  # type: ignore[override]
        sess = _guard(type(self), session, bulk_writer)
        await self._begin([self.id], sess, bulk_writer=bulk_writer)
        return await super().delete(session=sess, bulk_writer=bulk_writer, **kw)

    @classmethod
    @synced
    async def delete_many(cls, flt: Any, *, session: Any = None, bulk_writer: Any = None) -> Any:
        sess = _guard(cls, session, bulk_writer)
        ids = await cls._ids_matching(flt, sess)
        await cls._begin(ids, sess, bulk_writer=bulk_writer)
        return await cls.find({"_id": {"$in": ids}}).delete(bulk_writer=bulk_writer)  # ambient session

    @classmethod
    @synced
    async def delete_all(cls, *, session: Any = None, bulk_writer: Any = None, **kw: Any) -> Any:  # type: ignore[override]
        sess = _guard(cls, session, bulk_writer)
        ids = await cls._ids_matching({}, sess)
        await cls._begin(ids, sess, bulk_writer=bulk_writer)
        return await super().delete_all(session=sess, bulk_writer=bulk_writer, **kw)

    @classmethod
    def bulk_writer(  # type: ignore[override]
        cls, session: Any = None, ordered: bool = True, bypass_document_validation: bool | None = False, comment: Any = None
    ) -> BulkWriter:
        return HBulkWriter(session, ordered, cls, bypass_document_validation, comment)
