"""`@transactional`, `defer()`, `@synced` and the search guard.

One code path for backends A and B (real driver transaction, the engine is a parameter). On backend C (mongomock)
there are no sessions, so the scope runs one of two simulations chosen on the runtime:
  * `mock_tx="snapshot"` (design iii): snapshot the registered collections, restore them on failure.
  * `mock_tx="refuse"`   (design i): raise TransactionsUnsupportedError.
Everything the simulation does is *simulated*:.
"""
from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from typing import Any

from searchkit.lifecycle import SYNC_COLLECTION
from pymongo.errors import PyMongoError

from .errors import SearchInTransactionError, TransactionError, TransactionsUnsupportedError
from .runtime import Pending, Runtime, finalize, get_runtime


class Scope:
    """One outermost transaction. `session` is a real AsyncClientSession on A and B, None on C."""

    def __init__(self, rt: Runtime) -> None:
        self.rt = rt
        self.session: Any = None
        self.pending = Pending()
        self.ended = False
        self.attempt = 0

    def reset_attempt(self) -> None:
        """Called at the start of every attempt: the previous attempt's batch is dropped, legacy)."""
        self.attempt += 1
        self.rt.stats.attempts += 1
        self.pending.clear()


class Batch:
    """`defer()` and implicit `@synced` batch: one wait at the end, no transaction."""

    def __init__(self, implicit: bool = False) -> None:
        self.pending = Pending()
        self.implicit = implicit  # opened by @synced itself: does not make an explicit session= legal


_SCOPE: ContextVar[Scope | None] = ContextVar("txkit_scope", default=None)
_BATCH: ContextVar[Batch | None] = ContextVar("txkit_batch", default=None)


def current_scope() -> Scope | None:
    """The active scope. A task started inside a scope keeps a copy of the context, so an ended scope is ignored nesting test: `session is not None and not session.has_ended`)."""
    s = _SCOPE.get()
    return s if s is not None and not s.ended else None


def current_session() -> Any:
    s = current_scope()
    return s.session if s is not None else None


def current_batch() -> Batch | None:
    return _BATCH.get()


def track(entity: str, ids: list[Any]) -> None:
    s = current_scope()
    if s is not None:
        s.pending.add(entity, ids)
        return
    b = _BATCH.get()
    assert b is not None, "write outside a managed scope (internal error)"
    b.pending.add(entity, ids)


# ---- defer / synced ------------------------------------------------------------------------------------------


@asynccontextmanager
async def defer(*, _implicit: bool = False):  # noqa: ANN201
    """Batch every write made inside the block into one wait at exit. Inside a transaction it joins the scope."""
    if current_scope() is not None or _BATCH.get() is not None:
        yield
        return
    rt = get_runtime()
    batch = Batch(_implicit)
    token = _BATCH.set(batch)
    try:
        yield batch
    except BaseException:
        _BATCH.reset(token)
        try:  # the data is already written (no transaction): still sync what was touched, then re-raise the body's error
            await finalize(rt, batch.pending)
        except Exception:  # noqa: BLE001, S110
            pass
        raise
    else:
        _BATCH.reset(token)
        await finalize(rt, batch.pending)


def synced(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Wrap a write: joins the active scope or batch, else opens an implicit batch so the call returns searchable."""

    @wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if current_scope() is not None or _BATCH.get() is not None:
            return await fn(*args, **kwargs)
        async with defer(_implicit=True):
            return await fn(*args, **kwargs)

    return wrapper


# ---- search guard -------------------------------------------------------------------------------------


async def search(entity_cls: type, query: Any) -> Any:
    rt = get_runtime()
    spec = rt.spec_for_class(entity_cls)
    if current_scope() is not None:
        raise SearchInTransactionError(entity_cls.__name__)
    return await rt.engine.search(spec.name, spec.index.name, query)


# ---- transactional -------------------------------------------------------------------------------------------


class _Unset:
    __slots__ = ()


_UNSET = _Unset()
_RETRY_WINDOW = 120.0  # the driver's with_transaction window


def transactional(func: Callable[..., Awaitable[Any]] | None = None, /, *, on_conflict: Any = _UNSET, retry: bool = True) -> Any:
    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            is_nested = current_scope() is not None
            try:
                if is_nested:
                    return await fn(*args, **kwargs)  # shares the outer transaction, never opens a session
                return await _run_outermost(lambda: fn(*args, **kwargs), retry)
            except TransactionError:
                if is_nested or isinstance(on_conflict, _Unset):
                    raise
                return on_conflict

        return wrapper

    return decorator if func is None else decorator(func)


async def _run_outermost(body: Callable[[], Awaitable[Any]], retry: bool) -> Any:
    rt = get_runtime()
    scope = Scope(rt)
    rt.stats.scopes_opened += 1
    try:
        if rt.backend == "mongo":
            result = await _run_driver(rt, scope, body, retry)
        else:
            result = await _run_mock(rt, scope, body, retry)
    finally:
        scope.ended = True
    # committed (or simulated commit): index changes are applied now, one wait for everything
    await finalize(rt, scope.pending)
    return result


async def _attempt(scope: Scope, body: Callable[[], Awaitable[Any]]) -> Any:
    scope.reset_attempt()
    token = _SCOPE.set(scope)
    try:
        return await body()
    finally:
        _SCOPE.reset(token)


async def _run_driver(rt: Runtime, scope: Scope, body: Callable[[], Awaitable[Any]], retry: bool) -> Any:
    async with rt.client.start_session() as session:
        scope.session = session
        if retry:  # the driver may re-run the body on a transient error
            return await session.with_transaction(lambda _s: _attempt(scope, body))
        async with await session.start_transaction():  # exactly one run
            return await _attempt(scope, body)


async def _run_mock(rt: Runtime, scope: Scope, body: Callable[[], Awaitable[Any]], retry: bool) -> Any:
    if rt.mock_tx == "refuse":
        raise TransactionsUnsupportedError
    async with rt.mock_lock:  # simulated isolation: one scope at a time (no snapshot isolation exists here)
        snap = await _snapshot(rt)
        t0 = time.monotonic()
        while True:
            try:
                return await _attempt(scope, body)
            except BaseException as exc:
                await _restore(rt, snap)  # simulated rollback
                transient = isinstance(exc, PyMongoError) and exc.has_error_label("TransientTransactionError")
                if retry and transient and time.monotonic() - t0 < _RETRY_WINDOW:
                    continue  # simulated with_transaction retry
                raise


def _collections(rt: Runtime) -> list[str]:
    return sorted(rt.specs) + [SYNC_COLLECTION]


async def _snapshot(rt: Runtime) -> dict[str, list[dict[str, Any]]]:
    return {c: await rt.db[c].find({}).to_list(None) for c in _collections(rt)}


async def _restore(rt: Runtime, snap: dict[str, list[dict[str, Any]]]) -> None:
    for c, docs in snap.items():
        await rt.db[c].delete_many({})
        if docs:
            await rt.db[c].insert_many(docs)
