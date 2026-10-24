"""One scenario suite, run on backends A, B and C (tests/test_parity.py). Identifiers and counts only, no field values
beyond synthetic tokens (`tokN`). Each scenario takes an `Env` and asserts the behaviour the spec describes."""
from __future__ import annotations

import asyncio
from typing import Any

import pytest
from beanie import BulkWriter
from beanie.operators import Set
from searchkit.contract import Compound, Query, Text
from searchkit.data import oid
from searchkit.lifecycle import SYNC_COLLECTION, Timeouts
from txkit.entity import Entity
from txkit.env import Env, Item, Note
from txkit.errors import (
    InvalidSessionTypeError,
    SearchInTransactionError,
    StockBulkWriterError,
    TransactionError,
    UnmanagedSessionError,
)
from txkit.scope import current_scope, defer, search, synced, transactional
from pymongo.errors import PyMongoError

SYNC_BOUND = {"A": 4.0, "B": 1.0, "C": 1.0}  # A: longer than Atlas's index catch-up (see s14); the in-memory engine settles at once
NS = {"tx_item": 1, "tx_note": 2}


def tq(tok: str) -> Query:
    return Query(Compound(must=(Text(tok, ("title",)),)), limit=1000)


async def total(cls: type, tok: str) -> int:
    return (await search(cls, tq(tok))).total


async def ids_for(cls: type, tok: str) -> set[Any]:
    return {h.id for h in (await search(cls, tq(tok))).hits}


def mk(cls: type[Entity], n: int, title: str | None = None, **kw: Any) -> Any:
    return cls(id=oid(n, NS[cls.get_collection_name()]), title=title or f"tok{n} common", **kw)  # type: ignore[attr-defined]


async def ndocs(env: Env, cls: type) -> int:
    return await env.rt.db[cls.get_collection_name()].count_documents({})  # type: ignore[attr-defined]


async def markers(env: Env) -> list[dict[str, Any]]:
    return await env.rt.db[SYNC_COLLECTION].find({}).to_list(None)


async def in_engine(env: Env, doc: Entity) -> bool:
    """Is the document's current revision visible in the engine? (raw probe, bypasses the scope guard)"""
    spec = env.rt.spec_for_class(type(doc))
    seen = await env.spy.inner.probe(spec.name, spec.index.name, [(doc.id, doc.rev)])
    return doc.id in seen


async def no_leftovers(env: Env, *docs: Entity) -> None:
    assert await markers(env) == [], "markers left behind"
    for d in docs:
        assert not await in_engine(env, d), "engine entry left behind"
        assert await type(d).get(d.id) is None, "data left behind"


def transient() -> PyMongoError:
    return PyMongoError("simulated transient error", error_labels=["TransientTransactionError"])


# ---- Q2 scenarios in the transaction ----------------------------------------------------------------------------


async def s01_commit_one_wait(env: Env) -> None:
    a, b, c = mk(Item, 1), mk(Item, 2), mk(Note, 3, "tok3 note")
    seen_in_body: list[bool] = []

    @transactional
    async def body() -> None:
        await a.insert()
        await b.insert()
        await c.insert()
        await a.update({"$set": {"price": 5}})
        seen_in_body.append(await in_engine(env, a) or await in_engine(env, c))  # no engine entry before commit
        assert env.spy.writes_applied() == []  # nothing applied to the engine inside the scope

    w0 = env.rt.stats.waits
    await body()
    assert seen_in_body == [False]
    assert env.rt.stats.waits - w0 == 1, "one combined wait at commit"
    assert await total(Item, "common") == 2 and await total(Note, "tok3") == 1
    assert await markers(env) == []
    # engine changes applied after commit in canonical order: entity name, then id
    assert [e for e, _ in env.spy.writes_applied()] == ["tx_item", "tx_note"]
    assert list(env.spy.writes_applied()[0][1]) == sorted([a.id, b.id])


async def s02_rollback_by_exception(env: Env) -> None:
    a, b = mk(Item, 1), mk(Note, 2, "tok2 note")

    @transactional
    async def body() -> None:
        await a.insert()
        await b.insert()
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await body()
    await no_leftovers(env, a, b)
    assert await total(Item, "common") == 0
    assert env.spy.writes_applied() == []


async def s03_rollback_keeps_existing_state(env: Env) -> None:
    keep = mk(Item, 1, "tok1 keep")
    gone = mk(Item, 2, "tok2 gone")
    await keep.insert()
    await gone.insert()
    old_rev = keep.rev
    env.spy.calls.clear()

    @transactional
    async def body() -> None:
        await keep.update({"$set": {"title": "tok9 changed"}})
        await gone.delete()
        await Item(id=oid(3, 1), title="tok3 new").insert()
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await body()
    assert env.spy.writes_applied() == []
    assert await markers(env) == []
    assert await Item.get(keep.id) is not None and await Item.get(gone.id) is not None
    assert await Item.get(oid(3, 1)) is None
    fresh = await Item.get(keep.id)
    assert fresh.title == "tok1 keep" and fresh.rev == old_rev  # type: ignore[union-attr]
    assert await total(Item, "tok1") == 1 and await total(Item, "tok9") == 0 and await total(Item, "tok2") == 1


async def s04_nested_shares_and_rolls_back_with_outer(env: Env) -> None:
    inner_scope: list[Any] = []

    @transactional
    async def inner() -> None:
        inner_scope.append(current_scope())
        await mk(Item, 2).insert()

    @transactional
    async def outer() -> None:
        outer_scope = current_scope()
        await mk(Item, 1).insert()
        await inner()
        assert inner_scope[0] is outer_scope
        raise ValueError("boom after inner returned")

    s0 = env.rt.stats.scopes_opened
    with pytest.raises(ValueError):
        await outer()
    assert env.rt.stats.scopes_opened - s0 == 1, "only the outermost opens a session"
    assert await ndocs(env, Item) == 0 and await markers(env) == []
    assert env.spy.writes_applied() == []


async def s05_nested_transaction_error_rolls_back_outer(env: Env) -> None:
    class Conflict(TransactionError): ...

    @transactional(on_conflict="inner-fallback")
    async def inner() -> None:
        await mk(Item, 2).insert()
        raise Conflict

    @transactional(on_conflict="outer-fallback")
    async def outer() -> Any:
        await mk(Item, 1).insert()
        await inner()
        return "not reached"

    assert await outer() == "outer-fallback", "nested on_conflict is ignored, only the outermost absorbs"
    assert await ndocs(env, Item) == 0 and await markers(env) == []

    @transactional
    async def outer2() -> Any:
        await mk(Item, 1).insert()
        await inner()

    with pytest.raises(Conflict):  # nothing absorbs it: it propagates
        await outer2()
    assert await ndocs(env, Item) == 0 and env.spy.writes_applied() == []


async def s06_on_conflict_fallback(env: Env) -> None:
    @transactional(on_conflict=-1)
    async def conflict() -> int:
        await mk(Item, 1).insert()
        raise TransactionError

    @transactional(on_conflict=-1)
    async def other() -> int:
        await mk(Item, 2).insert()
        raise ValueError("not a conflict")

    @transactional(on_conflict=-1)
    async def fine() -> int:
        await mk(Item, 3).insert()
        return 7

    assert await conflict() == -1
    assert await ndocs(env, Item) == 0
    with pytest.raises(ValueError):
        await other()
    assert await ndocs(env, Item) == 0
    assert await fine() == 7 and await ndocs(env, Item) == 1 and await total(Item, "tok3") == 1


async def s07_retry_true_reruns_and_clears_batch(env: Env) -> None:
    attempts: list[int] = []

    @transactional
    async def body() -> str:
        attempts.append(1)
        if len(attempts) == 1:
            await mk(Item, 1).insert()
            await mk(Item, 2).insert()
            raise transient()
        await mk(Item, 3).insert()
        return "ok"

    assert await body() == "ok"
    assert len(attempts) == 2
    assert await ndocs(env, Item) == 1 and await markers(env) == []
    applied = env.spy.writes_applied()
    assert applied == [("tx_item", (oid(3, 1),))], "the first attempt's ids were cleared from the batch"
    assert await total(Item, "tok3") == 1 and await total(Item, "tok1") == 0


async def s08_retry_false_runs_once(env: Env) -> None:
    attempts: list[int] = []

    @transactional(retry=False)
    async def body() -> None:
        attempts.append(1)
        await mk(Item, 1).insert()
        raise transient()

    with pytest.raises(PyMongoError):
        await body()
    assert len(attempts) == 1
    assert await ndocs(env, Item) == 0 and await markers(env) == [] and env.spy.writes_applied() == []


async def s09_session_outside_scope(env: Env, real_session: Any) -> None:
    with pytest.raises(UnmanagedSessionError):
        await mk(Item, 1).insert(session=real_session)
    with pytest.raises(UnmanagedSessionError):
        await Item.delete_all(session=real_session)
    with pytest.raises(InvalidSessionTypeError):
        await mk(Item, 1).insert(session="not a session")
    assert await ndocs(env, Item) == 0 and await markers(env) == []


async def s10_search_inside_scope(env: Env) -> None:
    @transactional
    async def body() -> None:
        await mk(Item, 1).insert()
        await search(Item, tq("tok1"))

    with pytest.raises(SearchInTransactionError):
        await body()
    assert await ndocs(env, Item) == 0
    assert await total(Item, "tok1") == 0  # outside a scope the search works


async def s11_deletes_in_scope_commit(env: Env) -> None:
    docs = [mk(Item, n, f"tok{n} common", category="cat-1" if n % 2 else "cat-2") for n in range(1, 5)]
    note = mk(Note, 9, "tok9 note")
    for d in [*docs, note]:
        await d.insert()
    env.spy.calls.clear()

    @transactional
    async def body() -> None:
        await docs[0].delete()
        await Item.delete_many({"category": "cat-2"})
        await Note.delete_all()
        assert await in_engine(env, docs[1])  # still indexed inside the scope

    await body()
    assert await ndocs(env, Item) == 1 and await ndocs(env, Note) == 0
    assert await total(Item, "common") == 1 and await total(Note, "tok9") == 0
    assert await markers(env) == []


async def s11b_deletes_in_scope_rollback(env: Env) -> None:
    docs = [mk(Item, n) for n in range(1, 4)]
    for d in docs:
        await d.insert()
    env.spy.calls.clear()

    @transactional
    async def body() -> None:
        await docs[0].delete()
        await Item.delete_all()
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await body()
    assert await ndocs(env, Item) == 3 and await total(Item, "common") == 3
    assert env.spy.writes_applied() == [] and await markers(env) == []


async def s12_bulk_writes_in_scope(env: Env) -> None:
    @transactional
    async def body() -> None:
        await Item.insert_many([mk(Item, n) for n in range(1, 6)])
        await Item.update_many({"price": 1}, Set({"title": "tok0 bulk"}))
        await Item.delete_many({"_id": oid(5, 1)})

    w0 = env.rt.stats.waits
    await body()
    assert env.rt.stats.waits - w0 == 1
    assert await ndocs(env, Item) == 4 and await total(Item, "tok0") == 4 and await total(Item, "tok1") == 0
    assert await total(Item, "tok5") == 0 and await markers(env) == []


async def s13_failing_write_mid_body(env: Env) -> None:
    @transactional
    async def body() -> None:
        await mk(Item, 1).insert()
        await mk(Item, 2).insert()
        await Item(id=oid(1, 1), title="tok1 dup").insert()  # duplicate _id

    with pytest.raises(Exception) as ei:
        await body()
    assert "DuplicateKey" in type(ei.value).__name__
    assert await ndocs(env, Item) == 0 and await markers(env) == [] and env.spy.writes_applied() == []


async def s13b_failed_write_caught_in_body(env: Env) -> None:
    """The body swallows the failed write and returns. On a real server the failed command aborts the transaction;
    on mongomock nothing aborts. Recorded as a difference, not asserted equal."""
    await mk(Item, 1).insert()
    env.spy.calls.clear()
    outcome: dict[str, Any] = {}

    @transactional(retry=False)  # with retry=True the driver re-runs the body for 120 s (experiments/caught_error.py)
    async def body() -> None:
        await mk(Item, 2).insert()
        try:
            await Item(id=oid(1, 1), title="tok1 dup").insert()
        except Exception as exc:  # noqa: BLE001
            outcome["caught"] = type(exc).__name__
        await mk(Item, 3).insert()

    try:
        await body()
        outcome["result"] = "committed"
    except Exception as exc:  # noqa: BLE001
        outcome["result"] = f"raised {type(exc).__name__}"
    outcome["stored"] = await ndocs(env, Item)
    env.notes["s13b"] = outcome
    assert outcome["caught"].startswith("DuplicateKey")
    if env.backend == "C":
        assert outcome["result"] == "committed" and outcome["stored"] == 3  # nothing aborts on mongomock
    else:
        assert outcome["stored"] == 1  # real server aborted: only the pre-existing document remains


async def s14_partial_failure_at_commit(env: Env) -> None:
    good1, bad, good2 = mk(Item, 1), mk(Item, 2), mk(Note, 3, "tok3 note")
    if env.backend == "A":
        env.spy.hide_probe.add(bad.id)  # injected on A: the real index does index it, the probe is told otherwise
    else:
        env.spy.never_index.add(bad.id)  # on B and C the engine really never gets the document
    # The bound must exceed the real index's catch-up time: with 1.0 s the good documents were unsettled once in 100 runs on A
    # (stress run 44), because Atlas needs about 1.1 s (median, measured by experiments/settle_latency.py).
    env.rt.timeouts = Timeouts(sync=SYNC_BOUND[env.backend], index_build=60.0)

    @transactional
    async def body() -> None:
        await good1.insert()
        await bad.insert()
        await good2.insert()

    t = env.rt.stats.waits
    with pytest.raises(ExceptionGroup) as ei:
        await body()
    named = [getattr(e, "doc_id", None) for e in ei.value.exceptions]
    assert named == [bad.id], "the group names only the unsettled document"
    assert env.rt.stats.waits - t == 1
    assert await ndocs(env, Item) == 2 and await ndocs(env, Note) == 1  # data stays stored
    ms = await markers(env)
    assert [(m["entity"], m["doc_id"], m["state"]) for m in ms] == [("tx_item", bad.id, "failed")]
    assert await total(Item, "tok1") == 1 and await total(Note, "tok3") == 1


# ---- Q3: sync write path outside transactions -------------------------------------------------------------------


async def s20_synced_single_write(env: Env) -> None:
    it = mk(Item, 1)
    await it.insert()
    assert await total(Item, "tok1") == 1 and await markers(env) == []
    assert env.rt.stats.waits == 1


async def s21_synced_insert_many(env: Env) -> None:
    await Item.insert_many([mk(Item, n) for n in range(1, 21)])
    assert await total(Item, "common") == 20 and env.rt.stats.waits == 1 and await markers(env) == []


async def s22_synced_update_operator(env: Env) -> None:
    it = mk(Item, 1)
    await it.insert()
    await it.update({"$set": {"title": "tok7 renamed"}})
    assert await total(Item, "tok1") == 0 and await total(Item, "tok7") == 1
    await Item.update_many({"title": {"$regex": "^tok7"}}, Set({"title": "tok8 again"}))
    assert await total(Item, "tok7") == 0 and await total(Item, "tok8") == 1 and await markers(env) == []


async def s23_synced_replace(env: Env) -> None:
    it = mk(Item, 1)
    await it.insert()
    old_rev = it.rev
    it.title = "tok6 replaced"
    await it.replace()
    assert it.rev != old_rev
    assert await total(Item, "tok1") == 0 and await total(Item, "tok6") == 1 and await markers(env) == []


async def s24_synced_delete_variants(env: Env) -> None:
    docs = [mk(Item, n, f"tok{n} common", category="cat-1" if n < 4 else "cat-2") for n in range(1, 7)]
    await Item.insert_many(docs)
    await docs[0].delete()
    assert await total(Item, "tok1") == 0 and await total(Item, "common") == 5
    await Item.delete_many({"category": "cat-1"})
    assert await total(Item, "common") == 3
    await Item.delete_all()
    assert await total(Item, "common") == 0 and await ndocs(env, Item) == 0 and await markers(env) == []


async def s25_defer_batches_one_wait(env: Env) -> None:
    w0 = env.rt.stats.waits
    async with defer():
        for n in range(1, 6):
            await mk(Item, n).insert()
        await mk(Note, 9, "tok9 note").insert()
        assert env.spy.writes_applied() == [], "nothing is synced until the block ends"
    assert env.rt.stats.waits - w0 == 1
    assert await total(Item, "common") == 5 and await total(Note, "tok9") == 1
    w1 = env.rt.stats.waits
    for n in range(11, 16):
        await mk(Item, n).insert()
    assert env.rt.stats.waits - w1 == 5, "without defer(): one wait per write"

    @synced
    async def free_function() -> None:
        await mk(Item, 21).insert()
        await mk(Item, 22).insert()

    w2 = env.rt.stats.waits
    await free_function()
    assert env.rt.stats.waits - w2 == 1


async def s26_bulk_writer_guard(env: Env) -> None:
    for n in range(1, 4):
        await mk(Item, n).insert()
    env.spy.calls.clear()

    @transactional
    async def stock_inside() -> None:
        async with BulkWriter() as bw:  # a stock writer built by the application
            await Item.update_many({}, Set({"price": 7}), bulk_writer=bw)

    with pytest.raises(StockBulkWriterError):
        await stock_inside()
    assert await Item.find({"price": 7}).count() == 0, "no data command was sent"


async def s27_kit_bulk_writer_in_scope(env: Env) -> None:
    for n in range(1, 4):
        await mk(Item, n).insert()
    env.spy.calls.clear()

    @transactional
    async def body() -> None:
        async with Item.bulk_writer() as bw:
            await Item.update_many({}, Set({"title": "tok5 bulk"}), bulk_writer=bw)

    await body()
    assert await Item.find({"title": "tok5 bulk"}).count() == 3
    ms = await markers(env)
    assert [(m.get("entity"), m.get("kind"), m.get("state")) for m in ms] == [("tx_item", "entity", "dirty")]
    assert env.spy.writes_applied() == [], "a bulk writer skips verification"


async def s28_stock_bulk_writer_outside_scope(env: Env) -> None:
    for n in range(1, 4):
        await mk(Item, n).insert()
    async with BulkWriter() as bw:
        await Item.update_many({}, Set({"price": 7}), bulk_writer=bw)
    assert await Item.find({"price": 7}).count() == 3


async def s29_no_search_mode(env: Env) -> None:
    env.rt.no_search = True

    @transactional
    async def body() -> None:
        await mk(Item, 1).insert()
        await mk(Item, 2).insert()

    await body()
    assert await ndocs(env, Item) == 2 and await markers(env) == []
    assert env.spy.writes_applied() == [] and env.rt.stats.waits == 0


async def s30_after_commit_engine_failure_leaves_data(env: Env) -> None:
    """The engine itself fails after commit (not a probe timeout): the data stays committed, marker stays pending."""
    if env.backend == "A":
        pytest.skip("Atlas sync_write is a no-op, nothing to fail")
    boom = RuntimeError("engine down")
    real = env.spy.inner.sync_write

    async def failing(entity: str, index: str, docs: Any) -> None:
        raise boom

    env.spy.inner.sync_write = failing  # type: ignore[method-assign]
    try:
        @transactional
        async def body() -> None:
            await mk(Item, 1).insert()

        with pytest.raises(RuntimeError):
            await body()
    finally:
        env.spy.inner.sync_write = real  # type: ignore[method-assign]
    assert await ndocs(env, Item) == 1
    ms = await markers(env)
    assert [(m["doc_id"], m["state"]) for m in ms] == [(oid(1, 1), "pending")], "marker stays pending: detectable"


async def s31_bulk_writer_search_visibility(env: Env) -> None:
    """A document changed through this layer's bulk writer skips verification. Does it ever become searchable?
    Atlas follows the oplog on its own; the test engine only hears about writes this layer syncs. Recorded, not equal."""
    await mk(Item, 1).insert()
    async with Item.bulk_writer() as bw:
        await Item.update_many({}, Set({"title": "tok5 bulk"}), bulk_writer=bw)
    found = False
    for _ in range(40):  # up to 10 s
        if await total(Item, "tok5") == 1:
            found = True
            break
        await asyncio.sleep(0.25)
    env.notes["s31_found_within_10s"] = found
    assert found == (env.backend == "A")


SCENARIOS = {
    f.__name__: f
    for f in [
        s01_commit_one_wait, s02_rollback_by_exception, s03_rollback_keeps_existing_state,
        s04_nested_shares_and_rolls_back_with_outer, s05_nested_transaction_error_rolls_back_outer,
        s06_on_conflict_fallback, s07_retry_true_reruns_and_clears_batch, s08_retry_false_runs_once,
        s09_session_outside_scope, s10_search_inside_scope, s11_deletes_in_scope_commit,
        s11b_deletes_in_scope_rollback, s12_bulk_writes_in_scope, s13_failing_write_mid_body,
        s13b_failed_write_caught_in_body, s14_partial_failure_at_commit, s20_synced_single_write,
        s21_synced_insert_many, s22_synced_update_operator, s23_synced_replace, s24_synced_delete_variants,
        s25_defer_batches_one_wait, s26_bulk_writer_guard, s27_kit_bulk_writer_in_scope,
        s28_stock_bulk_writer_outside_scope, s29_no_search_mode, s31_bulk_writer_search_visibility, s30_after_commit_engine_failure_leaves_data,
    ]
}
