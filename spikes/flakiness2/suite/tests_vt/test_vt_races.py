"""Q1: each known race forced deterministically on the in-memory engine with virtual time (no container, no repetition).
Every test runs the OLD behaviour and the FIXED behaviour and asserts that the old one fails and the fixed one passes.
OBSERVED_TAIL is above the measured p99 of write visibility lag on the real engine (results/visibility_lag-*.json)."""
from __future__ import annotations

import random
import time

import pytest
from searchkit.contract import Compound, IndexNeverQueryableError, Query, Text
from searchkit.data import oid
from searchkit.lifecycle import Timeouts
from searchkit.ordering import sort_hits
from txkit.env import Item, Note
from txkit.scope import search, transactional

from conftest import row

OBSERVED_TAIL = 1.2  # virtual seconds, chosen from the measured tail


def mk(cls, n, title=None):
    ns = {"tx_item": 1, "tx_note": 2}[cls.get_collection_name()]
    return cls(id=oid(n, ns), title=title or f"tok{n} common")


def _q(tok):
    return Query(Compound(must=(Text(tok, ("title",)),)), limit=1000)


async def total(cls, tok):
    return (await search(cls, _q(tok))).total


async def hits(cls, tok):
    return len((await search(cls, _q(tok))).hits)


async def test_stale_total_after_delete(lag_env):
    """Race 1 (delete-then-search) and purge. The index applies deletes 0.8 s late (measured: up to 0.83 s)."""
    env = lag_env
    spec = env.rt.specs["tx_item"]
    env.lag.delete_lag = 0.8
    t0 = time.perf_counter()

    async def fill():
        await Item.insert_many([mk(Item, n) for n in range(1, 101)])
        assert await total(Item, "common") == 100

    # OLD 1: the legacy delete_all (delete the data, tell the engine, return)
    await fill()
    await env.harness.delete_all_unsafe(spec)
    old1 = (await total(Item, "common"), await hits(Item, "common"))
    assert old1 == (100, 0), "old: page empty, exact total still 100"
    env.lag.release_all()
    await env.lag._advance()

    # OLD 2: purge that waits for absence with a $search probe (reports settled at once)
    await fill()
    ids = [d["_id"] async for d in env.rt.db["tx_item"].find({}, {"_id": 1})]
    await env.rt.db["tx_item"].delete_many({})
    await env.spy.sync_delete("tx_item", spec.index.name, ids)
    left = await env.spy.probe_absent_by_hits("tx_item", spec.index.name, ids)
    old2 = (len(left), await total(Item, "common"))
    assert old2 == (0, 100), "old probe says settled while the total is stale"
    env.lag.release_all()
    await env.lag._advance()

    # FIXED: purge waits by counting index entries
    await fill()
    v0 = env.clock.now
    await env.harness.purge()
    assert await total(Item, "common") == 0
    row("stale total after delete / purge", "old: delete_all, no wait", "bug shown", total=old1[0], hits=old1[1])
    row("stale total after delete / purge", "old: absence probe by $search", "bug shown", total=old2[1])
    row("stale total after delete / purge", "fixed: count probe", "pass", virtual_wait_s=round(env.clock.now - v0, 3),
        real_s=round(time.perf_counter() - t0, 3))


@pytest.mark.parametrize("lag,bound,ok", [(0.0, 1.0, True), (0.5, 1.0, True), (1.0, 1.0, True), (1.0001, 1.0, False), (2.0, 1.0, False), (2.0, 4.0, True)])
async def test_settle_bound_is_exact(lag_env, lag, bound, ok):
    """Race 'settle': the wait ends exactly at its bound. A write visible at the bound passes, one tenth of a millisecond
    later fails with a named group, on every machine."""
    env = lag_env
    env.lag.write_lag = lag
    env.rt.timeouts = Timeouts(sync=bound, index_build=60.0)
    d = mk(Item, 1)
    if ok:
        await d.insert()
    else:
        with pytest.raises(ExceptionGroup) as ei:
            await d.insert()
        assert [e.doc_id for e in ei.value.exceptions] == [d.id]
    row("settle bound", f"lag {lag} s, bound {bound} s", "pass" if ok else "named error as designed")


@pytest.mark.parametrize(
    "bound,good_lag,expect_named_only_bad",
    [(1.0, 0.9, True), (1.0, OBSERVED_TAIL, False), (4.0, OBSERVED_TAIL, True)],
    ids=["bound1.0-lag0.9", "bound1.0-lag1.2-the-flake", "bound4.0-lag1.2-the-fix"],
)
async def test_partial_failure_in_transaction(lag_env, bound, good_lag, expect_named_only_bad):
    """Race 'partial failure in a transaction' (the stress-100 flake). One document never reaches the index, the others lag.
    The claim 'the group names only the unsettled document' holds only if the good documents settle inside the bound."""
    env = lag_env
    good1, bad, good2 = mk(Item, 1), mk(Item, 2), mk(Note, 3, "tok3 note")
    env.lag.never_index.add(bad.id)
    env.lag.write_lag_by_id.update({good1.id: good_lag, good2.id: good_lag})
    env.rt.timeouts = Timeouts(sync=bound, index_build=60.0)

    @transactional
    async def body():
        await good1.insert()
        await bad.insert()
        await good2.insert()

    with pytest.raises(ExceptionGroup) as ei:
        await body()
    named = sorted(e.doc_id for e in ei.value.exceptions)
    assert (named == [bad.id]) is expect_named_only_bad
    row("partial failure in a transaction", f"bound {bound} s, good-document lag {good_lag} s",
        "pass" if expect_named_only_bad else "old test fails (names good documents too)", named_count=len(named))


async def test_reset_order(lag_env):
    """Race 'reset order': the collection is dropped while its index exists (the legacy order). The ghost model makes the
    index never queryable, every time, instead of 2 times in 3."""
    env = lag_env
    env.harness.t = Timeouts(sync=5.0, index_build=3.0)
    t0 = env.clock.now
    with pytest.raises(IndexNeverQueryableError):
        await env.harness.reset(order="legacy-order")
    waited = env.clock.now - t0
    assert waited >= 3.0, "waited the whole bound, then raised a named error"
    steps = await env.harness.reset(order="fixed")  # recovers
    assert steps["total"] >= 0
    row("reset order", "old: collections first", "bug shown (IndexNeverQueryableError)", virtual_wait_s=round(waited, 2))
    row("reset order", "fixed: indexes first", "pass")


def test_tie_order_after_reindex():
    """Race 'tie order after reindex': tied scores come back in an engine-chosen order that changes per index build. Forced
    here by permuting each tie group with a seed per build (5 builds). The id tiebreak must erase the difference."""
    base = [(oid(i), round(1.0 + (i % 4) * 0.37, 6)) for i in range(40)]
    native, ordered = set(), set()
    for build in range(5):
        shuffled = base[:]
        random.Random(build).shuffle(shuffled)
        shuffled.sort(key=lambda h: -h[1])  # score order with the build's arbitrary order inside ties
        native.add(tuple(i for i, _ in shuffled))
        ordered.add(tuple(i for i, _ in sort_hits(shuffled, 6)))
    assert len(native) > 1, "old: native tie order differs between builds"
    assert len(ordered) == 1, "fixed: (score, id) order identical in every build"
    row("tie order after reindex", "old: native order", "bug shown", distinct_orders=len(native))
    row("tie order after reindex", "fixed: (score, id)", "pass", distinct_orders=len(ordered))


async def test_negative_control_purge_with_search_probe(lag_env):
    """Control: swap the fixed purge's count probe for the old $search probe. The same assertion must now fail, which shows
    the test can see the bug it is meant to see (not a test that passes whatever the code does)."""
    env = lag_env
    env.lag.delete_lag = 0.8
    await Item.insert_many([mk(Item, n) for n in range(1, 51)])
    env.lag.probe_absent = env.lag.probe_absent_by_hits  # the mutation
    await env.harness.purge()
    assert await total(Item, "common") == 50, "mutated purge returns with the stale total still visible"
    row("negative control", "fixed purge with the old probe", "bug shown (test is sensitive)")
