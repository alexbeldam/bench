"""The stress subset: only the race-prone tests, on backends A and B, repeated inside one
session-scoped container with `--stress-repeats N`. No retry plugin: each repetition is its own test id (r001, r002 ...).

Contents (each is a reproduction from the earlier measurement or a transaction test from this one):
  delete-then-search, purge, reset (fixed order), settle (batched write wait), the two bounds (documents never
  searchable, index never queryable), and the transaction commit wait / rollback / partial failure.
"""
import asyncio
import pathlib
import sys
import time

import pytest
import pytest_asyncio

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from searchkit.contract import IndexNeverQueryableError
from searchkit.lifecycle import Timeouts
from txkit.env import Item, Note
from txkit.scope import defer, transactional
from scenarios import (
    markers,
    mk,
    ndocs,
    s01_commit_one_wait,
    s02_rollback_by_exception,
    s03_rollback_keeps_existing_state,
    s14_partial_failure_at_commit,
    total,
)

from conftest import _get_env

pytestmark = pytest.mark.stress


@pytest.fixture(params=["A", "B"])
def sbackend(request):
    if request.param not in request.config.getoption("--backends").split(","):
        pytest.skip(f"backend {request.param} not selected")
    return request.param


@pytest_asyncio.fixture
async def senv(sbackend):
    e = await _get_env(sbackend)
    await e.activate()
    e.fresh()
    yield e
    await e.purge()


async def test_tx_commit_wait(senv, stress_rep):
    await s01_commit_one_wait(senv)


async def test_tx_rollback(senv, stress_rep):
    await s02_rollback_by_exception(senv)
    await senv.purge()
    await s03_rollback_keeps_existing_state(senv)


async def test_tx_partial_failure(senv, stress_rep):
    await s14_partial_failure_at_commit(senv)


async def test_delete_then_search(senv, stress_rep):
    """legacy failure 1: delete-all then an immediate search/total must not see the deleted documents."""
    await Item.insert_many([mk(Item, n) for n in range(1, 101)])
    assert await total(Item, "common") == 100
    await Item.delete_many({"price": 1})
    assert await total(Item, "common") == 0  # exact total read right after the delete returned
    await Item.insert_many([mk(Item, n) for n in range(1, 101)])
    await Item.delete_all()
    assert await total(Item, "common") == 0 and await ndocs(senv, Item) == 0


async def test_purge(senv, stress_rep):
    await Item.insert_many([mk(Item, n) for n in range(1, 201)])
    await Note.insert_many([mk(Note, n, f"tok{n} note") for n in range(1, 51)])
    await senv.harness.purge()
    assert await total(Item, "common") == 0 and await total(Note, "note") == 0
    assert await ndocs(senv, Item) == 0 and await markers(senv) == []


async def test_reset_fixed_order(senv, stress_rep):
    await Item.insert_many([mk(Item, n) for n in range(1, 51)])
    steps = await senv.harness.reset(order="fixed")
    assert steps["total"] >= 0
    assert await ndocs(senv, Item) == 0
    await Item.insert_many([mk(Item, n) for n in range(1, 21)])  # the recreated index is queryable and syncs
    assert await total(Item, "common") == 20


async def test_settle_batched(senv, stress_rep):
    w0 = senv.rt.stats.waits
    async with defer():
        for n in range(1, 31):
            await mk(Item, n).insert()
    assert senv.rt.stats.waits - w0 == 1
    assert await total(Item, "common") == 30


async def test_bound_document_never_searchable(senv, stress_rep):
    bad = mk(Item, 1)
    if senv.backend == "A":
        senv.spy.hide_probe.add(bad.id)
    else:
        senv.spy.never_index.add(bad.id)
    bound = 3.0 if senv.backend == "A" else 1.0  # on A a 1.0 s bound is shorter than the index's catch-up (see s14)
    senv.rt.timeouts = Timeouts(sync=bound, index_build=60.0)
    t0 = time.perf_counter()
    with pytest.raises(ExceptionGroup) as ei:
        async with defer():
            await bad.insert()
            await mk(Item, 2).insert()
    elapsed = time.perf_counter() - t0
    assert [e.doc_id for e in ei.value.exceptions] == [bad.id]
    assert bound <= elapsed < bound + 3.0, "bounded: raised at the bound, not before and not much after"


async def test_bound_index_never_queryable(senv, stress_rep):
    """Collections dropped before indexes (the legacy order) is a race: it may pass or raise, but never wait past the
    bound. On the in-memory engine it always passes."""
    h = senv.harness
    old = h.t
    h.t = Timeouts(sync=5.0, index_build=3.0)
    t0 = time.perf_counter()
    try:
        await h.reset(order="legacy-order")
        outcome = "ok"
    except IndexNeverQueryableError:
        outcome = "bounded-error"
    finally:
        h.t = old
    elapsed = time.perf_counter() - t0
    assert elapsed < 3.0 + 5.0, f"{outcome}: waited past the bound"
    await h.reset(order="fixed")  # always recovers with the fixed order


async def test_fanout_complete_and_ordered(senv, stress_rep):
    """Warm fan-out of 50 inside one transaction: complete, one wait, canonical apply order."""
    if senv.backend != "B":
        pytest.skip("ordering of engine calls is observed on the in-memory engine path")

    @transactional(retry=False)
    async def body():
        await mk(Item, 0).insert()
        await asyncio.gather(*(mk(Item, i).insert() for i in range(1, 51)))

    await body()
    assert await total(Item, "common") == 51 and senv.rt.stats.waits == 1
    (_, ids), = senv.spy.writes_applied()
    assert list(ids) == sorted(ids)
