"""Planted tests on the in-memory engine with virtual time (no container).

Each race has a buggy and a fixed variant. In natural mode the lag is drawn from a declared model, so the buggy variant is
a flake with a known rate. In forced mode (strategy S3) the same test runs with a deterministic injected value: the
tail lag 1.2 s, or the ghost, or the stale delete.

The lag models are inputs chosen for this experiment. They are anchored on the measured runner lag (median 1.003 s,
p99 1.013 s, max 1.021 s, 400 samples) but they are not measurements of any engine, so each truth below is the truth of the model.
"""
from __future__ import annotations

import asyncio
import math
from functools import partial

import pytest
from searchkit.contract import IndexNeverQueryableError
from searchkit.lifecycle import Timeouts
from searchkit.ordering import sort_hits
from searchkit.data import oid
from txkit.scope import transactional
from vt.vtime import VClock

from .core import Ctx, reg
from .env import Item, Note, lag_env, mk, total

TAIL = 1.2  # forced lag, above the largest lag seen on either machine
SET_MU, SET_SD = 1.003, 0.012


def _phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


P_SETTLE_EQ = 1 - _phi((1.0 - SET_MU) / SET_SD)  # P(lag > 1.0 s) of the normal model
P_EACH = 0.005
SCALE_TAIL = 0.5 / math.log(1 / P_EACH)  # lag = 0.5 + Exp(scale) gives P(lag > 1.0) = 0.005 exactly
P_TX = 1 - (1 - P_EACH) ** 2  # either of two good documents later than 1.0 s
P_GHOST = 0.9  # the reset-order model: the collection drop leaves a ghost index with this probability


async def stale_total(ctx: Ctx, fixed: bool) -> None:
    async with lag_env() as env:
        spec = env.rt.specs["tx_item"]
        env.lag.delete_lag = 0.8
        await Item.insert_many([mk(Item, n) for n in range(1, 101)])
        assert await total(Item, "common") == 100
        if fixed:
            await env.harness.purge()
        else:
            await env.harness.delete_all_unsafe(spec)
        assert await total(Item, "common") == 0


async def purge_probe(ctx: Ctx, fixed: bool) -> None:
    async with lag_env() as env:
        env.lag.delete_lag = 0.8
        await Item.insert_many([mk(Item, n) for n in range(1, 51)])
        if not fixed:
            env.lag.probe_absent = env.lag.probe_absent_by_hits  # the old probe
        await env.harness.purge()
        assert await total(Item, "common") == 0


async def reset_order(ctx: Ctx, fixed: bool) -> None:
    async with lag_env() as env:
        env.harness.t = Timeouts(sync=5.0, index_build=3.0)
        if fixed:
            await env.harness.reset(order="fixed")
            return
        if not (ctx.forced or ctx.urand() < P_GHOST):
            env.lag.collection_dropped = lambda entity: None  # the engine recovered by itself this time
        await env.harness.reset(order="legacy-order")


async def tie_order(ctx: Ctx, fixed: bool) -> None:
    """Page of 6 over scores with one tied pair at ranks 6 and 7. The native order of the pair is the engine's choice."""
    hits = [(oid(i), 10.0 - i) for i in range(1, 6)] + [(oid(6), 4.0), (oid(7), 4.0)] + [(oid(i), 3.0 - i / 100) for i in range(8, 40)]
    golden = [i for i, _ in sort_hits(hits)][:6]
    if fixed:
        got = [i for i, _ in sort_hits(hits)][:6]
    else:
        build = list(hits)
        pair = [h for h in build if h[1] == 4.0]
        if ctx.forced:
            pair = sorted(pair, key=lambda h: h[0], reverse=True)  # the other order of the pair
        elif ctx.urand() < 0.5:
            pair.reverse()
        rest = [h for h in build if h[1] != 4.0]
        native = sorted(rest, key=lambda h: -h[1])
        native[5:5] = pair
        got = [i for i, _ in native][:6]
    assert got == golden, "page differs from the expected total order"


async def settle_eq(ctx: Ctx, fixed: bool) -> None:
    async with lag_env() as env:
        env.lag.write_lag = TAIL if ctx.forced else max(0.0, ctx.rng().gauss(SET_MU, SET_SD))
        env.rt.timeouts = Timeouts(sync=4.0 if fixed else 1.0, index_build=60.0)
        await mk(Item, 1).insert()


async def tx_partial(ctx: Ctx, fixed: bool) -> None:
    async with lag_env() as env:
        good1, bad, good2 = mk(Item, 1), mk(Item, 2), mk(Note, 3, "tok3 note")
        env.lag.never_index.add(bad.id)
        r = ctx.rng()
        lags = [TAIL, TAIL] if ctx.forced else [0.5 - SCALE_TAIL * math.log(1 - r.random()) for _ in range(2)]
        env.lag.write_lag_by_id.update({good1.id: lags[0], good2.id: lags[1]})
        env.rt.timeouts = Timeouts(sync=4.0 if fixed else 1.0, index_build=60.0)

        @transactional
        async def body() -> None:
            await good1.insert()
            await bad.insert()
            await good2.insert()

        with pytest.raises(ExceptionGroup) as ei:
            await body()
        assert sorted(e.doc_id for e in ei.value.exceptions) == [bad.id], "the group names only the unsettled document"


MEAN_D = 1.0
BOUND_D = MEAN_D * math.log(100)  # P(Exp(mean) > bound) = 0.01 exactly


async def delay_1pct(ctx: Ctx, fixed: bool) -> None:
    """A bounded wait on an operation whose delay is injected: Exp(mean 1) virtual seconds against a bound that the delay exceeds
    with probability exactly 0.01. Forced: the delay is set to 1.01 x the bound."""
    clock = VClock()
    bound = BOUND_D * (3.0 if fixed else 1.0)
    delay = BOUND_D * 1.01 if ctx.forced else -MEAN_D * math.log(1 - ctx.urand())
    await clock.sleep(min(delay, bound))
    assert delay <= bound, "operation did not finish inside its bound"


def _r(name, kind, truth, fn, **kw):
    for variant, fixed in (("buggy", False), ("fixed", True)):
        reg(name if not fixed else name + "-fixed", kind, 0.0 if fixed else truth, "inmem", variant, partial(fn, fixed=fixed), **kw)


_r("stale-total", "race", 1.0, stale_total, forced=True, cost="env", note="delete then exact total with no wait; deterministic by the 0.8 s delete lag")
_r("purge-probe", "race", 1.0, purge_probe, forced=True, cost="env", note="purge waiting with a $search probe")
_r("reset-order", "race", P_GHOST, reset_order, forced=True, cost="env", note=f"legacy reset order; ghost with p={P_GHOST} (model)")
_r("tie-order", "race", 0.5, tie_order, forced=True, cost="micro", note="tied pair at the page boundary, native order is a coin flip")
_r("settle-eq", "race", P_SETTLE_EQ, settle_eq, forced=True, cost="env", note="bound 1.0 s, lag ~ N(1.003, 0.012)")
_r("tx-partial", "race", P_TX, tx_partial, forced=True, cost="env", note="s14 shape: bound 1.0 s, two good documents with lag 0.5+Exp, p=0.005 each")
_r("delay-1pct", "delay", 0.01, delay_1pct, forced=True, cost="micro", note="Exp(1) virtual delay against bound ln(100): P=0.01 exactly")
