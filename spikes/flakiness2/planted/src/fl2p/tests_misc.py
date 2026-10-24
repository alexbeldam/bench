"""Controlled-rate flakes with known truth, a wall-clock race, an order dependence, and the clean control set."""
from __future__ import annotations

import asyncio
import heapq
import itertools
import json
import os
import re
import threading
import time
from collections import Counter, deque
from decimal import Decimal
from functools import partial

from searchkit.ordering import sort_hits
from searchkit.data import oid

from .core import Ctx, reg
from .env import lag_env


# ---- controlled-rate flakes ----------------------------------------------------------------------------------------


async def rng_fail(ctx: Ctx, p: float) -> None:
    assert ctx.urand() >= p, "drawn below the failure probability"


reg("rng-1pct", "rate", 0.01, "none", "buggy", partial(rng_fail, p=0.01), note="fails iff a urandom-seeded uniform draw < 0.01")
reg("rng-0.1pct", "rate", 0.001, "none", "buggy", partial(rng_fail, p=0.001), note="fails iff a urandom-seeded uniform draw < 0.001")

# ---- a real wall-clock race ----------------------------------------------------------------------------------------

PROD_S = 0.002
MARGIN_S = float(os.environ.get("FL2P_RACE_MARGIN_MS", "1.0")) / 1000.0


async def timing_race(ctx: Ctx) -> None:
    """A worker thread sets a flag after PROD_S; the loop checks it PROD_S + margin after starting the thread. Under CPU
    pressure the thread's wake-up can be later than the loop's, and the check finds the flag unset."""
    flag = threading.Event()

    def work() -> None:
        time.sleep(PROD_S)
        flag.set()

    fut = asyncio.get_running_loop().run_in_executor(None, work)
    await asyncio.sleep(PROD_S + MARGIN_S)
    seen = flag.is_set()
    await fut
    assert seen, "flag not set when the loop checked"


reg("timing-race", "timing", "measured", "none", "buggy", timing_race, note=f"thread sets a flag after {PROD_S * 1000:.0f} ms, loop checks after +{MARGIN_S * 1000:.2f} ms")

# ---- order dependence ----------------------------------------------------------------------------------------------
# Default (definition) order: hidden, polluter, exposed. A random order puts the polluter before each victim half of the time.


async def victim(ctx: Ctx) -> None:
    assert not ctx.state.get("dirty"), "state left behind by another test"


async def polluter(ctx: Ctx) -> None:
    ctx.state["dirty"] = True


reg("order-dep-hidden", "order-dep", 0.5, "none", "buggy", victim, note="fails iff the polluter ran earlier; the default order puts it first, so it passes")
reg("order-dep-polluter", "helper", 0.0, "none", "helper", polluter, note="never fails; leaves state behind")
reg("order-dep-exposed", "order-dep", 0.5, "none", "buggy", victim, note="fails iff the polluter ran earlier; the default order puts it after, so it always fails")

# ---- controls (truth 0, must never fail) -----------------------------------------------------------------------------


def _c(name, fn, cost="micro", engine="none", note=""):
    reg(name, "control", 0.0, engine, "control", fn, cost=cost, note=note)


async def c_sort(ctx):
    xs = ctx.rng().sample(range(1000), 200)
    assert sorted(xs) == sorted(xs, key=lambda v: v) and sorted(xs)[0] == min(xs)


async def c_json(ctx):
    d = {"a": [1, 2, {"b": None}], "c": "x"}
    assert json.loads(json.dumps(d)) == d


async def c_counter(ctx):
    assert Counter("abracadabra")["a"] == 5


async def c_heap(ctx):
    xs = ctx.rng().sample(range(500), 50)
    h = list(xs)
    heapq.heapify(h)
    assert [heapq.heappop(h) for _ in range(5)] == sorted(xs)[:5]


async def c_deque(ctx):
    d = deque(range(10), maxlen=5)
    assert list(d) == [5, 6, 7, 8, 9]


async def c_regex(ctx):
    assert re.fullmatch(r"tok\d+ common", "tok42 common")


async def c_decimal(ctx):
    assert Decimal("0.1") + Decimal("0.2") == Decimal("0.3")


async def c_itertools(ctx):
    assert len(list(itertools.permutations(range(4)))) == 24


async def c_sets(ctx):
    a, b = set(range(10)), set(range(5, 15))
    assert a & b == set(range(5, 10)) and a | b == set(range(15))


async def c_bytes(ctx):
    assert bytes(range(5)).hex() == "0001020304"


async def c_order_stable(ctx):
    hits = [(oid(i), 1.0 + (i % 3)) for i in range(30)]
    r = ctx.rng()
    a = sort_hits(r.sample(hits, len(hits)))
    b = sort_hits(r.sample(hits, len(hits)))
    assert a == b


async def c_gather(ctx):
    async def f(i):
        await asyncio.sleep(0)
        return i

    assert await asyncio.gather(*(f(i) for i in range(20))) == list(range(20))


async def c_queue(ctx):
    q: asyncio.Queue = asyncio.Queue()
    for i in range(10):
        q.put_nowait(i)
    assert [q.get_nowait() for _ in range(10)] == list(range(10))


async def c_wait_for(ctx):
    await asyncio.wait_for(asyncio.sleep(0.01), timeout=2.0)


async def c_sleep_elapsed(ctx):
    t0 = time.perf_counter()
    await asyncio.sleep(0.01)
    assert time.perf_counter() - t0 < 1.0, "a 10 ms sleep took over 1 s (100x margin)"


async def c_executor(ctx):
    assert await asyncio.get_running_loop().run_in_executor(None, lambda: sum(range(1000))) == 499500


async def c_task_cancel(ctx):
    t = asyncio.ensure_future(asyncio.sleep(10))
    await asyncio.sleep(0)
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass
    assert t.cancelled()


for _n, _f in [("c-sort", c_sort), ("c-json", c_json), ("c-counter", c_counter), ("c-heap", c_heap), ("c-deque", c_deque),
               ("c-regex", c_regex), ("c-decimal", c_decimal), ("c-itertools", c_itertools), ("c-sets", c_sets),
               ("c-bytes", c_bytes), ("c-order-stable", c_order_stable), ("c-gather", c_gather), ("c-queue", c_queue),
               ("c-wait-for", c_wait_for), ("c-sleep-elapsed", c_sleep_elapsed), ("c-executor", c_executor),
               ("c-task-cancel", c_task_cancel)]:
    _c(_n, _f, note="pure or event loop only; wall-clock controls have a 100x margin" if "sleep" in _n or "wait" in _n else "")


def _scen(name):
    async def run(ctx):
        import scenarios

        async with lag_env() as env:
            env.fresh()
            await getattr(scenarios, name)(env)

    return run


for _s in ("s01_commit_one_wait", "s02_rollback_by_exception", "s03_rollback_keeps_existing_state", "s20_synced_single_write",
           "s21_synced_insert_many", "s25_defer_batches_one_wait"):
    _c("c-" + _s.split("_")[0], _scen(_s), cost="env", engine="inmem", note="suite scenario on the in-memory engine, no lag")


async def c_settle_ok(ctx):
    from searchkit.lifecycle import Timeouts

    from .env import Item, mk

    async with lag_env() as env:
        env.lag.write_lag = 0.5
        env.rt.timeouts = Timeouts(sync=4.0, index_build=60.0)
        await mk(Item, 1).insert()


_c("c-settle-ok", c_settle_ok, cost="env", engine="inmem", note="lag 0.5 s, bound 4.0 s")
