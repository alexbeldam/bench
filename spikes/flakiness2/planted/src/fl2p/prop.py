"""Strategy S6: stateful property-based testing (Hypothesis RuleBasedStateMachine) over write, settle, purge, reset and reindex
with virtual lag. One machine per planted race, with exactly one bug switched on, plus a fixed machine as the false-positive check.

Applies to races where the system under test is wrong (stale total after purge, reset order, tie order). It does not apply to
the two races that are test bugs (a bound equal to the lag): the machine checks the settle contract, which holds in every variant.
"""
from __future__ import annotations

import random
import time
from typing import Any

from hypothesis import HealthCheck, Phase, seed, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule, run_state_machine_as_test
from searchkit.contract import IndexNeverQueryableError
from searchkit.data import oid
from searchkit.lifecycle import Timeouts
from searchkit.ordering import sort_hits

from .core import loop, machine
from .env import Item, lag_env, mk, total
from .exp import dump, header, seed_or_new

BUGS = ("purge", "reset", "tie", "none")
RACE_OF = {"purge": "purge-probe", "reset": "reset-order", "tie": "tie-order", "none": "none"}


def run(coro: Any) -> Any:
    return loop().run_until_complete(coro)


HITS = [(oid(i), 10.0 - i) for i in range(1, 6)] + [(oid(6), 4.0), (oid(7), 4.0)] + [(oid(i), 3.0 - i / 100) for i in range(8, 40)]


def make(bug: str, counter: dict[str, int]):
    class Sys(RuleBasedStateMachine):
        def __init__(self) -> None:
            super().__init__()
            counter["examples"] += 1
            self.cm = lag_env()
            self.env = run(self.cm.__aenter__())
            self.env.lag.delete_lag = 0.8
            self.orig_probe = self.env.lag.probe_absent
            self.n = 0
            self.live: set[Any] = set()
            self.prev_page: list[Any] | None = None

        def teardown(self) -> None:
            run(self.cm.__aexit__(None, None, None))

        @rule(k=st.integers(1, 4))
        def insert(self, k: int) -> None:
            docs = []
            for _ in range(k):
                self.n += 1
                docs.append(mk(Item, self.n))
            run(Item.insert_many(docs))
            self.live |= {d.id for d in docs}

        @rule()
        def purge(self) -> None:
            self.env.lag.probe_absent = self.env.lag.probe_absent_by_hits if bug == "purge" else self.orig_probe
            run(self.env.harness.purge())
            self.live.clear()
            assert run(total(Item, "common")) == 0, "stale total after purge"

        @rule()
        def reset(self) -> None:
            self.env.harness.t = Timeouts(sync=5.0, index_build=3.0)
            try:
                run(self.env.harness.reset(order="legacy-order" if bug == "reset" else "fixed"))
            except IndexNeverQueryableError:
                raise AssertionError("ghost index after reset") from None
            self.live.clear()
            self.n += 1
            d = mk(Item, self.n)
            run(d.insert())
            self.live.add(d.id)

        @rule(build=st.integers(0, 10**6))
        def reindex(self, build: int) -> None:
            if bug == "tie":
                pair = [h for h in HITS if h[1] == 4.0]
                if random.Random(build).random() < 0.5:
                    pair.reverse()
                native = sorted((h for h in HITS if h[1] != 4.0), key=lambda h: -h[1])
                native[5:5] = pair
                page = [i for i, _ in native][:6]
            else:
                page = [i for i, _ in sort_hits(HITS)][:6]
            assert self.prev_page is None or page == self.prev_page, "page order changed after reindex"
            self.prev_page = page

        @rule(lag=st.floats(0.0, 2.0), bound=st.sampled_from([1.0, 4.0]))
        def settle(self, lag: float, bound: float) -> None:
            if abs(lag - bound) < 1e-6:
                return
            self.env.lag.write_lag = lag
            self.env.rt.timeouts = Timeouts(sync=bound, index_build=60.0)
            self.n += 1
            d = mk(Item, self.n)
            try:
                run(d.insert())
                ok = True
            except ExceptionGroup as eg:
                ok = False
                assert [e.doc_id for e in eg.exceptions] == [d.id]
            assert ok == (lag < bound), "settle contract"
            self.env.lag.write_lag = 0.0
            self.env.lag.release_all()
            run(self.env.lag._advance())
            self.live.add(d.id)

        @invariant()
        def total_matches(self) -> None:
            assert run(total(Item, "common")) == len(self.live), "total differs from the live documents"

    return Sys


def find(bug: str, k: int, examples: int, steps: int) -> dict[str, Any]:
    counter = {"examples": 0}
    M = seed(k)(make(bug, counter))
    t0 = time.perf_counter()
    msg = ""
    try:
        run_state_machine_as_test(M, settings=settings(max_examples=examples, stateful_step_count=steps, deadline=None, database=None,
                                                       suppress_health_check=list(HealthCheck), phases=[Phase.generate], print_blob=False))
        found = False
    except BaseException as e:  # noqa: BLE001
        found = True
        msg = str(e).strip().splitlines()[0][:100] if str(e).strip() else type(e).__name__
        for n in ("stale total after purge", "ghost index after reset", "page order changed after reindex", "settle contract", "total differs"):
            if n in str(e) or n in "".join(getattr(e, "__notes__", [])):
                msg = n
    return {"bug": bug, "seed": k, "found": found, "msg": msg, "examples_run": counter["examples"], "wall_s": time.perf_counter() - t0}


def cmd_prop(a: Any) -> None:
    base = seed_or_new(a.seed)
    rows = []
    for bug in BUGS:
        for i in range(a.k):
            rows.append(find(bug, base + i, a.examples, a.steps))
        r = [x for x in rows if x["bug"] == bug]
        print(f"S6 {bug:6s} found {sum(x['found'] for x in r)}/{len(r)} mean wall {sum(x['wall_s'] for x in r) / len(r):.2f}s", flush=True)
    dump(a.out, {**header("prop", base, k=a.k, examples=a.examples, steps=a.steps), "rows": rows})
