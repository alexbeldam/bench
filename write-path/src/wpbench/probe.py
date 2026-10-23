"""Search-index probes: query shapes, a chunked prober, polling schedules and a coalescing wait group.

An item is a pair (doc_id, rev). rev is an ObjectId when the write must become visible with that revision, and
None when the document must be absent (a delete).
"""

import asyncio
import time
from dataclasses import dataclass, field

from bson import ObjectId
from pymongo.errors import OperationFailure, PyMongoError

from .common import REV

Item = tuple[ObjectId, ObjectId | None]

SHAPES = ("rev", "rev_plain", "id_rev", "per_doc")
# Clauses each shape spends per document, as measured by the clauses phase (limit: 1,024 per query) (the engine allows 1,023 per query).
CLAUSES_PER_DOC = {"rev": 1, "rev_plain": 1, "id_rev": 2, "per_doc": 2}


class ProbeShapeError(Exception):
    """The engine refused the query itself (for example too many clauses). Not retried."""


@dataclass(frozen=True)
class Schedule:
    """Polling schedule: first wait `initial_ms`, multiplied by `factor` after every attempt up to `max_ms`."""

    initial_ms: float = 5.0
    factor: float = 1.5
    max_ms: float = 200.0
    timeout_s: float = 10.0

    def delay(self, k: int) -> float:
        return min(self.initial_ms * self.factor**k, self.max_ms) / 1000.0


SCHEDULES = {
    "default": Schedule(5, 1.5, 200),
    "fine": Schedule(2, 1.2, 10),  # near-continuous polling: used to measure engine latency
    "gentle": Schedule(20, 1.5, 500),
    "slow": Schedule(50, 2.0, 1000),
    "aggressive": Schedule(1, 1.3, 50),
}


def operator(shape: str, items: list[Item], present: bool) -> dict:
    """Search operator that matches exactly the given items (all present, or all to be absent)."""
    ids = [i for i, _ in items]
    if not present:
        return {"in": {"path": "_id", "value": ids}}
    revs = [r for _, r in items]
    if shape == "rev_plain":
        return {"in": {"path": REV, "value": revs}}
    if shape == "rev":
        return {"compound": {"filter": [{"in": {"path": REV, "value": revs}}]}}
    if shape == "id_rev":
        return {"compound": {"filter": [{"in": {"path": "_id", "value": ids}}, {"in": {"path": REV, "value": revs}}]}}
    if shape == "per_doc":
        should = [
            {"compound": {"filter": [{"equals": {"path": "_id", "value": i}}, {"equals": {"path": REV, "value": r}}]}}
            for i, r in items
        ]
        return {"compound": {"should": should, "minimumShouldMatch": 1}}
    raise ValueError(shape)


def pipeline(index: str, shape: str, items: list[Item], present: bool, result: str) -> list[dict]:
    op = operator(shape, items, present)
    if result == "count":
        return [{"$searchMeta": {"index": index, **op, "count": {"type": "total"}}}]
    return [
        {"$search": {"index": index, **op}},
        {"$limit": len(items)},
        {"$project": {"_id": 1, REV: 1}},
    ]


@dataclass
class CheckResult:
    remaining: list[Item]
    queries: int = 0
    errors: int = 0


class Prober:
    """Checks items against the search index, in chunks of `chunk` documents with `concurrency` queries in flight.

    result="count" runs a `$searchMeta` total count and learns only whether a chunk is fully visible.
    result="docs" returns the matching documents, so partial progress is known per item.
    """

    def __init__(self, coll, index: str, shape: str = "rev", result: str = "count", chunk: int = 500,
                 concurrency: int = 4) -> None:
        self.coll, self.index, self.shape, self.result = coll, index, shape, result
        self.chunk, self.concurrency = chunk, concurrency

    async def _one(self, items: list[Item], present: bool, budget_s: float, out: CheckResult) -> list[Item]:
        pipe = pipeline(self.index, self.shape, items, present, self.result)
        out.queries += 1
        try:
            max_ms = max(50, int(budget_s * 1000))
            cursor = await asyncio.wait_for(self.coll.aggregate(pipe, maxTimeMS=max_ms), budget_s + 1.0)
            rows = await asyncio.wait_for(cursor.to_list(), budget_s + 1.0)
        except OperationFailure as e:
            msg = str(e)
            if "nested clauses" in msg or "sub-queries" in msg or "maxClauseCount" in msg:
                raise ProbeShapeError(msg[:200]) from e
            out.errors += 1
            return items
        except (PyMongoError, TimeoutError):
            out.errors += 1
            return items
        if self.result == "count":
            total = rows[0]["count"]["total"] if rows else 0
            done = total == len(items) if present else total == 0
            return [] if done else items
        if present:
            seen = {r.get(REV) for r in rows}
            return [it for it in items if it[1] not in seen]
        seen_ids = {r["_id"] for r in rows}
        return [it for it in items if it[0] in seen_ids]

    async def check(self, items: list[Item], budget_s: float) -> CheckResult:
        out = CheckResult(remaining=[])
        present = [it for it in items if it[1] is not None]
        absent = [it for it in items if it[1] is None]
        jobs = [(present[k : k + self.chunk], True) for k in range(0, len(present), self.chunk)]
        jobs += [(absent[k : k + self.chunk], False) for k in range(0, len(absent), self.chunk)]
        sem = asyncio.Semaphore(self.concurrency)

        async def run(chunk, flag):
            async with sem:
                return await self._one(chunk, flag, budget_s, out)

        for left in await asyncio.gather(*(run(c, f) for c, f in jobs)):
            out.remaining.extend(left)
        return out


@dataclass
class WaitResult:
    ok: bool
    remaining: list[Item]
    attempts: list[tuple[float, float, int]] = field(default_factory=list)  # (start, end, items checked), perf_counter
    queries: int = 0
    errors: int = 0
    started: float = 0.0
    ended: float = 0.0


class IndependentWaiter:
    """Every wait polls on its own, with its own queries."""

    def __init__(self, prober: Prober, schedule: Schedule) -> None:
        self.prober, self.schedule = prober, schedule

    async def wait(self, items: list[Item], t_start: float | None = None) -> WaitResult:
        t0 = t_start if t_start is not None else time.perf_counter()
        deadline = t0 + self.schedule.timeout_s
        res = WaitResult(ok=False, remaining=list(items), started=t0)
        k = 0
        while res.remaining:
            now = time.perf_counter()
            await asyncio.sleep(max(0.0, min(self.schedule.delay(k), deadline - now)))
            now = time.perf_counter()
            if now >= deadline:
                break
            before = len(res.remaining)
            chk = await self.prober.check(res.remaining, deadline - now)
            res.attempts.append((now, time.perf_counter(), before))
            res.remaining, res.queries, res.errors = chk.remaining, res.queries + chk.queries, res.errors + chk.errors
            k += 1
        res.ok = not res.remaining
        res.ended = time.perf_counter()
        return res


@dataclass
class _Waiter:
    remaining: list[Item]
    deadline: float
    fut: asyncio.Future
    result: WaitResult
    due: float
    k: int = 0


class Coalescer:
    """A wait group: concurrent waits for the same index share one batched query per polling tick.

    Each waiter keeps its own backoff and deadline; the poller wakes at the earliest due time and checks every item
    that is due in one `Prober.check` call (chunked by the prober).
    """

    def __init__(self, prober: Prober, schedule: Schedule) -> None:
        self.prober, self.schedule = prober, schedule
        self._waiters: list[_Waiter] = []
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self.ticks = 0

    async def wait(self, items: list[Item], t_start: float | None = None) -> WaitResult:
        t0 = t_start if t_start is not None else time.perf_counter()
        res = WaitResult(ok=False, remaining=list(items), started=t0)
        w = _Waiter(list(items), t0 + self.schedule.timeout_s, asyncio.get_running_loop().create_future(), res,
                    t0 + self.schedule.delay(0))
        self._waiters.append(w)
        self._wake.set()
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll())
        await w.fut
        return res

    def _finish(self, w: _Waiter, ok: bool) -> None:
        w.result.ok, w.result.remaining, w.result.ended = ok, [] if ok else w.remaining, time.perf_counter()
        self._waiters.remove(w)
        if not w.fut.done():
            w.fut.set_result(None)

    async def _poll(self) -> None:
        while self._waiters:
            now = time.perf_counter()
            for w in [w for w in self._waiters if w.deadline <= now]:
                self._finish(w, False)
            due = [w for w in self._waiters if w.due <= now]
            if not due:
                if not self._waiters:
                    return
                nxt = min(min(w.due for w in self._waiters), min(w.deadline for w in self._waiters))
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), max(0.0, nxt - now))
                except TimeoutError:
                    pass
                continue
            items = [it for w in due for it in w.remaining]
            budget = max(0.05, min(w.deadline for w in due) - now)
            self.ticks += 1
            try:
                chk = await self.prober.check(items, budget)
            except ProbeShapeError:
                for w in due:
                    self._finish(w, False)
                continue
            end = time.perf_counter()
            left = set(chk.remaining)
            for w in due:
                w.result.attempts.append((now, end, len(w.remaining)))
                w.result.queries += chk.queries
                w.result.errors += chk.errors
                w.remaining = [it for it in w.remaining if it in left]
                w.k += 1
                w.due = end + self.schedule.delay(w.k)
                if not w.remaining:
                    self._finish(w, True)


class Sentinel:
    """A wait group that polls one cheap query per tick instead of re-checking every pending write.

    Each tick checks only the oldest pending item (one clause). When that item is visible, one full pass verifies
    every pending item by its revision, so a visibility batch costs one wide query and the tick rate stays constant
    whatever the write rate. Reliability does not depend on the engine applying changes in order: a full pass also
    runs at least every `fallback_ms`, and an item is released only when its own revision was seen.
    """

    def __init__(self, prober: Prober, schedule: Schedule, tick_ms: float = 10.0, fallback_ms: float = 250.0) -> None:
        self.prober, self.schedule = prober, schedule
        self.tick, self.fallback = tick_ms / 1000.0, fallback_ms / 1000.0
        self._waiters: list[_Waiter] = []
        self._task: asyncio.Task | None = None
        self.ticks = self.full_passes = 0

    async def wait(self, items: list[Item], t_start: float | None = None) -> WaitResult:
        t0 = t_start if t_start is not None else time.perf_counter()
        res = WaitResult(ok=False, remaining=list(items), started=t0)
        w = _Waiter(list(items), t0 + self.schedule.timeout_s, asyncio.get_running_loop().create_future(), res, t0)
        self._waiters.append(w)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll())
        await w.fut
        return res

    def _finish(self, w: _Waiter, ok: bool) -> None:
        w.result.ok, w.result.remaining, w.result.ended = ok, [] if ok else w.remaining, time.perf_counter()
        if w in self._waiters:
            self._waiters.remove(w)
        if not w.fut.done():
            w.fut.set_result(None)

    async def _poll(self) -> None:
        last_full = 0.0
        while self._waiters:
            await asyncio.sleep(self.tick)
            now = time.perf_counter()
            for w in [w for w in self._waiters if w.deadline <= now]:
                self._finish(w, False)
            if not self._waiters:
                return
            oldest = self._waiters[0]
            budget = max(0.05, oldest.deadline - now)
            self.ticks += 1
            try:
                everyone = list(self._waiters)
                # Two pending writes to one document: the older revision may be overwritten before the engine
                # ever indexes it, so it can never be seen. The newest write per document stands for all of them.
                newest: dict = {}
                for w in everyone:
                    for it in w.remaining:
                        newest[it[0]] = it
                sent = await self.prober.check([newest[oldest.remaining[0][0]]], budget)
                queries, errors = sent.queries, sent.errors
                if not sent.remaining or now - last_full >= self.fallback:
                    self.full_passes += 1
                    chk = await self.prober.check(list(newest.values()), budget)
                    end = time.perf_counter()
                    last_full = end
                    left = set(chk.remaining)
                    queries, errors = queries + chk.queries, errors + chk.errors
                    for w in everyone:
                        w.result.attempts.append((now, end, len(w.remaining)))
                        w.result.queries += queries
                        w.result.errors += errors
                        w.remaining = [it for it in w.remaining if newest[it[0]] in left]
                        if not w.remaining:
                            self._finish(w, True)
                else:
                    oldest.result.queries += queries
                    oldest.result.errors += errors
            except ProbeShapeError:
                for w in list(self._waiters):
                    self._finish(w, False)
