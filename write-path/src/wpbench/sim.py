"""Minimal simulation of the designed write path, using the async MongoDB driver directly.

This is a measurement tool, not the library. One write does:

1. stamp a revision token (an ObjectId, client-side) in the reserved field `_hoodie_rev`;
2. upsert a marker in `_hoodie_sync` (in the same transaction as the data where there is one);
3. write the data;
4. STRONG mode only: probe the search index until the token is visible (a delete: until the document is gone);
5. delete the marker on success, mark it `failed` on timeout.

EVENTUAL mode stops after step 3. A bulk write that skips synchronization writes one per-entity dirty marker and
makes no search calls. Every public method returns a dict of timings in milliseconds.
"""

import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from bson import ObjectId
from pymongo import DeleteOne, InsertOne, UpdateOne

from .common import DB, REV, SYNC
from .probe import Item, WaitResult

pc = time.perf_counter


@dataclass
class Op:
    kind: str  # insert | update | delete
    doc_id: ObjectId
    doc: dict | None = None  # full document for inserts (without the revision field)
    touch: int = 0  # new value of the plain field for updates


class SyncFailure(Exception):
    """Search verification timed out. The message carries identifiers only (no field values)."""

    def __init__(self, entity: str, doc_ids: list[ObjectId], waited_s: float) -> None:
        self.entity, self.doc_ids, self.waited_s = entity, doc_ids, waited_s
        shown = ", ".join(str(i) for i in doc_ids[:5]) + (", ..." if len(doc_ids) > 5 else "")
        super().__init__(f"entity {entity}: {len(doc_ids)} document(s) not searchable after {waited_s:.1f}s: ids=[{shown}]")


def _ms(a: float, b: float) -> float:
    return (b - a) * 1000.0


class WritePath:
    def __init__(self, client, entity: str, waiter, mode: str = "strong", fail_pct: float = 0.0,
                 seed: int = 0, db: str = DB) -> None:
        self.client, self.entity, self.waiter, self.mode = client, entity, waiter, mode
        self.fail_pct = fail_pct
        self.rng = random.Random(seed)
        self.coll = client[db][entity]
        self.sync = client[db][SYNC]
        self.waits = 0  # number of verification waits issued (one per wait call)

    # -- building blocks -------------------------------------------------------------------------------------------
    def _marker_filter(self, doc_id) -> dict:
        return {"entity": self.entity, "doc_id": doc_id}

    async def _marker_upsert(self, doc_id, session=None) -> None:
        await self.sync.update_one(
            self._marker_filter(doc_id),
            {"$set": {"state": "pending", "ts": datetime.now(UTC)}},
            upsert=True,
            session=session,
        )

    def _data_model(self, op: Op, token: ObjectId | None, stamp: bool = True):
        if op.kind == "insert":
            doc = dict(op.doc)
            if stamp:
                doc[REV] = token
            return InsertOne(doc)
        if op.kind == "update":
            fields = {"touch": op.touch}
            if stamp:
                fields[REV] = token
            return UpdateOne({"_id": op.doc_id}, {"$set": fields})
        return DeleteOne({"_id": op.doc_id})

    async def _data_write(self, op: Op, token, session=None, stamp: bool = True) -> None:
        if op.kind == "insert":
            doc = dict(op.doc)
            if stamp:
                doc[REV] = token
            await self.coll.insert_one(doc, session=session)
        elif op.kind == "update":
            fields = {"touch": op.touch}
            if stamp:
                fields[REV] = token
            await self.coll.update_one({"_id": op.doc_id}, {"$set": fields}, session=session)
        else:
            await self.coll.delete_one({"_id": op.doc_id}, session=session)

    async def _settle(self, items: list[Item], t_ack: float) -> WaitResult:
        self.waits += 1
        return await self.waiter.wait(items, t_ack)

    @staticmethod
    def _bounds(res: WaitResult, t_ack: float, total_ms: float) -> dict:
        """Bounds on the engine's time to searchable, and the overhead that remains when it is removed.

        The new revision became visible after the last failed query started and before the successful query ended.
        """
        if not res.ok or not res.attempts:
            return {}
        failed = res.attempts[:-1]
        lb = _ms(t_ack, failed[-1][0]) if failed else 0.0
        ub = _ms(t_ack, res.attempts[-1][1])
        return {"engine_lb_ms": lb, "engine_ub_ms": ub, "ovh_lb_ms": total_ms - ub, "ovh_ub_ms": total_ms - lb}

    # -- single document write -------------------------------------------------------------------------------------
    async def write(self, op: Op, txn: bool = False) -> dict:
        if txn:
            return await self.transaction([op])
        t0 = pc()
        token = ObjectId() if op.kind != "delete" else None
        t1 = pc()
        await self._marker_upsert(op.doc_id)
        t2 = pc()
        await self._data_write(op, token)
        t3 = pc()
        out = {"token_ms": _ms(t0, t1), "marker_ms": _ms(t1, t2), "data_ms": _ms(t2, t3), "commit_ms": 0.0}
        if self.mode == "eventual":
            out.update(wait_ms=0.0, cleanup_ms=0.0, total_ms=_ms(t0, t3), attempts=0, queries=0, ok=True)
            return out
        if self.rng.random() < self.fail_pct:  # injected failure: leave a `failed` marker without waiting
            await self.sync.update_one(self._marker_filter(op.doc_id), {"$set": {"state": "failed", "error": "Injected"}})
            t5 = pc()
            out.update(wait_ms=0.0, cleanup_ms=_ms(t3, t5), total_ms=_ms(t0, t5), attempts=0, queries=0, ok=True,
                       injected=True)
            return out
        res = await self._settle([(op.doc_id, token)], t3)
        t4 = pc()
        if res.ok:
            await self.sync.delete_one(self._marker_filter(op.doc_id))
        else:
            await self.sync.update_one(self._marker_filter(op.doc_id), {"$set": {"state": "failed", "error": "SyncTimeout"}})
        t5 = pc()
        out.update(wait_ms=_ms(t3, t4), cleanup_ms=_ms(t4, t5), total_ms=_ms(t0, t5), attempts=len(res.attempts),
                   queries=res.queries, errors=res.errors, ok=res.ok)
        out.update(self._bounds(res, t3, out["total_ms"]))
        if not res.ok:
            raise SyncFailure(self.entity, [op.doc_id], self.waiter.schedule.timeout_s)
        return out

    async def plain(self, op: Op, txn: bool = False) -> dict:
        """The same data write without token or marker: the baseline an application would have without the library."""
        t0 = pc()
        if txn:
            async with self.client.start_session() as s:
                async def body(session):
                    await self._data_write(op, None, session, stamp=False)

                await s.with_transaction(body)
        else:
            await self._data_write(op, None, stamp=False)
        return {"total_ms": _ms(t0, pc())}

    # -- transactions ----------------------------------------------------------------------------------------------
    async def transaction(self, ops: list[Op]) -> dict:
        return await run_transaction(self.client, [(self, ops)])

    # -- bulk writes -----------------------------------------------------------------------------------------------
    async def bulk(self, ops: list[Op], sync: bool = True) -> dict:
        """One bulk write. With sync=False: one dirty marker for the entity, no search calls."""
        t0 = pc()
        if not sync:
            await self.sync.update_one(
                {"entity": self.entity, "doc_id": None, "kind": "entity"},
                {"$set": {"state": "dirty", "ts": datetime.now(UTC)}},
                upsert=True,
            )
            t1 = pc()
            await self.coll.bulk_write([self._data_model(o, None, stamp=False) for o in ops], ordered=False)
            t2 = pc()
            return {"marker_ms": _ms(t0, t1), "data_ms": _ms(t1, t2), "total_ms": _ms(t0, t2), "queries": 0, "ok": True}
        tokens = [ObjectId() if o.kind != "delete" else None for o in ops]
        t1 = pc()
        await self.sync.bulk_write(
            [UpdateOne(self._marker_filter(o.doc_id), {"$set": {"state": "pending", "ts": datetime.now(UTC)}}, upsert=True)
             for o in ops],
            ordered=False,
        )
        t2 = pc()
        await self.coll.bulk_write([self._data_model(o, t) for o, t in zip(ops, tokens)], ordered=False)
        t3 = pc()
        out = {"token_ms": _ms(t0, t1), "marker_ms": _ms(t1, t2), "data_ms": _ms(t2, t3)}
        if self.mode == "eventual":
            out.update(total_ms=_ms(t0, t3), queries=0, ok=True)
            return out
        res = await self._settle([(o.doc_id, t) for o, t in zip(ops, tokens)], t3)
        t4 = pc()
        left = {i[0] for i in res.remaining}
        settled = [o.doc_id for o in ops if o.doc_id not in left]
        if settled:
            await self.sync.delete_many({"entity": self.entity, "doc_id": {"$in": settled}})
        t5 = pc()
        out.update(wait_ms=_ms(t3, t4), cleanup_ms=_ms(t4, t5), total_ms=_ms(t0, t5), attempts=len(res.attempts),
                   queries=res.queries, ok=res.ok)
        return out


async def run_transaction(client, groups: list[tuple["WritePath", list[Op]]]) -> dict:
    """One multi-document transaction over one or more entities, with one combined verification wait at commit.

    Raises an ExceptionGroup of SyncFailure naming only the unsettled documents. Data stays committed.
    """
    t0 = pc()
    state: dict = {}
    mode = groups[0][0].mode

    async with client.start_session() as session:

        async def body(s):
            tokens = {}
            t_a = pc()
            for wp, ops in groups:
                for o in ops:
                    tokens[(wp.entity, o.doc_id)] = ObjectId() if o.kind != "delete" else None
            t_b = pc()
            for wp, ops in groups:
                for o in ops:
                    await wp._marker_upsert(o.doc_id, s)
            t_c = pc()
            for wp, ops in groups:
                for o in ops:
                    await wp._data_write(o, tokens[(wp.entity, o.doc_id)], s)
            t_d = pc()
            state.update(tokens=tokens, token_ms=_ms(t_a, t_b), marker_ms=_ms(t_b, t_c), data_ms=_ms(t_c, t_d), t_body_end=t_d)

        t_s = pc()
        await session.with_transaction(body)
        t_commit = pc()

    out = {"token_ms": state["token_ms"], "marker_ms": state["marker_ms"], "data_ms": state["data_ms"],
           "commit_ms": _ms(state["t_body_end"], t_commit), "session_ms": _ms(t0, t_s), "writes": sum(len(o) for _, o in groups)}
    if mode == "eventual":
        out.update(wait_ms=0.0, cleanup_ms=0.0, total_ms=_ms(t0, t_commit), attempts=0, queries=0, ok=True, waits=0)
        return out
    failures, attempts, queries, ok_all = [], 0, 0, True
    bounds: dict = {}
    t_wait0 = pc()
    for wp, ops in groups:  # one wait per entity; a transaction on one entity makes exactly one
        items = [(o.doc_id, state["tokens"][(wp.entity, o.doc_id)]) for o in ops]
        res = await wp._settle(items, t_commit)
        attempts, queries = attempts + len(res.attempts), queries + res.queries
        left = {i[0] for i in res.remaining}
        settled = [o.doc_id for o in ops if o.doc_id not in left]
        if settled:
            await wp.sync.delete_many({"entity": wp.entity, "doc_id": {"$in": settled}})
        if left:
            ok_all = False
            await wp.sync.update_many({"entity": wp.entity, "doc_id": {"$in": list(left)}},
                                      {"$set": {"state": "failed", "error": "SyncTimeout"}})
            failures.append(SyncFailure(wp.entity, sorted(left), wp.waiter.schedule.timeout_s))
        elif len(groups) == 1:
            bounds = wp._bounds(res, t_commit, 0.0)
    t_end = pc()
    out.update(wait_ms=_ms(t_wait0, t_end), cleanup_ms=0.0, total_ms=_ms(t0, t_end), attempts=attempts, queries=queries,
               ok=ok_all, waits=len(groups))
    if bounds:
        out["engine_lb_ms"], out["engine_ub_ms"] = bounds["engine_lb_ms"], bounds["engine_ub_ms"]
        out["ovh_lb_ms"], out["ovh_ub_ms"] = out["total_ms"] - bounds["engine_ub_ms"], out["total_ms"] - bounds["engine_lb_ms"]
    if failures:
        raise ExceptionGroup("write committed but search verification failed", failures)
    return out
