"""Benchmark phases. See README.md for what each phase answers.

Shuffled phases (randomized block design, seeded): clauses, shapes, latency, coalesce.
Fixed-order stateful phases, each run alone and last: marker (long growth test), failure (pauses the engine).
"""

import asyncio
import json
import math
import random
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from bson import ObjectId
from pymongo import AsyncMongoClient

from .canary import Canary
from .cmdmon import CommandCounter
from .common import DB, REV, SYNC, SCALES, check_disk, doc_oid, entity_name, index_name, pct, seed_rev, write_json
from .loadgen import ShardPool, fast_doc, make_waiter, seq_of, shards_for
from .probe import SCHEDULES, SHAPES, CLAUSES_PER_DOC, IndependentWaiter, Prober, ProbeShapeError, Schedule, pipeline
from .sim import Op, SyncFailure, WritePath, run_transaction

pc = time.perf_counter


@dataclass
class Ctx:
    uri: str  # direct database URI
    label: str
    out: Path  # run directory
    seed: int
    smoke: bool = False
    reps: int = 5
    rtts: list[float] = field(default_factory=lambda: [0.0, 1.0, 5.0])
    rates: list[int] = field(default_factory=lambda: [10, 100, 500])
    duration_s: float = 30.0
    warm_s: float = 5.0
    container: str | None = None
    client: AsyncMongoClient | None = None
    counter: CommandCounter | None = None
    proxies: dict = field(default_factory=dict)  # rtt -> uri
    procs: list = field(default_factory=list)
    pool: ShardPool | None = None

    @property
    def n_docs(self) -> int:
        return SCALES[self.label]

    @property
    def entity(self) -> str:
        return entity_name(self.label)

    @property
    def db(self):
        return self.client[DB]

    def phase_dir(self, name: str) -> Path:
        d = self.out / name
        d.mkdir(parents=True, exist_ok=True)
        return d


# -- infrastructure --------------------------------------------------------------------------------------------------
def start_proxies(ctx: Ctx, base_port: int = 28100) -> None:
    """Start one delaying TCP proxy per non-zero round-trip time. rtt 0 connects directly."""
    m = re.search(r"//([^:/]+):(\d+)", ctx.uri)
    host, port = m.group(1), int(m.group(2))
    ctx.proxies = {0.0: ctx.uri}
    for rtt in ctx.rtts:
        if rtt <= 0:
            continue
        lp = base_port + int(rtt * 10)
        p = subprocess.Popen([sys.executable, "-m", "wpbench.netproxy", "--listen", str(lp), "--target", f"{host}:{port}",
                              "--rtt", str(rtt)])
        ctx.procs.append(p)
        ctx.proxies[rtt] = f"mongodb://127.0.0.1:{lp}/?directConnection=true"
    time.sleep(1.5)


def stop_proxies(ctx: Ctx) -> None:
    for p in ctx.procs:
        p.terminate()
    ctx.procs.clear()


def blocks(names: list[str], reps: int, seed: int, tag: str) -> list[tuple[int, str]]:
    """Randomized block design: every case once per repetition, in a shuffled order per repetition."""
    out = []
    for r in range(reps):
        order = list(names)
        random.Random(f"{seed}:{tag}:{r}").shuffle(order)
        out += [(r, n) for n in order]
    return out


def done_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {json.loads(line)["key"] for line in path.read_text().splitlines() if line.strip()}


def append_jsonl(path: Path, obj: dict) -> None:
    with open(path, "a") as f:
        f.write(json.dumps(obj, default=str) + "\n")


# -- helpers ---------------------------------------------------------------------------------------------------------
async def current_items(coll, seqs: list[int]) -> list[tuple[ObjectId, ObjectId]]:
    ids = [doc_oid(s) for s in seqs]
    got = {d["_id"]: d[REV] async for d in coll.find({"_id": {"$in": ids}}, {REV: 1})}
    return [(i, got[i]) for i in ids if i in got]


async def setup_pool(ctx: Ctx, uid: int, shard: int, count: int) -> list[int]:
    """Create `count` documents and wait until they are searchable, so that a delete case deletes indexed documents."""
    coll = ctx.db[ctx.entity]
    seqs = [seq_of(uid, shard, k, pool=True) for k in range(count)]
    docs = []
    items = []
    for s in seqs:
        d = fast_doc(s)
        d[REV] = ObjectId()
        docs.append(d)
        items.append((d["_id"], d[REV]))
    for i in range(0, len(docs), 5000):
        await coll.insert_many(docs[i : i + 5000], ordered=False)
    waiter = IndependentWaiter(Prober(coll, index_name(ctx.entity), "rev", "count", 500, 4), Schedule(20, 1.5, 500, 180))
    res = await waiter.wait(items)
    if not res.ok:
        raise RuntimeError("pool documents never became searchable")
    return seqs


async def run_case(ctx: Ctx, phase: str, name: str, rep: int, spec: dict) -> dict:
    """Run one open-loop case (sharded over processes) and write its rows and summary."""
    rate = spec["rate"]
    nshards = spec.get("nshards") or shards_for(rate)
    uid = int(time.time() * 10) - 17_000_000_000
    d = ctx.phase_dir(phase)
    total_per_shard = int((spec["warm_s"] + spec["duration_s"]) * rate / nshards) + 5
    pools = [[] for _ in range(nshards)]
    if spec["op"] == "delete":
        pools = [await setup_pool(ctx, uid, s, total_per_shard) for s in range(nshards)]
    start_at = time.time() + 3.0
    specs = []
    for s in range(nshards):
        sp = dict(spec)
        sp.update(shard=s, nshards=nshards, uid=uid, start_at=start_at, entity=spec.get("entity", ctx.entity),
                  n_docs=ctx.n_docs, pool_seqs=pools[s], seed=ctx.seed + rep,
                  uri=ctx.proxies.get(spec.get("rtt", 0.0), ctx.uri),
                  out=str(d / f"{name}__r{rep}.s{s}.jsonl.gz"))
        specs.append(sp)
    results = await ctx.pool.run(specs)
    summary = {
        "key": f"{rep}:{name}", "phase": phase, "case": name, "rep": rep, "uid": uid, "start_at": start_at,
        "end_at": time.time(), "nshards": nshards,
        "spec": {k: v for k, v in spec.items() if k not in ("uri", "out", "pool_seqs")},
        "search_queries": sum(r["search_queries"] for r in results),
        "commands": {k: sum(r["commands"].get(k, 0) for r in results) for r in results[:1] for k in r["commands"]},
        "failed": sum(r["failed"] for r in results),
        "ops": sum(r["ops"] for r in results),
    }
    append_jsonl(d / "cases.jsonl", summary)
    return summary


async def run_blocks(ctx: Ctx, phase: str, cases: dict[str, dict]) -> None:
    """Run all cases in randomized blocks under the canary; resumable."""
    d = ctx.phase_dir(phase)
    order = blocks(sorted(cases), ctx.reps, ctx.seed, phase)
    write_json(d / "order.json", {"seed": ctx.seed, "order": [f"{r}:{n}" for r, n in order]})
    done = done_keys(d / "cases.jsonl")
    todo = [(r, n) for r, n in order if f"{r}:{n}" not in done]
    print(f"[{phase}] {len(todo)} case runs to do ({len(done)} done)", flush=True)
    with Canary(ctx.uri, DB, d / "canary.jsonl"):
        for i, (r, n) in enumerate(todo, 1):
            check_disk(f"{phase} case {i}")
            t = pc()
            s = await run_case(ctx, phase, n, r, cases[n])
            print(f"[{phase}] {i}/{len(todo)} rep {r} {n}: {pc() - t:.0f}s failed={s['failed']}/{s['ops']}", flush=True)
            await asyncio.sleep(1.0)


# -- phase: clauses --------------------------------------------------------------------------------------------------
async def phase_clauses(ctx: Ctx) -> None:
    """How many documents can one query verify, per probe shape (bisect on batch size)."""
    d = ctx.phase_dir("clauses")
    coll = ctx.db[ctx.entity]
    idx = index_name(ctx.entity)
    cap = min(ctx.n_docs, 5000)
    items = await current_items(coll, list(range(cap)))
    out: dict = {"engine_note": "max clauses per query; limit found earlier on one engine version: 1,023"}

    async def try_n(shape: str, n: int, present: bool = True) -> tuple[bool, str]:
        sub = items[:n]
        try:
            cur = await coll.aggregate(pipeline(idx, shape, sub, present, "count"))
            rows = await cur.to_list()
            total = rows[0]["count"]["total"] if rows else 0
            return (total == n, "" if total == n else f"count {total} != {n}")
        except Exception as e:  # noqa: BLE001
            return False, f"{type(e).__name__}: {str(e)[:160]}"

    for shape in SHAPES + ("id_only",):
        present = shape != "id_only"
        sh = "rev" if shape == "id_only" else shape
        ok_n, fail_n, err = 0, None, ""
        for n in [1, 10, 100, 340, 341, 500, 510, 511, 512, 1000, 1022, 1023, 1024, 1025, 2000, 5000]:
            if n > len(items):
                break
            ok, e = await try_n(sh, n, present)
            if ok:
                ok_n = n
            else:
                fail_n, err = n, e
                break
        lo, hi = ok_n, fail_n
        while hi is not None and hi - lo > 1:  # bisect between the last good and first bad size
            mid = (lo + hi) // 2
            ok, e = await try_n(sh, mid, present)
            if ok:
                lo = mid
            else:
                hi, err = mid, e
        out[shape] = {"max_documents_ok": lo, "first_failure": hi, "error": err,
                      "clauses_per_document": CLAUSES_PER_DOC.get(shape, 1) if shape != "id_only" else 1,
                      "tested_up_to": min(len(items), 5000)}
        print(f"[clauses] {shape}: max ok {lo}, first failure {hi}", flush=True)
    write_json(d / "clauses.json", out)


# -- phase: shapes ---------------------------------------------------------------------------------------------------
def shape_configs() -> dict[str, dict]:
    big = 10**9
    cfg: dict[str, dict] = {}
    for sh in SHAPES:
        cfg[f"{sh}-count-single"] = dict(shape=sh, result="count", chunk=big, conc=1)
    for sh in ("rev", "id_rev"):
        cfg[f"{sh}-docs-single"] = dict(shape=sh, result="docs", chunk=big, conc=1)
    for chunk in (250, 500, 1000):
        for conc in (1, 4, 8):
            cfg[f"rev-count-c{chunk}-x{conc}"] = dict(shape="rev", result="count", chunk=chunk, conc=conc)
    cfg["rev-docs-c1000-x4"] = dict(shape="rev", result="docs", chunk=1000, conc=4)
    cfg["id_rev-count-c500-x4"] = dict(shape="id_rev", result="count", chunk=500, conc=4)
    cfg["per_doc-count-c300-x4"] = dict(shape="per_doc", result="count", chunk=300, conc=4)
    return cfg


async def phase_shapes(ctx: Ctx, batch_sizes: list[int]) -> None:
    """Probe shapes against batch sizes: time to verify N documents after a bulk write, and the steady cost of a probe."""
    d = ctx.phase_dir("shapes")
    cfgs = shape_configs()
    cases = {f"{name}|{n}": dict(cfg, n=n) for name, cfg in cfgs.items() for n in batch_sizes}
    coll = ctx.db[ctx.entity]
    idx = index_name(ctx.entity)
    order = blocks(sorted(cases), ctx.reps, ctx.seed, "shapes")
    write_json(d / "order.json", {"seed": ctx.seed, "order": [f"{r}:{n}" for r, n in order]})
    done = done_keys(d / "cases.jsonl")
    rng = random.Random(ctx.seed)
    span = min(ctx.n_docs, 50_000)
    print(f"[shapes] {len(order) - len(done)} case runs", flush=True)
    with Canary(ctx.uri, DB, d / "canary.jsonl"):
        for i, (rep, name) in enumerate(order, 1):
            if f"{rep}:{name}" in done:
                continue
            c = cases[name]
            n = c["n"]
            start = rng.randrange(0, max(1, span - n))
            seqs = list(range(start, start + n))
            from pymongo import UpdateOne

            new = {doc_oid(s): ObjectId() for s in seqs}
            prober = Prober(coll, idx, c["shape"], c["result"], c["chunk"], c["conc"])
            waiter = IndependentWaiter(prober, Schedule(5, 1.5, 200, 10.0))
            await coll.bulk_write([UpdateOne({"_id": i}, {"$set": {REV: r}}) for i, r in new.items()], ordered=False)
            t_ack = pc()
            row = {"key": f"{rep}:{name}", "case": name, "rep": rep, "n": n, "shape": c["shape"], "result": c["result"],
                   "chunk": min(c["chunk"], n), "conc": c["conc"], "ts": time.time(), "clauses": n * CLAUSES_PER_DOC[c["shape"]]}
            try:
                res = await waiter.wait(list(new.items()), t_ack)
                row.update(ok=res.ok, verify_ms=(res.ended - t_ack) * 1000, attempts=len(res.attempts), queries=res.queries,
                           errors=res.errors)
                if res.ok:  # steady cost: the same probe when everything is visible (engine latency removed)
                    costs = []
                    for _ in range(5):
                        t = pc()
                        chk = await prober.check(list(new.items()), 10.0)
                        costs.append((pc() - t) * 1000)
                        if chk.remaining:
                            row["steady_incomplete"] = True
                    row["steady_cost_ms"] = pct(costs, 50)
                    row["steady_queries"] = chk.queries
            except ProbeShapeError as e:
                row.update(ok=False, error="clause_limit", detail=str(e)[:160])
            append_jsonl(d / "cases.jsonl", row)
    print("[shapes] done", flush=True)


# -- phase: semantics ------------------------------------------------------------------------------------------------
async def phase_semantics(ctx: Ctx) -> None:
    """Does the revision token prove freshness? Stale token, wrong id, deletes."""
    d = ctx.phase_dir("semantics")
    coll = ctx.db[ctx.entity]
    idx = index_name(ctx.entity)
    prober = Prober(coll, idx, "id_rev", "docs", 500, 1)
    wp = WritePath(ctx.client, ctx.entity, IndependentWaiter(prober, Schedule(5, 1.5, 200, 10.0)))
    out: dict = {}
    s = 100
    doc = (await current_items(coll, [s, s + 1]))
    (id0, rev0), (id1, rev1) = doc
    r = await wp.write(Op("update", id0, touch=777))
    new_rev = (await coll.find_one({"_id": id0}, {REV: 1}))[REV]
    chk = lambda items, shape="rev", present=True: Prober(coll, idx, shape, "docs", 500, 1).check(items, 10)  # noqa: E731
    out["new_rev_visible_after_wait"] = not (await chk([(id0, new_rev)])).remaining
    out["old_rev_not_found_after_update"] = bool((await chk([(id0, rev0)])).remaining)
    out["wrong_id_with_right_rev_rejected_by_id_rev_shape"] = bool((await chk([(id1, new_rev)], "id_rev")).remaining)
    out["right_id_wrong_rev_rejected_by_id_rev_shape"] = bool((await chk([(id1, ObjectId())], "id_rev")).remaining)
    out["rev_alone_matches_the_right_document"] = not (await chk([(id0, new_rev)], "rev")).remaining
    ins = Op("insert", doc_oid(seq_of(int(time.time()) % 1000 + 5, 0, 1)), None)
    ins.doc = fast_doc(int(ins.doc_id.binary[4:].hex(), 16))
    await wp.write(ins)
    dele = Op("delete", ins.doc_id)
    await wp.write(dele)
    out["deleted_document_probed_absent"] = not (await chk([(ins.doc_id, None)])).remaining
    out["write_result_ok"] = bool(r["ok"])
    write_json(d / "semantics.json", out)
    print("[semantics]", out, flush=True)


# -- phase: counts (NFR-04, NFR-05) ----------------------------------------------------------------------------------
async def phase_counts(ctx: Ctx, sizes: list[int]) -> None:
    """Command monitoring: one verification wait per transaction, and zero search queries for skipped bulk writes.

    This phase counts driver commands. It does not measure latency, so it is valid anywhere."""
    d = ctx.phase_dir("counts")
    coll = ctx.db[ctx.entity]
    idx = index_name(ctx.entity)
    counter = ctx.counter
    chunk = 500
    rows = []

    def mk(prober_chunk=chunk):
        return WritePath(ctx.client, ctx.entity,
                         IndependentWaiter(Prober(coll, idx, "rev", "count", prober_chunk, 4), Schedule(5, 1.5, 200, 20.0)))

    async def measured(label: str, n: int, fn, wp: WritePath) -> dict:
        counter.reset()
        w0 = wp.waits
        res = await fn()
        snap = counter.snapshot()
        row = {"scenario": label, "writes": n, "waits": wp.waits - w0, "search_queries": snap["search_queries"],
               "commands": snap["commands"], "attempts": res.get("attempts", 0), "ok": res.get("ok"),
               "expected_chunks": math.ceil(n / chunk)}
        rows.append(row)
        print(f"[counts] {row}", flush=True)
        return row

    base = 200
    for n in sizes:
        seqs = list(range(base, base + n))
        base += n
        wp = mk()
        ops = [Op("update", doc_oid(s), touch=n) for s in seqs]
        await measured("transaction_strong", n, lambda: wp.transaction(ops), wp)
        wp = mk()
        small = min(n, 20)  # sequential single writes for contrast: one wait per write
        sops = [Op("update", doc_oid(s), touch=n + 1) for s in seqs[:small]]

        async def seq_writes():
            last = {}
            for o in sops:
                last = await wp.write(o)
            return last

        await measured("n_single_strong_writes", small, seq_writes, wp)
        wp = mk()
        bops = [Op("update", doc_oid(s), touch=n + 2) for s in seqs]
        await measured("bulk_skip_sync", n, lambda: wp.bulk(bops, sync=False), wp)
        wp = mk()
        await measured("bulk_with_sync", n, lambda: wp.bulk(bops, sync=True), wp)
    for r in rows:
        if r["scenario"] == "transaction_strong":
            r["pass_nfr04"] = r["waits"] == 1 and r["search_queries"] <= max(1, r["attempts"]) * r["expected_chunks"] + 0
        if r["scenario"] == "bulk_skip_sync":
            r["pass_nfr05"] = r["search_queries"] == 0
            r["extra_marker_writes"] = r["commands"].get("update", 0)
    write_json(d / "counts.json", rows)


# -- phase: latency --------------------------------------------------------------------------------------------------
def latency_cases(ctx: Ctx) -> dict[str, dict]:
    cases: dict[str, dict] = {}
    base = dict(duration_s=ctx.duration_s, warm_s=ctx.warm_s, txn=False)
    short = dict(duration_s=max(4.0, ctx.duration_s / 2), warm_s=ctx.warm_s)
    hi = max(ctx.rates)
    mid = sorted(ctx.rates)[len(ctx.rates) // 2]
    for op in ("insert", "update", "delete"):
        for rate in ctx.rates:  # Q1 and Q2 at rtt 0: engine latency measured with fine polling
            cases[f"tts_{op}_r{rate}_rtt0"] = dict(base, op=op, variant="sim", mode="strong", rate=rate, rtt=0.0,
                                                  schedule="fine")
            cases[f"strong_{op}_r{rate}_rtt0"] = dict(base, op=op, variant="sim", mode="strong", rate=rate, rtt=0.0,
                                                     schedule="default")
        for rtt in ctx.rtts:
            if rtt > 0:
                cases[f"strong_{op}_r{mid}_rtt{rtt:g}"] = dict(base, op=op, variant="sim", mode="strong", rate=mid,
                                                              rtt=rtt, schedule="default")
    for op in ("insert", "update"):
        for txn in (False, True):
            for rtt in ctx.rtts:
                for variant in ("plain", "sim"):
                    cases[f"eventual_{op}_{'txn' if txn else 'notxn'}_{variant}_rtt{rtt:g}"] = dict(
                        short, op=op, variant=variant, mode="eventual", rate=mid, rtt=rtt, txn=txn, schedule="default")
    return cases


async def phase_latency(ctx: Ctx) -> None:
    start_proxies(ctx)
    try:
        await run_blocks(ctx, "latency", latency_cases(ctx))
    finally:
        stop_proxies(ctx)


# -- phase: coalesce -------------------------------------------------------------------------------------------------
async def phase_coalesce(ctx: Ctx) -> None:
    rate = max(ctx.rates)
    cases = {}
    for sched in ("default", "gentle", "slow"):
        for co in (False, True):
            cases[f"update_r{rate}_{'coalesced' if co else 'independent'}_{sched}"] = dict(
                duration_s=ctx.duration_s, warm_s=ctx.warm_s, txn=False, op="update", variant="sim", mode="strong",
                rate=rate, rtt=0.0, schedule=sched, coalesce=co)
    await run_blocks(ctx, "coalesce", cases)


# -- phase: marker (stateful, long) ----------------------------------------------------------------------------------
async def _sample_marker(ctx: Ctx, path: Path, stop: asyncio.Event, period: float, tag: str) -> None:
    db = ctx.db
    while not stop.is_set():
        row = {"ts": time.time(), "tag": tag}
        try:
            st = await db.command("collStats", SYNC)
            row.update(count=st["count"], size=st["size"], storage_size=st["storageSize"], total_index_size=st["totalIndexSize"],
                       index_sizes=st["indexSizes"])
            ix = await (await db[SYNC].aggregate([{"$indexStats": {}}])).to_list()
            row["index_ops"] = {i["name"]: i["accesses"]["ops"] for i in ix}
            ss = await db.command("serverStatus")
            row["opcounters"] = {k: ss["opcounters"][k] for k in ("insert", "query", "update", "delete", "command")}
            row["wt_cache_dirty_bytes"] = ss.get("wiredTiger", {}).get("cache", {}).get("tracked dirty bytes in the cache")
        except Exception as e:  # noqa: BLE001
            row["error"] = type(e).__name__
        append_jsonl(path, row)
        try:
            await asyncio.wait_for(stop.wait(), period)
        except TimeoutError:
            pass


async def phase_marker(ctx: Ctx, minutes: float, fail_pct: float) -> None:
    """Marker growth: strong writes at the design rate, all markers cleaned, then with a fixed share left `failed`."""
    d = ctx.phase_dir("marker")
    rate = max(ctx.rates)
    for tag, fp in (("clean", 0.0), ("failures", fail_pct)):
        key = f"marker:{tag}"
        if key in done_keys(d / "cases.jsonl"):
            continue
        await ctx.db[SYNC].delete_many({})
        stop = asyncio.Event()
        period = 3.0 if ctx.smoke else 30.0
        sampler = asyncio.create_task(_sample_marker(ctx, d / "samples.jsonl", stop, period, tag))
        spec = dict(duration_s=minutes * 60, warm_s=ctx.warm_s, txn=False, op="update", variant="sim", mode="strong",
                    rate=rate, rtt=0.0, schedule="default", fail_pct=fp)
        with Canary(ctx.uri, DB, d / f"canary_{tag}.jsonl"):
            ss0 = (await ctx.db.command("serverStatus"))["opcounters"]
            s = await run_case(ctx, "marker", tag, 0, spec)
            ss1 = (await ctx.db.command("serverStatus"))["opcounters"]
        stop.set()
        await sampler
        s["key"] = key
        s["fail_pct"] = fp
        s["server_opcounter_delta"] = {k: ss1[k] - ss0[k] for k in ("insert", "update", "delete", "query", "command")}
        append_jsonl(d / "cases.jsonl", s)


# -- phase: failure (stateful, fixed order, last) --------------------------------------------------------------------
def _mongot_pids(container: str) -> list[str]:
    script = ('for p in /proc/[0-9]*; do c=$(tr "\\0" " " < $p/cmdline 2>/dev/null); '
              'case "$c" in */mongo[t]/bin/jdk/bin/java*) echo ${p#/proc/};; esac; done')
    out = subprocess.run(["docker", "exec", container, "sh", "-c", script], capture_output=True, text=True, timeout=30)
    return [x for x in out.stdout.split() if x.isdigit()]


def _signal_mongot(container: str, sig: str) -> list[str]:
    pids = _mongot_pids(container)
    if pids:
        subprocess.run(["docker", "exec", container, "sh", "-c", f"kill -{sig} {' '.join(pids)}"], timeout=30)
    return pids


async def phase_failure(ctx: Ctx, timeout_s: float, reps: int) -> None:
    d = ctx.phase_dir("failure")
    out: dict = {"timeout_s": timeout_s}
    db = ctx.db
    blind = f"blind_{ctx.label}"
    sentinel = f"SENTINEL{random.Random(ctx.seed).randrange(10**9)}"

    def wp_for(entity: str) -> WritePath:
        coll = db[entity]
        w = IndependentWaiter(Prober(coll, index_name(entity), "rev", "count", 500, 4), Schedule(5, 1.5, 200, timeout_s))
        return WritePath(ctx.client, entity, w)

    # (a) the index can never show the token: the write must fail after the timeout, data stored, marker failed
    wp = wp_for(blind)
    runs = []
    for i in range(reps):
        did = doc_oid(500 + i)
        t = pc()
        err = None
        try:
            await wp.write(Op("update", did, touch=1000 + i))
            # put a recognisable field value into the document so the message can be checked for leaks
            raise AssertionError("write unexpectedly succeeded")
        except SyncFailure as e:
            err = e
        el = pc() - t
        marker = await db[SYNC].find_one({"entity": blind, "doc_id": did})
        data = await db[blind].find_one({"_id": did}, {"touch": 1, "title": 1, "body": 1})
        msg = str(err) + repr(err)
        values = [str(v) for v in (data or {}).values() if isinstance(v, str)] if data else []
        runs.append({"elapsed_s": el, "over_timeout_ms": (el - timeout_s) * 1000, "marker_state": marker and marker["state"],
                     "data_stored": bool(data and data.get("touch") == 1000 + i),
                     "message_has_field_value": any(v and v in msg for v in values),
                     "message": str(err)[:300], "type": type(err).__name__})
    out["blind_index_runs"] = runs
    # (b) partial failure in a transaction over two entities: an ExceptionGroup naming only the unsettled documents
    ok_wp, bad_wp = wp_for(ctx.entity), wp_for(blind)
    gops = [(ok_wp, [Op("update", doc_oid(600 + i), touch=2000) for i in range(5)]),
            (bad_wp, [Op("update", doc_oid(600 + i), touch=2000) for i in range(3)])]
    t = pc()
    try:
        await run_transaction(ctx.client, gops)
        out["transaction_group"] = {"raised": False}
    except ExceptionGroup as eg:
        named = sorted(str(i) for e in eg.exceptions for i in e.doc_ids)
        want_bad = sorted(str(op.doc_id) for op in gops[1][1])
        out["transaction_group"] = {
            "raised": True, "elapsed_s": pc() - t, "n_failures": len(eg.exceptions),
            "entities": [e.entity for e in eg.exceptions], "names_only_unsettled": named == want_bad,
            "ok_entity_markers_left": await db[SYNC].count_documents({"entity": ctx.entity, "doc_id": {"$in": [o.doc_id for o in gops[0][1]]}}),
            "failed_markers": await db[SYNC].count_documents({"entity": blind, "state": "failed"}),
            "messages": [str(e)[:300] for e in eg.exceptions]}
    # (c) the real engine paused (SIGSTOP of the search process), when a container is given
    if ctx.container:
        wp = wp_for(ctx.entity)
        pids = _signal_mongot(ctx.container, "STOP")
        out["pause"] = {"mongot_pids": len(pids)}
        if pids:
            try:
                runs = []
                for i in range(max(1, reps // 2)):
                    did = doc_oid(700 + i)
                    t = pc()
                    try:
                        await wp.write(Op("update", did, touch=3000 + i))
                        res = "succeeded"
                    except SyncFailure:
                        res = "SyncFailure"
                    el = pc() - t
                    marker = await db[SYNC].find_one({"entity": ctx.entity, "doc_id": did})
                    runs.append({"result": res, "elapsed_s": el, "over_timeout_ms": (el - timeout_s) * 1000,
                                 "marker_state": marker and marker["state"]})
                out["pause"]["runs"] = runs
            finally:
                _signal_mongot(ctx.container, "CONT")
            t = pc()
            recheck = IndependentWaiter(Prober(db[ctx.entity], index_name(ctx.entity), "rev", "count", 500, 4),
                                        Schedule(50, 1.5, 1000, 120))
            did = doc_oid(700)
            cur = (await current_items(db[ctx.entity], [700]))
            res = await recheck.wait(cur)
            out["pause"]["searchable_after_resume_s"] = pc() - t if res.ok else None
    write_json(d / "failure.json", out)
    print("[failure] done", flush=True)


# -- phase: m0 (informational, hosted Atlas free tier) -----------------------------------------------------------------
class _Limiter:
    def __init__(self, per_s: float) -> None:
        self.gap, self.next = 1.0 / per_s, 0.0

    async def acquire(self) -> None:
        now = time.monotonic()
        self.next = max(self.next, now) + self.gap
        await asyncio.sleep(max(0.0, self.next - self.gap - now))


class _TColl:
    def __init__(self, coll, lim) -> None:
        self._c, self._l = coll, lim

    def __getattr__(self, name):
        attr = getattr(self._c, name)

        async def call(*a, **k):
            await self._l.acquire()
            return await attr(*a, **k)

        return call


class _TDB:
    def __init__(self, db, lim) -> None:
        self._d, self._l = db, lim

    def __getitem__(self, name):
        return _TColl(self._d[name], self._l)


async def phase_m0(ctx: Ctx, atlas_uri: str) -> None:
    """Hosted Atlas free tier: scale-independent questions only. Never prints or stores the connection string."""
    from .gen import SEARCH_DEFINITION, ensure_index, make_docs

    d = ctx.phase_dir("m0")
    client = AsyncMongoClient(atlas_uri)
    lim = _Limiter(40.0)  # at most 40 operations per second (the cluster allows 100; the brief says stay under 50)
    raw = client[DB]
    name = "m0_entity"
    out: dict = {"limits_note": "M0: 0.5 GB, 100 ops/s, 3 search indexes; harness throttled to 40 ops/s"}
    try:
        # Leftovers of a crashed run: the name is fixed, and M0 jobs never overlap (they share one concurrency
        # group in CI), so start from a clean collection. Dropping it also drops its search index.
        await raw[name].drop()
        await raw[SYNC].delete_many({"entity": name})
        coll = _TColl(raw[name], lim)
        n = 1500 if ctx.smoke else 3000
        for i in range(0, n, 500):
            await coll.insert_many(make_docs(i, 500), ordered=False)
            st = await raw.command("collStats", name)
            if st["storageSize"] + st["totalIndexSize"] > 0.8 * 512 * 2**20:
                break
        await ensure_index(raw[name], index_name(name), SEARCH_DEFINITION)
        items = await current_items(raw[name], list(range(n)))
        idx = index_name(name)
        clauses = {}
        for shape in ("rev", "id_rev", "per_doc"):
            lo = 0
            for k in (100, 340, 341, 500, 511, 512, 1000, 1023, 1024):
                if k > len(items):
                    break
                try:
                    r = await (await coll.aggregate(pipeline(idx, shape, items[:k], True, "count"))).to_list()
                    lo = k if r and r[0]["count"]["total"] == k else lo
                except Exception:  # noqa: BLE001
                    break
            clauses[shape] = lo
        for kind, path in (("in_on_objectId_id", "_id"), ("in_on_objectId_rev", REV)):
            lo = 0
            vals = [i for i, _ in items] if path == "_id" else [r for _, r in items]
            for k in (100, 500, 1000, 1023, 1024, 1025):
                if k > len(vals):
                    break
                try:
                    r = await (await coll.aggregate([{"$searchMeta": {"index": idx, "in": {"path": path, "value": vals[:k]},
                                                                      "count": {"type": "total"}}}])).to_list()
                    lo = k if r else lo
                except Exception:  # noqa: BLE001
                    break
            clauses[kind] = lo
        out["max_ok_documents_tested_ladder"] = clauses
        # time to searchable at <= 10 writes per second, with throttled commands
        wp = WritePath(client, name, IndependentWaiter(Prober(coll, idx, "rev", "count", 500, 1), Schedule(100, 1.5, 500, 30)))
        wp.coll, wp.sync = coll, _TDB(raw, lim)[SYNC]
        tts = []
        for i in range(10 if ctx.smoke else 120):
            t0 = time.monotonic()
            try:
                r = await wp.write(Op("update", doc_oid(i), touch=i + 1))
                tts.append(r.get("engine_ub_ms"))
            except SyncFailure:
                tts.append(None)
            await asyncio.sleep(max(0.0, 0.2 - (time.monotonic() - t0)))
        out["time_to_searchable_ub_ms"] = tts
    finally:
        try:
            await raw[name].drop()
            await raw[SYNC].delete_many({"entity": name})
        finally:
            await client.close()
    write_json(d / "m0.json", out)
