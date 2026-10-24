"""Q3 node process. Modes: own (one db-level stream in this process), marker (one stream on the sync-marker collection),
capped / redis (leader elected through a lease document; the leader holds the one stream and republishes, every node tails)."""
import asyncio, json, os, sys, time

from pymongo import CursorType
from pymongo.errors import DuplicateKeyError

from common import *

name, mode, out, colls = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(",")
KEEP = bool(os.environ.get("KEEP_FIRST"))
TTL = float(sys.argv[5]) if len(sys.argv) > 5 else 3.0
SYNC = "_sync"
buf, nrec = [], 0
terms = []


def rec(c, d, rev, via):
    global nrec
    nrec += 1
    buf.append(f"{c} {d} {rev} {now_us()} {via}\n")


async def flusher(fh):
    while True:
        await asyncio.sleep(0.2)
        if buf:
            fh.write("".join(buf)); buf.clear(); fh.flush()


async def stream_events(db, readyf):
    """Yield (token, coll, doc, rev) from the one stream this process (or the leader) holds."""


async def main():
    c = client(maxPoolSize=60)
    db = c[DB]
    fh = open(out, "w")
    asyncio.create_task(flusher(fh))
    if mode == "redis":
        import redis.asyncio as aredis
        r = aredis.from_url(REDIS)
    role = {"leader": False, "task": None}

    # ---- the one stream -------------------------------------------------------------------------------------------
    async def open_stream(tok):
        kw = {"resume_after": {"_data": tok}} if tok else {}
        if mode == "marker":
            s = await db[SYNC].watch(pipe(None, ops=("insert", "update", "replace")), max_await_time_ms=100, **kw)
        else:
            s = await db.watch(pipe(colls), max_await_time_ms=100, **kw)
        return s, await s.try_next()

    def parse(ev):
        coll, doc, rev = ev_rev(ev)
        if mode == "marker":
            coll, doc = doc.split(":")
            doc = int(doc)
        return coll, doc, rev

    # ---- publishing ---------------------------------------------------------------------------------------------
    async def last_tok():
        if mode == "capped":
            d = await db.feed.find_one(sort=[("$natural", -1)])
            return (d.get("tok"), d["_id"]) if d else (None, 0)
        e = await r.xrevrange("feed", "+", "-", count=1)
        if not e:
            return None, 0
        f = {k.decode(): v.decode() for k, v in e[0][1].items()}
        return f.get("tok") or None, int(f["n"])

    async def publisher(gen):
        tok, seq = await last_tok()
        s, first = await open_stream(tok)
        q = asyncio.Queue()
        if KEEP and first is not None:
            q.put_nowait(first)
        if gen is not None:
            gen.append(time.time())

        async def reader():
            while True:
                ev = await s.try_next()
                if ev is not None:
                    await q.put(ev)
        rt = asyncio.create_task(reader())
        try:
            while True:
                batch = [await q.get()]
                while not q.empty() and len(batch) < 500:
                    batch.append(q.get_nowait())
                docs = []
                for ev in batch:
                    seq += 1
                    cc, dd, rv = parse(ev)
                    if rv is None:
                        continue
                    docs.append({"_id": seq, "c": cc, "d": dd, "r": str(rv), "tok": ev["_id"]["_data"]})
                if not docs:
                    continue
                if mode == "capped":
                    await db.feed.insert_many(docs)
                else:
                    p = r.pipeline(transaction=False)
                    for x in docs:
                        p.xadd("feed", {"n": x["_id"], "c": x["c"], "d": x["d"], "r": x["r"], "tok": x["tok"]}, maxlen=200000, approximate=True)
                    await p.execute()
        finally:
            rt.cancel()
            try:
                await s.close()
            except Exception:
                pass

    async def lease_loop():
        while True:
            now = now_us()
            if role["leader"]:
                res = await db.lease.update_one({"_id": "L", "o": name, "exp": {"$gte": now}}, {"$set": {"exp": now + int(TTL * 1e6)}})
                if res.matched_count == 0:
                    role["leader"] = False
                    role["task"].cancel()
                    terms[-1].append(time.time())
            else:
                try:
                    res = await db.lease.update_one({"_id": "L", "exp": {"$lt": now}}, {"$set": {"o": name, "exp": now + int(TTL * 1e6)}}, upsert=True)
                    got = bool(res.matched_count or res.upserted_id)
                except DuplicateKeyError:
                    got = False
                if got:
                    role["leader"] = True
                    terms.append([time.time()])
                    role["task"] = asyncio.create_task(publisher(None))
            await asyncio.sleep(1.0 if role["leader"] else 0.25)

    # ---- consuming ----------------------------------------------------------------------------------------------
    async def tail_capped():
        _, last = await last_tok()
        while True:
            cur = db.feed.find({"_id": {"$gt": last}}, cursor_type=CursorType.TAILABLE_AWAIT).max_await_time_ms(100)
            async for d in cur:
                last = d["_id"]
                rec(d["c"], d["d"], d["r"], "feed")
            await asyncio.sleep(0.005)

    async def tail_redis():
        e = await r.xrevrange("feed", "+", "-", count=1)
        last = e[0][0].decode() if e else "0-0"
        while True:
            res = await r.xread({"feed": last}, block=100, count=1000)
            for _, items in res or []:
                for i, f in items:
                    last = i.decode()
                    rec(f[b"c"].decode(), f[b"d"].decode(), f[b"r"].decode(), "feed")

    async def own_stream():
        s, first = await open_stream(None)
        if KEEP and first is not None:
            cc, dd, rv = parse(first)
            if rv is not None:
                rec(cc, dd, rv, "direct")
        while True:
            ev = await s.try_next()
            if ev is not None:
                cc, dd, rv = parse(ev)
                if rv is not None:
                    rec(cc, dd, rv, "direct")

    if mode in ("own", "marker"):
        t = asyncio.create_task(own_stream())
        await asyncio.sleep(0.5)
    else:
        t = asyncio.create_task(tail_capped() if mode == "capped" else tail_redis())
        asyncio.create_task(lease_loop())
        await asyncio.sleep(1.0)
    print("READY", flush=True)
    until = float((await asyncio.get_running_loop().run_in_executor(None, sys.stdin.readline)).split()[1])
    g = Guard(); g.start()
    while time.time() < until:
        await asyncio.sleep(0.2)
        if t.done():
            t.result()
    fh.write("".join(buf)); fh.close()
    print("RESULT " + json.dumps({"name": name, "events": nrec, "terms": terms, "guard": g.stop()}), flush=True)
    import os
    os._exit(0)


asyncio.run(main())
