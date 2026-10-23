"""Q5: why can a stream not be opened. Records the exact server errors. usage: exp5.py causes | pool | zombie"""
import asyncio, json, sys, time
from bson import Timestamp
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError
from common import *

R = {}


def err(e):
    d = {"type": type(e).__name__, "code": getattr(e, "code", None), "codeName": (getattr(e, "details", None) or {}).get("codeName"),
         "msg": str(e)[:400]}
    return d


async def try_watch(label, coll, **kw):
    try:
        s = await coll.watch(**kw)
        await s.try_next()
        await s.close()
        R[label] = "OK"
    except PyMongoError as e:
        R[label] = err(e)
    print(label, R[label], flush=True)


async def causes():
    c = AsyncMongoClient("mongodb://127.0.0.1:27215/?directConnection=true", serverSelectionTimeoutMS=3000)
    h = await c.admin.command("hello")
    R["standalone_hello"] = {k: h.get(k) for k in ("isWritablePrimary", "setName", "msg", "maxWireVersion") if k in h}
    await try_watch("standalone_collection_watch", c.t.c)
    await try_watch("standalone_db_watch", c.t)
    await try_watch("standalone_cluster_watch", c)
    m = client()
    h = await m.admin.command("hello")
    R["replset_hello"] = {k: h.get(k) for k in ("isWritablePrimary", "setName", "hosts", "me", "maxWireVersion") if k in h}
    for u in ("u_find", "u_cscoll", "u_csdb"):
        a = AsyncMongoClient(f"mongodb://{u}:pw@127.0.0.1:27216/?directConnection=true&authSource=admin", serverSelectionTimeoutMS=3000)
        cs = await a.admin.command({"connectionStatus": 1, "showPrivileges": True})
        R[f"{u}_privileges_from_connectionStatus"] = [(p["resource"], p["actions"]) for p in cs["authInfo"]["authenticatedUserPrivileges"]]
        await try_watch(f"{u}_watch_e0", a.scf.e0)
        await try_watch(f"{u}_watch_e1", a.scf.e1)
        await try_watch(f"{u}_db_watch_with_match", a.scf, pipeline=pipe(["e0"]))
        await a.close()
    for d in ("admin", "local", "config"):
        await try_watch(f"db_watch_on_{d}", m[d])
    await try_watch("start_at_ancient_optime", m[DB].e0, start_at_operation_time=Timestamp(100, 1))
    await try_watch("start_at_future_optime", m[DB].e0, start_at_operation_time=Timestamp(int(time.time()) + 3600, 1))
    await try_watch("resume_with_garbage_token", m[DB].e0, resume_after={"_data": "00"})
    p = await m.admin.command({"getParameter": "*"})
    R["server_params_matching_cursor_stream_maxconn"] = {k: v for k, v in p.items() if any(t in k.lower() for t in ("changestream", "cursor", "maxconn", "maxincomingconn"))}
    save("exp5_causes", R)


async def pool():
    """One stream holds a pooled connection while it awaits: does a small pool starve many streams?"""
    out = {}
    for size, n in ((10, 8), (10, 12), (100, 120)):
        c = client(maxPoolSize=size, waitQueueTimeoutMS=2000)
        db = c[DB]
        names = [f"p{i}" for i in range(n)]
        for x in names:
            await db[x].insert_one({"a": 1})
        streams, e1 = [], None
        try:
            for x in names:
                streams.append(await db[x].watch(max_await_time_ms=2000))
        except PyMongoError as e:
            e1 = err(e)
        errs = []

        async def run(s):
            t = time.time()
            try:
                while time.time() - t < 6:
                    await s.try_next()
            except PyMongoError as e:
                errs.append(err(e))
        await asyncio.gather(*(run(s) for s in streams))
        out[f"pool={size},streams={n}"] = {"opened": len(streams), "open_error": e1, "read_errors": len(errs), "first_read_error": errs[0] if errs else None}
        print(size, n, out[f"pool={size},streams={n}"], flush=True)
        for s in streams:
            await s.close()
        await c.close()
    save("exp5_pool", out)


async def zombie():
    """A process killed without closing its streams: how long does the server keep the cursors?"""
    m = client()

    async def n():
        return (await m.admin.command("serverStatus"))["metrics"]["cursor"]["open"]["total"]
    out = {"default_cursorTimeoutMillis": (await m.admin.command({"getParameter": 1, "cursorTimeoutMillis": 1}))["cursorTimeoutMillis"]}
    base = await n()
    p = spawn("cons1.py", "percoll", "e0,e1,e2")
    wait_line(p, "READY")
    during = await n()
    p.kill()
    p.wait()
    out["baseline"], out["with_3_streams"] = base, during
    out["after_kill_s"] = {}
    t0 = time.time()
    for t in (1, 5, 15, 30, 60):
        await asyncio.sleep(max(0, t - (time.time() - t0)))
        out["after_kill_s"][str(t)] = await n()
    await m.admin.command({"setParameter": 1, "cursorTimeoutMillis": 20000})
    t0 = time.time()
    while time.time() - t0 < 120 and await n() > base:
        await asyncio.sleep(2)
    out["with_cursorTimeoutMillis_20000_seconds_until_baseline"] = round(time.time() - t0, 1) if await n() <= base else "not reached in 120 s"
    await m.admin.command({"setParameter": 1, "cursorTimeoutMillis": out["default_cursorTimeoutMillis"]})
    print(json.dumps(out))
    save("exp5_zombie", out)


asyncio.run({"causes": causes, "pool": pool, "zombie": zombie}[sys.argv[1]]())
