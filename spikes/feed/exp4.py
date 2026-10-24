"""Q4: kill the stream's connection during a hot-document run and resume. usage: exp4.py auto|manual"""
import asyncio, json, sys, time
from pymongo.errors import PyMongoError
from common import *
from proxy import Proxy
from exp3 import check

COLLS = ["e0", "e1", "e2"]


async def main(kind):
    c = client(); db = c[DB]
    await seed(c, COLLS, 5)
    px = Proxy(27213); await px.start()
    pc = client(f"mongodb://127.0.0.1:{px.port}/?directConnection=true", serverSelectionTimeoutMS=3000)
    rdb = pc[DB]
    ref, got, errs, resumes = [], [], [], []
    stop = False

    async def refrun():
        s = await db.watch(pipe(COLLS), max_await_time_ms=100)
        await s.try_next()
        while not stop:
            ev = await s.try_next()
            if ev is not None:
                cc, d, r = ev_rev(ev)
                ref.append((cc, d, str(r), 0))
    rt = asyncio.create_task(refrun())
    s = await rdb.watch(pipe(COLLS), max_await_time_ms=100)
    await s.try_next()
    await asyncio.sleep(0.3)
    w = spawn("writer.py", "--colls", ",".join(COLLS), "--rate", 300, "--dur", 20, "--ndocs", 5, "--wid", 1)
    t0 = time.time()
    tok = None
    killed_at = None
    severed = None
    down_until = None
    last_ev_t = time.time()
    first_after = None
    g = Guard(); g.start()
    while time.time() - t0 < 25:
        now = time.time()
        if killed_at is None and now - t0 > 8:
            killed_at = now
            if kind == "outage":
                px.paused = True
                down_until = now + 3
            severed = px.sever()
        if down_until and now > down_until:
            px.paused = False
            down_until = None
        try:
            ev = await s.try_next()
        except PyMongoError as e:
            t_err = time.time()
            errs.append({"t_after_kill_s": round(t_err - killed_at, 2), "type": type(e).__name__, "code": getattr(e, "code", None), "msg": str(e)[:200]})
            # manual resume with the last event's token, retried every 0.5 s until it works, 20 s at most; the outage ends inside this loop
            while True:
                if down_until and time.time() > down_until:
                    px.paused = False
                    down_until = None
                if time.time() - t_err > 20:
                    resumes.append({"gave_up_after_s": 20})
                    break
                try:
                    s = await rdb.watch(pipe(COLLS), resume_after=tok, max_await_time_ms=100) if tok else await rdb.watch(pipe(COLLS), max_await_time_ms=100)
                    await s.try_next()
                    resumes.append({"seconds_from_error_to_open": round(time.time() - t_err, 3)})
                    break
                except PyMongoError as e2:
                    errs.append({"retry_error": type(e2).__name__, "msg": str(e2)[:120]})
                    await asyncio.sleep(0.5)
            continue
        if ev is not None:
            cc, d, r = ev_rev(ev)
            tok = ev["_id"]["_data"]
            got.append((cc, d, str(r), now_us()))
            if killed_at and first_after is None and time.time() > killed_at:
                first_after = time.time()
        if killed_at and time.time() - killed_at > 4 and w.poll() is not None:
            pass
    stop = True
    await rt
    print(wait_line(w, "DONE", 30)); w.wait()
    gd = g.stop()
    ts = [t for *_, t in got]
    gaps = [(b - a, a) for a, b in zip(ts, ts[1:])]
    mg = max(gaps)
    out = {"kind": kind, "reference_events": len(ref), "node": check(ref, got), "errors_seen": errs[:6], "n_errors": len(errs), "manual_resumes": resumes,
           "max_delivery_gap_ms": round(mg[0] / 1e3, 1), "gap_starts_s_after_kill": round(mg[1] / 1e6 - killed_at, 2),
           "severed_sockets": severed, "driver_auto_resume": kind == "auto", "guard": gd}
    print(json.dumps(out)); save(f"exp4_{kind}", out)

asyncio.run(main(sys.argv[1]))
