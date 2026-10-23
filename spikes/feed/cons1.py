"""Q1 consumer process: opens streams per collection or one database-level stream, counts events, latency, CPU, RSS."""
import asyncio, json, sys, time, psutil

from common import *

mode, colls = sys.argv[1], sys.argv[2].split(",")
rss0 = psutil.Process().memory_info().rss


async def main():
    c = client(maxPoolSize=100)
    db = c[DB]
    lats, events = [], 0
    seen = []
    streams = []
    srv0 = (await c.admin.command("serverStatus"))["metrics"]["cursor"]["open"]["total"]
    if mode == "percoll":
        for n in colls:
            streams.append(await db[n].watch(pipe(), max_await_time_ms=500))
    else:  # one database-level stream with a $match on ns.coll
        streams.append(await db.watch(pipe(colls), max_await_time_ms=500))
    for s in streams:
        await s.try_next()
    srv = (await c.admin.command("serverStatus"))["metrics"]["cursor"]["open"]["total"]
    print("READY", flush=True)
    until = float((await asyncio.get_running_loop().run_in_executor(None, sys.stdin.readline)).split()[1])
    cpu0 = time.process_time()
    g = Guard(); g.start()

    async def run(s):
        nonlocal events
        idle = 0
        while time.time() < until or idle < 2:
            ev = await s.try_next()
            if ev is None:
                idle += 1 if time.time() >= until else 0
                continue
            idle = 0
            _, _, rev = ev_rev(ev)
            events += 1
            seen.append(f"{ev['ns']['coll']} {ev['documentKey']['_id']} {rev}")
            if rev is not None:
                lats.append(now_us() - rev_us(rev))

    await asyncio.gather(*(run(s) for s in streams))
    for s in streams:
        await s.close()
    import os
    if os.environ.get("DUMP"):
        open(os.environ["DUMP"], "w").write(chr(10).join(seen))
    out = {"mode": mode, "streams": len(streams), "events": events, "lat": lat(lats), "cpu_s": round(time.process_time() - cpu0, 2),
           "rss_mb": round(psutil.Process().memory_info().rss / 1e6, 1), "server_open_cursors_added": srv - srv0, "guard": g.stop()}
    print("RESULT " + json.dumps(out), flush=True)


asyncio.run(main())
