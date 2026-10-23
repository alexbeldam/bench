"""Writer process: fixed-rate update writes of a new revision to random documents (optionally in a transaction with a marker)."""
import argparse, asyncio, json, random, time

from common import *

ap = argparse.ArgumentParser()
ap.add_argument("--colls", required=True)
ap.add_argument("--rate", type=float, required=True)
ap.add_argument("--dur", type=float, required=True)
ap.add_argument("--ndocs", type=int, default=200)
ap.add_argument("--wid", type=int, default=1)
ap.add_argument("--marker", action="store_true", help="write inside a transaction together with a marker upsert")
ap.add_argument("--sync", default="_sync")
ap.add_argument("--start", type=float, default=0)
ap.add_argument("--noise", default="", help="extra collection that receives the same rate (not searchable)")
ap.add_argument("--log", default="")
ap.add_argument("--uri", default=URI)
a = ap.parse_args()


async def main():
    c = client(a.uri, maxPoolSize=200)
    db = c[DB]
    colls = a.colls.split(",")
    rnd = random.Random(a.wid)
    sem = asyncio.Semaphore(128)
    n = 0
    counts = {}
    errs = 0
    lag = []
    wl = []

    async def one(coll, i, sched, k):
        nonlocal errs
        async with sem:
            lag.append(time.perf_counter() - sched)
            rev = mk_rev(a.wid, k)
            try:
                if a.marker and coll != a.noise:
                    async with c.start_session() as s:
                        async def body(s):
                            await db[coll].update_one({"_id": i}, {"$set": {REV: rev}}, session=s)
                            await db[a.sync].update_one({"_id": f"{coll}:{i}"}, {"$set": {REV: rev, "st": "pending"}},
                                                        upsert=True, session=s)
                        await s.with_transaction(body)
                    await db[a.sync].delete_one({"_id": f"{coll}:{i}"})
                else:
                    r = await db[coll].update_one({"_id": i}, {"$set": {REV: rev}})
                    if r.modified_count != 1:
                        errs += 1000
                if a.log and coll != a.noise:
                    wl.append(f"{coll} {i} {rev}")
            except Exception:
                errs += 1

    if a.start:
        while time.time() < a.start:
            await asyncio.sleep(0.01)
    t0 = time.perf_counter()
    tasks = []
    k = 0
    targets = colls + ([a.noise] if a.noise else [])
    while True:
        sched = t0 + k / a.rate
        if sched - t0 > a.dur:
            break
        d = sched - time.perf_counter()
        if d > 0:
            await asyncio.sleep(d)
        coll = targets[k % len(targets)] if not a.noise else (a.noise if k % 2 else colls[(k // 2) % len(colls)])
        counts[coll] = counts.get(coll, 0) + 1
        n += 1
        tasks.append(asyncio.create_task(one(coll, rnd.randrange(a.ndocs), sched, k)))
        k += 1
    await asyncio.gather(*tasks)
    if a.log:
        open(a.log, "w").write(chr(10).join(wl))
    print("DONE " + json.dumps({"writes": k, "errors": errs, "per_coll": counts, "rate_actual": round(k / (time.perf_counter() - t0), 1),
                                "lag_p95_ms": round(pct(lag, 95) * 1e3, 1)}), flush=True)


asyncio.run(main())
