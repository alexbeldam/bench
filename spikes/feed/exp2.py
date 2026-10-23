"""Q2: lazy open. Write, take the write's operation time, open a stream at that time, read, close. Count misses on a hot document."""
import asyncio, bisect, json, sys, time
from bson import Timestamp
from common import *


def hpipe():
    return [{"$match": {"ns.coll": "hot"}},
            {"$project": {"documentKey": 1, "operationType": 1, "clusterTime": 1, f"fullDocument.{REV}": 1,
                          f"updateDescription.updatedFields.{REV}": 1}}]


def ev_t(ev):
    _, _, r = ev_rev(ev | {"ns": {"coll": "hot"}})
    return ev["clusterTime"], r


async def main(trials, conc, variant, idle):
    c = client(maxPoolSize=400)
    db = c[DB]
    await seed(c, ["hot"], 1)
    ws = [spawn("writer.py", "--colls", "hot", "--ndocs", 1, "--rate", 150, "--dur", 3000, "--wid", w) for w in (2, 3, 4)]
    await asyncio.sleep(1.5)
    ref = []
    ref_stream = await db.watch(hpipe(), max_await_time_ms=200)
    stop = False

    async def refrun():
        while not stop:
            ev = await ref_stream.try_next()
            if ev is not None:
                ref.append(ev_t(ev))
    rt = asyncio.create_task(refrun())
    g = Guard(); g.start()
    res = []
    sem = asyncio.Semaphore(conc)
    k = 0

    async def trial(i):
        async with sem:
            rev = mk_rev(9, i)
            async with c.start_session() as s:
                if variant == "cmd":
                    r = await db.command({"update": "hot", "updates": [{"q": {"_id": 0}, "u": {"$set": {REV: rev}}}]}, session=s)
                    ts = r["operationTime"]
                elif variant == "read":  # negative control: the time observed after a read, not the write's own
                    await db.hot.update_one({"_id": 0}, {"$set": {REV: rev}}, session=s)
                    await db.hot.find_one({"_id": 0}, session=s)
                    ts = s.operation_time
                else:
                    await db.hot.update_one({"_id": 0}, {"$set": {REV: rev}}, session=s)
                    ts = s.operation_time
            t0 = time.perf_counter()
            st = await db.watch(hpipe(), start_at_operation_time=ts, max_await_time_ms=100)
            t_open = time.perf_counter() - t0
            got = []
            seen_own = None
            deadline = t0 + 5
            idle_until = None
            while time.perf_counter() < deadline:
                ev = await st.try_next()
                if ev is not None:
                    got.append(ev_t(ev))
                    if seen_own is None and got[-1][1] == rev:
                        seen_own = time.perf_counter() - t0
                        idle_until = time.perf_counter() + idle
                if seen_own is not None and (len(got) >= 6 and idle == 0 or idle and time.perf_counter() > idle_until):
                    break
            await st.close()
            res.append({"ts": ts, "open_ms": t_open * 1e3, "own_ms": None if seen_own is None else seen_own * 1e3, "got": got, "rev": rev})

    await asyncio.gather(*(trial(i) for i in range(trials)))
    await asyncio.sleep(1.0)
    stop = True
    await rt
    gd = g.stop()
    for w in ws:
        w.kill()
    await ref_stream.close()
    # verification against the always-on reference stream
    rts = [t for t, _ in ref]
    miss = bad = first_incl = first_after = 0
    empty_first = 0
    for r in res:
        if r["own_ms"] is None:
            miss += 1
            continue
        i = bisect.bisect_left(rts, r["ts"])
        want = ref[i:i + len(r["got"])]
        if want != r["got"]:
            bad += 1
        if r["got"] and r["got"][0][0] == r["ts"]:
            first_incl += 1
    oms = [r["open_ms"] * 1e3 for r in res]
    own = [r["own_ms"] * 1e3 for r in res if r["own_ms"] is not None]
    n_ev = sum(len(r["got"]) for r in res)
    out = {"variant": variant, "idle_s": idle, "trials": trials, "conc": conc, "own_event_missed": miss, "slice_mismatch_vs_reference": bad,
           "first_event_ts_equals_op_time": first_incl, "ref_events": len(ref), "open_ms": lat(oms), "open_to_own_event_ms": lat(own),
           "events_read": n_ev, "guard": gd}
    print(json.dumps(out), flush=True)
    return out


if __name__ == "__main__":
    variant, trials, conc, idle = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
    out = asyncio.run(main(trials, conc, variant, idle))
    save(f"exp2_{variant}_{trials}_{int(idle)}", out)
