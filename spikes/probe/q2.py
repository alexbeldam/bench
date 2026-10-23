"""Q2: many writers $inc hot documents; each waits with probe P1/P2/P3. Counts failed waits and false positives."""
import multiprocessing as mp, random, sys, threading
import common
from common import *
common.MAXMS = 3000

TIMEOUT = 10.0
TICK = 0.02
QC = [0]


def attribute(fn, co, by):
    """by: doc -> sorted list of pending versions. The index state per doc is monotone, so the number of satisfied
    pending versions is found by rounds of one pair per doc (first the highest, then bisect). Returns doc -> count satisfied."""
    lo = {d: -1 for d in by}
    hi = {d: len(v) for d, v in by.items()}
    first = True
    while True:
        act = [d for d in by if hi[d] - lo[d] > 1]
        if not act: break
        mid = {d: (hi[d] - 1 if first else (lo[d] + hi[d]) // 2) for d in act}
        first = False
        hit = fn(co, "ix", [(d, by[d][mid[d]]) for d in act]); QC[0] += 1
        for d in act:
            if d in hit: lo[d] = mid[d]
            else: hi[d] = mid[d]
    return {d: lo[d] + 1 for d in by}


def writer(pid, form, rate, dur, hot, t_start, outq):
    try:
        _writer(pid, form, rate, dur, hot, t_start, outq)
    except BaseException:
        import traceback; traceback.print_exc(); outq.put({'pid': pid, 'error': True, 'writes': 0, 'ok': 0, 'fail': 0, 'fp': 0, 'back': 0, 'lat': [], 'queries': 0, 'qerr': 0})


def _writer(pid, form, rate, dur, hot, t_start, outq):
    import faulthandler; faulthandler.dump_traceback_later(60, exit=False)
    c = cli(); co = c.q2.a
    pend = []                 # [doc, v, t_write]
    lock = threading.Lock()
    res = {"pid": pid, "writes": 0, "ok": 0, "fail": 0, "fp": 0, "back": 0, "lat": [], "queries": 0, "qerr": 0, "ticks": 0}
    last_seen = {}
    st = {"stop": False}

    def verify(done):
        need = {}
        for d, v in done: need[d] = max(need.get(d, 0), v)
        try:
            got = p1(co, "ix", list(need))
        except PyMongoError:
            return
        for d, v in need.items():
            if got.get(d, 0) < v: res["fp"] += 1

    def poller():
      try:
        pl()
      except BaseException:
        import traceback; traceback.print_exc(); st['dead'] = True

    def pl():
        while True:
            t0 = time.time()
            with lock: snap = list(pend)
            if not snap:
                if st["stop"]: return
                time.sleep(TICK); continue
            res["ticks"] += 1
            satids = set()
            try:
                if form == "P1":
                    docs = list({it[0] for it in snap})
                    got = p1(co, "ix", docs); res["queries"] += 1
                    for d, v in got.items():
                        if v < last_seen.get(d, 0): res["back"] += 1
                        last_seen[d] = max(last_seen.get(d, 0), v)
                    for it in snap:
                        if got.get(it[0], 0) >= it[1]: satids.add(id(it))
                else:
                    by = {}
                    for it in snap: by.setdefault(it[0], set()).add(it[1])
                    by = {d: sorted(s) for d, s in by.items()}
                    q0 = QC[0]
                    cnt = attribute(p2 if form == "P2" else p3, co, by)
                    res['queries'] += QC[0] - q0
                    ok_v = {d: set(by[d][:cnt[d]]) for d in by}
                    for it in snap:
                        if it[1] in ok_v[it[0]]: satids.add(id(it))
            except PyMongoError:
                res["qerr"] += 1
                time.sleep(TICK); continue
            now = time.time()
            done = []
            with lock:
                new = []
                for it in pend:
                    if id(it) in satids:
                        res["ok"] += 1; res["lat"].append(now - it[2]); done.append((it[0], it[1]))
                    elif now - it[2] > TIMEOUT:
                        res["fail"] += 1
                    else:
                        new.append(it)
                pend[:] = new
            if done: verify(done)
            dt = time.time() - t0
            if dt < TICK: time.sleep(TICK - dt)

    th = threading.Thread(target=poller, daemon=True); th.start()
    while time.time() < t_start: time.sleep(0.001)
    k = 0
    t0 = time.time()
    while True:
        tgt = t0 + k / rate
        if tgt - t0 > dur: break
        s = tgt - time.time()
        if s > 0: time.sleep(s)
        d = hot[k % len(hot)] if len(hot) == 1 else random.choice(hot)
        if form == "P3":
            upd = [{"$set": {"_hoodie_v": {"$add": [{"$ifNull": ["$_hoodie_v", 0]}, 1]}, "title": "w%d" % pid}},
                   {"$set": {"_hoodie_t": {"$let": {"vars": {"s": {"$toString": "$_id"}, "n": {"$toString": "$_hoodie_v"}},
                             "in": {"$concat": [{"$toString": {"$strLenCP": "$$s"}}, ":", "$$s", ":",
                                                {"$substrCP": ["0123456789abcdefghij", {"$strLenCP": "$$n"}, 1]}, "$$n"]}}}}}]
        else:
            upd = {"$inc": {"_hoodie_v": 1}, "$set": {"title": "w%d" % pid}}
        r = co.find_one_and_update({"_id": d}, upd, projection={"_hoodie_v": 1}, return_document=True)
        with lock: pend.append([d, int(r["_hoodie_v"]), time.time()])
        res["writes"] += 1
        k += 1
    t_end = time.time() + TIMEOUT + 3
    while time.time() < t_end:
        with lock:
            if not pend: break
        time.sleep(0.05)
    st["stop"] = True
    th.join(timeout=15)
    with lock:
        res["fail"] += len(pend)
    outq.put(res)


def monitor(hot, t_stop, outd):
    c = cli(); co = c.q2.a
    last = {}; back = 0; n = 0
    while time.time() < t_stop:
        try:
            got = p1(co, "ix", hot)
        except PyMongoError:
            continue
        n += 1
        for d, v in got.items():
            if v < last.get(d, 0): back += 1
            last[d] = max(last.get(d, 0), v)
        time.sleep(0.004)
    outd["mon_queries"], outd["mon_back"] = n, back


def run(form, nhot, rate, dur=20, procs=4):
    c = cli(); co = c.q2.a
    hot = [mkid("oid", 100000 + i) for i in range(nhot)]
    for h in hot:
        co.update_one({"_id": h}, {"$setOnInsert": {"title": "hot"}, "$max": {"_hoodie_v": Int64(0)}}, upsert=True)
    cur = {d["_id"]: d["_hoodie_v"] for d in co.find({"_id": {"$in": hot}})}
    t0 = time.time()
    while time.time() - t0 < 30:
        got = p1(co, "ix", hot)
        if all(got.get(h, -1) >= cur[h] for h in hot): break
        time.sleep(0.1)
    g = Guard()
    outq = mp.Queue()
    t_start = time.time() + 3
    ps = [mp.Process(target=writer, args=(i, form, rate / procs, dur, hot, t_start, outq)) for i in range(procs)]
    for p in ps: p.start()
    md = {}
    mt = threading.Thread(target=monitor, args=(hot, t_start + dur + 8, md)); mt.start()
    rs = [outq.get(timeout=dur + 90) for _ in ps]
    for p in ps: p.join()
    mt.join()
    lat = sorted(x for r in rs for x in r["lat"])
    return {
        "form": form, "hot": nhot, "rate": rate, "procs": procs, "dur": dur,
        "writes": sum(r["writes"] for r in rs), "ok": sum(r["ok"] for r in rs), "fail": sum(r["fail"] for r in rs),
        "fp": sum(r["fp"] for r in rs), "back": sum(r["back"] for r in rs) + md.get("mon_back", 0),
        "queries": sum(r["queries"] for r in rs), "qerr": sum(r["qerr"] for r in rs), "mon_queries": md.get("mon_queries"),
        "p50": round(lat[len(lat) // 2], 3) if lat else None, "p95": round(lat[int(len(lat) * .95)], 3) if lat else None,
        "max": round(lat[-1], 3) if lat else None, "slept": g.done(),
    }


if __name__ == "__main__":
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    only = sys.argv[2].split(",") if len(sys.argv) > 2 else ["P1", "P2", "P3"]
    dur = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    tag = sys.argv[4] if len(sys.argv) > 4 else "_".join(only)
    c = cli(); db = c.q2
    if "a" not in db.list_collection_names():
        co = db.a
        co.insert_many([{"_id": mkid("oid", i), "_hoodie_v": Int64(1), "title": "bg"} for i in range(1000)])
        mk_index(co, "ix", sdef("ok", "oid", t=True)); wait_count(co, "ix", 1000)
    allr = []
    for rep in range(reps):
        for form in only:
            for nhot in (1, 5):
                for rate in (200, 1000):
                    for attempt in range(3):
                        r = run(form, nhot, rate, dur)
                        r["rep"] = rep
                        print(json.dumps(r), flush=True)
                        if not r["slept"]: break
                        print("slept, repeating", flush=True)
                    allr.append(r)
                    save("q2_%s.json" % tag, allr)
                    time.sleep(4)
