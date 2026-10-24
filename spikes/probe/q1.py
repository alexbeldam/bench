"""Q1: probe forms P1..P3 per id type: correctness, clause limit, latency, wrong mappings."""
import random, statistics, sys
from common import *

N = 3000
random.seed(20261023)
g = Guard()
c = cli()
out = {"n": N, "kinds": {}}


def lat(fn, reps=15):
    ts = []
    for _ in range(reps):
        t = time.perf_counter(); fn(); ts.append((time.perf_counter() - t) * 1000)
    ts.sort()
    return {"p50": round(statistics.median(ts), 1), "max": round(ts[-1], 1)}


def check(form, coll, name, kind, docs, S=500):
    """returns dict of mismatch counts; exceptions counted."""
    res = {"false_hit": 0, "false_miss": 0, "error": None, "asked": 0}
    sample = random.sample(docs, S)
    try:
        if form == "P1":
            ids = [d["_id"] for d in sample]
            got = p1(coll, name, ids)
            for d in sample:
                res["asked"] += 3
                have = got.get(d["_id"])
                for dv, exp in ((-1, True), (0, True), (1, False)):
                    mine = d["_hoodie_v"] + dv
                    ok = have is not None and have >= mine
                    if ok and not exp: res["false_hit"] += 1
                    if exp and not ok: res["false_miss"] += 1
        else:
            fn = p2 if form == "P2" else p3
            for dv, exp in ((-1, True), (0, True), (1, False)):
                pairs = [(d["_id"], d["_hoodie_v"] + dv) for d in sample]
                hit = set()
                step = 250
                for i in range(0, len(pairs), step):
                    hit |= fn(coll, name, pairs[i:i + step])
                for d in sample:
                    res["asked"] += 1
                    h = d["_id"] in hit
                    if h and not exp: res["false_hit"] += 1
                    if exp and not h: res["false_miss"] += 1
            # ids that do not exist
            ghost = [(mkid(kind, N + 10 + i), 1) for i in range(100)]
            if fn(coll, name, ghost): res["false_hit"] += 1
    except PyMongoError as e:
        res["error"] = str(e)[:160]
    return res


def limit(form, coll, name, kind, docs):
    """largest number of documents accepted in one query (bisect), real v values."""
    def ok(n):
        sub = docs[:n]
        try:
            if form == "P1": p1(coll, name, [d["_id"] for d in sub])
            elif form == "P2": p2(coll, name, [(d["_id"], d["_hoodie_v"]) for d in sub])
            else: p3(coll, name, [(d["_id"], d["_hoodie_v"]) for d in sub])
            return True
        except PyMongoError:
            return False
    hi = len(docs)
    if ok(hi): return {"max": ">=%d" % hi, "n": hi}
    lo = 1
    while lo < hi:
        m = (lo + hi + 1) // 2
        if ok(m): lo = m
        else: hi = m - 1
    return {"max": lo, "n": lo}


def chunked(form, coll, docs, n, lim):
    sub = docs[:n]
    parts = [sub[i:i + lim] for i in range(0, n, lim)]
    def f():
        for p in parts:
            if form == "P1": p1(coll, "ok", [d["_id"] for d in p])
            elif form == "P2": p2(coll, "ok", [(d["_id"], 1) for d in p])
            else: p3(coll, "ok", [(d["_id"], 1) for d in p])
    r = lat(f)
    r["queries"] = len(parts)
    return r


for kind in ("oid", "str", "uuid", "int"):
    db = c["q1r2_" + kind]; co = db.a
    docs = []
    for i in range(N):
        x = mkid(kind, i)
        v = random.choice([random.randint(1, 9), random.randint(10, 99), random.randint(100, 9999), random.randint(10**4, 10**7)])
        docs.append({"_id": x, "_hoodie_v": Int64(v), "_hoodie_t": packed(x, v), "title": "t%d" % i})
    for i in range(0, N, 1000): co.insert_many(docs[i:i + 1000])
    K = out["kinds"][kind] = {"index": {}, "forms": {}}
    defs = {"ok": sdef("ok", kind, t=True)}
    for wn, wm in WRONG[kind].items(): defs["w_" + wn] = sdef(wm, kind, t=True)
    for nm, d in defs.items():
        tm, _ = mk_index(co, nm, d)
        K["index"][nm] = round(tm + wait_count(co, nm, N), 1)
    for form in ("P1", "P2", "P3"):
        F = K["forms"][form] = {}
        for nm in defs:
            F[nm] = check(form, co, nm, kind, docs)
        F["limit"] = limit(form, co, "ok", kind, docs)
        F["lat"] = {}
        lim = F["limit"]["n"]
        for n in (1, 100, 1000):
            try:
                F["lat"][n] = chunked(form, co, docs, n, min(lim, 1000))
            except PyMongoError as e:
                F["lat"][n] = "ERR " + str(e)[:80]
        print(kind, form, json.dumps(F, default=str)[:900], flush=True)
out["slept"] = g.done()
save("q1.json", out)
print("slept", out["slept"])
bad = [(k, f) for k, K in out["kinds"].items() for f, F in K["forms"].items()
       if F["ok"]["error"] or F["ok"]["false_hit"] or F["ok"]["false_miss"] or any(isinstance(v, str) for v in F["lat"].values())]
if bad:
    print("FAILED with the correct mapping:", bad)
    sys.exit(1)
