from common import *
c = cli(); co = c.q1_oid.a
docs = list(co.find().limit(1100))
def run(sp, n, rsc=True):
    try:
        s = dict(sp); s["index"] = "ok"
        if rsc: s["returnStoredSource"] = True
        return "ok %d" % len(q(co, [{"$search": s}, {"$limit": n}]))
    except PyMongoError as e:
        return "FAIL " + str(e)[:40]
def eq(d): return {"equals": {"path": "_hoodie_t", "value": d["_hoodie_t"]}}
def rg(d): return packed_range(d["_id"], 1)
def nr(d): return {"range": {"path": "_hoodie_v", "gte": d["_hoodie_v"]}}
for nm, mk in (("eq", eq), ("tokrange", rg), ("numrange", nr)):
    for ln, wrap in (("should", lambda sh: {"compound": {"should": sh}}),
                     ("should+msm", lambda sh: {"compound": {"should": sh, "minimumShouldMatch": 1}}),
                     ("filter[should]", lambda sh: {"compound": {"filter": [{"compound": {"should": sh}}]}})):
        print(nm, ln, {n: run(wrap([mk(d) for d in docs[:n]]), n) for n in (512, 513, 700, 1000, 1024, 1025)})
print("----")
def bis(mk, rsc, hi=1100):
    lo = 1
    while lo < hi:
        m = (lo + hi + 1) // 2
        if run({"compound": {"should": [mk(d) for d in docs[:m]]}}, m, rsc).startswith("ok"): lo = m
        else: hi = m - 1
    return lo
for nm, mk in (("eq", eq), ("tokrange", rg), ("numrange", nr)):
    print(nm, "max with rsc", bis(mk, True), "without rsc", bis(mk, False))
