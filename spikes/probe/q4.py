"""Q4: index size and build time at 1M documents for the index-definition variants."""
import random, subprocess, sys
from common import *

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
TAG = sys.argv[2] if len(sys.argv) > 2 else "m"
WORDS = "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu".split()


def dsize(idx_id):
    r = subprocess.run(["docker", "exec", CN, "sh", "-c", "du -sb /data/mongot/%s_* | cut -f1" % idx_id], capture_output=True, text=True).stdout.split()
    return sum(int(x) for x in r)


def variant(name):
    f = {"_id": {"type": "objectId"}, "title": {"type": "string"}}
    st = []
    if name in ("V1", "V2", "V3", "V4", "V6"):
        f["_hoodie_v"] = {"type": "number", "representation": "int64"}
    if name in ("V2", "V3", "V4", "V6"): st.append("_hoodie_v")
    if name in ("V3", "V6"):
        f["_hoodie_e"] = {"type": "number", "representation": "int64"}; st.append("_hoodie_e")
    if name in ("V4", "V5", "V6"):
        f["_hoodie_t"] = {"type": "token"}
    if name in ("V4", "V6"): st.append("_hoodie_t")
    d = {"mappings": {"dynamic": False, "fields": f}}
    if st: d["storedSource"] = {"include": st}
    return d


DESC = {"V0": "_id + title (baseline, no hoodie field)", "V1": "+ _hoodie_v number, not stored", "V2": "+ _hoodie_v number, stored",
        "V3": "V2 + _hoodie_e number, stored", "V4": "V2 + _hoodie_t token, stored", "V5": "V0 + _hoodie_t token, not stored",
        "V6": "V2 + _hoodie_e + _hoodie_t, all stored"}

if __name__ == "__main__":
    g = Guard()
    c = cli(); db = c["q4_" + TAG]; co = db.a
    random.seed(7)
    if co.estimated_document_count() < N:
        co.drop()
        t0 = time.time()
        B = 5000
        for s in range(0, N, B):
            docs = []
            for i in range(s, min(N, s + B)):
                x = mkid("oid", i)
                v = random.randint(1, 50)
                docs.append({"_id": x, "title": " ".join(random.choices(WORDS, k=6)), "_hoodie_v": Int64(v),
                             "_hoodie_e": Int64(1_790_000_000_000 + i), "_hoodie_t": packed(x, v)})
            co.insert_many(docs, ordered=False)
        print("loaded", N, round(time.time() - t0, 1), flush=True)
    out = []
    for name in sys.argv[3].split(",") if len(sys.argv) > 3 else ["V0", "V1", "V2", "V3", "V4", "V5", "V6"]:
        for i in co.list_search_indexes(): co.drop_search_index(i["name"]); time.sleep(3)
        while list(co.list_search_indexes()): time.sleep(1)
        gs = Guard()
        t0 = time.time()
        tq, ix = mk_index(co, name, variant(name), timeout=3600)
        pth = "title" if name == "V0" else ("_hoodie_t" if name == "V5" else "_hoodie_v")
        tf = wait_count(co, name, N, timeout=7200, path=pth) if name != "V0" else wait_count(co, name, N, timeout=7200, path="_id") if False else None
        if name == "V0":
            # count via a text clause present in every document would be heavy; use range on _id
            t1 = time.time()
            while True:
                r = q(co, [{"$searchMeta": {"index": name, "range": {"path": "_id", "gte": ObjectId("000000000000000000000000")}, "count": {"type": "total"}}}])
                if r and r[0]["count"]["total"] >= N: break
                time.sleep(1)
            tf = time.time() - t1
        total = time.time() - t0
        time.sleep(15)  # let merges settle
        sz = dsize(ix["id"])
        row = {"variant": name, "desc": DESC[name], "docs": N, "t_queryable": round(tq, 1), "t_fully_indexed": round(total, 1), "bytes": sz, "mib": round(sz / 2**20, 1), "slept": gs.done()}
        print(json.dumps(row), flush=True)
        out.append(row)
        save("q4_%s.json" % TAG, out)
    print("slept", g.done())
