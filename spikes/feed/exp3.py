"""Q3: sharing prototypes. usage: exp3.py MODE [kill] [marker] [keep] -- MODE own|capped|redis|marker.
(keep: nodes keep the event the first read after opening a stream returns; otherwise it is dropped)"""
import asyncio, json, os, sys, time, collections
from common import *

COLLS = ["e0", "e1", "e2"]
P = 4


async def prep(mode):
    c = client()
    await seed(c, COLLS, 20)
    db = c[DB]
    for n in ("feed", "lease", "_sync"):
        await db[n].drop()
    await db.create_collection("feed", capped=True, size=64 * 2**20)
    await db.feed.insert_one({"_id": 0, "seed": 1})
    await db.create_collection("_sync")
    if mode == "redis":
        import redis.asyncio as aredis
        r = aredis.from_url(REDIS)
        await r.delete("feed")
    return None


async def owner_():
    return (await client()[DB].lease.find_one({"_id": "L"}))["o"]


async def srv(c=None):
    c = client()
    s = await c.admin.command("serverStatus")
    return {"op": dict(s["opcounters"]), "net_in": s["network"]["bytesIn"], "net_out": s["network"]["bytesOut"],
            "cursors": s["metrics"]["cursor"]["open"]["total"]}


def load(path):
    out = []
    for line in open(path):
        p = line.split()
        if len(p) == 5:
            out.append((p[0], int(p[1]), p[2], int(p[3])))
    return out


def check(ref, ev):
    """Per document: missing, duplicate and out-of-order revisions against the reference stream's commit order."""
    pos = {}
    for k, (c, d, r, _) in enumerate(ref):
        pos[(c, d, r)] = k
    seen = collections.Counter((c, d, r) for c, d, r, _ in ev)
    dup = sum(v - 1 for v in seen.values() if v > 1)
    miss = sum(1 for k in pos if k not in seen)
    extra = sum(1 for k in seen if k not in pos)
    last = {}
    bad = 0
    for c, d, r, _ in ev:
        p = pos.get((c, d, r))
        if p is None:
            continue
        if (c, d) in last and p < last[(c, d)]:
            bad += 1
        last[(c, d)] = max(p, last.get((c, d), -1))
    lats = [t - rev_us(ObjectId(r)) for _, _, r, t in ev]
    mids = [{"ref_index": k, "key": list(kk)} for kk, k in pos.items() if kk not in seen][:5]
    return {"events": len(ev), "missing": miss, "missing_events": mids, "duplicates": dup, "unknown": extra, "out_of_order": bad, "lat": lat(lats)}


def main(mode, kill, marker, keep=False, rate=200, dur=24):
    tag = f"{mode}{'_kill' if kill else ''}{'_marker' if marker else ''}{'_keep' if keep else ''}"
    os.environ["KEEP_FIRST"] = "1" if keep else ""
    asyncio.run(prep(mode))
    cl = ",".join(COLLS)
    ref = spawn("node.py", "ref", "own", f"results/_n_ref.txt", cl)
    nodes = []
    for i in range(P):
        nodes.append(spawn("node.py", f"n{i}", mode, f"results/_n_{i}.txt", cl))
        wait_line(nodes[-1], "READY", 60)  # n0 first, so it holds the lease
    wait_line(ref, "READY")
    until = time.time() + dur + 10
    for p in nodes + [ref]:
        go(p, until)
    s0 = asyncio.run(srv())
    time.sleep(0.3)
    w = spawn("writer.py", "--colls", cl, "--rate", rate, "--dur", dur, "--ndocs", 20, "--wid", 1, *(["--marker"] if marker else []))
    killed = None
    t_kill = None
    if kill:
        time.sleep(1.5 + dur / 3)
        owner = asyncio.run(owner_())
        killed = int(owner[1:])
        t_kill = time.time()
        nodes[killed].kill()
    cpu = []
    time.sleep(dur / 3 if kill else dur / 2)
    cpu.append(dstats())
    wr = json.loads(wait_line(w, "DONE", dur + 60))
    s1 = asyncio.run(srv())
    res = {}
    for i, p in enumerate(nodes):
        if i != killed:
            res[f"n{i}"] = json.loads(wait_line(p, "RESULT", 60))
    json.loads(wait_line(ref, "RESULT", 60))
    for p in nodes + [ref, w]:
        try:
            p.wait(5)
        except Exception:
            p.kill()
    R = load("results/_n_ref.txt")
    out = {"tag": tag, "mode": mode, "kill": kill, "marker_workload": marker, "writer": wr, "reference_events": len(R),
           "db_load": {"ops": {k: s1["op"][k] - s0["op"][k] for k in s0["op"]}, "net_in_mb": round((s1["net_in"] - s0["net_in"]) / 1e6, 2),
                       "net_out_mb": round((s1["net_out"] - s0["net_out"]) / 1e6, 2), "open_cursors_during": s1["cursors"], "mongod_cpu_mem": cpu},
           "nodes": {}, "guards": {k: v["guard"] for k, v in res.items()}}
    for i in range(P):
        if i == killed:
            ev = load(f"results/_n_{i}.txt")
            out["nodes"][f"n{i}(killed)"] = {"events_before_kill": len(ev)}
            continue
        ev = load(f"results/_n_{i}.txt")
        r = check(R, ev)
        r["terms"] = res[f"n{i}"]["terms"]
        ts = sorted(t for *_, t in ev)
        if kill and ts:  # largest gap in delivery (in ms) between consecutive events, around and after the kill
            gaps = [(b - a, a) for a, b in zip(ts, ts[1:])]
            g = max(gaps)
            r["max_delivery_gap_ms"] = round(g[0] / 1e3, 1)
            r["gap_starts_s_after_kill"] = round(g[1] / 1e6 - t_kill, 2)
        out["nodes"][f"n{i}"] = r
    if kill:
        out["killed"] = f"n{killed}"
    print(json.dumps(out), flush=True)
    save(f"exp3_{tag}", out)


if __name__ == "__main__":
    a = sys.argv[1:]
    main(a[0], "kill" in a, "marker" in a, "keep" in a)
