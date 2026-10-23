"""Timing and race experiments against the search-enabled MongoDB container. Plain pymongo, synthetic data.

  startup  cold-start stages (own container), then W workers sharing one container, each with its own database and index
  lag      write-to-searchable lag (polling every 10 ms) and delete-to-total-zero lag, one document per cycle
  stale    exact total right after a delete, with and without pausing the index process around the delete
  ghost    drop a collection while its search index exists, then recreate; does the index become queryable within a bound
  ties     tie order of the engine's native order against sort {score, _id}, over several index builds

Usage: python bench.py startup NAME PORT OUT [WORKERS=1,2,4] [CYCLES=15]
       python bench.py lag|stale|ghost|ties N OUT       (engine already running: FL_CONTAINER, FL_PORT)
"""
import asyncio
import json
import os
import random
import subprocess
import sys
import time

from pymongo import AsyncMongoClient
from pymongo.operations import SearchIndexModel

DEFN = {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}}
DEFN_ID = {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}, "_id": {"type": "objectId"}}}}
IMAGE = "mongodb/mongodb-atlas-local:8.0"
CONTAINER = os.environ.get("FL_CONTAINER", "hoodie-s4-engine")
PORT = os.environ.get("FL_PORT", "27217")
URI = f"mongodb://127.0.0.1:{PORT}/?directConnection=true"
FIND_ENGINE = "for p in /proc/[0-9]*; do if grep -qa '^/opt/mongot/bin/jdk/bin/java' $p/cmdline; then echo ${p#/proc/}; fi; done"


def q(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 3)


def summary(xs):
    return {"n": len(xs), "min": round(min(xs), 3), "p50": q(xs, .5), "p90": q(xs, .9), "p95": q(xs, .95), "p99": q(xs, .99),
            "max": round(max(xs), 3), **{f"over_{t}": sum(x > t for x in xs) for t in (0.9, 1.0, 1.1, 1.2, 1.5)}}


async def queryable(coll, name):
    while True:
        try:
            async for ix in await coll.list_search_indexes(name):
                if ix.get("queryable"):
                    return
        except Exception:  # noqa: BLE001  the port forward can drop while the container starts
            pass
        await asyncio.sleep(0.05)


async def make_index(coll, name, defn=DEFN):
    await coll.create_search_index(SearchIndexModel(definition=defn, name=name))
    await queryable(coll, name)


def text(name, key):
    return {"index": name, "text": {"query": key, "path": "title"}}


async def visible(coll, name, key):
    while not [d async for d in await coll.aggregate([{"$search": text(name, key)}, {"$limit": 1}])]:
        await asyncio.sleep(0.01)


async def total(coll, name, key):
    async for d in await coll.aggregate([{"$searchMeta": {**text(name, key), "count": {"type": "total"}}}]):
        return d["count"]["total"]
    return 0


async def zero(coll, name, key):
    while await total(coll, name, key) != 0:
        await asyncio.sleep(0.01)


def sh(cmd):
    return subprocess.run(["docker", "exec", CONTAINER, "sh", "-c", cmd], capture_output=True, text=True)


# ---- startup -------------------------------------------------------------------------------------------------------


async def startup(name, port, out, workers, cycles):
    r = {"image": IMAGE, "cpus": os.cpu_count()}
    t0 = time.perf_counter()
    now = lambda: round(time.perf_counter() - t0, 2)  # noqa: E731
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", name, "-p", f"{port}:27017", IMAGE], check=True, capture_output=True)
    r["docker_run_returned"] = now()
    c = AsyncMongoClient(f"mongodb://127.0.0.1:{port}/?directConnection=true", serverSelectionTimeoutMS=1000)
    while True:
        try:
            await c.admin.command("ping")
            break
        except Exception:  # noqa: BLE001
            await asyncio.sleep(0.2)
    r["mongod_ping"] = now()
    coll = c["bs"]["a"]
    fails = 0
    while True:
        try:
            await coll.insert_one({"_id": 1, "title": "key1 hello"})
            await coll.create_search_index(SearchIndexModel(definition=DEFN, name="i"))
            break
        except Exception as e:  # noqa: BLE001  writes can fail while mongod restarts; the index call fails until the engine connects
            fails += 1
            r.setdefault("first_error", f"{now()} {type(e).__name__}")
            await asyncio.sleep(0.25)
    r["engine_connected"] = now()
    r["retries_before_connected"] = fails
    await queryable(coll, "i")
    r["index_queryable"] = now()
    await visible(coll, "i", "key1")
    r["first_doc_searchable"] = now()
    r["parallel"] = {}

    async def worker(w, o):
        k0 = c[f"bw{w}"]["c"]
        await k0.insert_one({"_id": 0, "title": "seed"})
        s = time.perf_counter()
        await make_index(k0, "i")
        o["index_s"] = time.perf_counter() - s
        o["lags"] = []
        for k in range(1, cycles + 1):
            await k0.insert_one({"_id": k, "title": f"key{w}x{k} word"})
            s = time.perf_counter()
            await visible(k0, "i", f"key{w}x{k}")
            o["lags"].append(time.perf_counter() - s)

    for w in workers:
        outs = [{} for _ in range(w)]
        s = time.perf_counter()
        await asyncio.gather(*(worker(f"{w}_{i}", outs[i]) for i in range(w)))
        wall = time.perf_counter() - s
        lags = [x for o in outs for x in o["lags"]]
        r["parallel"][str(w)] = {"wall_s": round(wall, 2), "index_create_to_queryable_max_s": round(max(o["index_s"] for o in outs), 2),
                                 "cycles_total": len(lags), "cycles_per_s": round(len(lags) / wall, 2), "visible_lag": summary(lags)}
    await c.close()
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    json.dump(r, open(out, "w"), indent=1)
    print(json.dumps(r, indent=1))


# ---- lag -----------------------------------------------------------------------------------------------------------


async def lag(n, out):
    c = AsyncMongoClient(URI)
    coll = c["fl_lag"]["c"]
    await coll.insert_one({"title": "seed"})
    await make_index(coll, "i")
    vis, dele = [], []
    for k in range(1, n + 1):
        key = f"tok{k}"
        res = await coll.insert_one({"title": f"{key} x"})
        s = time.perf_counter()
        await visible(coll, "i", key)
        vis.append(time.perf_counter() - s)
        await coll.delete_one({"_id": res.inserted_id})
        s = time.perf_counter()
        await zero(coll, "i", key)
        dele.append(time.perf_counter() - s)
    r = {"write_to_searchable_s": summary(vis), "delete_to_total_zero_s": summary(dele), "visible_lags": [round(x, 3) for x in vis],
         "delete_lags": [round(x, 3) for x in dele]}
    json.dump(r, open(out, "w"))
    print(json.dumps({k: v for k, v in r.items() if not k.endswith("lags")}, indent=1))
    await c.drop_database("fl_lag")


# ---- stale ---------------------------------------------------------------------------------------------------------


async def stale(n, out):
    c = AsyncMongoClient(URI)
    coll = c["fl_stale"]["c"]
    await coll.insert_one({"title": "seed"})
    await make_index(coll, "i")
    pids = sh(FIND_ENGINE).stdout.split()
    r = {"engine_pids": pids, "runs": []}
    docs = lambda: [{"title": "tok common"} for _ in range(100)]  # noqa: E731

    async def refill():
        ids = (await coll.insert_many(docs())).inserted_ids
        while await total(coll, "i", "common") != 100:
            await asyncio.sleep(0.02)
        return ids

    for k in range(n):
        ids = await refill()
        await coll.delete_many({"_id": {"$in": ids}})
        plain = await total(coll, "i", "common")
        await zero(coll, "i", "common")
        ids = await refill()
        sh("kill -STOP " + " ".join(pids))
        await coll.delete_many({"_id": {"$in": ids}})
        sh("kill -CONT " + " ".join(pids))
        s = time.perf_counter()
        forced = await asyncio.wait_for(total(coll, "i", "common"), 60)
        r["runs"].append({"plain_total": plain, "forced_total": forced, "forced_query_s": round(time.perf_counter() - s, 3)})
        await zero(coll, "i", "common")
    r["plain_stale"] = f"{sum(x['plain_total'] != 0 for x in r['runs'])}/{n}"
    r["forced_stale"] = f"{sum(x['forced_total'] != 0 for x in r['runs'])}/{n}"
    json.dump(r, open(out, "w"), indent=1)
    print(r["plain_stale"], r["forced_stale"], r["runs"][:3])
    await c.drop_database("fl_stale")


# ---- ghost ---------------------------------------------------------------------------------------------------------


async def ghost(n, out):
    c = AsyncMongoClient(URI)
    r = {"bound_s": 20, "variants": {}}
    for variant, pause in (("collection dropped, recreate at once", 0.0), ("collection dropped, wait 5 s", 5.0), ("index dropped first, then collection", None)):
        res = []
        for k in range(n):
            tag = f"{abs(hash(variant)) % 9999}_{k}"
            db = c[f"fl_g_{tag}"]
            coll = db["c"]
            await coll.insert_many([{"title": "x common"} for _ in range(50)])
            await make_index(coll, "i")
            s = time.perf_counter()
            if pause is None:
                await coll.drop_search_index("i")
                while [ix async for ix in await coll.list_search_indexes("i")]:
                    await asyncio.sleep(0.1)
                await coll.drop()
            else:
                await coll.drop()
                await asyncio.sleep(pause)
            await db.create_collection("c")
            coll = db["c"]
            if not [ix async for ix in await coll.list_search_indexes("i")]:
                await coll.create_search_index(SearchIndexModel(definition=DEFN, name="i"))
            try:
                await asyncio.wait_for(queryable(coll, "i"), 20)
                res.append(["ok", round(time.perf_counter() - s, 2)])
            except asyncio.TimeoutError:
                res.append(["never_queryable_within_20s", 20.0])
            await c.drop_database(db.name)
        r["variants"][variant] = {"ok": sum(x[0] == "ok" for x in res), "n": n, "runs": res}
        print(variant, r["variants"][variant]["ok"], "ok of", n, flush=True)
    json.dump(r, open(out, "w"), indent=1)


# ---- ties ----------------------------------------------------------------------------------------------------------


async def ties(n, out):
    c = AsyncMongoClient(URI)
    coll = c["fl_ties"]["c"]
    rng = random.Random(20261022)
    await coll.insert_many([{"title": " ".join(f"w{rng.choices(range(40), [1 / (j + 1) for j in range(40)])[0]:04d}" for _ in range(rng.randint(2, 4)))} for _ in range(2000)])
    key = "w0000 w0001 w0002"
    seqs = {"native": [], "score_then_id": []}
    for b in range(n):
        name = f"t{b}"
        await make_index(coll, name, DEFN_ID)
        while await total(coll, name, key) == 0:
            await asyncio.sleep(0.2)
        await asyncio.sleep(2)
        nat = [d["_id"] async for d in await coll.aggregate([{"$search": text(name, key)}, {"$limit": 200}, {"$project": {"_id": 1}}])]
        srt = [d["_id"] async for d in await coll.aggregate([{"$search": {**text(name, key), "sort": {"score": {"$meta": "searchScore"}, "_id": 1}}}, {"$limit": 200}, {"$project": {"_id": 1}}])]
        seqs["native"].append([str(x) for x in nat])
        seqs["score_then_id"].append([str(x) for x in srt])
    r = {k: {"builds": len(v), "distinct_sequences": len({tuple(s) for s in v}), "hits": len(v[0])} for k, v in seqs.items()}
    json.dump(r, open(out, "w"), indent=1)
    print(r)
    await c.drop_database("fl_ties")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "startup":
        ws = [int(x) for x in (sys.argv[5] if len(sys.argv) > 5 else "1,2,4").split(",")]
        asyncio.run(startup(sys.argv[2], int(sys.argv[3]), sys.argv[4], ws, int(sys.argv[6]) if len(sys.argv) > 6 else 15))
    else:
        asyncio.run({"lag": lag, "stale": stale, "ghost": ghost, "ties": ties}[cmd](int(sys.argv[2]), sys.argv[3]))
