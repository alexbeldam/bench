"""Shared helpers for the version-probe spike."""
import json, os, subprocess, sys, threading, time, uuid
from bson import ObjectId, Int64
from bson.binary import Binary, UuidRepresentation
from pymongo import MongoClient
from pymongo.errors import PyMongoError

MAXMS = None
URI = "mongodb://127.0.0.1:27212/?directConnection=true"
CN = "hoodie-s2-engine"
RES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
os.makedirs(RES, exist_ok=True)


def cli(**kw):
    return MongoClient(URI, uuidRepresentation="standard", **kw)


# ---- sleep guard: a run is flagged if the wall clock jumps (laptop sleep) ----
class Guard:
    def __init__(self, gap=4.0):
        self.gap, self.slept, self.stop = gap, False, False
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()

    def run(self):
        last = time.time()
        while not self.stop:
            time.sleep(0.5)
            now = time.time()
            if now - last > self.gap:
                self.slept = True
            last = now

    def done(self):
        self.stop = True
        return self.slept


def save(name, obj):
    with open(os.path.join(RES, name), "w") as f:
        json.dump(obj, f, indent=1, default=str)


# ---- id types ----
IDMAP = {
    "oid": {"type": "objectId"},
    "str": {"type": "token"},
    "uuid": {"type": "uuid"},
    "int": {"type": "number", "representation": "int64"},
}
# deliberately wrong mappings (silent failures); "none" = no _id mapping
WRONG = {
    "oid": {"none": None, "token": {"type": "token"}},
    "str": {"none": None, "string": {"type": "string"}},
    "uuid": {"none": None, "token": {"type": "token"}},
    "int": {"none": None, "token": {"type": "token"}},
}


def mkid(kind, i):
    if kind == "oid":
        return ObjectId(i.to_bytes(4, "big") + b"\x01\x02\x03\x04\x05\x06\x07\x08")
    if kind == "str":
        return "doc-%06d-%s" % (i, uuid.UUID(int=i * 7919 + 12345).hex[:8])
    if kind == "uuid":
        return uuid.UUID(int=(i * 2654435761) % (1 << 128) | (4 << 76) | (2 << 62))
    if kind == "int":
        return Int64((1 << 40) + i * 3 + (1 << 55))  # above 2^53: needs int64 mapping
    raise ValueError(kind)


def idstr(x):
    return str(x)  # ObjectId hex, str, UUID with dashes, int decimal


def ctok(i):
    return "0123456789abcdefghij"[len(str(i))] + str(i)


def packed(x, v):
    s = idstr(x)
    n = len(s.encode())
    return "%d:%s:%s" % (n, s, ctok(v))


def packed_range(x, v):
    s = idstr(x)
    n = len(s.encode())
    return {"range": {"path": "_hoodie_t", "gte": "%d:%s:%s" % (n, s, ctok(v)), "lt": "%d:%s;" % (n, s)}}


def sdef(idmap="ok", kind=None, v=True, store=True, e=False, t=False, text=True):
    f = {}
    if idmap is not None and idmap != "ok":
        f["_id"] = idmap
    elif idmap == "ok":
        f["_id"] = IDMAP[kind]
    if text:
        f["title"] = {"type": "string"}
    if v:
        f["_hoodie_v"] = {"type": "number", "representation": "int64"}
    if e:
        f["_hoodie_e"] = {"type": "number", "representation": "int64"}
    if t:
        f["_hoodie_t"] = {"type": "token"}
    d = {"mappings": {"dynamic": False, "fields": f}}
    if store:
        inc = [k for k in ("_hoodie_v", "_hoodie_e", "_hoodie_t", "g") if k in f or k == "g"]
        inc = [k for k in inc if k != "g"] if True else inc
        d["storedSource"] = {"include": inc}
    return d


def mk_index(coll, name, definition, timeout=900):
    t0 = time.time()
    coll.create_search_index({"name": name, "definition": definition})
    while True:
        ix = [i for i in coll.list_search_indexes(name)]
        if ix and ix[0].get("queryable"):
            return time.time() - t0, ix[0]
        if time.time() - t0 > timeout:
            raise RuntimeError("index not queryable " + name)
        time.sleep(0.5)


def q(coll, pipe):
    return list(coll.aggregate(pipe))


def count_idx(coll, name, path="_hoodie_v"):
    r = q(coll, [{"$searchMeta": {"index": name, "range": {"path": path, "gte": -1}, "count": {"type": "total"}}}])
    return r[0]["count"]["total"] if r else 0


def wait_count(coll, name, n, timeout=900, path="_hoodie_v"):
    t0 = time.time()
    while True:
        try:
            c = count_idx(coll, name, path)
        except PyMongoError:
            c = -1
        if c >= n:
            return time.time() - t0
        if time.time() - t0 > timeout:
            raise RuntimeError("index count %s of %s" % (c, n))
        time.sleep(0.25)


# ---- probes. return {id: indexed_v} for ids found (P1), or set of satisfied indexes ----
def p1(coll, name, ids):
    pipe = [{"$search": {"index": name, "in": {"path": "_id", "value": list(ids)}, "returnStoredSource": True}}, {"$limit": len(ids) + 5}]
    return {d["_id"]: d.get("_hoodie_v") for d in coll.aggregate(pipe, maxTimeMS=MAXMS)}


def p2(coll, name, pairs):
    sh = [{"compound": {"must": [{"equals": {"path": "_id", "value": i}}, {"range": {"path": "_hoodie_v", "gte": v}}]}} for i, v in pairs]
    pipe = [{"$search": {"index": name, "compound": {"should": sh, "minimumShouldMatch": 1}, "returnStoredSource": True}}, {"$limit": len(pairs) + 5}]
    return {d["_id"] for d in coll.aggregate(pipe, maxTimeMS=MAXMS)}


def p3(coll, name, pairs):
    sh = [packed_range(i, v) for i, v in pairs]
    pipe = [{"$search": {"index": name, "compound": {"should": sh, "minimumShouldMatch": 1}, "returnStoredSource": True}}, {"$limit": len(pairs) + 5}]
    return {d["_id"] for d in coll.aggregate(pipe, maxTimeMS=MAXMS)}


def mongot_signal(sig):
    pid = subprocess.run(["docker", "exec", CN, "sh", "-c",
                          "for p in /proc/[0-9]*; do if tr '\\0' ' ' < $p/cmdline 2>/dev/null | grep -q '/opt/mongot/bin/jdk/bin/java'; then basename $p; fi; done | head -1"],
                         capture_output=True, text=True).stdout.strip()
    subprocess.run(["docker", "exec", CN, "kill", "-" + sig, pid], check=True)
    return pid
