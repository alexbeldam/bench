"""Q3: delete then recreate under rules A (epoch), B (tombstone), C (nothing), plus an optional DB-confirm layer.

Waits observed per rep:
  W = a waiting earlier write (the v=7 write), D = a waiting delete, R = a waiting write on the re-created document.
"""
import datetime, random, sys, threading
from common import *

T = 8.0       # wait timeout (s)
TICK = 0.02
GRACE = 1.0   # confirm layer starts after this


def sdef3():
    f = {"_id": {"type": "objectId"}, "title": {"type": "string"},
         "_hoodie_v": {"type": "number", "representation": "int64"},
         "_hoodie_e": {"type": "number", "representation": "int64"},
         "g": {"type": "number", "representation": "int64"}}
    return {"mappings": {"dynamic": False, "fields": f}, "storedSource": {"include": ["_hoodie_v", "_hoodie_e", "g"]}}


class Lib:
    """rule: C, A, A2 (server clock via $$NOW), B. off = client clock offset in ms of the creating call."""
    def __init__(self, co, tomb, rule):
        self.co, self.tomb, self.rule = co, tomb, rule
        self.reads = self.writes = 0
        self.lock = threading.Lock()

    def _n(self, r=0, w=0):
        with self.lock:
            self.reads += r; self.writes += w

    def _row(self, d):
        return None if d is None else (d["g"], d.get("_hoodie_e"), int(d["_hoodie_v"]))

    def create(self, i, g, off=0):
        pr = {"g": 1, "_hoodie_e": 1, "_hoodie_v": 1}
        if self.rule == "A2":
            upd = [{"$set": {"_hoodie_e": {"$ifNull": ["$_hoodie_e", {"$toLong": "$$NOW"}]},
                             "_hoodie_v": {"$add": [{"$ifNull": ["$_hoodie_v", 0]}, 1]},
                             "g": {"$ifNull": ["$g", g]}, "title": "x"}}]
        else:
            inc = 1
            ins = {"g": g}
            if self.rule == "A":
                ins["_hoodie_e"] = int(time.time() * 1000) + off
            if self.rule == "B":
                t = self.tomb.find_one({"_id": i}); self._n(r=1)
                if t: inc = t["last_v"] + 1
            upd = {"$inc": {"_hoodie_v": inc}, "$setOnInsert": ins, "$set": {"title": "x"}}
        return self._row(self.co.find_one_and_update({"_id": i}, upd, upsert=True, projection=pr, return_document=True))

    def write(self, i):
        pr = {"g": 1, "_hoodie_e": 1, "_hoodie_v": 1}
        if self.rule == "A2":
            upd = [{"$set": {"_hoodie_v": {"$add": ["$_hoodie_v", 1]}}}]
        else:
            upd = {"$inc": {"_hoodie_v": 1}}
        return self._row(self.co.find_one_and_update({"_id": i}, upd, projection=pr, return_document=True))

    def delete(self, i):
        d = self.co.find_one_and_delete({"_id": i}, projection={"g": 1, "_hoodie_e": 1, "_hoodie_v": 1})
        if self.rule == "B" and d is not None:
            self.tomb.replace_one({"_id": i}, {"_id": i, "last_v": int(d["_hoodie_v"]),
                                               "expireAt": datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=TTL_S)}, upsert=True)
            self._n(w=1)
        return self._row(d)


TTL_S = 60


def idx_state(co, i):
    r = list(co.aggregate([{"$search": {"index": "ix", "in": {"path": "_id", "value": [i]}, "returnStoredSource": True}}], maxTimeMS=1500))
    if not r: return None
    d = r[0]
    return (d["g"], d.get("_hoodie_e"), int(d["_hoodie_v"]))


def ge_write(rule, s, m):
    if s is None: return False
    if rule in ("A", "A2"): return ((s[1] or 0), s[2]) >= ((m[1] or 0), m[2])
    return s[2] >= m[2]


def gt_delete(rule, s, d):
    if s is None: return True
    if rule in ("A", "A2"): return ((s[1] or 0), s[2]) > ((d[1] or 0), d[2])
    if rule == "B": return s[2] > d[2]
    return False       # rule C: only absence proves the delete


def truth_write(s, m):  # harness truth: (incarnation, version)
    return s is not None and (s[0], s[2]) >= (m[0], m[2])


def truth_delete(s, d):
    return s is None or s[0] > d[0]


def waiter(co, i, kind, mine, rule, confirm, out, t0):
    """kind 'W' (write) or 'D' (delete). Records outcome."""
    tl = []
    start = time.time()
    res = {"kind": kind, "outcome": "timeout", "t": None, "by": None, "fp_now": False, "back": False}
    last = "init"
    while time.time() - start < T:
        try:
            s = idx_state(co, i)
        except PyMongoError:
            time.sleep(TICK); continue
        if s != last:
            tl.append((round(time.time() - t0, 2), s)); last = s
        ok = ge_write(rule, s, mine) if kind != "D" else gt_delete(rule, s, mine)
        by = "rule"
        if not ok and confirm and time.time() - start > GRACE:
            db = co.find_one({"_id": i})
            dbs = None if db is None else (db["g"], db.get("_hoodie_e"), int(db["_hoodie_v"]))
            if dbs == s:
                ok, by = True, "confirm"
        if ok:
            res["outcome"], res["t"], res["by"] = "done", round(time.time() - start, 2), by
            res["fp_now"] = not (truth_write(s, mine) if kind != "D" else truth_delete(s, mine))
            if by == "confirm": res["fp_now"] = False
            # post observation: the index must never go backwards after the wait returned
            end = time.time() + 2.0
            while time.time() < end:
                try:
                    s2 = idx_state(co, i)
                except PyMongoError:
                    continue
                if s2 != last:
                    tl.append((round(time.time() - t0, 2), s2)); last = s2
                if kind == "D":
                    if s2 is not None and s2[0] <= mine[0]: res["back"] = True
                else:
                    if s2 is not None and (s2[0], s2[2]) < (mine[0], mine[2]): res["back"] = True
                time.sleep(TICK)
            break
        time.sleep(TICK)
    res["tl"] = tl
    out[kind + str(id(mine))] = res


SCEN = {
    "same_young": dict(same=True, age="young", stall=False, gap=0.0),
    "same_old": dict(same=True, age="old", stall=False, gap=0.0),
    "same_old_stall": dict(same=True, age="old", stall=True, gap=0.0),
    "new_old": dict(same=False, age="old", stall=False, gap=0.0),
    "new_old_stall": dict(same=False, age="old", stall=True, gap=0.0),
    "same_slow_control": dict(same=True, age="old", stall=False, gap=2.5),
}


def rep(co, tomb, rule, skew, confirm, sc):
    lib = Lib(co, tomb, "A" if (rule.startswith("A") and rule != "A2") else rule)
    S = SCEN[sc]
    X, Y = ObjectId(), ObjectId()
    off = -skew
    lib.create(X, 1)
    for _ in range(5): lib.write(X)
    if S["age"] == "old":
        t = time.time()
        while time.time() - t < 20:
            s = idx_state(co, X)
            if s and s[2] >= 6: break
            time.sleep(0.05)
        time.sleep(0.3)
    t0 = time.time()
    outs = {"W": {}, "D": {}, "R": {}}
    th = []
    if S["stall"]: mongot_signal("STOP")
    try:
        mine = lib.write(X)
        th.append(threading.Thread(target=waiter, args=(co, X, "W", mine, lib.rule, confirm, outs["W"], t0)))
        dele = lib.delete(X)
        th.append(threading.Thread(target=waiter, args=(co, X, "D", dele, lib.rule, confirm, outs["D"], t0)))
        if S["gap"]: time.sleep(S["gap"])
        tgt = X if S["same"] else Y
        rec = lib.create(tgt, 2, off=off)
        rw = lib.write(tgt)
        th.append(threading.Thread(target=waiter, args=(co, tgt, "R", rw, lib.rule, confirm, outs["R"], t0)))
        for t in th: t.start()
        if S["stall"]: time.sleep(1.5)
    finally:
        if S["stall"]: mongot_signal("CONT")
    for t in th: t.join()
    time.sleep(1.0)
    try: fin = idx_state(co, tgt)
    except PyMongoError: fin = "err"
    def pick(d):
        v = list(d.values())[0]
        return {k: v[k] for k in ("outcome", "t", "by", "fp_now", "back")} | {"states": [s for t_, s in v["tl"]][:6]}
    return {"rule": rule, "skew": skew, "confirm": confirm, "scen": sc, "mine": mine, "dele": dele, "recreated": rec, "rw": rw,
            "final_idx": fin, "reads": lib.reads, "writes": lib.writes,
            "W": pick(outs["W"]), "D": pick(outs["D"]), "R": pick(outs["R"])}


RULES = [("C", 0, False), ("A", 0, False), ("A", 50, False), ("A", 500, False), ("A2", 0, False), ("B", 0, False),
         ("C", 0, True), ("A", 500, True), ("B", 0, True)]

if __name__ == "__main__":
    reps = int(sys.argv[1]); scens = sys.argv[2].split(","); tag = sys.argv[3]
    only = sys.argv[4].split(",") if len(sys.argv) > 4 else None
    c = cli(); db = c["q3r_" + tag]
    co, tomb = db.d, db.tomb
    co.insert_one({"_id": "boot", "g": 0, "_hoodie_v": Int64(0)})
    mk_index(co, "ix", sdef3()); wait_count(co, "ix", 1)
    allr = []
    for sc in scens:
        for rule, skew, conf in RULES:
            name = "%s%s%s" % (rule, skew if rule == "A" else "", "+D" if conf else "")
            if only and name not in only: continue
            rows = []
            for r in range(reps):
                g = Guard()
                x = rep(co, tomb, rule, skew, conf, sc)
                if g.done():
                    x = rep(co, tomb, rule, skew, conf, sc); x["repeat_after_sleep"] = True
                rows.append(x)
            def summ(k):
                from collections import Counter
                return dict(Counter(("FP" if r[k]["fp_now"] else "") + ("BACK" if r[k]["back"] else "") + r[k]["outcome"] + ("(" + str(r[k]["by"]) + ")" if r[k]["by"] == "confirm" else "") for r in rows))
            print(sc, name, "W", summ("W"), "D", summ("D"), "R", summ("R"), flush=True)
            allr += rows
            save("q3_%s.json" % tag, allr)
