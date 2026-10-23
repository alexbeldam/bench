"""Q6: memory of the per-document event log. Part 1: bytes per entry (tracemalloc). Part 2: a live consumer at a given rate.
usage: exp6.py static | exp6.py live IMPL RATE NDOCS   (IMPL: sketch = the probe.py ChangeFeed log, fixed = same plus a time-ordered sweep)"""
import asyncio, collections, json, sys, time, tracemalloc, gc
import psutil
from common import *


class Sketch:
    """The log of probe.ChangeFeed: trims a document's old entries only when a newer entry for the same document arrives."""

    def __init__(self, ttl):
        self.ttl, self.log, self.pos, self.latest, self._seq = ttl, {}, {}, {}, 0

    def add(self, doc_id, rev):
        now = time.perf_counter()
        self._seq += 1
        e = self.log.setdefault(doc_id, [])
        e.append((self._seq, rev, now))
        self.latest[doc_id] = self._seq
        self.pos[rev] = (doc_id, self._seq)
        while e and now - e[0][2] > self.ttl:
            _, old, _ = e.pop(0)
            self.pos.pop(old, None)

    def n(self):
        return sum(len(v) for v in self.log.values())


class Fixed(Sketch):
    """Same structure, plus a global arrival-ordered queue so that every entry expires after ttl, and empty documents are removed."""

    def __init__(self, ttl):
        super().__init__(ttl)
        self.q = collections.deque()

    def add(self, doc_id, rev):
        super().add(doc_id, rev)
        now = time.perf_counter()
        self.q.append((now, doc_id, rev))
        while self.q and now - self.q[0][0] > self.ttl:
            _, d, r = self.q.popleft()
            e = self.log.get(d)
            if e and e[0][1] == r:
                e.pop(0)
                self.pos.pop(r, None)
                if not e:
                    del self.log[d]
                    self.latest.pop(d, None)


def static():
    out = {}
    for name, idf in (("int_id", lambda i: i), ("objectid_id", lambda i: ObjectId(b"\x00\x0c\x00\x03" + i.to_bytes(8, "big")))):
        for impl in (Sketch, Fixed):
            gc.collect()
            tracemalloc.start()
            s0 = tracemalloc.get_traced_memory()[0]
            f = impl(ttl=1e9)
            n = 100_000
            for i in range(n):
                f.add(idf(i), mk_rev(1, i))
            used = tracemalloc.get_traced_memory()[0] - s0
            tracemalloc.stop()
            out[f"{impl.__name__}_{name}_1_entry_per_doc"] = round(used / n, 1)
            gc.collect()
            tracemalloc.start()
            s0 = tracemalloc.get_traced_memory()[0]
            f = impl(ttl=1e9)
            ids = [idf(i) for i in range(1000)]
            for i in range(n):
                f.add(ids[i % 1000], mk_rev(1, i))
            used = tracemalloc.get_traced_memory()[0] - s0
            tracemalloc.stop()
            out[f"{impl.__name__}_{name}_100_entries_per_doc"] = round(used / n, 1)
    print(json.dumps(out)); save("exp6_static", out)


async def live(impl, rate, ndocs, dur=45, ttl=10.0):
    c = client(); db = c[DB]
    await seed(c, ["e0"], ndocs)
    f = (Sketch if impl == "sketch" else Fixed)(ttl)
    s = await db.watch(pipe(["e0"]), max_await_time_ms=100)
    await s.try_next()
    gc.collect()
    rss0 = psutil.Process().memory_info().rss
    tracemalloc.start()
    ws = [spawn("writer.py", "--colls", "e0", "--rate", rate / 3, "--dur", dur, "--ndocs", ndocs, "--wid", i) for i in (1, 2, 3)]
    t0 = time.time(); n = 0; samples = []
    g = Guard(); g.start()
    while time.time() - t0 < dur + 1:
        ev = await s.try_next()
        if ev is not None:
            _, d, r = ev_rev(ev)
            if r is not None:
                f.add(d, r); n += 1
        if time.time() - t0 > len(samples) * 5:
            samples.append({"t": round(time.time() - t0), "entries": f.n() if impl == "fixed" or True else 0, "traced_mb": round(tracemalloc.get_traced_memory()[0] / 1e6, 2),
                            "rss_delta_mb": round((psutil.Process().memory_info().rss - rss0) / 1e6, 1)})
    cur = tracemalloc.get_traced_memory()[0]
    out = {"impl": impl, "rate": rate, "ndocs": ndocs, "ttl_s": ttl, "events": n, "entries_at_end": f.n(), "traced_mb_at_end": round(cur / 1e6, 2),
           "bytes_per_retained_entry": round(cur / max(1, f.n()), 1), "samples": samples, "guard": g.stop()}
    for w in ws:
        w.wait()
    print(json.dumps(out)); save(f"exp6_{impl}_{rate}_{ndocs}", out)


if __name__ == "__main__":
    if sys.argv[1] == "static":
        static()
    else:
        asyncio.run(live(sys.argv[2], float(sys.argv[3]), int(sys.argv[4])))
