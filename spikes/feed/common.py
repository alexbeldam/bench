"""Shared helpers for the change-feed spike."""
import asyncio, json, math, os, struct, subprocess, sys, time
from pathlib import Path

from bson import ObjectId
from pymongo import AsyncMongoClient

REV = "_hoodie_rev"
DB = "scf"
HERE = Path(__file__).resolve().parent
RES = HERE / "results"
RES.mkdir(exist_ok=True)
URI = os.environ.get("SCF_URI", "mongodb://127.0.0.1:27213/?directConnection=true")
REDIS = os.environ.get("SCF_REDIS", "redis://127.0.0.1:27214/0")
PAYLOAD = "x" * 200


def mk_rev(wid: int, n: int) -> ObjectId:
    """An ObjectId-typed revision that carries the creation time in microseconds (measurement only)."""
    return ObjectId(struct.pack(">QB", time.time_ns() // 1000, wid) + (n & 0xFFFFFF).to_bytes(3, "big"))


def rev_us(r: ObjectId) -> int:
    return struct.unpack(">Q", r.binary[:8])[0]


def now_us() -> int:
    return time.time_ns() // 1000


def pct(v, q):
    if not len(v):
        return math.nan
    s = sorted(v)
    k = (len(s) - 1) * q / 100
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def lat(v):
    """Latency summary in ms from a list of microsecond values."""
    return {"n": len(v), "p50": round(pct(v, 50) / 1e3, 2), "p95": round(pct(v, 95) / 1e3, 2),
            "p99": round(pct(v, 99) / 1e3, 2), "max": round(max(v) / 1e3, 2) if v else None}


def client(uri: str = URI, **kw) -> AsyncMongoClient:
    return AsyncMongoClient(uri, **kw)


def pipe(colls=None, ops=("insert", "update", "replace", "delete")):
    """Change stream pipeline: optional $match on ns.coll, then a projection of key, type, ns, revision."""
    m = {"operationType": {"$in": list(ops)}}
    if colls:
        m["ns.coll"] = {"$in": list(colls)}
    return [{"$match": m},
            {"$project": {"documentKey": 1, "operationType": 1, "ns.coll": 1, f"fullDocument.{REV}": 1,
                          f"updateDescription.updatedFields.{REV}": 1}}]


def ev_rev(ev):
    """(coll, doc id, rev or None) of a projected event; rev None for a delete or a write without a revision."""
    op = ev["operationType"]
    rev = None
    if op == "update":
        rev = ev.get("updateDescription", {}).get("updatedFields", {}).get(REV)
    elif op != "delete":
        rev = (ev.get("fullDocument") or {}).get(REV)
    return ev["ns"]["coll"], ev["documentKey"]["_id"], rev


async def seed(c, colls, n):
    db = c[DB]
    for name in colls:
        await db[name].drop()
        await db[name].insert_many([{"_id": i, REV: mk_rev(255, i), "p": PAYLOAD} for i in range(n)])


class Guard:
    """Detects a laptop sleep: the loop ticks every 100 ms and records the largest gap in wall-clock time."""

    def __init__(self):
        self.max_gap = 0.0
        self.t0 = time.time()
        self.p0 = time.perf_counter()
        self._task = None

    async def _run(self):
        last = time.time()
        while True:
            await asyncio.sleep(0.1)
            n = time.time()
            self.max_gap = max(self.max_gap, n - last)
            last = n

    def start(self):
        self._task = asyncio.create_task(self._run())

    def stop(self):
        if self._task:
            self._task.cancel()
        drift = (time.time() - self.t0) - (time.perf_counter() - self.p0)
        return {"max_gap_s": round(self.max_gap, 2), "clock_drift_s": round(drift, 2),
                "slept": self.max_gap > 2.0 or abs(drift) > 1.0}


def spawn(script, *args, **kw):
    return subprocess.Popen([sys.executable, "-u", str(HERE / script), *map(str, args)], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, text=True, cwd=str(HERE), **kw)


def wait_line(p, prefix, timeout=60):
    t = time.time()
    while time.time() - t < timeout:
        line = p.stdout.readline()
        if not line:
            raise RuntimeError(f"{prefix}: process ended (rc={p.poll()})")
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    raise TimeoutError(prefix)


def go(p, until):
    p.stdin.write(f"go {until}\n")
    p.stdin.flush()


def save(name, obj):
    (RES / f"{name}.json").write_text(json.dumps(obj, indent=1, default=str))


def dstats():
    """CPU % and memory of the mongo container (one sample)."""
    r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}", "hoodie-s3-mongo"],
                       capture_output=True, text=True)
    cpu, mem = r.stdout.strip().split("|")
    return float(cpu.strip("%")), mem.split("/")[0].strip()
