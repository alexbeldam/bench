"""Shared helpers for the odm-search checks."""
import asyncio, json, os, re, statistics, sys, time
from pymongo import AsyncMongoClient
from pymongo.operations import SearchIndexModel

RES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
os.makedirs(RES, exist_ok=True)
ROWS = []


def url(kind):
    if kind == "hosted":
        u = (os.environ.get("HOODIE_BENCH_ATLAS_URL") or "").strip()
        if not u:
            sys.exit("hosted URL is not set")
        return u
    return os.environ.get("ODM_URL", "mongodb://127.0.0.1:27300/?directConnection=true")


def redact(s):
    return re.sub(r"mongodb(\+srv)?://[^\s'\"]+", "<url>", str(s))


def client(kind):
    return AsyncMongoClient(url(kind), serverSelectionTimeoutMS=30000, uuidRepresentation="standard")


def row(check, case, **kw):
    r = {"check": check, "case": case, **kw}
    ROWS.append(r)
    print(f"  {check:10} {case:52} " + " ".join(f"{k}={redact(v)}" for k, v in kw.items()), flush=True)


def save(label, meta):
    with open(os.path.join(RES, f"{label}.json"), "w") as f:
        json.dump({"meta": meta, "rows": ROWS}, f, indent=1, default=str)


async def observe(check, case, coro, show=lambda r: r, **extra):
    """Run a call whose failure is itself an observation."""
    t = time.perf_counter()
    try:
        r = await coro
        row(check, case, outcome="ok", result=show(r), ms=round((time.perf_counter() - t) * 1000), **extra)
        return r
    except Exception as e:  # noqa: BLE001
        row(check, case, outcome="error", error=f"{type(e).__name__}: {str(e)[:110]}", **extra)


async def agg(coll, pipe):
    return await (await coll.aggregate(pipe)).to_list()


async def make_index(coll, name, mappings, timeout=300):
    """Create a search index and wait until it can be queried."""
    await coll.create_search_index(SearchIndexModel(name=name, definition={"mappings": mappings}))
    end = time.time() + timeout
    while time.time() < end:
        ix = await (await coll.list_search_indexes(name)).to_list()
        if ix and ix[0].get("queryable"):
            return
        await asyncio.sleep(2)
    raise TimeoutError(f"index {name} not queryable after {timeout}s")


async def total(coll, name, clause=None):
    r = await agg(coll, [{"$searchMeta": {"index": name, "count": {"type": "total"}, **({"compound": {"filter": [clause]}} if clause else {"exists": {"path": "_id"}})}}])
    return r[0]["count"]["total"] if r else 0


async def wait_docs(coll, name, n, timeout=600):
    """Wait until the index reports n documents; returns the seconds waited."""
    t = time.time()
    while time.time() - t < timeout:
        try:
            if await total(coll, name) >= n:
                return round(time.time() - t, 2)
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(1)
    raise TimeoutError(f"index {name} did not reach {n} documents")


async def drop_index(coll, name, timeout=120):
    try:
        await coll.drop_search_index(name)
    except Exception:  # noqa: BLE001
        return
    end = time.time() + timeout
    while time.time() < end:
        if not await (await coll.list_search_indexes(name)).to_list():
            return
        await asyncio.sleep(2)


def pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(len(xs) * p))], 1) if xs else None
