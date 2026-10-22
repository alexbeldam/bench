"""Planner path check on the smoke data set: every branch of the planner is taken at least once, with asserts."""

from __future__ import annotations

from pymongo import AsyncMongoClient

from .common import DB, LIMIT, coll_name, make_filter, uri
from .planner import Opts, Planner


async def run(scale: str = "smoke", n: int = 5000) -> None:
    client = AsyncMongoClient(uri())
    p = Planner(client[DB][coll_name(scale)], scale)
    cap, thr = 300, 0.2  # small numbers so every branch is reachable at 5,000 documents
    checks = [
        ("zero", {"tenant": "none"}, "frequent", "zero", 0, Opts()),
        ("A", make_filter("even", n, 200).flt, "frequent", "A", LIMIT, Opts()),
        ("A_lowsel", make_filter("even", n, 600).flt, "frequent", "A_lowsel", LIMIT, Opts()),
        ("B", make_filter("even", n, 2000).flt, "frequent", "B", LIMIT, Opts()),
        ("A_fallback", make_filter("even", n, 2000).flt, "rare", "A_fallback", None, Opts()),
        ("B+count+facet", make_filter("even", n, 2000).flt, "frequent", "B", LIMIT, Opts(count=True, facet="low")),
    ]
    for name, flt, term, want, nres, opts in checks:
        r = await p.plan(flt, term, cap=cap, threshold=thr, opts=opts)
        assert r.info.get("strategy") == want, (name, r.info)
        if nres is not None:
            assert len(r.hits) == nres, (name, len(r.hits))
        print(f"ok {name}: strategy {r.info['strategy']}, {len(r.hits)} hits")
    # exact total: chunked A sums to the true number of matching documents; search-first does not
    ids = await p.prefilter(make_filter("even", n, 1500).flt, 2000)
    a = await p.chunked(ids, "frequent", Opts(count=True), chunk=500, inflight=2)
    truth = await p.coll.count_documents({"rank": {"$lt": 1500}, "$or": [{"title": {"$regex": "(^| )w0000( |$)"}}, {"body": {"$regex": "(^| )w0000( |$)"}}]})
    assert a.total == truth, (a.total, truth)
    print(f"ok exact total of chunked A: {a.total}")
    await client.close()
