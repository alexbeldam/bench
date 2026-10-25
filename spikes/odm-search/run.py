"""Search clauses and a Beanie document layer against the search engine (local container) or a hosted cluster.

usage: python run.py <engine|hosted> --label L [--docs N] [--only c1,c2,...] [--reps R]
Every check prints rows (check, case, observed values) and writes results/<label>.json. A harness error fails the run;
an engine error inside a case is an observation."""
import argparse, asyncio, datetime as dt, os, platform, secrets, time

from beanie import Document, Link, PydanticObjectId, WriteRules, init_beanie
from beanie.odm.utils.parsing import parse_obj
from bson import ObjectId
from pydantic import BaseModel

from common import agg, client, drop_index, make_index, observe, pct, redact, row, save, total, wait_docs

BASE = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
HOSTED = False


def nap():
    return asyncio.sleep(0.05 if HOSTED else 0)


# ------------------------------------------------------------------ c1: relevance score next to the parsed document
class Sc(Document):
    title: str
    n: int = 0

    class Settings:
        name = "odm_sc"


class ScP(BaseModel):
    title: str
    rel: float = 0.0


async def c1(db, a):
    coll = Sc.get_pymongo_collection()
    await Sc.insert_many([Sc(title=("alpha beta " if i % 2 else "alpha gamma ") + str(i), n=i) for i in range(30)])
    await make_index(coll, "sc_idx", {"dynamic": False, "fields": {"_id": {"type": "objectId"}, "title": {"type": "string"}, "n": {"type": "number"}}})
    await wait_docs(coll, "sc_idx", 30)
    pipe = [{"$search": {"index": "sc_idx", "compound": {"must": [{"text": {"path": "title", "query": "alpha beta"}}]}}}]
    raw = await agg(coll, pipe + [{"$addFields": {"_rel": {"$meta": "searchScore"}}}, {"$limit": 10}])
    pairs = [(parse_obj(Sc, {k: v for k, v in d.items() if k != "_rel"}), d["_rel"]) for d in raw]
    row("c1", "stitch: parsed documents with score", n=len(pairs), types=sorted({type(i).__name__ for i, _ in pairs}), scores_positive=all(s > 0 for _, s in pairs), non_increasing=all(pairs[k][1] >= pairs[k + 1][1] for k in range(len(pairs) - 1)))
    ids = {i.id for i, _ in pairs}
    got = await Sc.find({"_id": {"$in": list(ids)}}).to_list()
    row("c1", "same ids through a plain find", equal=ids == {g.id for g in got})
    hits = await Sc.aggregate(pipe + [{"$addFields": {"rel": {"$meta": "searchScore"}}}], projection_model=ScP).to_list()
    row("c1", "score as projection-model field (reference)", n=len(hits), types=sorted({type(h).__name__ for h in hits}))
    fo = await agg(coll, [{"$search": {"index": "sc_idx", "compound": {"filter": [{"range": {"path": "n", "gte": 0}}]}}}, {"$addFields": {"_rel": {"$meta": "searchScore"}}}, {"$limit": 3}])
    row("c1", "score under a filter-only query", scores=[d["_rel"] for d in fo])
    n_all = await total(coll, "sc_idx", {"text": {"path": "title", "query": "alpha beta"}})
    row("c1", "total with a cap of 10 (cap applied by the caller)", total=n_all, capped=min(n_all, 10))
    await drop_index(coll, "sc_idx")


# ------------------------------------------------------------------ shared small collection for c2/c3
async def small(db):
    c = db["odm_m"]
    ref = ObjectId()
    await c.insert_many([{"i": i, "kind": "abc"[i % 3], "words": f"alpha {i % 4}", "code": f"C-{i:02d}", "qty": i % 10, "flag": i % 2 == 0,
                          "created": BASE + dt.timedelta(days=i), "ref": ref} for i in range(40)])
    await make_index(c, "m_idx", {"dynamic": False, "fields": {
        "_id": {"type": "objectId"}, "i": {"type": "number"}, "kind": [{"type": "token"}, {"type": "stringFacet"}],
        "words": {"type": "string"}, "code": {"type": "string", "analyzer": "lucene.keyword"},
        "qty": [{"type": "number"}, {"type": "numberFacet"}], "flag": {"type": "boolean"}, "created": [{"type": "date"}, {"type": "dateFacet"}],
        "ref": {"type": "objectId"}}})
    await wait_docs(c, "m_idx", 40)
    return c, ref


MAP = {"kind": {"token"}, "words": {"string"}, "code": {"string_keyword"}, "qty": {"number"}, "flag": {"boolean"}, "created": {"date"}, "ref": {"objectId"}, "i": {"number"}}
VALUE_KIND = {str: {"token", "string", "string_keyword"}, bool: {"boolean"}, int: {"number"}, dt.datetime: {"date"}, ObjectId: {"objectId"}}


def predict(op):
    """The rule table under test: what an index definition lets each clause do ('ok' hits, 'empty', 'error')."""
    (kind, spec), = op.items()
    types = MAP.get(spec["path"])
    if not types:
        return "empty"
    if kind in ("equals", "in"):
        if types == {"string"}:
            return "error"
        vs = spec["value"] if isinstance(spec["value"], list) else [spec["value"]]
        return "ok" if all(VALUE_KIND.get(type(v), set()) & types for v in vs) else "empty"
    if kind == "range":
        return "ok" if types & {"number", "date"} else None
    if kind == "regex":
        return "ok" if "string_keyword" in types else ("empty" if "token" in types else None)
    return None


async def c3(db, a, c, ref):
    cases = [
        ("equals token kind='a'", {"equals": {"path": "kind", "value": "a"}}),
        ("equals token kind=3", {"equals": {"path": "kind", "value": 3}}),
        ("equals analyzed words", {"equals": {"path": "words", "value": "alpha 1"}}),
        ("in analyzed words", {"in": {"path": "words", "value": ["alpha 1"]}}),
        ("in token kind", {"in": {"path": "kind", "value": ["a", "b"]}}),
        ("equals number qty=3", {"equals": {"path": "qty", "value": 3}}),
        ("equals number qty='x'", {"equals": {"path": "qty", "value": "x"}}),
        ("equals boolean", {"equals": {"path": "flag", "value": True}}),
        ("equals date", {"equals": {"path": "created", "value": BASE + dt.timedelta(days=3)}}),
        ("equals objectId", {"equals": {"path": "ref", "value": ref}}),
        ("range number", {"range": {"path": "qty", "gt": 7}}),
        ("range date", {"range": {"path": "created", "gt": BASE + dt.timedelta(days=30)}}),
        ("range token", {"range": {"path": "kind", "gt": "a"}}),
        ("regex token", {"regex": {"path": "kind", "query": "a.*", "allowAnalyzedField": True}}),
        ("regex keyword string", {"regex": {"path": "code", "query": "C-0.*", "allowAnalyzedField": True}}),
        ("regex analyzed string", {"regex": {"path": "words", "query": "alp.*", "allowAnalyzedField": True}}),
        ("exists unmapped", {"exists": {"path": "nope"}}),
        ("equals unmapped", {"equals": {"path": "nope", "value": 1}}),
        ("text on token", {"text": {"path": "kind", "query": "a"}}),
        ("text on number", {"text": {"path": "qty", "query": "3"}}),
        ("phrase on number", {"phrase": {"path": "qty", "query": "3"}}),
        ("autocomplete on a string without autocomplete", {"autocomplete": {"path": "words", "query": "alp"}}),
    ]
    agree = disagree = 0
    for label, op in cases:
        try:
            n = len(await agg(c, [{"$search": {"index": "m_idx", "compound": {"filter": [op]}}}, {"$project": {"_id": 1}}]))
            seen = "ok" if n else "empty"
        except Exception as e:  # noqa: BLE001
            n, seen = None, "error"
        want = predict(op)
        if want is not None:
            agree, disagree = (agree + 1, disagree) if want == seen else (agree, disagree + 1)
        row("c3", label, observed=seen, hits=n, predicted=want, agrees=(want == seen) if want else "n/a")
        await nap()
    row("c3", "rule table against the engine", agree=agree, disagree=disagree)


# ------------------------------------------------------------------ c2: scoring clauses, negation, buckets
async def c2(db, a, c):
    t = {"text": {"path": "words", "query": "alpha 1"}}
    e = {"equals": {"path": "kind", "value": "a"}}
    ex = {"exists": {"path": "_id"}}
    cases = [
        ("must[text]", {"must": [t]}), ("filter[text]", {"filter": [t]}), ("should[text]", {"should": [t]}),
        ("mustNot[text] alone", {"mustNot": [t]}), ("mustNot[equals] alone", {"mustNot": [e]}),
        ("filter[exists] + mustNot[text]", {"filter": [ex], "mustNot": [t]}),
        ("must[text] + mustNot[equals]", {"must": [t], "mustNot": [e]}),
        ("filter[equals] + should[text]", {"filter": [e], "should": [t]}),
        ("should[text, equals] min 2", {"should": [t, e], "minimumShouldMatch": 2}),
        ("should[text, equals] min 1", {"should": [t, e], "minimumShouldMatch": 1}),
        ("empty compound", {}),
    ]
    for label, comp in cases:
        try:
            r = await agg(c, [{"$search": {"index": "m_idx", "compound": comp}}, {"$set": {"_s": {"$meta": "searchScore"}}}, {"$project": {"_s": 1}}])
            row("c2", label, n=len(r), top_score=round(max((d["_s"] for d in r), default=0), 3), all_zero=all(d["_s"] == 0 for d in r) if r else "n/a")
        except Exception as ex_:  # noqa: BLE001
            row("c2", label, outcome="error", error=f"{type(ex_).__name__}: {str(ex_)[:100]}")
        await nap()


# ------------------------------------------------------------------ c4: index named 'default'
async def c4(db, a):
    d1, d2 = db["odm_d1"], db["odm_d2"]
    for c in (d1, d2):
        await c.insert_many([{"t": f"alpha {i}"} for i in range(5)])
    m = {"dynamic": False, "fields": {"_id": {"type": "objectId"}, "t": {"type": "string"}}}
    await make_index(d1, "default", m)
    await make_index(d2, "other", m)
    await wait_docs(d1, "default", 5)
    await wait_docs(d2, "other", 5)
    q = {"text": {"path": "t", "query": "alpha"}}
    for label, c, pipe in (("'default' index, no index key", d1, [{"$search": q}]), ("'default' index, explicit name", d1, [{"$search": {"index": "default", **q}}]),
                           ("only 'other' index, no index key", d2, [{"$search": q}]), ("only 'other' index, explicit name", d2, [{"$search": {"index": "other", **q}}]),
                           ("missing index name", d2, [{"$search": {"index": "nope", **q}}])):
        await observe("c4", label, agg(c, pipe), show=len)
    await drop_index(d1, "default")
    await drop_index(d2, "other")


# ------------------------------------------------------------------ c5: highlights and autocomplete
class Hl(BaseModel):
    title: str
    hl: list[dict]


async def c5(db, a):
    c = db["odm_h"]
    await c.insert_many([{"title": t, "body": b} for t, b in [("Alpha beta gamma", "the quick brown fox jumps"), ("Alphabet soup", "a lazy dog sleeps"), ("Beta test", "quick results"), ("Gamma ray", "brown fox again")]])
    await make_index(c, "h_idx", {"dynamic": False, "fields": {"_id": {"type": "objectId"}, "title": [{"type": "string"}, {"type": "autocomplete", "tokenization": "edgeGram", "minGrams": 2, "maxGrams": 8}], "body": {"type": "string"}}})
    await wait_docs(c, "h_idx", 4)
    pipe = [{"$search": {"index": "h_idx", "highlight": {"path": ["body", "title"]}, "compound": {"must": [{"text": {"path": ["body", "title"], "query": "quick fox"}}]}}},
            {"$addFields": {"hl": {"$meta": "searchHighlights"}}}]
    r = await agg(c, pipe)
    row("c5", "highlight, two paths", n=len(r), shape=[(h["path"], [(t["type"], t["value"]) for t in h["texts"]][:3]) for h in r[0]["hl"]] if r else None)
    class H(Document):
        title: str
        hl: list[dict] = []
        class Settings:
            name = "odm_h"
    await init_beanie(database=db, document_models=[H])
    hs = await H.aggregate(pipe, projection_model=Hl).to_list()
    row("c5", "highlights parsed as a projection-model field", n=len(hs), type=type(hs[0]).__name__ if hs else None, first=hs[0].hl[0]["path"] if hs and hs[0].hl else None)
    for label, op in (("autocomplete 'alph'", {"autocomplete": {"path": "title", "query": "alph"}}), ("autocomplete 'alph' fuzzy", {"autocomplete": {"path": "title", "query": "alph", "fuzzy": {"maxEdits": 1}}}),
                      ("autocomplete on an unmapped path", {"autocomplete": {"path": "nope", "query": "alph"}})):
        await observe("c5", label, agg(c, [{"$search": {"index": "h_idx", "compound": {"must": [op]}}}, {"$project": {"title": 1, "_id": 0}}]), show=lambda r: [d["title"] for d in r])
    await drop_index(c, "h_idx")


# ------------------------------------------------------------------ c6: saving an owner whose link field holds a document
class Tg(Document):
    name: str = ""

    class Settings:
        name = "odm_tg"


class Ow(Document):
    label: str = ""
    tg: Link[Tg] | None = None

    class Settings:
        name = "odm_ow"
        use_state_management = True


async def c6(db, a):
    t = await Tg(name="a").insert()
    o = await Ow(label="o", tg=t).insert()
    raw = lambda: Ow.get_pymongo_collection().find_one({"_id": o.id})
    traw = lambda: Tg.get_pymongo_collection().find_one({"_id": t.id})
    f = await Ow.get(o.id, fetch_links=True)
    f.tg.name = "fetched-edit"; f.label = "L1"
    await observe("c6", "fetch_links doc, save()", f.save(), show=lambda _: None)
    row("c6", "  target name / owner link type", target=(await traw())["name"], link=type((await raw())["tg"]).__name__)
    f.tg.name = "fetched-edit2"
    await observe("c6", "fetch_links doc, save(link_rule=WRITE)", f.save(link_rule=WriteRules.WRITE), show=lambda _: None)
    row("c6", "  target name / owner link type", target=(await traw())["name"], link=type((await raw())["tg"]).__name__)
    g = await Ow.get(o.id)
    g.tg = Tg(id=t.id, name="stitched-edit"); g.label = "L2"
    await observe("c6", "stitched doc in link field, save()", g.save(), show=lambda _: None)
    row("c6", "  target name / owner link type", target=(await traw())["name"], link=type((await raw())["tg"]).__name__)
    await observe("c6", "stitched doc in link field, save(link_rule=WRITE)", g.save(link_rule=WriteRules.WRITE), show=lambda _: None)
    row("c6", "  target name / owner link type", target=(await traw())["name"], link=type((await raw())["tg"]).__name__)
    h = await Ow.get(o.id)
    h.tg = Tg(id=t.id, name="state-edit"); h.label = "L3"
    await observe("c6", "stitched doc, save_changes()", h.save_changes(), show=lambda _: None)
    row("c6", "  target name / owner label", target=(await traw())["name"], label=(await raw())["label"])


# ------------------------------------------------------------------ c7: an explicit session outside a transaction
class W(Document):
    n: int = 0

    class Settings:
        name = "odm_w"


def guard(session):
    if session is not None and not session.in_transaction:
        raise RuntimeError("session is not in a transaction")


async def put(n, session=None, checked=True):
    if checked:
        guard(session)
    await W(n=n).insert(session=session)


async def c7(db, a, cl):
    cnt = lambda: W.get_pymongo_collection().count_documents({})
    async with cl.start_session() as s:
        await observe("c7", "unchecked write with an idle session", put(1, s, checked=False), show=lambda _: None)
        row("c7", "  documents stored", n=await cnt())
        await W.get_pymongo_collection().delete_many({})
        await observe("c7", "checked write with an idle session", put(2, s), show=lambda _: None)
        row("c7", "  documents stored", n=await cnt())
        async with await s.start_transaction():
            await observe("c7", "checked write inside a transaction", put(3, s), show=lambda _: None)
            await s.abort_transaction()
        row("c7", "  documents stored after abort", n=await cnt())


# ------------------------------------------------------------------ c8: scale
class Big(Document):
    title: str
    n: int = 0

    class Settings:
        name = "odm_big"


WORDS = ["alpha", "beta", "gamma", "delta", "omega", "sigma", "kappa", "theta", "lambda", "zeta"]


async def c8(db, a):
    n = a.docs
    coll = Big.get_pymongo_collection()
    t = time.perf_counter()
    for s in range(0, n, 1000):
        await coll.insert_many([{"title": f"{WORDS[i % 10]} {WORDS[(i // 10) % 10]} {WORDS[(i // 100) % 10]} item{i}", "n": i} for i in range(s, min(n, s + 1000))])
        await nap()
    row("c8", "insert", docs=n, seconds=round(time.perf_counter() - t, 1))
    t = time.perf_counter()
    await make_index(coll, "big_idx", {"dynamic": False, "fields": {"_id": {"type": "objectId"}, "title": {"type": "string"}, "n": {"type": "number"}}}, timeout=900)
    ready = round(time.perf_counter() - t, 1)
    waited = await wait_docs(coll, "big_idx", n, timeout=1800)
    row("c8", "index queryable / all documents searchable (s)", queryable=ready, searchable=waited)
    q = {"text": {"path": "title", "query": "alpha beta"}}
    base = [{"$search": {"index": "big_idx", "compound": {"must": [q]}}}, {"$set": {"_s": {"$meta": "searchScore"}}}, {"$sort": {"_s": -1, "_id": 1}}]
    tot = await total(coll, "big_idx", q)
    row("c8", "total for the query", total=tot)
    size = 50
    last = max(1, min(tot, 5000) // size)
    for pg in sorted({1, 2, max(1, last // 2), last}):
        ms = []
        for _ in range(a.reps):
            t0 = time.perf_counter()
            await agg(coll, base + [{"$skip": (pg - 1) * size}, {"$limit": size}, {"$project": {"_s": 1}}])
            ms.append((time.perf_counter() - t0) * 1000)
            await nap()
        row("c8", f"page {pg} of {size}", p50_ms=pct(ms, .5), p95_ms=pct(ms, .95))
    seen, dup = [], 0
    for pg in range(1, min(last, 12) + 1):
        ids = [d["_id"] for d in await agg(coll, base + [{"$skip": (pg - 1) * size}, {"$limit": size}, {"$project": {"_id": 1}}])]
        dup += len(set(ids) & set(seen)); seen += ids
        await nap()
    whole = [d["_id"] for d in await agg(coll, base + [{"$limit": len(seen)}, {"$project": {"_id": 1}}])]
    row("c8", "pages are consistent with one long query", pages=min(last, 12), results=len(seen), duplicates=dup, same_order=seen == whole)
    k = min(1000, tot)
    ms_raw, ms_par = [], []
    for _ in range(a.reps):
        t0 = time.perf_counter(); raw = await agg(coll, base + [{"$limit": k}]); ms_raw.append((time.perf_counter() - t0) * 1000)
        t0 = time.perf_counter(); [parse_obj(Big, {x: y for x, y in d.items() if x != "_s"}) for d in raw]; ms_par.append((time.perf_counter() - t0) * 1000)
        await nap()
    row("c8", f"{k} results: query / parse into documents", query_p50_ms=pct(ms_raw, .5), parse_p50_ms=pct(ms_par, .5))
    await drop_index(coll, "big_idx", timeout=600)


# ------------------------------------------------------------------ c9: the facet type follows from the field type
class Fct(BaseModel):
    kind: str
    qty: int
    price: float
    created: dt.datetime
    flag: bool
    tags: list[str]
    plain: str


def facet_type(ann):
    """The rule under test: the facet type a field's Python type implies (None: not facetable)."""
    if ann is str or ann == list[str]:
        return "string"
    if ann in (int, float):
        return "number"
    if ann is dt.datetime:
        return "date"
    return None


async def c9(db, a):
    c = db["odm_f"]
    await c.insert_many([{"kind": "abc"[i % 3], "qty": i % 10, "price": i * 0.5, "created": BASE + dt.timedelta(days=i), "flag": i % 2 == 0, "tags": ["x", "y"] if i % 2 else ["z"], "plain": f"p{i}"} for i in range(40)])
    await make_index(c, "f_idx", {"dynamic": False, "fields": {
        "_id": {"type": "objectId"}, "kind": [{"type": "token"}, {"type": "stringFacet"}], "qty": [{"type": "number"}, {"type": "numberFacet"}],
        "price": [{"type": "number"}, {"type": "numberFacet"}], "created": [{"type": "date"}, {"type": "dateFacet"}], "flag": {"type": "boolean"},
        "tags": {"type": "stringFacet"}, "plain": {"type": "string"}}})
    await wait_docs(c, "f_idx", 40)
    opts = {"string": {"numBuckets": 3}, "number": {"boundaries": [0, 5, 10], "default": "other"}, "date": {"boundaries": [BASE, BASE + dt.timedelta(days=20), BASE + dt.timedelta(days=60)]}}

    async def run(path, ftype):
        r = await agg(c, [{"$searchMeta": {"index": "f_idx", "facet": {"operator": {"exists": {"path": "_id"}}, "facets": {"f": {"type": ftype, "path": path, **opts[ftype]}}}}}])
        return [(b.get("_id"), b["count"]) for b in r[0]["facet"]["f"]["buckets"]]

    agree = disagree = 0
    declared = {"kind", "qty", "price", "created", "flag", "tags"}  # fields declared facetable
    for name, fi in Fct.model_fields.items():
        want = facet_type(fi.annotation) if name in declared else None
        if want:
            try:
                b = await run(name, want)
                seen = "ok" if b else "empty"
            except Exception as e:  # noqa: BLE001
                b, seen = str(e)[:80], "error"
            agree, disagree = (agree + 1, disagree) if seen == "ok" else (agree, disagree + 1)
            row("c9", f"{name}: {fi.annotation} -> {want}", observed=seen, buckets=b)
        else:
            row("c9", f"{name}: {fi.annotation} -> not facetable", predicted="definition-time error" if name in declared else "field not declared facetable")
        await nap()
    row("c9", "inferred facet types against the engine", agree=agree, disagree=disagree)
    for label, path, ftype in (("wrong type: string facet on a number field", "qty", "string"), ("wrong type: number facet on a string field", "kind", "number"),
                               ("field mapped without a facet type (plain string)", "plain", "string"), ("boolean field", "flag", "string"), ("unmapped path", "nope", "string")):
        try:
            b = await run(path, ftype)
            row("c9", label, observed="ok" if b else "empty", buckets=b[:3])
        except Exception as e:  # noqa: BLE001
            row("c9", label, observed="error", error=f"{type(e).__name__}: {str(e)[:100]}")
        await nap()
    await drop_index(c, "f_idx")


async def clean(db):
    for n in await db.list_collection_names():
        if n.startswith("odm_"):
            c = db[n]
            for ix in await (await c.list_search_indexes()).to_list():
                await drop_index(c, ix["name"])
            await c.drop()


async def main():
    global HOSTED
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["engine", "hosted"])
    ap.add_argument("--label", required=True)
    ap.add_argument("--docs", type=int, default=2000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--only", default="c1,c2,c3,c4,c5,c6,c7,c8,c9")
    a = ap.parse_args()
    HOSTED = a.kind == "hosted"
    only = set(a.only.split(","))
    cl = client(a.kind)
    # the hosted user may only touch its own database: use it, and clean up only the collections this harness creates
    dbname = (os.environ.get("HOODIE_BENCH_ATLAS_DB") or "bench").strip() if HOSTED else f"odm_{secrets.token_hex(4)}"
    db = cl[dbname]
    await clean(db)
    print(f"kind={a.kind} db={dbname} python={platform.python_version()}")
    meta = {"kind": a.kind, "docs": a.docs, "reps": a.reps, "only": sorted(only), "python": platform.python_version(), "platform": platform.platform()}
    try:
        meta["server"] = (await cl.admin.command("buildInfo"))["version"]
        await init_beanie(database=db, document_models=[Sc, Tg, Ow, W, Big])
        if "c1" in only: await c1(db, a)
        if only & {"c2", "c3"}:
            c, ref = await small(db)
            if "c3" in only: await c3(db, a, c, ref)
            if "c2" in only: await c2(db, a, c)
            await drop_index(c, "m_idx")
        if "c4" in only: await c4(db, a)
        if "c5" in only: await c5(db, a)
        if "c6" in only: await c6(db, a)
        if "c7" in only: await c7(db, a, cl)
        if "c8" in only: await c8(db, a)
        if "c9" in only: await c9(db, a)
    except BaseException as e:  # noqa: BLE001
        print("HARNESS ERROR:", redact(f"{type(e).__name__}: {e}"))
        save(a.label, {**meta, "harness_error": redact(f"{type(e).__name__}: {e}")})
        raise SystemExit(1)
    finally:
        try:
            await clean(db)
        finally:
            await cl.close()
    save(a.label, meta)


asyncio.run(main())
