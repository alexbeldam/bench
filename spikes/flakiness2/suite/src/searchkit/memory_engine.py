"""In-memory search engine implementing the harness's `SearchEngine` contract. Standard library only.

Writes apply synchronously, so there is nothing to wait for. Scores follow Lucene BM25 over index-wide statistics.
Results always come back in a total order (explicit sort keys, then score, then id ascending).
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from . import analysis
from .contract import (
    COMMON_CLAUSES,
    Clause,
    Compound,
    Equals,
    Hit,
    In,
    IndexDef,
    IndexableDoc,
    IndexNeverQueryableError,
    IndexNotFoundError,
    Phrase,
    Query,
    Range,
    SearchResult,
    Text,
    UnsupportedClauseError,
)

CONSTANT_SCORE = 1.0  # score of a non-text clause used in `must` / `should` (calibrated against Atlas)
FUZZY_MAX_EXPANSIONS = 50


@dataclass
class _FieldStats:
    doc_count: int = 0
    total_terms: int = 0


@dataclass
class _Index:
    definition: IndexDef
    docs: dict[Any, tuple[Any, dict[str, Any]]] = field(default_factory=dict)  # id -> (rev, values)
    # text field -> term -> id -> positions
    postings: dict[str, dict[str, dict[Any, list[int]]]] = field(default_factory=dict)
    lengths: dict[str, dict[Any, int]] = field(default_factory=dict)
    stats: dict[str, _FieldStats] = field(default_factory=dict)
    vocab_sorted: dict[str, list[str]] | None = None
    revs: dict[Any, Any] = field(default_factory=dict)  # rev -> id

    def text_fields(self) -> list[str]:
        return [f.path for f in self.definition.fields if f.kind == "string"]


_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_MS = timedelta(milliseconds=1)


def _norm(v: Any) -> Any:
    """Dates become epoch milliseconds (Mongo keeps milliseconds and returns naive UTC)."""
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=_UTC)
        return (v - _EPOCH) // _MS
    return v


class InMemoryEngine:
    name = "memory"

    def __init__(self, *, score_decimals: int | None = None) -> None:
        self._indexes: dict[tuple[str, str], _Index] = {}
        self.score_decimals = score_decimals  # optional rounding before the id tiebreak (see ordering.py)
        self.force_status: dict[tuple[str, str], str] = {}  # test hook: simulate a broken index

    # ---- capability ---------------------------------------------------------------------------------------
    def capabilities(self) -> frozenset[str]:
        return COMMON_CLAUSES

    # ---- index lifecycle ----------------------------------------------------------------------------------
    async def create_index(self, entity: str, index: IndexDef) -> None:
        self._indexes[(entity, index.name)] = _Index(index)
        for f in index.fields:
            if f.kind == "string":
                ix = self._indexes[(entity, index.name)]
                ix.postings[f.path] = defaultdict(dict)
                ix.lengths[f.path] = {}
                ix.stats[f.path] = _FieldStats()

    async def update_index(self, entity: str, index: IndexDef) -> None:
        old = self._indexes.get((entity, index.name))
        await self.create_index(entity, index)
        if old:
            await self.sync_write(
                entity, index.name, [IndexableDoc(i, r, v) for i, (r, v) in old.docs.items()]
            )

    async def drop_index(self, entity: str, index: str) -> None:
        self._indexes.pop((entity, index), None)
        self.force_status.pop((entity, index), None)

    async def index_status(self, entity: str, index: str) -> str:
        if (entity, index) in self.force_status:
            return self.force_status[(entity, index)]
        return "queryable" if (entity, index) in self._indexes else "missing"

    async def wait_queryable(self, entity: str, index: str, timeout: float) -> None:
        status = await self.index_status(entity, index)
        if status != "queryable":
            raise IndexNeverQueryableError(entity, index, 0.0, status)

    def _ix(self, entity: str, index: str) -> _Index:
        try:
            return self._indexes[(entity, index)]
        except KeyError:
            raise IndexNotFoundError(index) from None

    # ---- write sync ---------------------------------------------------------------------------------------
    def _remove(self, ix: _Index, doc_id: Any) -> None:
        entry = ix.docs.pop(doc_id, None)
        if entry is None:
            return
        ix.revs.pop(entry[0], None)
        for path in ix.text_fields():
            n = ix.lengths[path].pop(doc_id, None)
            if n is None:
                continue
            st = ix.stats[path]
            st.doc_count -= 1
            st.total_terms -= n
            text = entry[1].get(path)
            for term in set(analysis.tokenize(text)) if isinstance(text, str) else ():
                plist = ix.postings[path].get(term)
                if plist is not None:
                    plist.pop(doc_id, None)
                    if not plist:
                        del ix.postings[path][term]
        ix.vocab_sorted = None

    async def sync_write(self, entity: str, index: str, docs: Sequence[IndexableDoc]) -> None:
        ix = self._ix(entity, index)
        for d in docs:
            self._remove(ix, d.id)
            values = {k: _norm(v) if not isinstance(v, str) else v for k, v in d.fields.items()}
            ix.docs[d.id] = (d.rev, values)
            if d.rev is not None:
                ix.revs[d.rev] = d.id
            for path in ix.text_fields():
                text = d.fields.get(path)
                if not isinstance(text, str):
                    continue
                toks = analysis.tokenize(text)
                if not toks:
                    continue
                ix.lengths[path][d.id] = len(toks)
                st = ix.stats[path]
                st.doc_count += 1
                st.total_terms += len(toks)
                for pos, term in enumerate(toks):
                    ix.postings[path][term].setdefault(d.id, []).append(pos)
        ix.vocab_sorted = None

    async def sync_delete(self, entity: str, index: str, ids: Sequence[Any]) -> None:
        ix = self._ix(entity, index)
        for i in ids:
            self._remove(ix, i)

    async def probe(self, entity: str, index: str, pairs: Sequence[tuple[Any, Any]]) -> set[Any]:
        ix = self._ix(entity, index)
        # match on the (id, rev) pair. The earlier measurement probed by revision alone, which is
        # ambiguous when one query-based update stamps the same revision on many documents.
        return {i for i, r in pairs if (e := ix.docs.get(i)) is not None and e[0] == r}

    async def probe_absent(self, entity: str, index: str, ids: Sequence[Any]) -> set[Any]:
        ix = self._ix(entity, index)
        return {i for i in ids if i in ix.docs}

    # ---- evaluation ---------------------------------------------------------------------------------------
    def _text_scores(self, ix: _Index, path: str, term: str, boost: float = 1.0, df: int | None = None) -> dict[Any, float]:
        plist = ix.postings[path].get(term)
        st = ix.stats[path]
        if not plist or st.doc_count == 0:
            return {}
        avg = st.total_terms / st.doc_count
        w = analysis.idf(st.doc_count, len(plist) if df is None else df) * boost
        lens = ix.lengths[path]
        return {i: analysis.bm25(w, len(pos), lens[i], avg) for i, pos in plist.items()}

    def _fuzzy_terms(self, ix: _Index, path: str, term: str, max_edits: int) -> list[tuple[str, float]]:
        if ix.vocab_sorted is None:
            ix.vocab_sorted = {}
        vocab = ix.vocab_sorted.get(path)
        if vocab is None:
            vocab = ix.vocab_sorted[path] = sorted(ix.postings[path])
        out = []
        for cand in vocab:
            ed = analysis.edit_distance(term, cand, max_edits)
            if ed <= max_edits:
                sim = 1.0 - ed / min(len(term), len(cand)) if ed else 1.0
                out.append((cand, sim))
        out.sort(key=lambda t: (-t[1], t[0]))
        return out[:FUZZY_MAX_EXPANSIONS]

    def _eval_text(self, ix: _Index, c: Text) -> dict[Any, float]:
        terms = analysis.tokenize(c.query)
        per_path: list[dict[Any, float]] = []
        for path in c.paths:
            if ix.definition.kind_of(path) != "string":
                raise UnsupportedClauseError(self.name, f"text on non-string path {path!r}")
            acc: dict[Any, float] = defaultdict(float)
            for t in terms:
                if c.max_edits:
                    variants = self._fuzzy_terms(ix, path, t, c.max_edits)
                    # Lucene blends statistics: every variant is scored with the largest document frequency
                    st = ix.stats[path]
                    df_max = max((len(ix.postings[path][v]) for v, _ in variants), default=0)
                    for cand, sim in variants:
                        for i, s in self._text_scores(ix, path, cand, boost=sim, df=df_max).items():
                            acc[i] = analysis.f32(acc[i] + s)
                else:
                    for i, s in self._text_scores(ix, path, t).items():
                        acc[i] = analysis.f32(acc[i] + s)
            per_path.append(acc)
        out: dict[Any, float] = defaultdict(float)
        for acc in per_path:  # multi-path: sum
            for i, s in acc.items():
                out[i] = analysis.f32(out[i] + s)
        return dict(out)

    def _eval_phrase(self, ix: _Index, c: Phrase) -> dict[Any, float]:
        terms = analysis.tokenize(c.query)
        path = c.path
        if not terms:
            return {}
        lists = [ix.postings[path].get(t) for t in terms]
        if any(pl is None for pl in lists):
            return {}
        st = ix.stats[path]
        avg = st.total_terms / st.doc_count
        w = sum(analysis.idf(st.doc_count, len(pl)) for pl in lists)  # type: ignore[arg-type]
        cands = set(lists[0])  # type: ignore[arg-type]
        for pl in lists[1:]:
            cands &= set(pl)  # type: ignore[arg-type]
        out: dict[Any, float] = {}
        for i in cands:
            first = lists[0][i]  # type: ignore[index]
            freq = 0
            if c.slop == 0:
                for p in first:
                    if all((p + k) in set(lists[k][i]) for k in range(1, len(terms))):  # type: ignore[index]
                        freq += 1
            else:  # Lucene sloppy frequency: each match counts 1 / (1 + gap), gap = total displacement
                freq = 0.0
                for p in first:
                    gap = 0
                    cur = p
                    ok = True
                    for k in range(1, len(terms)):
                        nxt = [q for q in lists[k][i] if cur < q <= cur + 1 + c.slop]  # type: ignore[index]
                        if not nxt:
                            ok = False
                            break
                        q = nxt[0]
                        gap += q - cur - 1
                        cur = q
                    if ok:
                        freq += 1.0 / (1.0 + gap)
            if freq:
                out[i] = analysis.bm25(w, freq, ix.lengths[path][i], avg)
        return out

    def _values(self, ix: _Index, path: str) -> list[tuple[Any, Any]]:
        if path == "_id":
            return [(i, i) for i in ix.docs]
        return [(i, v[1].get(path)) for i, v in ix.docs.items()]

    def _eval_equals(self, ix: _Index, c: Equals) -> dict[Any, float]:
        want = _norm(c.value) if not isinstance(c.value, str) else c.value
        return {i: CONSTANT_SCORE for i, v in self._values(ix, c.path) if v is not None and v == want}

    def _eval_in(self, ix: _Index, c: In) -> dict[Any, float]:
        want = {_norm(v) if not isinstance(v, str) else v for v in c.values}
        return {i: CONSTANT_SCORE for i, v in self._values(ix, c.path) if v is not None and v in want}

    def _eval_range(self, ix: _Index, c: Range) -> dict[Any, float]:
        gt, gte, lt, lte = (None if x is None else _norm(x) for x in (c.gt, c.gte, c.lt, c.lte))
        out = {}
        for i, v in self._values(ix, c.path):
            if v is None:
                continue
            if gt is not None and not v > gt:
                continue
            if gte is not None and not v >= gte:
                continue
            if lt is not None and not v < lt:
                continue
            if lte is not None and not v <= lte:
                continue
            out[i] = CONSTANT_SCORE
        return out

    def _eval(self, ix: _Index, c: Clause) -> dict[Any, float]:
        match c:
            case Text():
                return self._eval_text(ix, c)
            case Phrase():
                return self._eval_phrase(ix, c)
            case Equals():
                return self._eval_equals(ix, c)
            case In():
                return self._eval_in(ix, c)
            case Range():
                return self._eval_range(ix, c)
            case Compound():
                return self._eval_compound(ix, c)
        raise UnsupportedClauseError(self.name, type(c).__name__)

    def _eval_compound(self, ix: _Index, c: Compound) -> dict[Any, float]:
        must = [self._eval(ix, x) for x in c.must]
        filt = [self._eval(ix, x) for x in c.filter]
        should = [self._eval(ix, x) for x in c.should]
        must_not = [self._eval(ix, x) for x in c.must_not]
        required = must + filt
        if required:
            cand = set(required[0])
            for r in required[1:]:
                cand &= set(r)
        elif should:
            cand = set().union(*should)
        else:
            cand = set(ix.docs)  # only must_not: everything not excluded
        for n in must_not:
            cand -= set(n)
        # minimumShouldMatch applies to the should clauses; with no required clause at least one must match
        msm = c.minimum_should_match or (1 if (should and not required) else 0)
        out: dict[Any, float] = {}
        for i in cand:
            hit = 0
            s = 0.0
            for m in must:
                s = analysis.f32(s + m[i])
            for sh in should:
                if i in sh:
                    hit += 1
                    s = analysis.f32(s + sh[i])
            if hit < msm:
                continue
            out[i] = s
        return out

    # ---- query --------------------------------------------------------------------------------------------
    async def search(self, entity: str, index: str, query: Query) -> SearchResult:
        ix = self._ix(entity, index)
        status = await self.index_status(entity, index)
        if status != "queryable":
            raise IndexNeverQueryableError(entity, index, 0.0, status)
        scored = self._eval_compound(ix, query.root)
        dec = self.score_decimals

        def key(i: Any) -> tuple[Any, ...]:
            parts: list[Any] = []
            for s in query.sort or ():
                if s.path == "score":
                    sc = scored[i] if dec is None else round(scored[i], dec)
                    parts.append(-sc if s.direction < 0 else sc)
                else:
                    v = ix.docs[i][1].get(s.path)
                    parts.append(_Dir(v, s.direction))
            if not query.sort:
                parts.append(-(scored[i] if dec is None else round(scored[i], dec)))
            parts.append(i)  # id ascending, the final tiebreak
            return tuple(parts)

        total = len(scored)
        want = query.skip + query.limit
        if want < total:  # partial sort (heap) keeps large result sets cheap
            import heapq

            ordered = heapq.nsmallest(want, scored, key=key)
        else:
            ordered = sorted(scored, key=key)
        page = ordered[query.skip : query.skip + query.limit]
        return SearchResult([Hit(i, scored[i]) for i in page], total)


class _Dir:
    """Sort wrapper that reverses comparison for descending keys (works for any comparable value)."""

    __slots__ = ("d", "v")

    def __init__(self, v: Any, d: int) -> None:
        self.v, self.d = v, d

    def __lt__(self, other: _Dir) -> bool:
        return self.v < other.v if self.d > 0 else self.v > other.v

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Dir) and self.v == other.v
