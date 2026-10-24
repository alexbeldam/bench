"""The common-clause query set used for parity (in-memory engine versus Atlas Search)."""
from datetime import datetime, timezone

from .contract import Compound, Equals, In, Phrase, Query, Range, SortKey, Text

N = 5000  # `limit` for "all matches"
C = Compound
def Q(*, must=(), should=(), filt=(), mn=(), msm=0, sort=(), limit=N, skip=0):
    return Query(C(must, should, filt, mn, msm), tuple(sort), limit, skip)
d0 = datetime(2025, 3, 1, tzinfo=timezone.utc); d1 = datetime(2025, 6, 1, tzinfo=timezone.utc)
QUERIES = {
 "text.single-term/title": Q(must=(Text("w0001", ("title",)),)),
 "text.rare-term/title": Q(must=(Text("w0035", ("title",)),)),
 "text.multi-term/title": Q(must=(Text("w0001 w0002 w0003", ("title",)),)),
 "text.single-term/body": Q(must=(Text("w0003", ("body",)),)),
 "text.multi-path": Q(must=(Text("w0002 w0007", ("title", "body")),)),
 "text.fuzzy1": Q(must=(Text("w0011", ("title",), 1),)),
 "text.fuzzy2": Q(must=(Text("w0011", ("title",), 2),)),
 "text.no-match": Q(must=(Text("zzzzzz", ("title",)),)),
 "phrase.2-terms": Q(must=(Phrase("w0000 w0001", "title"),)),
 "phrase.body": Q(must=(Phrase("w0000 w0001", "body"),)),
 "phrase.slop1": Q(must=(Phrase("w0000 w0002", "title", 1),)),
 "equals.token": Q(filt=(Equals("category", "cat-3"),)),
 "equals.number": Q(filt=(Equals("price", 50),)),
 "equals.bool": Q(filt=(Equals("in_stock", True),)),
 "in.token": Q(filt=(In("category", ("cat-1", "cat-5")),)),
 "range.number": Q(filt=(Range("price", gte=40, lt=60),)),
 "range.date": Q(filt=(Range("created", gte=d0, lt=d1),)),
 "equals.in-must(scored)": Q(must=(Equals("category", "cat-3"),)),
 "must_not": Q(must=(Text("w0001", ("title",)),), mn=(Equals("category", "cat-3"),)),
 "compound.text+filter": Q(must=(Text("w0001 w0004", ("title", "body")),), filt=(Range("price", lte=100), Equals("in_stock", True))),
 "compound.should-only": Q(should=(Text("w0005", ("title",)), Text("w0006", ("title",)))),
 "compound.should+msm2": Q(should=(Text("w0001", ("title",)), Text("w0002", ("title",)), Text("w0003", ("title",))), msm=2),
 "compound.must+should": Q(must=(Text("w0001", ("title",)),), should=(Text("w0002", ("title",)),)),
 "compound.nested-should": Q(must=(Text("w0001", ("title",)),), should=(C(should=(Text("w0004", ("title",)),)), )),
 "sort.price-asc": Q(must=(Text("w0001", ("title",)),), sort=(SortKey("price", 1),)),
 "sort.price-desc": Q(must=(Text("w0001", ("title",)),), sort=(SortKey("price", -1),)),
 "sort.created-desc": Q(must=(Text("w0001", ("title",)),), sort=(SortKey("created", -1),)),
 "sort.score-then-price": Q(must=(Text("w0001", ("title",)),), sort=(SortKey("score", -1), SortKey("price", 1))),
 "paging.limit20": Q(must=(Text("w0001", ("title",)),), limit=20),
 "paging.skip40-limit20": Q(must=(Text("w0001", ("title",)),), limit=20, skip=40),
 "paging.skip-past-end": Q(must=(Text("w0035", ("title",)),), limit=20, skip=10000),
}
