"""Synthetic, seeded data. Documents carry no real values: words are `wNNNN` tokens and categories are `cat-N`."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId

from .contract import FieldDef, IndexDef, IndexableDoc

SEED = 20261022
VOCAB_TITLE = 40  # small vocabulary on purpose: many documents share the same title terms, so scores tie
VOCAB_BODY = 600
CATEGORIES = [f"cat-{k}" for k in range(8)]
EPOCH = datetime(2025, 1, 1, tzinfo=UTC)

INDEX = IndexDef(
    name="item_idx",
    fields=(
        FieldDef("title", "string"),
        FieldDef("body", "string"),
        FieldDef("category", "token"),
        FieldDef("price", "number"),
        FieldDef("created", "date"),
        FieldDef("in_stock", "boolean"),
        FieldDef("_rev", "objectId"),
        FieldDef("_id", "objectId"),
    ),
)


def atlas_definition(index: IndexDef = INDEX) -> dict[str, Any]:
    return {
        "mappings": {
            "dynamic": False,
            "fields": {f.path: {"type": f.kind} for f in index.fields},
        }
    }


def oid(i: int, ns: int = 1) -> ObjectId:
    return ObjectId(b"\x00\x0c\x00" + bytes([ns]) + i.to_bytes(8, "big"))


def word(k: int) -> str:
    return f"w{k:04d}"


def make_docs(n: int, seed: int = SEED, id_offset: int = 0) -> list[dict[str, Any]]:
    """`n` documents. Ids are a seeded permutation, so id order differs from insertion order (a naive "index
    order" tiebreak cannot pass by accident). Titles come from a 40-word vocabulary, so ties are common."""
    rng = random.Random(seed)
    perm = list(range(n))
    rng.shuffle(perm)
    title_w = [1.0 / (k + 1) for k in range(VOCAB_TITLE)]
    body_w = [1.0 / (k + 1) for k in range(VOCAB_BODY)]
    docs = []
    for i in range(n):
        tl = rng.randint(2, 4)
        bl = rng.randint(12, 30)
        title = " ".join(word(k) for k in rng.choices(range(VOCAB_TITLE), title_w, k=tl))
        body = " ".join(word(k) for k in rng.choices(range(VOCAB_BODY), body_w, k=bl))
        docs.append(
            {
                "_id": oid(perm[i] + id_offset),
                "title": title,
                "body": body,
                "category": rng.choice(CATEGORIES),
                "price": rng.randint(1, 200),
                "created": EPOCH + timedelta(days=rng.randint(0, 364)),
                "in_stock": rng.random() < 0.7,
            }
        )
    return docs


def with_rev(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for d in docs:
        d["_rev"] = ObjectId()
    return docs


def to_indexable(docs: list[dict[str, Any]], index: IndexDef = INDEX) -> list[IndexableDoc]:
    paths = [f.path for f in index.fields if f.path not in ("_id", "_rev")]
    return [IndexableDoc(d["_id"], d.get("_rev"), {p: d.get(p) for p in paths}) for d in docs]
