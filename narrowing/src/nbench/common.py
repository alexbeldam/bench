"""Shared constants, layout/filter definitions and small helpers."""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bson import ObjectId

SCALES = {"smoke": 5_000, "100k": 100_000, "1m": 1_000_000, "10m": 10_000_000}
DB = "narrowing_bench"
SEED_DATA = 20260101  # data generator seed: fixed, so every run sees identical documents
LIMIT = 20  # page size
CAP = 10_000  # planner cap under test
THRESHOLD = 0.01  # selectivity threshold under test
OVERFETCH = 100
CHUNK = 1000
INFLIGHT = 8
A_MAX_IDS = 100_000  # chunked A is not measured above this many IDs (8 s per query at 500k IDs in the first benchmark)
RESULTS = Path(os.environ.get("NBENCH_RESULTS", Path(__file__).resolve().parents[2] / "results"))

TERMS = {
    "frequent": "w0000",  # about 90% of documents
    "medium": "zzmid",  # 2% of documents (seq % 50 == 7)
    "rare": "zzrare",  # 0.1% of documents (seq % 1000 == 13)
}

# Tenant groups (clustered-by-field layout). Absolute groups first, then fractions of the entity.
ABS_GROUPS = [1000, 5000, 10000, 25000]
FRAC_GROUPS = [0.0025, 0.005, 0.01, 0.02, 0.05, 0.10]
SEL_GRID = [0.0025, 0.005, 0.01, 0.02, 0.05]
LAYOUTS = ["even", "contig", "group"]


def uri() -> str:
    return os.environ.get("NBENCH_MONGO_URI", "mongodb://127.0.0.1:27102/?directConnection=true")


def coll_name(label: str) -> str:
    return f"docs_{label}"


def index_name(label: str) -> str:
    return f"docs_{label}_idx"


def oid(i: int) -> ObjectId:
    return ObjectId(b"\x00\x0a\x00\x01" + i.to_bytes(8, "big"))


def coprime_multiplier(n: int, frac: float) -> int:
    p = int(n * frac) | 1
    while math.gcd(p, n) != 1:
        p += 2
    return p


def rank_of(i: int, n: int) -> int:
    """A deterministic low-discrepancy permutation of 0..n-1: `rank < k` selects k documents spread evenly."""
    return (i * coprime_multiplier(n, 0.6180339887)) % n


def group_sizes(n: int) -> list[tuple[str, int]]:
    """Ordered (tenant name, exact document count) pairs; the remainder is tenant `rest`."""
    out = [(f"c{k}", k) for k in ABS_GROUPS if k < n // 4]
    out += [(f"p{round(f * 10000)}", max(1, round(f * n))) for f in FRAC_GROUPS]
    return out


def tenant_thresholds(n: int) -> list[tuple[str, int]]:
    acc = 0
    out = []
    for name, size in group_sizes(n):
        acc += size
        out.append((name, acc))
    return out


def tenant_of(i: int, n: int, thresholds: list[tuple[str, int]]) -> str:
    u = (i * coprime_multiplier(n, 0.4142135623)) % n  # a second, independent permutation
    for name, upper in thresholds:
        if u < upper:
            return name
    return "rest"


@dataclass(frozen=True)
class Filt:
    """A narrowing filter in one of the three layouts, matching `k` documents (by construction)."""

    layout: str
    k: int
    flt: dict[str, Any]

    @property
    def label(self) -> str:
        return f"{self.layout}:{self.k}"


def make_filter(layout: str, n: int, k: int) -> Filt:
    if layout == "even":
        return Filt(layout, k, {"rank": {"$lt": k}})
    if layout == "contig":
        return Filt(layout, k, {"_id": {"$lt": oid(k)}})
    for name, size in group_sizes(n):
        if size == k:
            return Filt(layout, k, {"tenant": name})
    raise ValueError(f"no tenant group with {k} documents at n={n}")


def make_filter_or_none(layout: str, n: int, k: int) -> Filt | None:
    try:
        return make_filter(layout, n, k)
    except ValueError:
        return None


def sel_counts(n: int) -> list[int]:
    return [max(1, round(f * n)) for f in SEL_GRID]


def abs_counts(n: int) -> list[int]:
    return [k for k in ABS_GROUPS if k < n // 4]


def run_dir(label: str) -> Path:
    p = RESULTS / label
    p.mkdir(parents=True, exist_ok=True)
    return p


_lock = threading.Lock()


def append_jsonl(path: Path, rows: list[dict] | dict) -> None:
    if isinstance(rows, dict):
        rows = [rows]
    with _lock, path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")


def now() -> float:
    return time.time()


def redact(text: str, secrets: list[str]) -> str:
    """Remove every secret (and any mongodb URI) from a message before it is printed or stored."""
    for s in secrets:
        if s:
            text = text.replace(s, "<redacted>")
    return re.sub(r"mongodb(\+srv)?://[^\s'\"]+", "<redacted-uri>", text)
