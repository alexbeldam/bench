"""Shared constants and small helpers."""

import json
import math
import os
import shutil
import time
from pathlib import Path

from bson import ObjectId

SCALES = {"smoke": 5_000, "100k": 100_000, "1m": 1_000_000, "10m": 10_000_000}
DB = "wpbench"
SEED = 20260101
SYNC = "_hoodie_sync"  # marker collection (name used by the simulated write path)
REV = "_hoodie_rev"  # reserved revision field
RESULTS = Path(os.environ.get("WPBENCH_RESULTS", Path(__file__).resolve().parents[2] / "results"))

# Disjoint sequence ranges so that benchmark cases never collide.
INSERT_BASE = 1_000_000_000  # new documents written by insert cases
POOL_BASE = 2_000_000_000  # documents created just to be deleted (delete cases)
MARKER_BASE = 3_000_000_000  # marker-growth phase
MIN_FREE_GB = 1.5


def uri() -> str:
    return os.environ.get("WPBENCH_MONGO_URI", "mongodb://127.0.0.1:27103/?directConnection=true")


def entity_name(label: str) -> str:
    return f"entity_{label}"


def index_name(coll: str) -> str:
    return f"{coll}_idx"


def doc_oid(i: int) -> ObjectId:
    """Deterministic document id for sequence number i (the benchmark controls ids; revisions are random)."""
    return ObjectId(b"\x00\x0c\x00\x03" + i.to_bytes(8, "big"))


def seed_rev(i: int) -> ObjectId:
    """Deterministic initial revision token of a loaded document."""
    return ObjectId(b"\x00\x0d\x00\x04" + i.to_bytes(8, "big"))


def pct(values, q: float) -> float:
    """Percentile with linear interpolation; nan for empty input."""
    if not len(values):
        return math.nan
    s = sorted(values)
    k = (len(s) - 1) * q / 100
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def free_gb(path: str = "/") -> float:
    try:
        return shutil.disk_usage(path if os.path.exists(path) else ".").free / 1e9
    except OSError:
        return math.inf


class DiskLow(RuntimeError):
    pass


def check_disk(stage: str) -> None:
    free = free_gb()
    print(f"[disk] {stage}: {free:.1f} GB free", flush=True)
    if free < MIN_FREE_GB:
        raise DiskLow(f"free disk {free:.2f} GB below {MIN_FREE_GB} GB at {stage}")


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str))


def now() -> float:
    return time.time()
