"""Total order: score descending (optionally rounded), then explicit sort keys, then id ascending.

Standard library only. Also holds the chunk merge used by the ID-constraint strategy, strategy A chunked).
"""

from __future__ import annotations

import heapq
import math
from collections.abc import Iterable, Sequence
from typing import Any


def round_sig(score: float, digits: int | None) -> float:
    """Round to `digits` significant digits. None keeps the score as is."""
    if digits is None or score == 0.0 or not math.isfinite(score):
        return score
    return round(score, digits - 1 - math.floor(math.log10(abs(score))))


def order_key(hit: tuple[Any, float], digits: int | None = None) -> tuple[float, Any]:
    """(−rounded score, id): sorting ascending gives score descending, then id ascending."""
    i, s = hit
    return (-round_sig(s, digits), i)


def sort_hits(hits: Iterable[tuple[Any, float]], digits: int | None = None) -> list[tuple[Any, float]]:
    return sorted(hits, key=lambda h: order_key(h, digits))


def merge_chunks(
    chunks: Sequence[Sequence[tuple[Any, float]]], limit: int, skip: int = 0, digits: int | None = None
) -> list[tuple[Any, float]]:
    """Merge per-chunk top results (each chunk already holds its own top `skip + limit` in total order)."""
    merged = heapq.nsmallest(skip + limit, (h for c in chunks for h in c), key=lambda h: order_key(h, digits))
    return merged[skip : skip + limit]
