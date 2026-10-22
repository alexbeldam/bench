"""Robust statistics: percentiles with a bootstrap confidence interval over repetitions."""

from __future__ import annotations

import numpy as np


def pct(a: np.ndarray, q: float) -> float:
    return float(np.percentile(a, q)) if len(a) else float("nan")


def boot_ci(a, q: float, n_boot: int = 1000, seed: int = 7) -> tuple[float, float, float]:
    """Return (point, lo, hi): the q-th percentile and its 95% bootstrap interval."""
    a = np.asarray(a, dtype=float)
    if len(a) == 0:
        return (float("nan"),) * 3
    point = float(np.percentile(a, q))
    if len(a) < 5:
        return point, float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(a), size=(n_boot, len(a)))
    est = np.percentile(a[draws], q, axis=1)
    return point, float(np.percentile(est, 2.5)), float(np.percentile(est, 97.5))


def summarize(a, qs=(50, 95, 99)) -> dict:
    a = np.asarray(a, dtype=float)
    out = {"n": int(len(a))}
    for q in qs:
        p, lo, hi = boot_ci(a, q)
        out[f"p{q}"], out[f"p{q}_lo"], out[f"p{q}_hi"] = p, lo, hi
    return out
