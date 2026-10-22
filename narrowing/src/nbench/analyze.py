"""Turns raw results (one directory per workflow run) into the report tables.

Everything here is computed from the raw files, nothing is typed in except the laptop calibration constants (taken
from the first benchmark's published report). Sections: environment, noise windows (from the canary), calibration,
one section per question, between-run stability, the 10M disk-projection decision.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from .common import LIMIT, SCALES
from .stats import boot_ci, pct, summarize

WINDOW_S = 5.0
# First benchmark (laptop, 1M documents, 8-chunk chunked A of 1,000 IDs, 100x search-first; different document shape).
LAPTOP_1M = {"AC|even|10000": (137.5, 206.4, 247.0), "AC|even|25000": (367.5, 560.4, 581.3), "B100|even|10000": (77.5, 119.6, 138.3)}
LAPTOP_NOTE = ("first benchmark, Windows laptop, Docker VM 4 CPUs / 12 GB, client on the host, different document shape; "
               "B100 at 1% selectivity (10,000 IDs at 1M) returned 19 of 20 results there")


# ---- loading ---------------------------------------------------------------------------------------------


def _jsonl(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_json(path, lines=True)


def _jread(path: Path):
    return json.loads(path.read_text()) if path.exists() else None


def load_runs(dirs: list[Path], scale_filter: str = "") -> dict[str, dict]:
    runs = {}
    for d in dirs:
        r: dict = {"dir": d, "env": _jread(d / "environment.json") or {}, "progress": _jread(d / "progress.json") or {},
                   "meta": _jread(d / "run_meta.json") or {}, "scales": {}}
        scales = sorted({p.stem.split("_")[1] for p in d.glob("raw_*_*.jsonl")} | {p.stem.split("_")[1] for p in d.glob("load_*.jsonl") if "side" not in p.stem and "warmup" not in p.stem}
                        | {p.stem.split("_")[1] for p in d.glob("canary_*.jsonl")}, key=lambda s: SCALES.get(s, 0))
        for sc in scales:
            if scale_filter and sc != scale_filter:
                continue
            raw = pd.concat([_jsonl(p) for p in sorted(d.glob(f"raw_{sc}_*.jsonl"))], ignore_index=True) if list(d.glob(f"raw_{sc}_*.jsonl")) else pd.DataFrame()
            cases = {}
            for p in d.glob(f"cases_{sc}_*.json"):
                cases.update(_jread(p))
            canary = _jsonl(d / f"canary_{sc}.jsonl")
            r["scales"][sc] = {"raw": raw, "cases": cases, "canary": canary, "load": _jsonl(d / f"load_{sc}.jsonl"),
                               "side": _jsonl(d / f"load_side_{sc}.jsonl"), "load_summary": _jread(d / f"load_summary_{sc}.json"),
                               "load_info": _jread(d / f"load_{sc}.json"), "disk": _jread(d / f"disk_{sc}.json")}
        runs[d.name] = r
    return runs


# ---- noise windows ---------------------------------------------------------------------------------------


def noisy_windows(canary: pd.DataFrame, factor: float) -> tuple[set[int], dict]:
    """Window indexes (floor(t / 5 s)) whose canary median exceeds `factor` x the run's canary median."""
    bad: set[int] = set()
    info: dict = {}
    if canary.empty:
        return bad, info
    for kind in ("search", "cpu"):
        c = canary[(canary.kind == kind) & (canary.ok == 1)]
        if c.empty:
            continue
        glob_med = float(c.ms.median())
        win = (c.t // WINDOW_S).astype(int)
        med = c.groupby(win).ms.median()
        flagged = set(med[med > factor * glob_med].index)
        bad |= flagged
        info[kind] = {"samples": int(len(c)), "median_ms": glob_med, "p95_ms": pct(c.ms.to_numpy(), 95), "p99_ms": pct(c.ms.to_numpy(), 99),
                      "windows": int(len(med)), "noisy_windows": len(flagged)}
    return bad, info


def flag(df: pd.DataFrame, bad: set[int]) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    end = df.t + df.ms.fillna(0) / 1000.0
    df["noisy"] = (df.t // WINDOW_S).astype(int).isin(bad) | (end // WINDOW_S).astype(int).isin(bad)
    return df


# ---- helpers ---------------------------------------------------------------------------------------------


def md(header: list[str], rows: list[list]) -> list[str]:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join("" if c is None else str(c).replace("|", "/") for c in r) + " |" for r in rows]
    return out + [""]


def f0(x) -> str:
    return "-" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:,.0f}"


def f1(x) -> str:
    return "-" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:,.1f}"


def ci(p, lo, hi) -> str:
    if lo is None or lo != lo:
        return f0(p)
    return f"{f0(p)} [{f0(lo)}-{f0(hi)}]"


def spread(vals: list[float]) -> str:
    v = [x for x in vals if x == x]
    if len(v) < 2:
        return "-"
    m = float(np.median(v))
    return f"{100 * (max(v) - min(v)) / m:.0f}%" if m else "-"


def collect(runs: dict, scale: str, phase: str, bad_by_run: dict[str, set[int]]) -> pd.DataFrame:
    """Measured (non-warm, ok) rows of a phase across runs, with params joined and a `noisy` flag."""
    frames = []
    for name, r in runs.items():
        s = r["scales"].get(scale)
        if not s or s["raw"].empty:
            continue
        df = s["raw"]
        df = df[df.phase == phase].copy()
        if df.empty:
            continue
        df = flag(df, bad_by_run.get(name, set()))
        df["run"] = name
        params = pd.DataFrame.from_dict(s["cases"], orient="index").add_prefix("p_")
        df = df.join(params, on="case")
        for c in ("p_k", "p_chunk", "p_inflight", "p_factor"):
            if c in df:
                df[c] = df[c].astype("Int64")
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def measured(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df.warm == 0)] if not df.empty else df


def per_case(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Pooled p50/p95/p99 with bootstrap CI over repetitions, with and without noisy windows, plus per-run p50."""
    rows = []
    d = measured(df)
    for key, g in d.groupby(keys, sort=False, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        ok = g[g.ok == 1]
        clean = ok[~ok.noisy]
        s, sc = summarize(ok.ms.to_numpy()), summarize(clean.ms.to_numpy())
        pr = ok.groupby("run").ms.median()
        pr95 = ok.groupby("run").ms.apply(lambda a: pct(a.to_numpy(), 95))
        rows.append({**dict(zip(keys, key)), "n": s["n"], "errors": int((g.ok == 0).sum()), "p50": s["p50"], "p50_lo": s["p50_lo"], "p50_hi": s["p50_hi"],
                     "p95": s["p95"], "p95_lo": s["p95_lo"], "p95_hi": s["p95_hi"], "p99": s["p99"], "p99_lo": s["p99_lo"], "p99_hi": s["p99_hi"],
                     "n_clean": sc["n"], "p50_clean": sc["p50"], "p95_clean": sc["p95"],
                     "run_p50": list(pr.values), "run_p95": list(pr95.values), "runs": len(pr),
                     "full_page": float((ok.results == LIMIT).mean()) if "results" in ok and len(ok) else float("nan"),
                     "results_med": float(ok.results.median()) if "results" in ok and len(ok) else float("nan"),
                     "total_med": float(ok.total.median()) if "total" in ok and ok.total.notna().any() else float("nan"),
                     "prefilter_p50": float(ok.prefilter_ms.median()) if "prefilter_ms" in ok and ok.prefilter_ms.notna().any() else float("nan")})
    return pd.DataFrame(rows)


ORDER = ["even", "contig", "group", "plain", "count", "count+facet_low", "count+facet_high", "early", "full_ids", "count_full", "count_cap", "count_threshold"]


def pivot(pc: pd.DataFrame, rows: str, cols: str, val: str = "p50") -> tuple[list[str], list[list]]:
    cs = sorted(dict.fromkeys(pc[cols]), key=lambda c: (ORDER.index(c) if c in ORDER else 99, str(c)))
    rs = list(dict.fromkeys(pc[rows]))
    out = []
    for r in rs:
        line = [r]
        for c in cs:
            m = pc[(pc[rows] == r) & (pc[cols] == c)]
            line.append(f0(m[val].iloc[0]) if len(m) else "-")
        out.append(line)
    return [rows] + [str(c) for c in cs], out


def stable(vals: dict[str, object]) -> str:
    return "stable" if len({str(v) for v in vals.values()}) == 1 else "NOT stable"


# ---- sections --------------------------------------------------------------------------------------------


def sec_environment(runs: dict) -> list[str]:
    out = ["## Environment per run (quoted from environment.json)", ""]
    rows = []
    for name, r in runs.items():
        e = r["env"]
        c = e.get("container", {})
        img = e.get("runner_image", {})
        rows.append([name, e.get("date_utc", "-"), f"{img.get('ImageOS', '-')} {img.get('ImageVersion', '-')}", e.get("nproc", "-"),
                     (e.get("lscpu_model") or "-").replace("Model name:", "").strip(), c.get("cpu_limit_nano", "-") and c["cpu_limit_nano"] / 1e9,
                     f"{(c.get('memory_limit_bytes') or 0) / 2**30:.1f} GiB", e.get("mongod_version", "-"), e.get("docker_version", "-"),
                     e.get("python", "-"), e.get("pymongo", "-"), e.get("workflow_run_url") or "local (not the runner)"])
    out += md(["run", "date (UTC)", "runner image", "nproc", "cpu model", "container CPUs", "container memory", "mongod", "docker", "python", "pymongo", "workflow run"], rows)
    for name, r in runs.items():
        seeds = {(m["scale"], tuple(m["phases"])): m["seed"] for m in r["meta"].get("runs", [])}
        if seeds:
            out.append(f"- `{name}` seeds (scale, phases): " + "; ".join(f"{s}/{','.join(p)}={sd}" for (s, p), sd in seeds.items()))
    out.append("")
    return out


def sec_noise(runs: dict, factor: float) -> tuple[list[str], dict]:
    bad_by: dict[tuple[str, str], set[int]] = {}
    out = [f"## Noise: canary windows (window {WINDOW_S:.0f} s, noisy if the window median exceeds {factor:g}x the run's canary median)", ""]
    rows = []
    for name, r in runs.items():
        for sc, s in r["scales"].items():
            bad, info = noisy_windows(s["canary"], factor)
            bad_by[(name, sc)] = bad
            for kind, i in info.items():
                rows.append([name, sc, kind, i["samples"], f1(i["median_ms"]), f1(i["p95_ms"]), f1(i["p99_ms"]), f"{i['noisy_windows']} / {i['windows']}"])
    out += md(["run", "scale", "canary", "samples", "median ms", "p95 ms", "p99 ms", "noisy windows / windows"], rows)
    out.append("A sample is flagged noisy if its start or end falls in a noisy window of either canary. Nothing is removed from the raw data; "
               "tables give results with all samples and, where shown, without flagged samples.")
    out.append("")
    return out, bad_by


def sec_calibration(runs, scale, bad) -> list[str]:
    df = collect(runs, scale, "calibration", bad)
    out = [f"### Calibration against the first benchmark ({scale})", ""]
    if df.empty:
        return out + ["pending: no calibration data", ""]
    pc = per_case(df, ["case"])
    rows = []
    for _, r in pc.iterrows():
        lap = LAPTOP_1M.get(r.case) if scale == "1m" else None
        rows.append([r.case, r.n, ci(r.p50, r.p50_lo, r.p50_hi), ci(r.p95, r.p95_lo, r.p95_hi), ci(r.p99, r.p99_lo, r.p99_hi),
                     "-" if not lap else f"{lap[0]:.0f} / {lap[1]:.0f} / {lap[2]:.0f}", f"{r.full_page:.0%}"])
    out += md(["case", "n", "GitHub runner p50 [95% CI] ms", "runner p95 ms", "runner p99 ms", "laptop p50 / p95 / p99 ms", "runner full page"], rows)
    out.append(f"Laptop column: {LAPTOP_NOTE}. Do not read conclusions from the laptop column beyond how different it is.")
    out.append("")
    return out


def sec_layouts(runs, scale, bad) -> list[str]:
    df = collect(runs, scale, "layouts", bad)
    out = [f"### Q2 Clustered ID layouts ({scale})", ""]
    if df.empty:
        return out + ["pending: no data", ""]
    pc = per_case(df, ["p_strategy", "p_layout", "p_k"])
    for strat, title in (("AC", "Chunked A (prefilter plus search), p50 ms"), ("prefilter", "ID prefilter alone (find on the filter, projection `_id`), p50 ms"), ("B100", "Search-first B100, p50 ms")):
        sub = pc[pc.p_strategy == strat].copy()
        if sub.empty:
            continue
        sub["layout"] = sub.p_layout
        sub["k_ids"] = sub.p_k
        h, rows = pivot(sub.sort_values(["p_k"]), "k_ids", "layout")
        out += [f"**{title}**", ""] + md(h, rows)
    sub = pc[pc.p_strategy == "B100"].copy()
    sub["layout"], sub["k_ids"] = sub.p_layout, sub.p_k
    sub["full_page_pct"] = (sub.full_page * 100).round(0)
    h, rows = pivot(sub.sort_values("p_k"), "k_ids", "layout", "full_page_pct")
    out += ["**B100 completeness: share of repetitions with a full page of 20 (percent)**", ""] + md(h, rows)
    # stability of the layout ranking for chunked A
    ac = per_case(df[df.p_strategy == "AC"], ["run", "p_layout", "p_k"]) if (df.p_strategy == "AC").any() else pd.DataFrame()
    if not ac.empty:
        rows = []
        for k, g in ac.groupby("p_k"):
            winners = {run: gg.sort_values("p50").p_layout.iloc[-1] for run, gg in g.groupby("run")}
            rows.append([k, json.dumps(winners), stable(winners)])
        out += ["**Which layout is slowest for chunked A, per run**", ""] + md(["IDs", "slowest layout per run", "stability"], rows)
    return out


def sec_count_facets(runs, scale, bad) -> list[str]:
    df = collect(runs, scale, "count_facets", bad)
    out = [f"### Q3 Cost of exact total and facets ({scale})", ""]
    if df.empty:
        return out + ["pending: no data", ""]
    pc = per_case(df, ["p_strategy", "p_k", "p_opts"])
    pc = pc.sort_values(["p_strategy", "p_k"])
    pc["row"] = pc.p_strategy + " @ " + pc.p_k.astype(str) + " IDs"
    h, rows = pivot(pc, "row", "p_opts")
    out += ["**p50 ms**", ""] + md(h, rows)
    rows = []
    for (st, k), g in pc.groupby(["p_strategy", "p_k"], sort=True):
        base = g[g.p_opts == "plain"]
        if base.empty:
            continue
        b = float(base.p50.iloc[0])
        line = [f"{st} @ {k}"]
        for o in ("count", "count+facet_low", "count+facet_high"):
            m = g[g.p_opts == o]
            line.append("-" if m.empty else f"+{m.p50.iloc[0] - b:,.0f} ms ({100 * (m.p50.iloc[0] - b) / b:+.0f}%)")
        rows.append(line)
    out += ["**Extra cost against the plain page (p50)**", ""] + md(["case", "count: total", "+ facet, low cardinality", "+ facet, high cardinality"], rows)
    rows = []
    for k, g in pc.groupby("p_k"):
        a, b = g[(g.p_strategy == "AC") & (g.p_opts == "count")], g[(g.p_strategy == "B100") & (g.p_opts == "count")]
        rows.append([k, "-" if a.empty else f0(a.total_med.iloc[0]), "-" if b.empty else f0(b.total_med.iloc[0]),
                     "-" if a.empty or b.empty else ("yes" if a.total_med.iloc[0] == b.total_med.iloc[0] else "NO")])
    out += ["**Is the total of search-first exact?** Chunked A sums disjoint chunks, so its total is exact. Search-first counts the unfiltered search matches.", ""]
    out += md(["filter size (IDs)", "chunked A total", "search-first total", "equal"], rows)
    rows = []
    for k, g in pc.groupby("p_k"):
        line = [k]
        for o in ("plain", "count", "count+facet_low", "count+facet_high"):
            a, b = g[(g.p_strategy == "AC") & (g.p_opts == o)], g[(g.p_strategy == "B100") & (g.p_opts == o)]
            line.append("-" if a.empty or b.empty else ("A" if a.p50.iloc[0] < b.p50.iloc[0] else "B100"))
        rows.append(line)
    out += ["**Cheaper strategy by p50 (cost only, ignoring completeness)**", ""] + md(["IDs", "plain", "count", "count + low facet", "count + high facet"], rows)
    return out


def sec_tuning(runs, scale, bad) -> list[str]:
    df = collect(runs, scale, "tuning", bad)
    out = [f"### Q4 Chunk size and in-flight chunks ({scale})", ""]
    if df.empty:
        return out + ["pending: no data", ""]
    pc = per_case(df, ["p_k", "p_chunk", "p_inflight"])
    for k, g in pc.groupby("p_k"):
        g = g.copy()
        g["chunk"] = g.p_chunk
        g["inflight"] = g.p_inflight
        h, rows = pivot(g, "chunk", "inflight")
        out += [f"**{k:,} IDs, p50 ms (columns: chunks in flight)**", ""] + md(h, rows)
        h, rows = pivot(g, "chunk", "inflight", "p95")
        out += [f"**{k:,} IDs, p95 ms**", ""] + md(h, rows)
        best = g.sort_values("p50").iloc[0]
        ref = g[(g.p_chunk == 1000) & (g.p_inflight == 8)]
        if len(ref):
            r0 = ref.iloc[0]
            overlap = best.p50_lo <= r0.p50_hi and r0.p50_lo <= best.p50_hi
            out.append(f"Best pair at {k:,} IDs: chunk {best.p_chunk}, {best.p_inflight} in flight (p50 {f0(best.p50)} ms). Pair 1000 x 8: p50 {f0(r0.p50)} ms; "
                       f"the 95% intervals of the two {'overlap' if overlap else 'do not overlap'}.")
        out.append("")
    per = per_case(df, ["run", "p_k", "p_chunk", "p_inflight"])
    rows = []
    for k, g in per.groupby("p_k"):
        w = {run: f"{gg.sort_values('p50').p_chunk.iloc[0]}x{gg.sort_values('p50').p_inflight.iloc[0]}" for run, gg in g.groupby("run")}
        rows.append([k, json.dumps(w), stable(w)])
    out += ["**Best pair per run**", ""] + md(["IDs", "best chunk x in-flight per run", "stability"], rows)
    return out


def sec_selectivity(runs, scale, bad) -> list[str]:
    df = collect(runs, scale, "selectivity", bad)
    out = [f"### Q5 The selectivity rule and the overfetch factor ({scale})", ""]
    if df.empty:
        return out + ["pending: no data", ""]
    n = SCALES.get(scale, 0)
    pc = per_case(df, ["p_term", "p_strategy", "p_k"])
    for term, g in pc.groupby("p_term"):
        ks = sorted(g.p_k.unique())
        rows = []
        flips = {}
        for k in ks:
            a = g[(g.p_strategy == "AC") & (g.p_k == k)]
            ref = float(a.results_med.iloc[0]) if len(a) else (LIMIT if term == "frequent" else float("nan"))
            line = [f"{100 * k / n:.2f}% ({k:,})", "-" if a.empty else f0(a.p50.iloc[0])]
            for fac in (50, 100, 200, 500):
                b = g[(g.p_strategy == f"B{fac}") & (g.p_k == k)]
                if b.empty:
                    line.append("-")
                    continue
                full = float((df[(df.p_term == term) & (df.p_strategy == f"B{fac}") & (df.p_k == k) & (df.warm == 0) & (df.ok == 1)].results >= min(LIMIT, ref)).mean()) if ref == ref else float("nan")
                line.append(f"{f0(b.p50.iloc[0])} ms, full {full:.0%}" if full == full else f"{f0(b.p50.iloc[0])} ms, full n/a")
                if full == full and full >= 0.95 and len(a) and b.p50.iloc[0] > a.p50.iloc[0]:
                    flips.setdefault(fac, []).append(k)
            rows.append(line)
        out += [f"**Term `{term}`: p50 and share of repetitions with a full page (against the reference count)**", ""]
        out += md(["selectivity (IDs)", "chunked A p50 ms", "B50", "B100", "B200", "B500"], rows)
        if flips:
            out.append("B complete but slower than chunked A (the rule should flip to A there): " + "; ".join(f"B{f}: at {', '.join(f'{k:,}' for k in v)} IDs" for f, v in flips.items()))
        else:
            out.append("No tested point where a complete B is slower than chunked A (A was measured up to 100,000 IDs only).")
        out.append("")
    per = per_case(df, ["run", "p_term", "p_strategy", "p_k"])
    rows = []
    for fac in (50, 100, 200, 500):
        w = {}
        for run, g in per.groupby("run"):
            sub = g[(g.p_term == "frequent") & (g.p_strategy == f"B{fac}") & (g.full_page >= 0.95)]
            w[run] = f"{100 * sub.p_k.min() / n:.2f}%" if len(sub) else "never in grid"
        rows.append([f"B{fac}", json.dumps(w), stable(w)])
    out += ["**Smallest selectivity at which B returns a full page in at least 95% of repetitions (term `frequent`), per run**", ""] + md(["variant", "per run", "stability"], rows)
    return out


def sec_prefilter(runs, scale, bad) -> list[str]:
    df = collect(runs, scale, "prefilter", bad)
    out = [f"### Q6 Early-exit prefilter and the selectivity estimate ({scale})", ""]
    if df.empty:
        return out + ["pending: no data", ""]
    pc = per_case(df, ["p_op", "p_layout", "p_k"])
    pc["case"] = pc.p_layout + " @ " + pc.p_k.astype(str)
    pc = pc.sort_values(["p_layout", "p_k"])
    h, rows = pivot(pc[pc.p_op != "estimate"], "case", "p_op")
    out += ["**p50 ms by operation** (`early` = find stopping at cap + 1; `full_ids` = find of every matching ID; `count_full` = count_documents; `count_cap` = count stopping at cap + 1; `count_threshold` = count stopping at the number the 1% rule needs)", ""] + md(h, rows)
    est = pc[pc.p_op == "estimate"]
    if len(est):
        out.append(f"`estimated_document_count`: p50 {f1(est.p50.iloc[0])} ms.")
        out.append("")
    rows = []
    for (lay, k), g in pc[pc.p_op != "estimate"].groupby(["p_layout", "p_k"]):
        e, f, c = g[g.p_op == "early"], g[g.p_op == "full_ids"], g[g.p_op == "count_threshold"]
        if e.empty:
            continue
        rows.append([lay, k, f1(e.p50.iloc[0]), "-" if f.empty else f1(f.p50.iloc[0]), "-" if f.empty else f"{f.p50.iloc[0] / max(e.p50.iloc[0], 1e-9):.1f}x",
                     "-" if c.empty else f1(c.p50.iloc[0])])
    out += ["**Early exit against reading every ID, and the cost of the estimate**", ""] + md(["layout", "matching IDs", "early exit p50 ms", "all IDs p50 ms", "all / early", "threshold count p50 ms"], rows)
    return out


def sec_noise_phase(runs, scale, bad) -> list[str]:
    df = collect(runs, scale, "noise", bad)
    out = [f"### Q7 Noise: repetitions and confidence of p95 and p99 for the cap-deciding cases ({scale})", ""]
    if df.empty:
        return out + ["pending: no data", ""]
    pc = per_case(df, ["p_k"])
    rows = []
    for _, r in pc.iterrows():
        w95 = 100 * (r.p95_hi - r.p95_lo) / 2 / r.p95 if r.p95 else float("nan")
        w99 = 100 * (r.p99_hi - r.p99_lo) / 2 / r.p99 if r.p99 else float("nan")
        rows.append([f"{r.p_k:,}", r.n, r.n_clean, ci(r.p50, r.p50_lo, r.p50_hi), ci(r.p95, r.p95_lo, r.p95_hi), f"+-{w95:.0f}%", ci(r.p99, r.p99_lo, r.p99_hi), f"+-{w99:.0f}%",
                     f1(r.p95_clean), "/".join(f0(x) for x in r.run_p95)])
    out += md(["IDs", "n (all runs)", "n without noisy windows", "p50 [95% CI] ms", "p95 [95% CI] ms", "p95 half-width", "p99 [95% CI] ms", "p99 half-width", "p95 without noisy ms", "p95 per run"], rows)
    return out


def sec_load(runs, scale, bad_by) -> tuple[list[str], dict]:
    out = [f"### Q1 Open-loop concurrent load ({scale})", ""]
    knees: dict[str, str] = {}
    any_data = False
    for name, r in runs.items():
        s = r["scales"].get(scale)
        if not s or s["load"].empty or not s["load_summary"]:
            continue
        any_data = True
        ld = flag(s["load"].copy(), bad_by.get((name, scale), set()))
        side = s["side"]
        rows = []
        base_p95 = None
        knee = "none in tested range"
        for st in s["load_summary"]:
            rate = st["rate"]
            g = ld[(ld.rate == rate)]
            ok = g[g.ok == 1]
            sm = summarize(ok.ms.to_numpy(), (50, 95, 99))
            clean = ok[~ok.noisy]
            eng = side[(side.kind == "engine") & (side.rate == rate)] if not side.empty else pd.DataFrame()
            cli = side[(side.kind == "client") & (side.rate == rate)] if not side.empty else pd.DataFrame()
            lag = side[(side.kind == "loop_lag") & (side.rate == rate)] if not side.empty else pd.DataFrame()
            fail = (st["error"] + st["timeout"] + st["dropped"]) / max(1, st["scheduled"])
            if base_p95 is None:
                base_p95 = sm["p95"]
            if knee.startswith("none") and (fail > 0.01 or sm["p95"] > 2 * base_p95):
                knee = f"{rate:g}/s"
            strat = ok.strategy.value_counts(normalize=True).round(2).to_dict() if "strategy" in ok else {}
            rows.append([f"{rate:g}", st["scheduled"], st["ok"], st["error"], st["timeout"], st["dropped"], f0(sm["p50"]), ci(sm["p95"], sm["p95_lo"], sm["p95_hi"]), ci(sm["p99"], sm["p99_lo"], sm["p99_hi"]),
                         f0(pct(clean.ms.to_numpy(), 95)), "-" if eng.empty else f"{eng.cpu_pct.mean():.0f} / {eng.cpu_pct.max():.0f}", "-" if eng.empty else f"{eng.mem_mib.max():,.0f}",
                         "-" if cli.empty else f"{cli.cpu_pct.mean():.0f}", "-" if lag.empty else f1(pct(lag.ms.to_numpy(), 99)), json.dumps(strat)])
        knees[name] = knee
        out += [f"**Run `{name}`.** Latency is measured from the scheduled arrival (queueing counts). Engine CPU is the container's percent of one core (the container limit applies).", ""]
        out += md(["rate q/s", "scheduled", "ok", "errors", "timeouts", "dropped", "p50 ms", "p95 ms [CI]", "p99 ms [CI]", "p95 ms no noisy", "engine CPU % mean / max", "engine mem MiB max",
                   "client CPU % mean", "loop lag p99 ms", "strategy share"], rows)
        # per query kind at each rate (p95)
        kinds = sorted(ld.kind.unique())
        rows = []
        for st in s["load_summary"]:
            g = ld[(ld.rate == st["rate"]) & (ld.ok == 1)]
            rows.append([f"{st['rate']:g}"] + [f0(pct(g[g.kind == k].ms.to_numpy(), 95)) for k in kinds])
        out += [f"p95 ms by query type, run `{name}`:", ""] + md(["rate q/s"] + kinds, rows)
    if not any_data:
        return out + ["pending: no data", ""], knees
    out.append("Knee = first rate where the failure share exceeds 1% or p95 exceeds 2x the lowest-rate p95. Per run: " + json.dumps(knees) + f" ({stable(knees)} across runs).")
    out.append("")
    return out, knees


# ---- disk projection -------------------------------------------------------------------------------------


def project_disk(dirs: list[Path], safety: float, min_free_gb: float, target: str) -> dict:
    found: dict[str, dict] = {}
    for d in dirs:
        for sc in ("100k", "1m"):
            j = _jread(d / f"disk_{sc}.json")
            if j and (sc not in found or j["peak_used_bytes"] > found[sc]["peak_used_bytes"]):
                found[sc] = j
    if len(found) < 2:
        return {"attempt": False, "reason": "disk measurements for both 100k and 1m are required", "have": sorted(found)}
    small, big = found["100k"], found["1m"]
    n_s, n_b, n_t = SCALES["100k"], SCALES["1m"], SCALES[target]
    slope = (big["peak_used_bytes"] - small["peak_used_bytes"]) / (n_b - n_s)
    projected = big["peak_used_bytes"] + slope * (n_t - n_b)
    safe = projected * safety
    free = small["start_free_bytes"]  # the first load starts from a fresh runner: that is the free space a 10M job begins with
    left = free - safe
    cu = big.get("container_usage_bytes") or {}
    steady = None
    if cu.get("mongod") and cu.get("mongot") and (small.get("container_usage_bytes") or {}).get("mongod"):
        su = small["container_usage_bytes"]
        steady = sum(cu[k] + (n_t - n_b) * (cu[k] - su[k]) / (n_b - n_s) for k in ("mongod", "mongot"))
    res = {"target": target, "peak_used_100k_bytes": small["peak_used_bytes"], "peak_used_1m_bytes": big["peak_used_bytes"], "slope_bytes_per_doc": slope,
           "projected_peak_bytes": projected, "safety_factor": safety, "projected_peak_with_safety_bytes": safe, "free_at_start_bytes": free,
           "projected_free_left_bytes": left, "min_free_required_bytes": min_free_gb * 2**30,
           "steady_state_projection_bytes_extrapolated": steady,
           "attempt": left >= min_free_gb * 2**30,
           "method": "linear extrapolation of the measured peak disk use (load plus index build) from 100k and 1M; an extrapolation, not a measurement"}
    res["reason"] = ("projected free space after the peak is at least the minimum" if res["attempt"] else "projected free space after the peak is below the minimum: do not attempt 10M")
    return res


def sec_variance(runs, scale, bad) -> list[str]:
    """Between-run variance of p50 and p95 for every case: the check on whether a result is stable."""
    out = [f"### Between-run variance ({scale}): p50 and p95 per run, spread = (max - min) / median of the per-run values", ""]
    rows = []
    for phase in ("calibration", "layouts", "count_facets", "tuning", "selectivity", "prefilter", "noise"):
        df = collect(runs, scale, phase, bad)
        if df.empty:
            continue
        pc = per_case(df, ["case"])
        for _, r in pc.iterrows():
            rows.append([phase, r.case, r.n, r.runs, "/".join(f0(x) for x in r.run_p50), spread(r.run_p50), "/".join(f0(x) for x in r.run_p95), spread(r.run_p95)])
    if not rows:
        return out + ["pending: no data", ""]
    return out + md(["phase", "case", "n", "runs", "p50 per run (ms)", "p50 spread", "p95 per run (ms)", "p95 spread"], rows)


def sec_disk(dirs: list[Path]) -> list[str]:
    out = ["## Disk use and the 10M decision", ""]
    rows = []
    for d in dirs:
        for sc in ("smoke", "100k", "1m", "10m"):
            j = _jread(d / f"disk_{sc}.json")
            if j:
                cu = j.get("container_usage_bytes") or {}
                rows.append([d.name, sc, f"{j['peak_used_bytes'] / 2**30:.2f}", f"{j['start_free_bytes'] / 2**30:.1f}", f"{(cu.get('mongod') or 0) / 2**30:.2f}", f"{(cu.get('mongot') or 0) / 2**30:.2f}"])
    out += md(["run", "scale", "peak disk used GiB (load + index build)", "free at start GiB", "mongod files GiB", "search index files GiB"], rows)
    proj = project_disk(dirs, 1.25, 2.0, "10m")
    out += ["Projection to 10M from the 100k and 1M measurements (computed now from the files above; the workflow step `project` writes the same decision to `disk_projection.json`):", "", "```json", json.dumps(proj, indent=1), "```", ""]
    return out


# ---- stability of the headline conclusions ---------------------------------------------------------------


def analyze(dirs: list[Path], noise_factor: float, scale_filter: str = "") -> str:
    runs = load_runs(dirs, scale_filter)
    out = ["# Narrowing follow-up: analysis tables", "",
           f"Generated by `nbench analyze` from {len(runs)} run director{'y' if len(runs) == 1 else 'ies'}: {', '.join(runs)}. "
           "Every number is computed from the raw files. If a directory is a smoke run, none of the numbers is a result.", ""]
    out += sec_environment(runs)
    noise_out, bad = sec_noise(runs, noise_factor)
    out += noise_out
    scales = sorted({sc for r in runs.values() for sc in r["scales"]}, key=lambda s: SCALES.get(s, 0))
    for sc in scales:
        bad_by_run = {name: bad.get((name, sc), set()) for name in runs}
        out += [f"## Scale {sc} ({SCALES.get(sc, 0):,} documents)", ""]
        for sec in (sec_calibration, sec_layouts, sec_count_facets, sec_tuning, sec_selectivity, sec_prefilter, sec_noise_phase, sec_variance):
            out += sec(runs, sc, bad_by_run)
        load_out, _ = sec_load(runs, sc, bad)
        out += load_out
        for name, r in runs.items():
            li = r["scales"].get(sc, {}).get("load_info")
            if li:
                out.append(f"Load `{name}`: {li.get('load_s')} s load, {li.get('btree_index_s')} s b-tree indexes, {li.get('search_index_s')} s until the search index is queryable.")
        out.append("")
    out += sec_disk(dirs)
    # reps and completeness
    out += ["## Repetitions and completeness", ""]
    rows = []
    for name, r in runs.items():
        for key, st in r["progress"].items():
            rows.append([name, key, st.get("rounds_measured", "-"), "yes" if st.get("complete") else "NO (cut short)", st.get("seed")])
    out += md(["run", "scale:phase", "measured repetitions per case", "complete", "seed"], rows)
    return "\n".join(out) + "\n"
