"""Analysis: raw run directories -> report tables (Markdown + JSON).

Reads every run directory given, pools repetitions, and writes `report-tables.md` and `summary.json`:
- percentiles with a bootstrap confidence interval over repetitions;
- results with and without the noisy windows flagged by the canary (nothing is dropped from the raw data);
- the verdict for NFR-02, NFR-03, NFR-04 and NFR-05, per run and overall;
- between-run variance and which conclusions are stable.
Warm-up samples are excluded from the statistics and stay in the raw files.
"""

import gzip
import json
import math
import re
from collections import defaultdict
from pathlib import Path

import numpy as np

FIELDS = ["ts", "lat_ms", "lag_ms", "total_ms", "token_ms", "marker_ms", "data_ms", "commit_ms", "cleanup_ms", "wait_ms",
          "engine_lb_ms", "engine_ub_ms", "ovh_lb_ms", "ovh_ub_ms", "attempts", "queries"]
NFR02_MS, NFR03_MS = 50.0, 15.0
NOISE_BUCKET_S = 5
RNG = np.random.default_rng(12345)


# -- loading ---------------------------------------------------------------------------------------------------------
class Run:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.meta = json.loads((path / "meta.json").read_text()) if (path / "meta.json").exists() else {}
        self.env = json.loads((path / "environment.json").read_text()) if (path / "environment.json").exists() else {}
        self.name = self.meta.get("run_id", path.name)
        self.smoke = bool(self.meta.get("smoke"))
        self._canary: dict[str, dict] = {}

    def jsonl(self, rel: str) -> list[dict]:
        p = self.path / rel
        if not p.exists():
            return []
        return [json.loads(x) for x in p.read_text().splitlines() if x.strip()]

    def json(self, rel: str):
        p = self.path / rel
        return json.loads(p.read_text()) if p.exists() else None

    def case_runs(self, phase: str) -> list[dict]:
        return self.jsonl(f"{phase}/cases.jsonl")

    def rows(self, phase: str, case: str, rep: int, fields=FIELDS) -> dict[str, np.ndarray]:
        cols: dict[str, list] = {f: [] for f in fields}
        warm: list = []
        for p in sorted((self.path / phase).glob(f"{case}__r{rep}.s*.jsonl.gz")):
            with gzip.open(p, "rt") as f:
                for line in f:
                    r = json.loads(line)
                    if r.get("err"):
                        r.setdefault("ok", False)
                    for k in fields:
                        cols[k].append(r.get(k, math.nan))
                    warm.append(bool(r.get("warm")) or r.get("ok") is False and False)
        out = {k: np.array(v, dtype=float) for k, v in cols.items()}
        out["warm"] = np.array(warm, dtype=bool)
        return out

    def noisy_buckets(self, phase: str, cpu_factor: float, db_factor: float) -> tuple[set[int], dict]:
        """Time buckets (epoch // 5 s) in which the canary was well above its own median."""
        key = f"{phase}"
        if key in self._canary:
            return self._canary[key]["set"], self._canary[key]["info"]
        samples = []
        for p in (self.path / phase).glob("canary*.jsonl"):
            samples += [json.loads(x) for x in p.read_text().splitlines() if x.strip()]
        if not samples:
            self._canary[key] = {"set": set(), "info": {"samples": 0}}
            return set(), {"samples": 0}
        ts = np.array([s["ts"] for s in samples])
        cpu = np.array([s["cpu_ms"] for s in samples])
        db = np.array([s["db_ms"] for s in samples], dtype=float)
        mc, md = float(np.median(cpu)), float(np.nanmedian(db))
        bucket = (ts // NOISE_BUCKET_S).astype(int)
        flagged = set()
        for b in np.unique(bucket):
            m = bucket == b
            if np.median(cpu[m]) > cpu_factor * mc or np.nanmedian(db[m]) > db_factor * md:
                flagged.add(int(b))
        info = {"samples": len(samples), "cpu_median_ms": mc, "db_median_ms": md, "buckets": int(len(np.unique(bucket))),
                "noisy_buckets": len(flagged), "cpu_factor": cpu_factor, "db_factor": db_factor}
        self._canary[key] = {"set": flagged, "info": info}
        return flagged, info


# -- statistics ------------------------------------------------------------------------------------------------------
def q(a: np.ndarray, p: float) -> float:
    a = a[~np.isnan(a)]
    return float(np.percentile(a, p)) if len(a) else math.nan


def boot_ci(vals: list[float], reps: int = 2000) -> tuple[float, float]:
    v = np.array([x for x in vals if not math.isnan(x)])
    if len(v) < 3:
        return math.nan, math.nan
    med = np.median(RNG.choice(v, size=(reps, len(v)), replace=True), axis=1)
    return float(np.percentile(med, 2.5)), float(np.percentile(med, 97.5))


class Metric:
    """Per-repetition percentiles of one field of one case, pooled over the given runs."""

    def __init__(self, runs: list[Run], phase: str, case: str, field: str, cpu_f: float, db_f: float,
                 exclude_noisy: bool = False) -> None:
        self.per_run: dict[str, dict] = {}
        pooled: list[np.ndarray] = []
        rep_p50, rep_p95 = [], []
        self.n_noisy = self.n_total = 0
        for run in runs:
            cs = [c for c in run.case_runs(phase) if c["case"] == case]
            if not cs:
                continue
            noisy, _ = run.noisy_buckets(phase, cpu_f, db_f)
            r50, r95 = [], []
            for c in cs:
                d = run.rows(phase, case, c["rep"])
                keep = ~d["warm"]
                vals = d[field]
                if "ts" in d:
                    mask = np.array([int(t // NOISE_BUCKET_S) in noisy for t in d["ts"]]) if len(d["ts"]) else keep
                    self.n_noisy += int(np.sum(keep & mask))
                    self.n_total += int(np.sum(keep))
                    if exclude_noisy:
                        keep = keep & ~mask
                v = vals[keep]
                v = v[~np.isnan(v)]
                if len(v):
                    pooled.append(v)
                    r50.append(q(v, 50))
                    r95.append(q(v, 95))
            if r50:
                self.per_run[run.name] = {"p50": float(np.median(r50)), "p95": float(np.median(r95)), "reps": len(r50)}
                rep_p50 += r50
                rep_p95 += r95
        allv = np.concatenate(pooled) if pooled else np.array([])
        self.n = len(allv)
        self.reps = len(rep_p50)
        self.p50, self.p95, self.p99, self.max = q(allv, 50), q(allv, 95), q(allv, 99), (float(allv.max()) if len(allv) else math.nan)
        self.p50_ci = boot_ci(rep_p50)
        self.p95_ci = boot_ci(rep_p95)
        self.p95_rep_median = float(np.median(rep_p95)) if rep_p95 else math.nan

    @property
    def noisy_share(self) -> float:
        return self.n_noisy / self.n_total if self.n_total else math.nan


def f(x, d=1) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "n/a"
    return f"{x:.{d}f}"


def ci(c) -> str:
    return "n/a" if math.isnan(c[0]) else f"{c[0]:.1f}..{c[1]:.1f}"


def md_table(header: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def spread(vals: list[float]) -> str:
    v = [x for x in vals if not math.isnan(x)]
    if len(v) < 2:
        return "n/a (one run)"
    med = float(np.median(v))
    return f"{min(v):.1f}..{max(v):.1f} (spread {100 * (max(v) - min(v)) / med:.0f}% of median)" if med else "n/a"


# -- report sections -------------------------------------------------------------------------------------------------
def case_names(runs: list[Run], phase: str) -> list[str]:
    return sorted({c["case"] for r in runs for c in r.case_runs(phase)})


def sec_environment(runs: list[Run]) -> str:
    rows = []
    for r in runs:
        e = r.env
        cont = e.get("container", {})
        rows.append([r.name, "smoke" if r.smoke else "full", r.meta.get("scale"), r.meta.get("seed"),
                     e.get("date_utc", ""), e.get("nproc"), (e.get("lscpu_model") or "").replace("Model name:", "").strip()[:40],
                     cont.get("cpus"), cont.get("memory_limit_bytes"), e.get("engine", {}).get("mongod"),
                     e.get("python"), e.get("pymongo"), e.get("workflow_run_url") or ""])
    return "## Environment (from environment.json)\n\n" + md_table(
        ["run", "kind", "scale", "seed", "date (UTC)", "nproc", "cpu", "container cpus", "container memory (bytes)", "mongod",
         "python", "pymongo", "workflow run"], rows)


def sec_canary(runs: list[Run], phases: list[str], cf: float, df: float) -> str:
    rows = []
    for r in runs:
        for ph in phases:
            _, info = r.noisy_buckets(ph, cf, df)
            if info.get("samples"):
                rows.append([r.name, ph, info["samples"], f(info["cpu_median_ms"], 2), f(info["db_median_ms"], 2),
                             f"{info['noisy_buckets']} of {info['buckets']}"])
    return ("## Canary and noisy windows\n\nThe canary runs in its own process at 4 Hz. A 5-second window is noisy when the "
            f"canary's CPU loop median is above {cf}x, or its database read above {df}x, its own median for the phase. "
            "At high write rates the benchmark itself loads the machine, so a noisy flag can be self-inflicted; read both "
            "columns of each table.\n\n" + md_table(["run", "phase", "canary samples", "cpu median (ms)", "db median (ms)", "noisy windows"], rows))


def sec_clauses(runs: list[Run]) -> str:
    rows = []
    for r in runs:
        c = r.json("clauses/clauses.json")
        if not c:
            continue
        for shape, v in c.items():
            if isinstance(v, dict):
                rows.append([r.name, shape, v["max_documents_ok"], v["first_failure"], v["clauses_per_document"],
                             v["tested_up_to"], (v["error"] or "")[:90]])
    return "## Q4 clause limit per probe shape (observed, not a timing)\n\n" + md_table(
        ["run", "shape", "max documents in one query", "first failure at", "clauses per document", "tested up to", "error"], rows)


def sec_shapes(runs: list[Run]) -> str:
    agg = defaultdict(list)
    for r in runs:
        for c in r.case_runs("shapes"):
            agg[(c["case"], r.name)].append(c)
    keys = sorted({k[0] for k in agg}, key=lambda k: (int(k.split("|")[1]), k))
    rows = []
    for k in keys:
        allc = [c for (kk, _), cs in agg.items() if kk == k for c in cs]
        ok = [c for c in allc if c.get("ok")]
        v = np.array([c["verify_ms"] for c in ok])
        sc = np.array([c["steady_cost_ms"] for c in ok if "steady_cost_ms" in c])
        name, n = k.split("|")
        fails = len(allc) - len(ok)
        err = next((c.get("error") for c in allc if c.get("error")), "")
        rows.append([n, name, allc[0]["clauses"], f"{len(ok)}/{len(allc)}", f(q(v, 50)), f(q(v, 95)),
                     f(q(sc, 50)), f(float(np.median([c['queries'] for c in ok])) if ok else math.nan, 0), err or ""])
    return ("## Q4 probe shapes: time to verify N documents\n\n`verify` is from the write acknowledgement until every "
            "document is verified, so it includes the engine's indexing latency and the polling schedule; `steady cost` is "
            "the same probe repeated once everything is visible, so it is the probe's own cost. Both are pending "
            "runner data when this table is empty.\n\n"
            + md_table(["N docs", "shape-result-chunk-concurrency", "clauses", "ok runs", "verify p50 (ms)", "verify p95 (ms)",
                        "steady cost p50 (ms)", "queries (median)", "error"], rows))


def sec_semantics(runs: list[Run]) -> str:
    rows = []
    for r in runs:
        s = r.json("semantics/semantics.json")
        if s:
            rows += [[r.name, k, v] for k, v in s.items()]
    return "## Token semantics (observed)\n\n" + md_table(["run", "check", "result"], rows)


def sec_counts(runs: list[Run]) -> tuple[str, dict]:
    rows, verdict = [], {"nfr04": [], "nfr05": []}
    for r in runs:
        for c in r.json("counts/counts.json") or []:
            rows.append([r.name, c["scenario"], c["writes"], c["waits"], c["search_queries"], c["attempts"], c["expected_chunks"],
                         json.dumps(c["commands"]), c.get("pass_nfr04", c.get("pass_nfr05", ""))])
            if "pass_nfr04" in c:
                verdict["nfr04"].append(c["pass_nfr04"])
            if "pass_nfr05" in c:
                verdict["nfr05"].append(c["pass_nfr05"])
    text = ("## Q5 and Q6 command counts (driver command monitoring)\n\n`waits` is the number of verification waits issued. A "
            "bulk write shows `update` commands because the driver sends update models as one `update` command: for the "
            "skipped bulk it is one command for the dirty marker plus one for the data.\n\n"
            + md_table(["run", "scenario", "writes", "waits", "search queries", "poll attempts", "chunks needed", "commands", "pass"], rows))
    return text, verdict


def metric_row(m: Metric) -> list:
    return [m.reps, m.n, f(m.p50), ci(m.p50_ci), f(m.p95), ci(m.p95_ci), f(m.p99), f(m.max)]


MH = ["reps", "samples", "p50", "p50 CI", "p95", "p95 CI", "p99", "max"]


def sec_tts(runs, cf, df) -> str:
    rows = []
    for case in [c for c in case_names(runs, "latency") if c.startswith("tts_")]:
        for fld, lab in (("engine_ub_ms", "upper bound"), ("engine_lb_ms", "lower bound")):
            a = Metric(runs, "latency", case, fld, cf, df)
            b = Metric(runs, "latency", case, fld, cf, df, exclude_noisy=True)
            rows.append([case, lab, *metric_row(a), f(b.p50), f(b.p95), f"{100 * a.noisy_share:.0f}%" if not math.isnan(a.noisy_share) else "n/a"])
    return ("## Q1 time to searchable (engine latency, context only, excluded from NFR-02)\n\nMilliseconds from the data write "
            "acknowledgement to the new revision being visible, with fine polling (2 to 10 ms). The true value lies "
            "between the lower and upper bound.\n\n"
            + md_table(["case", "bound", *MH, "p50 w/o noisy", "p95 w/o noisy", "samples in noisy windows"], rows))


def sec_overhead(runs, cf, df) -> tuple[str, dict]:
    rows, parts, verdict = [], [], {}
    for case in [c for c in case_names(runs, "latency") if c.startswith("strong_")]:
        up = Metric(runs, "latency", case, "ovh_ub_ms", cf, df)
        lo = Metric(runs, "latency", case, "ovh_lb_ms", cf, df)
        upx = Metric(runs, "latency", case, "ovh_ub_ms", cf, df, exclude_noisy=True)
        gen = Metric(runs, "latency", case, "lag_ms", cf, df)
        ok = up.p95 <= NFR02_MS if not math.isnan(up.p95) else None
        verdict[case] = {"p95_ub": up.p95, "pass": ok, "per_run_p95": {k: v["p95"] for k, v in up.per_run.items()}}
        rows.append([case, *metric_row(up), f(lo.p95), f(upx.p95), f(gen.p95), "yes" if ok else "NO" if ok is False else "n/a"])
        comp = {k: Metric(runs, "latency", case, k, cf, df).p95 for k in ("token_ms", "marker_ms", "data_ms", "commit_ms", "cleanup_ms")}
        comp["probe_overhead_ms"] = Metric(runs, "latency", case, "ovh_ub_ms", cf, df).p50 - sum(
            Metric(runs, "latency", case, k, cf, df).p50 for k in ("token_ms", "marker_ms", "data_ms", "commit_ms", "cleanup_ms"))
        parts.append([case, *[f(comp[k]) for k in ("token_ms", "marker_ms", "data_ms", "commit_ms", "cleanup_ms")], f(comp["probe_overhead_ms"])])
    text = ("## Q2 Hoodie's own overhead in STRONG mode (NFR-02, target p95 <= 50 ms)\n\nOverhead = the write call's total time "
            "minus the engine's time to searchable. The engine time is bounded (see Q1), so the overhead is a range: the "
            "**upper bound** (conservative) is used for the verdict, the lower bound is the optimistic reading. `gen lag p95` "
            "is the load generator's own lateness: values above a few ms mean the client, not the system under test, was "
            "saturated. The upper bound is pessimistic by at most one polling interval plus one query: the wait between "
            "the revision becoming visible and the next poll is counted as overhead on purpose, because it is a cost of "
            "the polling schedule, but it makes the verdict depend on the schedule; compare the `default` schedule with "
            "the recommended one.\n\n"
            + md_table(["case", *MH, "p95 lower bound", "p95 upper bound w/o noisy", "gen lag p95", "meets 50 ms"], rows)
            + "\nSplit by part (p95 of each part; the last column is the median of the residual: probe issuing and polling):\n\n"
            + md_table(["case", "token", "marker upsert", "data write", "commit", "marker cleanup", "probe residual (p50)"], parts))
    return text, verdict


def sec_eventual(runs, cf, df) -> tuple[str, dict]:
    rows, verdict = [], {}
    names = case_names(runs, "latency")
    sims = [c for c in names if c.startswith("eventual_") and "_sim_" in c]
    for s in sims:
        p = s.replace("_sim_", "_plain_")
        if p not in names:
            continue
        a, b = Metric(runs, "latency", s, "total_ms", cf, df), Metric(runs, "latency", p, "total_ms", cf, df)
        ax, bx = (Metric(runs, "latency", s, "total_ms", cf, df, True), Metric(runs, "latency", p, "total_ms", cf, df, True))
        added95, added50 = a.p95 - b.p95, a.p50 - b.p50
        # confidence interval of the added p95 over repetition pairs
        reps = [(a.p95_rep_median, b.p95_rep_median)]
        ok = added95 <= NFR03_MS if not math.isnan(added95) else None
        verdict[s] = {"added_p95": added95, "pass": ok, "per_run": {k: a.per_run[k]["p95"] - b.per_run[k]["p95"]
                                                                  for k in a.per_run if k in b.per_run}}
        rows.append([s.replace("_sim_", "_"), a.reps, a.n, f(b.p50), f(b.p95), f(a.p50), f(a.p95), f(added50), f(added95),
                     f(ax.p95 - bx.p95), "yes" if ok else "NO" if ok is False else "n/a"])
    text = ("## Q3 overhead in EVENTUAL mode (NFR-03, target added p95 <= 15 ms)\n\nThe same data write with and without the "
            "token and the marker upsert, run interleaved in randomized order. Added = p95 of the write with Hoodie minus p95 "
            "of the plain write (and the same for the median).\n\n"
            + md_table(["case", "reps", "samples", "plain p50", "plain p95", "hoodie p50", "hoodie p95", "added p50", "added p95",
                        "added p95 w/o noisy", "meets 15 ms"], rows))
    return text, verdict


def sec_coalesce(runs, cf, df) -> str:
    rows = []
    for case in case_names(runs, "coalesce"):
        m = Metric(runs, "coalesce", case, "lat_ms", cf, df)
        o = Metric(runs, "coalesce", case, "ovh_ub_ms", cf, df)
        qs, secs = [], []
        for r in runs:
            for c in r.case_runs("coalesce"):
                if c["case"] == case:
                    dur = c["spec"]["duration_s"] + c["spec"]["warm_s"]
                    qs.append(c["search_queries"] / dur)
                    secs.append(c["failed"])
        rows.append([case, m.reps, f(float(np.median(qs)) if qs else math.nan, 0), f(m.p50), f(m.p95), f(o.p95), sum(secs)])
    return ("## Q4 coalescing concurrent probes (a shared wait group) at the design rate\n\n"
            "`search queries/s` is counted by command monitoring over warm-up and measurement.\n\n"
            + md_table(["case", "reps", "search queries/s", "latency p50 (ms)", "latency p95 (ms)", "overhead p95 upper bound (ms)", "failed writes"], rows))


def sec_marker(runs: list[Run]) -> str:
    parts = []
    for r in runs:
        samples = r.jsonl("marker/samples.jsonl")
        cases = {c["case"]: c for c in r.case_runs("marker")}
        if not samples:
            continue
        rows = []
        for tag in ("clean", "failures"):
            s = [x for x in samples if x.get("tag") == tag and "count" in x]
            if not s:
                continue
            pick = [s[0], s[len(s) // 2], s[-1]]
            for lab, x in zip(("start", "middle", "end"), pick):
                rows.append([tag, lab, f"{(x['ts'] - s[0]['ts']) / 60:.1f}", x["count"], f"{x['size'] / 1e6:.2f}", f"{x['storage_size'] / 1e6:.2f}",
                             f"{x['total_index_size'] / 1e6:.2f}", json.dumps(x.get("index_sizes", {})), json.dumps(x.get("index_ops", {}))])
        c = [f"{k}: app writes {v['ops']}, server opcounters {v.get('server_opcounter_delta')}, failed ops {v['failed']}, commands {v['commands']}"
             for k, v in cases.items()]
        parts.append(f"### Run {r.name}\n\n" + md_table(["state", "sample", "minutes", "markers", "data MB", "storage MB", "index MB", "index sizes (bytes)", "index ops"], rows)
                     + "\n" + "\n".join(f"- {x}" for x in c) + "\n")
    return "## Q7 marker cost at scale\n\n" + ("\n".join(parts) if parts else "pending: run on the GitHub Actions runner\n")


def sec_failure(runs: list[Run]) -> str:
    parts = []
    for r in runs:
        d = r.json("failure/failure.json")
        if not d:
            continue
        rows = [[i, f(x["elapsed_s"], 2), f(x["over_timeout_ms"], 0), x["marker_state"], x["data_stored"], x["message_has_field_value"], x["type"]]
                for i, x in enumerate(d.get("blind_index_runs", []))]
        parts.append(f"### Run {r.name} (timeout {d['timeout_s']} s)\n\n"
                     + md_table(["repetition", "elapsed (s)", "over timeout (ms)", "marker", "data stored", "message leaks a value", "exception"], rows)
                     + f"\nTransaction partial failure: `{json.dumps(d.get('transaction_group'))}`\n\nEngine paused: `{json.dumps(d.get('pause'))}`\n")
    return "## Q8 failure behaviour\n\n" + ("\n".join(parts) if parts else "pending: run on the GitHub Actions runner\n")


def sec_m0(runs: list[Run]) -> str:
    parts = []
    for r in runs:
        d = r.json("m0/m0.json")
        if d:
            tts = [x for x in d.get("time_to_searchable_ub_ms", []) if x is not None]
            parts.append(f"- run {r.name}: ladder {json.dumps(d.get('max_ok_documents_tested_ladder'))}; time to searchable upper bound "
                         f"p50 {f(q(np.array(tts), 50))} ms, p95 {f(q(np.array(tts), 95))} ms over {len(tts)} writes")
        elif (r.path / "m0" / "skipped.json").exists():
            parts.append(f"- run {r.name}: skipped, HOODIE_BENCH_ATLAS_URL not set")
    return ("## Hosted Atlas M0 (informational, a different environment)\n\nM0 limits: 0.5 GB, 100 operations/s, "
            "500 connections, at most 3 search indexes; the harness is throttled to 40 operations/s.\n\n" + ("\n".join(parts) or "not run") + "\n")


def sec_variance(runs: list[Run], oh: dict, ev: dict, cf, df) -> tuple[str, dict]:
    stable = {}
    rows = []
    if len(runs) < 2:
        return "## Between-run variance\n\nOnly one run was given: variance cannot be computed (the protocol needs three).\n", stable
    for case, v in oh.items():
        vals = list(v["per_run_p95"].values())
        passes = [x <= NFR02_MS for x in vals]
        stable[case] = len(set(passes)) == 1
        rows.append([case, "NFR-02 overhead p95 upper bound", spread(vals), "stable" if stable[case] else "FLIPS between runs"])
    for case, v in ev.items():
        vals = list(v["per_run"].values())
        passes = [x <= NFR03_MS for x in vals]
        stable[case] = len(set(passes)) == 1
        rows.append([case, "NFR-03 added p95", spread(vals), "stable" if stable[case] else "FLIPS between runs"])
    for case in [c for c in case_names(runs, "latency") if c.startswith("tts_")]:
        m = Metric(runs, "latency", case, "engine_ub_ms", cf, df)
        rows.append([case, "engine time to searchable p50 / p95", f"p50 {spread([x['p50'] for x in m.per_run.values()])}; p95 {spread([x['p95'] for x in m.per_run.values()])}", ""])
    return ("## Between-run variance\n\nEach row compares the per-run medians over repetitions of the same case across the runs given. "
            "A verdict that differs between runs is not a conclusion.\n\n" + md_table(["case", "metric", "range over runs", "verdict"], rows)), stable


# -- main ------------------------------------------------------------------------------------------------------------
def analyze(run_dirs: list[Path], out: Path, cpu_factor: float = 1.5, db_factor: float = 3.0) -> None:
    runs = [Run(p) for p in run_dirs if (p / "meta.json").exists() or (p / "environment.json").exists()]
    if not runs:
        raise SystemExit("no run directories with meta.json found")
    if len({r.meta.get("scale") for r in runs}) > 1:
        raise SystemExit("runs of different scales given: analyze each scale separately")
    out.mkdir(parents=True, exist_ok=True)
    smoke = any(r.smoke for r in runs)
    parts = ["# Write-path benchmark: report tables\n"]
    if smoke:
        parts.append("> **SMOKE DATA.** At least one run is a smoke run (a few thousand documents, a few writes per second, on a "
                     "non-reference machine). It proves the pipeline works. Its timings are not results and must not be quoted.\n")
    parts.append(f"Runs: {', '.join(r.name for r in runs)}. Warm-up samples are excluded; every number states its repetitions.\n")
    parts.append(sec_environment(runs))
    lat_phases = [p for p in ("shapes", "latency", "coalesce", "marker") if any(r.case_runs(p) for r in runs)]
    parts.append(sec_canary(runs, lat_phases, cpu_factor, db_factor))
    parts.append(sec_clauses(runs))
    parts.append(sec_shapes(runs))
    parts.append(sec_semantics(runs))
    t5, cv = sec_counts(runs)
    parts.append(t5)
    parts.append(sec_tts(runs, cpu_factor, db_factor))
    t2, oh = sec_overhead(runs, cpu_factor, db_factor)
    parts.append(t2)
    t3, ev = sec_eventual(runs, cpu_factor, db_factor)
    parts.append(t3)
    parts.append(sec_coalesce(runs, cpu_factor, db_factor))
    parts.append(sec_marker(runs))
    parts.append(sec_failure(runs))
    var_text, stable = sec_variance(runs, oh, ev, cpu_factor, db_factor)
    parts.append(var_text)
    parts.append(sec_m0(runs))

    def agg(v: dict) -> str:
        vals = [x["pass"] for x in v.values() if x["pass"] is not None]
        return "n/a" if not vals else f"{sum(vals)} of {len(vals)} cases meet the target"

    verdict = {
        "NFR-02": agg(oh), "NFR-03": agg(ev),
        "NFR-04": ("pass" if cv["nfr04"] and all(cv["nfr04"]) else "n/a" if not cv["nfr04"] else "FAIL"),
        "NFR-05": ("pass" if cv["nfr05"] and all(cv["nfr05"]) else "n/a" if not cv["nfr05"] else "FAIL"),
        "stable_conclusions": sum(stable.values()), "unstable_conclusions": [k for k, v in stable.items() if not v],
        "smoke": smoke}
    parts.insert(2, "## Verdict by target\n\n" + md_table(["target", "result"], [[k, v] for k, v in verdict.items()]))
    (out / "report-tables.md").write_text("\n".join(parts))
    (out / "summary.json").write_text(json.dumps({"verdict": verdict, "nfr02": oh, "nfr03": ev}, indent=2, default=str))
    print(f"wrote {out / 'report-tables.md'}")
