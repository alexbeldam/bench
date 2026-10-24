"""Analysis: strategy x planted flake tables from the result files of one or more run directories (one directory per occasion).

Everything printed is computed from the files. Cells are `x/K pct [Wilson 95%]`. Wall time is measured where the strategy ran
and modeled (N x measured pass time) where it is derived from per-test run sequences; the cost table says which.
Billed minutes = sum over jobs of ceil((measured job set-up + wall) / 60) and are modeled, not billed: a public repository
bills nothing.
"""
from __future__ import annotations

import gzip
import json
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

from .exp import wilson

NS = (1, 5, 10, 20, 50, 100, 300)
KS = (10, 20, 50, 100, 300)


# ---- loading -----------------------------------------------------------------------------------------------------------


def load_dir(d: str) -> dict[str, Any]:
    out: dict[str, Any] = defaultdict(list)
    for p in sorted(Path(d).rglob("*")):
        if p.suffix == ".gz" and p.name.endswith(".json.gz"):
            obj = json.load(gzip.open(p, "rt", encoding="utf8"))
        elif p.suffix == ".json":
            obj = json.loads(p.read_text(encoding="utf8"))
        elif p.name.endswith(".jsonl") and "canary" in p.name:
            rows = [json.loads(x) for x in p.read_text(encoding="utf8").splitlines() if x.strip()]
            out["canary"].append({"name": p.name, "rows": rows})
            continue
        else:
            continue
        if not isinstance(obj, dict) or "kind" not in obj:
            continue
        obj["_name"] = p.name
        out[obj["kind"]].append(obj)
    return out


def fmt(x: int, n: int) -> str:
    if n == 0:
        return "n/a"
    lo, hi = wilson(x, n)
    return f"{x}/{n} {100 * x / n:.1f}% [{100 * lo:.1f}-{100 * hi:.1f}]"


def short(x: int, n: int) -> str:
    if n == 0:
        return "n/a"
    lo, hi = wilson(x, n)
    return f"{100 * x / n:.0f}% [{100 * lo:.0f}-{100 * hi:.0f}] K={n}"


def blocks(bits: str, n: int) -> list[bool]:
    return ["1" in bits[i : i + n] for i in range(0, len(bits) - n + 1, n)]


# ---- scorers over a run history ----------------------------------------------------------------------------------------


def _betacf(a: float, b: float, x: float) -> float:
    fpmin, c, d = 1e-300, 1.0, 1.0 - (a + b) * x / (a + 1)
    d = 1.0 / (fpmin if abs(d) < fpmin else d)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((a - 1 + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (fpmin if abs(d) < fpmin else d)
        c = 1.0 + aa / c
        c = fpmin if abs(c) < fpmin else c
        h *= d * c
        aa = -(a + m) * (a + b + m) * x / ((a + m2) * (a + 1 + m2))
        d = 1.0 + aa * d
        d = 1.0 / (fpmin if abs(d) < fpmin else d)
        c = 1.0 + aa / c
        c = fpmin if abs(c) < fpmin else c
        de = d * c
        h *= de
        if abs(de - 1.0) < 3e-14:
            break
    return h


def betainc(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta I_x(a, b)."""
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x))
    if x < (a + 1) / (a + b + 2):
        return bt * _betacf(a, b, x) / a
    return 1 - bt * _betacf(b, a, 1 - x) / b


def beta_tail(f: int, k: int, theta: float, a0: float = 1.0, b0: float = 1.0) -> float:
    """P(p > theta) for the posterior Beta(a0 + f, b0 + k - f) of a failure rate p after f failures in k runs."""
    return 1 - betainc(a0 + f, b0 + k - f, theta)


def flips(w: str) -> int:
    return sum(a != b for a, b in zip(w, w[1:]))


RULES = {
    "mixed (a pass and a failure in the window)": lambda w: "0" in w and "1" in w,
    "flips>=2": lambda w: flips(w) >= 2,
    "beta(1,1) P(p>0.02)>=0.9": lambda w: beta_tail(w.count("1"), len(w), 0.02) >= 0.9,
    "beta(0.1,20) P(p>0.005)>=0.9": lambda w: beta_tail(w.count("1"), len(w), 0.005, 0.1, 20.0) >= 0.9,
}


# ---- the experiment table ----------------------------------------------------------------------------------------------


class Book:
    """flags[strategy][run][test] = list of per-experiment booleans; cost[strategy][run] = dict."""

    def __init__(self) -> None:
        self.flags: dict[str, dict[int, dict[str, list[bool]]]] = defaultdict(lambda: defaultdict(dict))
        self.cost: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        self.order: list[str] = []
        self.note: dict[str, str] = {}

    def add(self, strat: str, run: int, test: str, fl: list[bool]) -> None:
        if strat not in self.order:
            self.order.append(strat)
        self.flags[strat][run].setdefault(test, [])
        self.flags[strat][run][test] = list(self.flags[strat][run][test]) + list(fl)

    def set_cost(self, strat: str, run: int, **kw: Any) -> None:
        self.cost[strat][run] = kw


def inm(o: dict[str, Any]) -> list[str]:
    return [t for t, l in o["labels"].items() if l["engine"] != "real"]


def nat(s: str) -> list[Any]:
    import re

    return [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", s)]


def mean_wall(exps: list[dict[str, Any]]) -> float:
    return statistics.fmean(e["wall_s"] for e in exps) if exps else float("nan")


def overhead(m: dict[str, Any]) -> float:
    try:
        return float(m.get("fl2p_setup_s", 0.0))
    except ValueError:
        return 0.0


def build(runs: list[dict[str, Any]]) -> tuple[Book, dict[str, Any]]:
    b = Book()
    info: dict[str, Any] = {"labels": {}, "machines": [], "pool_rate": {}, "pool_n": {}}
    for r, data in enumerate(runs):
        ms = [o["machine"] for k in data if k != "canary" for o in data[k] if isinstance(o, dict) and "machine" in o]
        info["machines"].append(ms[0] if ms else {})
        oh = overhead(ms[0]) if ms else 0.0
        for k in data:
            if k != "canary":
                for o in data[k]:
                    info["labels"].update(o.get("labels", {}))
        pass_wall = None
        # suites
        for o in data["suite"]:
            nm, st, ps, ar = o["_name"], o["strategy"], o["passes"], o["arm"]
            ex = o["exps"]
            if nm.startswith("noise-"):
                strat = f"S4 noise {ar} N=1"
                for t in inm(o):
                    b.add(strat, r, t, [t in e["passes"][0]["fail"] for e in ex])
                b.set_cost(strat, r, wall=mean_wall(ex), modeled=False, runners=1, overhead=oh)
                continue
            if st == "s0":
                strat = "S0 single run, default order"
                pass_wall = mean_wall(ex)
            elif ar == "jitter":
                strat = f"S5 random order x{ps} + loop jitter"
            elif ps == 1:
                strat = "S1 single run, random order"
            else:
                strat = f"S5 random order x{ps} seeds"
            for t in inm(o):
                b.add(strat, r, t, [any(t in p["fail"] for p in e["passes"]) for e in ex])
            b.set_cost(strat, r, wall=mean_wall(ex), modeled=False, runners=1, overhead=oh)
            if st == "s1" and ps > 1 and ar != "jitter":  # outcome differs between passes: the order-based verdict
                s2 = f"S5 order-diff flag x{ps} (fails in some passes, passes in others)"
                for t in inm(o):
                    fl = []
                    for e in ex:
                        f = [t in p["fail"] for p in e["passes"]]
                        fl.append(any(f) and not all(f))
                    b.add(s2, r, t, fl)
                b.set_cost(s2, r, wall=mean_wall(ex), modeled=False, runners=1, overhead=oh)
        # forced
        for o in data["forced"]:
            ex = o["exps"]
            forced = [t for t, lb in o["labels"].items() if lb["forced"]]
            for t in forced:
                b.add("S3 deterministic fault injection (forced)", r, t, [t in e["fail"] for e in ex])
            b.set_cost("S3 deterministic fault injection (forced)", r, wall=mean_wall(ex), modeled=False, runners=1, overhead=oh)
        # repeats (order scope)
        for o in data["repeat"]:
            ex = o["exps"]
            for n in NS:
                strat = f"S2{'f' if o['scope'] == 'function' else 's'} repeat-job N={n} ({o['scope']} scope, shared state)"
                for t in o["tests"]:
                    b.add(strat, r, t, [0 <= e["first_fail"][t] < n for e in ex])
                b.set_cost(strat, r, wall=mean_wall(ex) * n / o["nmax"], modeled=True, runners=1, overhead=oh, subset=True)
        # prop
        for o in data["prop"]:
            race = {"purge": "purge-probe", "reset": "reset-order", "tie": "tie-order"}
            for bug, t in race.items():
                rows = [x for x in o["rows"] if x["bug"] == bug]
                b.add("S6 stateful property test (Hypothesis)", r, t, [x["found"] for x in rows])
            fx = [x for x in o["rows"] if x["bug"] == "none"]
            info.setdefault("prop_fixed", []).append((sum(x["found"] for x in fx), len(fx)))
            rows = o["rows"]
            b.set_cost("S6 stateful property test (Hypothesis)", r, wall=statistics.fmean(x["wall_s"] for x in rows), modeled=False, runners=1, overhead=oh)
            info.setdefault("prop_rows", []).extend(rows)
        # pools
        for o in data["pool"]:
            noise = o["_name"].startswith("noise-")
            arm = o["arm"]
            for t, d in o["tests"].items():
                if not noise:
                    info["pool_rate"].setdefault(t, [0, 0])
                    info["pool_rate"][t][0] += d["fail"].count("1")
                    info["pool_rate"][t][1] += d["n"]
            tests = o["tests"]
            pw = pass_wall
            if noise:
                for n in (5,):
                    strat = f"S4 noise {arm} N={n}"
                    for t, d in tests.items():
                        b.add(strat, r, t, blocks(d["fail"], n))
                    b.set_cost(strat, r, wall=None, modeled=True, runners=1, overhead=oh, nmul=n)
                continue
            dsum_all = sum(d["dur_mean"] for d in tests.values())
            for n in NS:
                strat = f"S2f pool N={n} (function scope, isolated runs)"
                for t, d in tests.items():
                    b.add(strat, r, t, blocks(d["fail"], n))
                b.set_cost(strat, r, wall=n * (pw if pw else dsum_all), modeled=True, runners=1, overhead=oh)
            for k in (20, 50, 100):
                for rule in (list(RULES)[0], list(RULES)[2], list(RULES)[3]):
                    strat = f"S7 history K={k} rule {rule}"
                    for t, d in tests.items():
                        w = [d["fail"][i : i + k] for i in range(0, len(d["fail"]) - k + 1, k)]
                        b.add(strat, r, t, [RULES[rule](x) for x in w])
                    b.set_cost(strat, r, wall=0.0, modeled=True, runners=0, overhead=oh)
            # S9 retry on failure: first run fails, flagged when a rerun passes (flaky) ; any failure flagged separately
            strat_any, strat_fl = "S9 rerun x3 on failure: any failure seen", "S9 rerun x3 on failure: classified flaky (fail then pass)"
            for t, d in tests.items():
                fl_any, fl_fl = [], []
                for i in range(0, len(d["fail"]) - 3, 4):
                    w = d["fail"][i : i + 4]
                    fl_any.append(w[0] == "1")
                    fl_fl.append(w[0] == "1" and "0" in w[1:])
                b.add(strat_any, r, t, fl_any)
                b.add(strat_fl, r, t, fl_fl)
            tt = tests
            for st in (strat_any, strat_fl):
                nfail = statistics.fmean(d["fail"].count("1") / max(d["n"], 1) for d in tt.values())
                b.set_cost(st, r, wall=(pw if pw else dsum_all) + 3 * nfail * dsum_all, modeled=True, runners=1, overhead=oh)
            # S8: PR touches k of M tests, repeat the changed ones N=20 once on top of the full pass
            rnd = random.Random(20261023 + r)
            names = list(tests)
            M = len(names)
            for kk in (1, 3, 5):
                strat = f"S8 changed-tests repeat N=20, PR touches {kk} of {M} (flake pre-existing)"
                for t, d in tests.items():
                    fl = []
                    nb1, nb21 = len(d["fail"]), len(d["fail"]) - 21
                    for _ in range(2000):
                        changed = t in rnd.sample(names, kk)
                        if changed:
                            i = rnd.randrange(nb21 + 1)
                            fl.append("1" in d["fail"][i : i + 21])
                        else:
                            fl.append(d["fail"][rnd.randrange(nb1)] == "1")
                    b.add(strat, r, t, fl)
                b.set_cost(strat, r, wall=(pw if pw else dsum_all) + kk * 20 * dsum_all / M, modeled=True, runners=1, overhead=oh)
            strat = "S8 changed-tests repeat N=20, the flaky test is the one the PR adds"
            for t, d in tests.items():
                b.add(strat, r, t, blocks(d["fail"], 21))
            b.set_cost(strat, r, wall=(pw if pw else dsum_all) + 20 * (dsum_all / M), modeled=True, runners=1, overhead=oh)
        # S4 N=5 wall: five single-pass walls
        for o in data["suite"]:
            pass
        for strat, cst in list(b.cost.items()):
            if r in cst and cst[r].get("wall") is None:
                arm = strat.split()[2]
                one = b.cost.get(f"S4 noise {arm} N=1", {}).get(r, {}).get("wall")
                cst[r]["wall"] = one * cst[r]["nmul"] if one else float("nan")
    return b, info


# ---- rendering -----------------------------------------------------------------------------------------------------------


def pooled(b: Book, strat: str, test: str) -> tuple[int, int]:
    x = n = 0
    for run in b.flags[strat].values():
        fl = run.get(test)
        if fl is not None:
            x += sum(fl)
            n += len(fl)
    return x, n


def per_run(b: Book, strat: str, test: str) -> list[tuple[int, int]]:
    return [(sum(run[test]), len(run[test])) for run in b.flags[strat].values() if test in run]


def group_fp(b: Book, strat: str, tests: list[str]) -> tuple[int, int, int]:
    """experiments with at least one flagged test in `tests`, experiments, total flagged test-experiments"""
    x = n = tot = 0
    for run in b.flags[strat].values():
        ls = [run[t] for t in tests if t in run]
        if not ls:
            continue
        k = min(len(l) for l in ls)
        for j in range(k):
            f = sum(l[j] for l in ls)
            x += f > 0
            tot += f
        n += k
    return x, n, tot


def cost_row(b: Book, strat: str) -> str:
    cs = list(b.cost.get(strat, {}).values())
    if not cs:
        return "| n/a | n/a | n/a | n/a |"
    wall = statistics.fmean(c["wall"] for c in cs if c["wall"] == c["wall"]) if any(c["wall"] == c["wall"] for c in cs) else float("nan")
    oh = statistics.fmean(c["overhead"] for c in cs)
    runners = cs[0]["runners"]
    billed = runners * math.ceil((oh + wall) / 60) if runners and wall == wall else 0
    tag = "modeled" if cs[0]["modeled"] else "measured"
    sub = ", subset of the suite" if cs[0].get("subset") else ""
    return f"| {wall:.2f} ({tag}{sub}) | {billed} | {runners} | {oh:.0f} |"


def planted_cols(info: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    lab = info["labels"]
    planted = [t for t, l in lab.items() if l["engine"] != "real" and l["variant"] == "buggy"]
    controls = [t for t, l in lab.items() if l["engine"] != "real" and l["variant"] in ("control", "helper")]
    fixed = [t for t, l in lab.items() if l["engine"] != "real" and l["variant"] == "fixed"]
    return planted, controls, fixed


def md_table(head: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def real_tables(runs: list[dict[str, Any]]) -> str:
    """Real-engine pools: truth from the first half of each test's sequence, detection experiments from the second half."""
    seqs: dict[tuple[str, str], list[str]] = defaultdict(list)
    durs: dict[tuple[str, str], list[float]] = defaultdict(list)
    errs: dict[str, dict[str, int]] = defaultdict(dict)
    meta = []
    for data in runs:
        for o in data["real-pool"]:
            meta.append((o["arm"], o["shard"], o["machine"], o["wall_s"], o["workers"]))
            for t, d in o["tests"].items():
                seqs[(t, o["arm"])].append(d["fail"])
                durs[(t, o["arm"])].append(d["dur_mean"])
                for k, v in d["errs"].items():
                    errs[t][k] = errs[t].get(k, 0) + v
    if not seqs:
        return "No real-engine files in the run directories.\n"
    out = ["## Real engine\n"]
    out.append(md_table(["arm", "shard", "CPU", "image", "engine digest", "workers", "wall s"],
                        [[a, s, m.get("cpu", ""), m.get("runner_image", ""), (m.get("fl2p_engine_digest", "") or "")[:80], str(w), f"{wl:.0f}"] for a, s, m, wl, w in meta]))
    out.append("\n### Ground truth and detection (arm base)\n")
    out.append("Truth = first half of each test's run sequence (time order, pooled over occasions only inside each half). Detection = non-overlapping blocks of N consecutive runs of the second half. Analytic = 1-(1-p)^N with p the truth-half rate. K is the number of blocks. Wall is modeled: N x mean run time, one test on one runner, billed minutes = ceil((set-up + engine start + wall) / 60) with the set-up of the first shard job (0 when not recorded), modeled and not billed. Runs inside a shard share the container and four worker processes, so the mean run time is the time under that load.\n")
    head = ["test", "truth n", "truth rate [Wilson]", "second-half rate"] + [f"N={n} measured (K) / analytic" for n in (1, 5, 10, 20, 50)] + ["mean run s", "N=20 wall s (billed min)", "N=50 wall s (billed min)", "errors seen"]
    oh = 0.0
    for a_, s_, m_, w_, k_ in meta:
        try:
            oh = float(m_.get("fl2p_setup_s", 0)) + float(m_.get("fl2p_engine_start_s", 0))
        except ValueError:
            pass
        break
    rows = []
    for (t, a), ss in sorted(seqs.items()):
        if a != "base":
            continue
        tr_x = tr_n = h2_x = h2_n = 0
        cells = {n: [0, 0] for n in (1, 5, 10, 20, 50)}
        for s in ss:
            h = len(s) // 2
            first, second = s[:h], s[h:]
            tr_x += first.count("1")
            tr_n += len(first)
            h2_x += second.count("1")
            h2_n += len(second)
            for n in cells:
                bl = blocks(second, n)
                cells[n][0] += sum(bl)
                cells[n][1] += len(bl)
        p = tr_x / tr_n if tr_n else 0.0
        lo, hi = wilson(tr_x, tr_n)
        dm = statistics.fmean(durs[(t, a)])
        row = [t, str(tr_n), f"{100 * p:.1f}% [{100 * lo:.1f}-{100 * hi:.1f}]", f"{h2_x}/{h2_n}"]
        for n, (x, k) in cells.items():
            row.append(f"{short(x, k)} / {100 * (1 - (1 - p) ** n):.0f}%" if k else "n/a")
        row += [f"{dm:.2f}", f"{20 * dm:.0f} ({math.ceil((oh + 20 * dm) / 60)})", f"{50 * dm:.0f} ({math.ceil((oh + 50 * dm) / 60)})", "; ".join(f"{k}: {v}" for k, v in list(errs[t].items())[:2])]
        rows.append(row)
    out.append(md_table(head, rows))
    out.append("\n### Environment arms on the real engine (rate over the whole sequence, Wilson 95%)\n")
    arms = sorted({a for (_, a) in seqs})
    rows = []
    for t in sorted({t for (t, _) in seqs}):
        row = [t]
        for a in arms:
            ss = seqs.get((t, a))
            if not ss:
                row.append("-")
                continue
            x = sum(s.count("1") for s in ss)
            n = sum(len(s) for s in ss)
            row.append(fmt(x, n))
        rows.append(row)
    out.append(md_table(["test"] + arms, rows))
    return "\n".join(out) + "\n"


def analyze(run_dirs: list[str], out_dir: str) -> None:
    runs = [load_dir(d) for d in run_dirs]
    b, info = build(runs)
    planted, controls, fixed = planted_cols(info)
    lab = info["labels"]
    b.order.sort(key=nat)
    md: list[str] = ["# Planted-flake strategy benchmark: tables\n"]
    md.append(f"Run directories (one occasion each): {len(run_dirs)}. {', '.join(run_dirs)}\n")
    md.append("## Hardware of each run\n")
    mrows = []
    for d, m in zip(run_dirs, info["machines"]):
        mrows.append([d, m.get("cpu", "?"), str(m.get("nproc", "?")), m.get("runner_image", "") or "local", m.get("python", ""), (m.get("sha", "") or "")[:8], str(m.get("fl2p_setup_s", "not recorded"))])
    md.append(md_table(["dir", "CPU", "nproc", "runner image", "python", "commit", "set-up s"], mrows))
    md.append("\nCPU and image are those of the first job file found in the directory. Jobs of one occasion can land on different CPUs; compare strategies within one run only.\n")

    md.append("## Registry and truth\n")
    rows = []
    for t, l in lab.items():
        if l["engine"] == "real":
            continue
        pr = info["pool_rate"].get(t)
        meas = ""
        if pr and pr[1]:
            lo, hi = wilson(pr[0], pr[1])
            meas = f"{pr[0]}/{pr[1]} = {100 * pr[0] / pr[1]:.2f}% [{100 * lo:.2f}-{100 * hi:.2f}]"
        tr = l["truth"]
        rows.append([t, l["kind"], l["variant"], l["engine"], f"{tr:.4g}" if isinstance(tr, float) else str(tr), "yes" if l["forced"] else "no", meas, l["note"]])
    md.append(md_table(["test", "kind", "variant", "engine", "truth per run", "S3 injection", "measured in the pool (isolated runs)", "note"], rows))
    md.append("\nTruth is by construction or analytic for the in-memory models (inputs of the experiment, not engine measurements), `measured` where it can only be measured, 0 for controls and fixed variants. The order-dependent pair has truth 0.5 per random-order run.\n")

    def col_group(title: str, cols: list[str]) -> None:
        md.append(f"### {title}\n")
        rows = []
        for s in b.order:
            if not any(t in b.flags[s].get(r, {}) for r in b.flags[s] for t in cols):
                continue
            rows.append([s] + [short(*pooled(b, s, t)) if pooled(b, s, t)[1] else "n/a" for t in cols])
        md.append(md_table(["strategy"] + cols, rows))
        md.append("")

    md.append("## Detection: strategy x planted flake (in-memory set)\n")
    md.append("Cell: percentage of experiments in which the test was flagged (any failure, except the S5 order-diff rows and the S9 classified row, which say so), Wilson 95% interval, K experiments. n/a: the strategy does not apply or was not run. S3 and S6 detect only what was modeled; see the notes.\n")
    races = [t for t in planted if lab[t]["kind"] in ("race", "delay")]
    rates = [t for t in planted if t not in races]
    col_group("Races and delay", races)
    col_group("Rate-controlled, timing and order-dependent flakes", rates)

    md.append("## Cost per strategy (one CI invocation of the strategy)\n")
    md.append("Wall seconds are the mean over the runs; `measured` when the strategy itself ran, `modeled` when derived from the measured full-suite pass time x N (S2, S9, S8) or from the measured repeat job. Billed minutes: runners x ceil((set-up + wall)/60), modeled. Runners: parallel jobs the strategy needs (S7 reads the history of runs that exist anyway: 0 marginal).\n")
    md.append("| strategy | wall s | billed min | runners | set-up s |\n|---|---|---|---|---|")
    for s in b.order:
        md.append(f"| {s} {cost_row(b, s)}")
    md.append("")

    md.append("## False positives\n")
    md.append(f"Controls: {len([c for c in controls if lab[c]['variant']=='control'])} tests that must never fail (plus the order-dep polluter, a helper). Fixed variants: {len(fixed)}. Per strategy: experiments in which at least one control (or fixed variant) was flagged, and the number of flagged test-experiments.\n")
    rows = []
    for s in b.order:
        cx, cn, ct = group_fp(b, s, controls)
        fx, fn, ft = group_fp(b, s, fixed)
        if cn == 0 and fn == 0:
            continue
        rows.append([s, fmt(cx, cn), str(ct), fmt(fx, fn), str(ft)])
    md.append(md_table(["strategy", "experiments with a flagged control", "flagged control-tests", "experiments with a flagged fixed variant", "flagged fixed-tests"], rows))
    if "prop_fixed" in info:
        md.append(f"\nS6 on the fixed machine: {sum(x for x, _ in info['prop_fixed'])} of {sum(n for _, n in info['prop_fixed'])} runs found a counterexample.\n")
    bad = [(s, t) for s in b.order for t in controls + fixed if pooled(b, s, t)[0] > 0]
    if bad:
        md.append("\nTests that were flagged although they must never fail (strategy, test, count):\n")
        md.append("\n".join(f"- {s}: {t} {pooled(b, s, t)[0]}/{pooled(b, s, t)[1]}" for s, t in bad[:60]) + "\n")

    # analytic against measured, iid tests, pool-based S2f
    md.append("## Analytic against measured detection (isolated runs, S2 function scope)\n")
    md.append("Analytic = 1-(1-p)^N with p the truth of the model. Measured = blocks of N runs of the pool. The in-memory pools are i.i.d. by construction, so this checks the harness arithmetic, not an engine.\n")
    rows = []
    for t in planted:
        tr = lab[t]["truth"]
        if not isinstance(tr, float) or t in ("order-dep-hidden", "order-dep-exposed"):
            continue
        row = [t, f"{tr:.4g}"]
        for n in NS:
            s = f"S2f pool N={n} (function scope, isolated runs)"
            x, k = pooled(b, s, t) if s in b.flags else (0, 0)
            row.append(f"{short(x, k)} / {100 * (1 - (1 - tr) ** n):.1f}%" if k else "n/a")
        rows.append(row)
    md.append(md_table(["test", "p"] + [f"N={n} measured / analytic" for n in NS], rows))

    # S7 detail
    md.append("\n## S7 history scoring: detection and false positives against the window K\n")
    md.append("The history is the per-test run sequence of the pool (each run is one CI run of that test). A window of the last K runs is scored; windows are disjoint. Rules: mixed = a pass and a failure in the window; flips>=2; Beta-Binomial: flag when P(p > theta | window) >= 0.9, with a uniform prior and theta 0.02, or with a Beta(0.1, 20) prior (mean 0.5%, about 20 pseudo-runs) and theta 0.005. A flat prior flags a window with no failure when K is small and theta is small; the informative prior does not, and its strength is a choice, not a measurement. A deterministically failing test is not `mixed` (it is failing, not flaky) but is flagged by the Beta rules.\n")
    runs_pool = [d for d in runs if d["pool"]]
    seqs: dict[str, str] = {}
    for d in runs_pool:
        for o in d["pool"]:
            if o["_name"].startswith("noise-"):
                continue
            for t, v in o["tests"].items():
                seqs[t] = seqs.get(t, "") + v["fail"]
    if seqs:
        cols = [t for t in planted if t in seqs]
        rows = []
        for rule, fn in RULES.items():
            for k in KS:
                row = [rule, str(k)]
                for t in cols:
                    w = [seqs[t][i : i + k] for i in range(0, len(seqs[t]) - k + 1, k)]
                    row.append(short(sum(fn(x) for x in w), len(w)) if w else "n/a")
                ctl = [t for t in controls + fixed if t in seqs]
                fpx = fpn = 0
                kk = min((len(seqs[t]) // k for t in ctl), default=0)
                for j in range(kk):
                    fpn += 1
                    fpx += any(fn(seqs[t][j * k : (j + 1) * k]) for t in ctl)
                row.append(fmt(fpx, fpn))
                rows.append(row)
        md.append(md_table(["rule", "K"] + cols + ["windows with a flagged control or fixed variant"], rows))
        md.append("\nHistory comes from the benchmark's own isolated pools. Runs here are i.i.d. with a fixed rate, which is the easy case for a scorer: no drift, no code change, no correlated failures between tests.\n")

    # noise
    noise_rows = []
    for s in b.order:
        if s.startswith("S4 noise") and s.endswith("N=1"):
            arm = s.split()[2]
            x, k = pooled(b, s, "timing-race")
            s5 = f"S4 noise {arm} N=5"
            x5, k5 = pooled(b, s5, "timing-race") if s5 in b.flags else (0, 0)
            cx, cn, ct = group_fp(b, s, controls)
            cx5, cn5, ct5 = group_fp(b, s5, controls) if s5 in b.flags else (0, 0, 0)
            noise_rows.append([arm, short(x, k), short(x5, k5), fmt(cx, cn), fmt(cx5, cn5)])
    if noise_rows:
        md.append("\n## S4 noise: the timing race and the controls per arm\n")
        md.append("cpus2 pins the harness (and stress-ng) to two cores of the four-vCPU runner: an emulation of a two-vCPU runner, not the real thing. stressL: stress-ng, one cpu worker per allowed core at L percent. The pool rate of the timing race is the rate per isolated run.\n")
        md.append(md_table(["arm", "timing-race flagged N=1", "timing-race flagged N=5", "controls flagged N=1 (exps)", "controls flagged N=5 (exps)"], noise_rows))
        tr_rows = []
        for data in runs:
            for o in data["pool"]:
                if o["_name"].startswith("noise-"):
                    d = o["tests"].get("timing-race")
                    if d:
                        tr_rows.append([o["arm"], fmt(d["fail"].count("1"), d["n"])])
        md.append("\nTiming-race per-run failure rate by arm (all runs of the pool):\n")
        md.append(md_table(["arm", "failures/runs"], tr_rows))
    cal = [(o, d) for d in runs for o in d["calib"]]
    if cal:
        md.append("\n## Timing-race calibration (margin against failure rate)\n")
        rows = [[r["arm"], str(r["margin_ms"]), fmt(r["fails"], r["n"])] for o, _ in cal for r in o["rows"]]
        md.append(md_table(["arm", "margin ms", "failures/runs"], rows))
    # canary
    can = [c for d in runs for c in d["canary"]]
    if can:
        md.append("\n## Canary (own process: 1M-iteration CPU loop and a search query)\n")
        rows = []
        for c in can:
            cpu = [r["cpu_ms"] for r in c["rows"]]
            qq = [r["query_ms"] for r in c["rows"]]
            if cpu:
                rows.append([c["name"], str(len(cpu)), f"{statistics.median(cpu):.1f} / {max(cpu):.1f}", f"{statistics.median(qq):.2f} / {max(qq):.2f}"])
        md.append(md_table(["file", "samples", "cpu ms median / max", "query ms median / max"], rows))
        md.append("\nSamples taken while a noise arm is active show the load. A canary far from its own median in an arm without noise means the run overlapped a disturbance and should be discarded.\n")
    # pytest cost
    pc = [o for d in runs for o in d["pytestcost"]]
    if pc:
        md.append("\n## Cost per repeat: pytest-repeat against an in-process loop\n")
        md.append("Same nine cheap tests (eight micro and one in-memory scenario). Wall of a subprocess pytest run (includes interpreter start, collection and the plugins) for pytest, wall of the loop for in-process. Least-squares fit wall = a + b x N per mechanism.\n")
        rows = []
        by: dict[str, list[tuple[int, float]]] = defaultdict(list)
        for o in pc:
            for r in o["rows"]:
                by[r["mech"]].append((r["n"], r["wall_s"]))
        for mech, pts in by.items():
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            if len(set(xs)) > 1:
                mx, my = statistics.fmean(xs), statistics.fmean(ys)
                sl = sum((x - mx) * (y - my) for x, y in pts) / sum((x - mx) ** 2 for x in xs)
                ic = my - sl * mx
                rows.append([mech, f"{ic:.2f}", f"{1000 * sl:.2f}", str(len(pts)), ", ".join(f"N={n}: {statistics.fmean(y for x, y in pts if x == n):.2f}s" for n in sorted(set(xs)))])
            else:
                rows.append([mech, f"{my:.2f}", "-", str(len(pts)), ""])
        md.append(md_table(["mechanism", "a (s) per invocation", "b (ms) per repeat of the 9 tests", "runs", "mean wall by N"], rows))
    pk = [o for d in runs for o in d["pytestcheck"]]
    if pk:
        md.append("\n## Conformance: pytest-randomly against the in-process model\n")
        for o in pk:
            rr = [r for r in o["rows"] if r["order"] == "random"]  # noqa
            n = len(rr)
            h = sum(r["res"].get("test_planted[order-dep-hidden]") == "fail" for r in rr)
            e = sum(r["res"].get("test_planted[order-dep-exposed]") == "fail" for r in rr)
            dflt = [r for r in o["rows"] if r["order"] == "default"][0]["res"]
            for r in o["rows"]:
                if r["order"].startswith("repeat-"):
                    hh = "".join("F" if v == "fail" else "." for k2, v in sorted(r["res"].items()) if "hidden" in k2)
                    ee = "".join("F" if v == "fail" else "." for k2, v in sorted(r["res"].items()) if "exposed" in k2)
                    md.append(f"- pytest-repeat --count=4 {r['order']}: hidden victim repeats {hh}, exposed victim repeats {ee} (F = failed; ids sorted)")
            md.append(f"- {n} random seeds: hidden victim failed {fmt(h, n)}, exposed victim failed {fmt(e, n)} (model: 50% each). Default order: hidden {dflt.get('test_planted[order-dep-hidden]')}, exposed {dflt.get('test_planted[order-dep-exposed]')} (model: pass, fail).")
    md.append("\n" + real_tables(runs))

    # between occasion
    if len(runs) > 1:
        md.append("## Between-occasion spread\n")
        md.append("Per-occasion detection rate of each cell, in run-directory order, with the range. Differences between occasions include different runners and CPUs; a cell that moves more than its Wilson interval says is varying with the environment.\n")
        cols = planted
        rows = []
        for s in b.order:
            row = [s]
            has = False
            for t in cols:
                pr = [x / n for x, n in per_run(b, s, t) if n]
                if len(pr) > 1:
                    has = True
                    row.append(" / ".join(f"{100 * p:.0f}" for p in pr) + f" (range {100 * (max(pr) - min(pr)):.0f} pts)")
                else:
                    row.append("-")
            if has:
                rows.append(row)
        md.append(md_table(["strategy"] + cols, rows))
    md.append("\n## What these tables do not show\n")
    md.append("- Detection of a flake that no strategy has been told about: S3 and S6 detect the races their authors modeled. The planted set contains six races, two controlled rates, one wall-clock race and one order dependence; other flake classes are not represented.\n- Costs of billed minutes are modeled from measured pass times; no strategy was run as a billed CI job at the listed N.\n- The in-memory lag models are inputs of the experiment, not measurements of the engine.\n")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    Path(out_dir, "tables.md").write_text("\n".join(md), encoding="utf8")
    summ = {s: {t: list(pooled(b, s, t)) for t in planted} for s in b.order}
    Path(out_dir, "summary.json").write_text(json.dumps(summ, indent=1), encoding="utf8")
    print(f"wrote {out_dir}/tables.md ({len(b.order)} strategy rows)")


def cmd_analyze(a: Any) -> None:
    analyze(a.runs, a.out)
