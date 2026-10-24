"""fl2p: planted-flake strategy benchmark. Groups: inmem (no container, one job per strategy) and real (needs the engine)."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from . import registry  # noqa: F401
from .core import REG, Ctx, arm, run_one, selected
from .exp import cmd_forced, cmd_pool, cmd_repeat, cmd_suite, dump, header, pool, pool_sizes, seed_or_new, suite_exps


def cmd_registry(a: Any) -> None:
    print(json.dumps([t.label() for t in REG.values()], indent=1))


def calibrate(arms: list[str], margins: list[float], n: int, seed: int) -> list[dict[str, Any]]:
    """Timing-race failure count against its margin, per arm. Each margin is run as a pool with a few micro tests interleaved, the
    same arrangement as the pools of the noise job, so the rates are comparable with them."""
    from . import tests_misc

    tr = REG["timing-race"]
    mix = [REG[x] for x in ("c-sort", "c-json", "c-heap", "c-regex", "c-gather")]
    rows = []
    old = tests_misc.MARGIN_S
    try:
        for ar in arms:
            with arm(ar):
                for m in margins:
                    tests_misc.MARGIN_S = m / 1000.0
                    p = pool([tr] + mix, {t.name: n for t in [tr] + mix}, seed)
                    fails = p["tests"]["timing-race"]["fail"].count("1")
                    rows.append({"arm": ar, "margin_ms": m, "n": n, "fails": fails})
                    print(f"calib arm={ar} margin={m}ms {fails}/{n}", flush=True)
    finally:
        tests_misc.MARGIN_S = old
    return rows


def choose_margin(rows: list[dict[str, Any]], target: float = 0.05) -> tuple[float | None, str]:
    """The margin whose failure rate under stress100 is closest to `target` (and not zero). None when no margin failed."""
    st = [r for r in rows if r["arm"] == "stress100" and r["fails"] > 0]
    if not st:
        return None, "no margin produced a failure under stress100; the default margin is kept"
    best = min(st, key=lambda r: abs(r["fails"] / r["n"] - target))
    return best["margin_ms"], f"closest stress100 rate to {target:.0%}: {best['fails']}/{best['n']} at {best['margin_ms']} ms"


def cmd_calib(a: Any) -> None:
    """Timing-race failure rate against its margin, baseline and under stress."""
    rows = calibrate(a.arms.split(","), [float(x) for x in a.margins.split(",")], a.n, seed_or_new(getattr(a, "seed", None)))
    dump(a.out, {**header("calib", 0), "rows": rows})


def cmd_noise(a: Any) -> None:
    """S4: calibrate the timing-race margin on this machine, then each arm gets a pool (every test, the timing race with a larger n)
    and K single suite passes."""
    from . import tests_misc

    seed = seed_or_new(a.seed)
    out_dir = Path(a.out_dir)
    rows = calibrate(["base", "stress100"], [float(x) for x in a.calib_margins.split(",")], a.calib_n, seed)
    margin, why = choose_margin(rows)
    if margin is not None:
        tests_misc.MARGIN_S = margin / 1000.0
    t_tr = REG["timing-race"]
    t_tr.note = f"thread sets a flag after {tests_misc.PROD_S * 1000:.0f} ms, loop checks after +{tests_misc.MARGIN_S * 1000:.2f} ms (calibrated in this job)"
    dump(out_dir / "calib-noise.json", {**header("calib", seed, margin_ms=tests_misc.MARGIN_S * 1000, chosen=why), "rows": rows})
    print(f"noise margin {tests_misc.MARGIN_S * 1000:.2f} ms: {why}", flush=True)
    for ar in a.arms.split(","):
        with arm(ar):
            tests = [t for t in selected("all") if t.kind not in ("order-dep", "helper")]
            sizes = pool_sizes(tests, a.n, a.budget, det_budget_s=a.det_budget)
            sizes["timing-race"] = a.race_n
            print(f"noise arm={ar} runs={sum(sizes.values())}", flush=True)
            p = pool(tests, sizes, seed)
            s = suite_exps("s0", a.k, seed)
        dump(out_dir / f"noise-pool-{ar}.json.gz", {**p, **header("pool", seed, arm=ar, n=a.n, budget=a.budget, margin_ms=tests_misc.MARGIN_S * 1000)})
        dump(out_dir / f"noise-suite-{ar}.json", {**header("suite", seed, strategy="s0", passes=1, k=a.k, arm=ar, margin_ms=tests_misc.MARGIN_S * 1000), "exps": s})


def cmd_smoke(a: Any) -> None:
    """Tiny scale through every inmem code path, for the push workflow and local checks. Asserts only that the paths run and
    that the labels hold where they hold by construction (controls pass, fixed variants pass, deterministic bugs fail)."""
    from .analyze import analyze
    from .prop import cmd_prop
    from .pyt import cmd_pytestcheck, cmd_pytestcost

    out = Path(a.out_dir)
    ns = argparse.Namespace
    t0 = time.time()
    cmd_pool(ns(seed=1, tests="all", extra="timing-race=60", arm="base", n=60, budget=5.0, det_budget=8.0, out=str(out / "pool-base.json.gz")))
    for st, passes in (("s0", 1), ("s1", 1), ("s1", 3)):
        cmd_suite(ns(seed=2, strategy=st, k=6, passes=passes, arm="base", out=str(out / f"suite-{st}-p{passes}-base.json")))
    cmd_suite(ns(seed=3, strategy="s1", k=4, passes=3, arm="jitter", out=str(out / "suite-s5j-jitter.json")))
    cmd_forced(ns(k=3, out=str(out / "forced.json")))
    for sc in ("function", "session"):
        cmd_repeat(ns(scope=sc, k=2, nmax=12, out=str(out / f"repeat-{sc}.json")))
    cmd_prop(ns(seed=4, k=2, examples=15, steps=8, out=str(out / "prop.json")))
    cmd_noise(ns(seed=5, arms="base,cpus2,stress100,jitter", n=40, budget=3.0, det_budget=4.0, race_n=60, k=3, calib_margins="0.5,1.0", calib_n=20, out_dir=str(out)))
    cmd_pytestcost(ns(seed=6, ns=[1, 5], reps=1, out=str(out / "pytestcost.json")))
    cmd_pytestcheck(ns(seed=7, seeds=3, out=str(out / "pytestcheck.json")))
    analyze([str(out)], str(out / "report"))
    print(f"smoke inmem done in {time.time() - t0:.0f}s")


def main() -> None:
    p = argparse.ArgumentParser(prog="fl2p")
    sp = p.add_subparsers(dest="cmd", required=True)

    def add(name, fn, **args):
        s = sp.add_parser(name)
        for k, v in args.items():
            s.add_argument("--" + k.replace("_", "-"), **v)
        s.set_defaults(fn=fn)
        return s

    add("registry", cmd_registry)
    add("pool", cmd_pool, tests=dict(default="all"), extra=dict(default=""), arm=dict(default="base"), n=dict(type=int, default=9000),
        budget=dict(type=float, default=45.0), det_budget=dict(type=float, default=150.0), seed=dict(type=int), out=dict(required=True))
    add("suite", cmd_suite, strategy=dict(choices=["s0", "s1"], default="s0"), k=dict(type=int, default=100), passes=dict(type=int, default=1),
        arm=dict(default="base"), seed=dict(type=int), out=dict(required=True))
    add("forced", cmd_forced, k=dict(type=int, default=30), out=dict(required=True))
    add("repeat", cmd_repeat, scope=dict(choices=["function", "session"]), k=dict(type=int, default=30), nmax=dict(type=int, default=300), out=dict(required=True))
    from .prop import cmd_prop

    add("prop", cmd_prop, k=dict(type=int, default=30), examples=dict(type=int, default=50), steps=dict(type=int, default=12), seed=dict(type=int), out=dict(required=True))
    add("noise", cmd_noise, arms=dict(default="base,cpus2,stress50,stress100,cpus2+stress100,jitter"), n=dict(type=int, default=1500), budget=dict(type=float, default=6.0),
        det_budget=dict(type=float, default=25.0), calib_margins=dict(default="0.25,0.5,1.0,2.0,4.0"), calib_n=dict(type=int, default=2000),
        race_n=dict(type=int, default=5000), k=dict(type=int, default=60), seed=dict(type=int), out_dir=dict(required=True))
    add("calib", cmd_calib, arms=dict(default="base,stress100"), margins=dict(default="0.25,0.5,1.0,2.0"), n=dict(type=int, default=3000), seed=dict(type=int), out=dict(required=True))
    from .real import cmd_real

    add("real", cmd_real, shard=dict(required=True), workers=dict(type=int, default=4), n_scale=dict(choices=["smoke", "full"], default="full"),
        smoke_n=dict(type=int, default=4), mult=dict(type=float, default=1.0), arms=dict(default="base"), arm_tests=dict(default=""),
        arm_frac=dict(type=float, default=0.3), seed=dict(type=int), out_dir=dict(required=True))
    from .canary import cmd_canary

    add("canary", cmd_canary, out=dict(required=True), interval=dict(type=float, default=10.0), uri=dict(default=None), count=dict(type=int, default=None))
    from .pyt import cmd_pytestcheck, cmd_pytestcost

    add("pytestcost", cmd_pytestcost, ns=dict(type=int, nargs="+", default=[1, 10, 50, 100]), reps=dict(type=int, default=3), seed=dict(type=int), out=dict(required=True))
    add("pytestcheck", cmd_pytestcheck, seeds=dict(type=int, default=30), seed=dict(type=int), out=dict(required=True))
    add("smoke", cmd_smoke, out_dir=dict(required=True))
    from .analyze import cmd_analyze

    s = sp.add_parser("analyze")
    s.add_argument("runs", nargs="+")
    s.add_argument("--out", required=True)
    s.set_defaults(fn=cmd_analyze)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
