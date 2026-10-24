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


def cmd_calib(a: Any) -> None:
    """Timing-race failure rate against its margin, baseline and under stress, to show why the planted margin was chosen."""
    from . import tests_misc

    rows = []
    t = REG["timing-race"]
    for ar in a.arms.split(","):
        with arm(ar):
            for m in [float(x) for x in a.margins.split(",")]:
                tests_misc.MARGIN_S = m / 1000.0
                fails = sum(not run_one(t, Ctx()).ok for _ in range(a.n))
                rows.append({"arm": ar, "margin_ms": m, "n": a.n, "fails": fails})
                print(f"calib arm={ar} margin={m}ms {fails}/{a.n}", flush=True)
    dump(a.out, {**header("calib", 0), "rows": rows})


def cmd_noise(a: Any) -> None:
    """S4: each arm gets a pool (every test, the timing race with a larger n) and K single suite passes."""
    seed = seed_or_new(a.seed)
    out_dir = Path(a.out_dir)
    for ar in a.arms.split(","):
        with arm(ar):
            tests = [t for t in selected("all") if t.kind not in ("order-dep", "helper")]
            sizes = pool_sizes(tests, a.n, a.budget)
            sizes["timing-race"] = a.race_n
            print(f"noise arm={ar} runs={sum(sizes.values())}", flush=True)
            p = pool(tests, sizes, seed)
            s = suite_exps("s0", a.k, seed)
        dump(out_dir / f"noise-pool-{ar}.json.gz", {**p, **header("pool", seed, arm=ar, n=a.n, budget=a.budget)})
        dump(out_dir / f"noise-suite-{ar}.json", {**header("suite", seed, strategy="s0", passes=1, k=a.k, arm=ar), "exps": s})


def cmd_smoke(a: Any) -> None:
    """Tiny scale through every inmem code path, for the push workflow and local checks. Asserts only that the paths run and
    that the labels hold where they hold by construction (controls pass, fixed variants pass, deterministic bugs fail)."""
    from .analyze import analyze
    from .prop import cmd_prop
    from .pyt import cmd_pytestcheck, cmd_pytestcost

    out = Path(a.out_dir)
    ns = argparse.Namespace
    t0 = time.time()
    cmd_pool(ns(seed=1, tests="all", extra="timing-race=60", arm="base", n=60, budget=5.0, out=str(out / "pool-base.json.gz")))
    for st, passes in (("s0", 1), ("s1", 1), ("s1", 3)):
        cmd_suite(ns(seed=2, strategy=st, k=6, passes=passes, arm="base", out=str(out / f"suite-{st}-p{passes}-base.json")))
    cmd_suite(ns(seed=3, strategy="s1", k=4, passes=3, arm="jitter", out=str(out / "suite-s5j-jitter.json")))
    cmd_forced(ns(k=3, out=str(out / "forced.json")))
    for sc in ("function", "session"):
        cmd_repeat(ns(scope=sc, k=2, nmax=12, out=str(out / f"repeat-{sc}.json")))
    cmd_prop(ns(seed=4, k=2, examples=15, steps=8, out=str(out / "prop.json")))
    cmd_noise(ns(seed=5, arms="base,cpus2,stress100", n=40, budget=3.0, race_n=60, k=3, out_dir=str(out)))
    cmd_calib(ns(arms="base", margins="1.0", n=30, out=str(out / "calib.json")))
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
        budget=dict(type=float, default=45.0), seed=dict(type=int), out=dict(required=True))
    add("suite", cmd_suite, strategy=dict(choices=["s0", "s1"], default="s0"), k=dict(type=int, default=100), passes=dict(type=int, default=1),
        arm=dict(default="base"), seed=dict(type=int), out=dict(required=True))
    add("forced", cmd_forced, k=dict(type=int, default=30), out=dict(required=True))
    add("repeat", cmd_repeat, scope=dict(choices=["function", "session"]), k=dict(type=int, default=30), nmax=dict(type=int, default=300), out=dict(required=True))
    from .prop import cmd_prop

    add("prop", cmd_prop, k=dict(type=int, default=30), examples=dict(type=int, default=50), steps=dict(type=int, default=12), seed=dict(type=int), out=dict(required=True))
    add("noise", cmd_noise, arms=dict(default="base,cpus2,stress50,stress100,cpus2+stress100"), n=dict(type=int, default=1500), budget=dict(type=float, default=20.0),
        race_n=dict(type=int, default=5000), k=dict(type=int, default=60), seed=dict(type=int), out_dir=dict(required=True))
    add("calib", cmd_calib, arms=dict(default="base,stress100"), margins=dict(default="0.25,0.5,1.0,2.0"), n=dict(type=int, default=3000), out=dict(required=True))
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
