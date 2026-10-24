"""Real-engine pools: planted tests run many times against the container, in worker processes that each own a database.

The shuffled work list (seeded, logged) interleaves the tests; each result carries its completion time so that the per-test
sequence is in time order. The first half of a test's sequence is the truth sample, the second half is cut into blocks for
the detection experiments (see analyze).
"""
from __future__ import annotations

import multiprocessing as mp
import os
import random
import subprocess
import time
from typing import Any

from .core import REG, Ctx, arm, run_one
from .exp import dump, header, seed_or_new


def _init() -> None:
    from . import registry  # noqa: F401  (register the tests in the worker)


def _work(name: str) -> tuple[str, bool, float, str, float, dict]:
    from . import tests_real
    from .core import loop

    t = REG[name]
    res = run_one(t, Ctx())
    post = tests_real.POST.get(name, tests_real.after_run)
    try:
        loop().run_until_complete(post())
        if post is not tests_real.after_run:
            loop().run_until_complete(tests_real.after_run())
    except Exception as e:  # noqa: BLE001
        res.err += f" | cleanup {type(e).__name__}"
    return name, res.ok, res.dur, res.err, time.time(), res.val


def real_pool(names: list[str], sizes: dict[str, int], seed: int, workers: int) -> dict[str, Any]:
    order = [nm for nm in names for _ in range(sizes[nm])]
    random.Random(seed).shuffle(order)
    ctx = mp.get_context("spawn")
    rows: list[tuple[str, bool, float, str, float, dict]] = []
    t0 = time.perf_counter()
    with ctx.Pool(workers, initializer=_init) as pool:
        for k, r in enumerate(pool.imap_unordered(_work, order, chunksize=1)):
            rows.append(r)
            if (k + 1) % 50 == 0:
                print(f"  {k + 1}/{len(order)} runs, {time.perf_counter() - t0:.0f}s", flush=True)
    wall = time.perf_counter() - t0
    rows.sort(key=lambda r: r[4])
    tests: dict[str, Any] = {}
    for nm in names:
        rs = [r for r in rows if r[0] == nm]
        durs = sorted(r[2] for r in rs)
        errs: dict[str, int] = {}
        for r in rs:
            if not r[1]:
                errs[r[3][:70]] = errs.get(r[3][:70], 0) + 1
        tests[nm] = {"label": REG[nm].label(), "n": len(rs), "fail": "".join("0" if r[1] else "1" for r in rs), "errs": errs,
                     "dur_mean": sum(durs) / max(len(durs), 1), "dur_p95": durs[int(0.95 * (len(durs) - 1))] if durs else 0.0,
                     "dur_sum": sum(durs), "dur_max": durs[-1] if durs else 0.0,
                     "vals": {k: [r[5].get(k) for r in rs] for k in sorted({k for r in rs for k in r[5]})}}
    return {"order_seed": seed, "wall_s": wall, "workers": workers, "tests": tests}


def docker(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True)  # noqa: S603, S607


def engine_digest(container: str) -> str:
    r = docker("inspect", container, "--format", "{{.Image}}")
    img = r.stdout.strip()
    r2 = docker("image", "inspect", img, "--format", "{{json .RepoDigests}}")
    return (img + " " + r2.stdout.strip()) if img else ""


def cmd_real(a: Any) -> None:
    from .tests_real import FULL_N, SHARDS

    seed = seed_or_new(a.seed)
    names = SHARDS[a.shard] if a.shard in SHARDS else a.shard.split(",")
    if a.n_scale == "smoke":
        sizes = {nm: a.smoke_n for nm in names}
    else:
        sizes = {nm: max(2, int(FULL_N[nm] * a.mult)) for nm in names}
    container = os.environ.get("WP_CONTAINER", "hoodie-fl2-planted")
    digest = engine_digest(container)
    if digest:
        os.environ["FL2P_ENGINE_DIGEST"] = digest
    arms = [x for x in a.arms.split(",") if x]
    arm_tests = [x for x in a.arm_tests.split(",") if x and x in names]
    print(f"real shard={a.shard} seed={seed} arms={arms} runs/base={sum(sizes.values())}", flush=True)
    plan = ["base"]
    rest = (["ref"] if arm_tests else []) + [x for x in arms if x != "base"]
    random.Random(seed + 1).shuffle(rest)  # arm order is shuffled with the logged seed; `ref` = the arm tests again with no noise
    plan += rest if arm_tests else []
    print(f"arm order {plan}", flush=True)
    for ar in plan:
        sz = sizes if ar == "base" else {nm: max(2, int(sizes[nm] * a.arm_frac)) for nm in arm_tests}
        undo = None
        if ar == "engine1":
            docker("update", "--cpus", "1.0", container)
            undo = lambda: docker("update", "--cpus", os.environ.get("WP_CPUS", "3"), container)  # noqa: E731
        try:
            with arm("stress100" if ar == "stress100" else "base"):
                out = real_pool(list(sz), sz, seed, a.workers)
        finally:
            if undo:
                undo()
        out.update(header("real-pool", seed, arm=ar, shard=a.shard, n_scale=a.n_scale, arm_order=plan))
        dump(f"{a.out_dir}/real-{a.shard}-{ar}.json.gz", out)
        print(f"real arm={ar} wall={out['wall_s']:.0f}s", flush=True)
