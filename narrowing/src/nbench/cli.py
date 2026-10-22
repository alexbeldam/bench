"""Command line: one command per step. See README.md and RUNBOOK.md."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

from pymongo import AsyncMongoClient

from .canary import Canary
from .common import DB, SCALES, coll_name, oid, run_dir, uri
from .env import DiskSampler, container_usage, environment


def _client() -> AsyncMongoClient:
    return AsyncMongoClient(uri(), maxPoolSize=200)


async def cmd_load(a) -> None:
    from .gen import prepare

    out = run_dir(a.label)
    for scale in a.scale.split(","):
        sampler = DiskSampler(a.disk_path)
        info = await prepare(uri(), scale, a.label, a.disk_path)
        disk = sampler.stop()
        disk["container_usage_bytes"] = container_usage(a.container) if a.container else {}
        disk["dbstats"] = info.get("dbstats")
        disk["documents"] = SCALES[scale]
        (out / f"disk_{scale}.json").write_text(json.dumps(disk, indent=2))
        print(f"[{scale}] load {info['load_s']}s, search index {info['search_index_s']}s, peak disk use {disk['peak_used_bytes'] / 2**30:.2f} GiB")


async def cmd_run(a) -> None:
    from .phases import PHASES, build, run_phase
    from .planner import Planner

    out = run_dir(a.label)
    scale = a.scale
    n = SCALES[scale]
    phases = PHASES if a.phases == "all" else a.phases.split(",")
    terms = a.terms.split(",")
    client = _client()
    canary = Canary(uri(), scale, out / f"canary_{scale}.jsonl")
    meta_path = out / "run_meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {"runs": []}
    meta["runs"].append({"scale": scale, "phases": phases, "seed": a.seed, "reps": a.reps, "warm": a.warm, "started": time.time()})
    meta_path.write_text(json.dumps(meta, indent=1))
    try:
        planner = Planner(client[DB][coll_name(scale)], scale)
        await planner.estimate_n()
        await canary.start()
        for ph in phases:
            reps = a.noise_reps if ph == "noise" and a.noise_reps else a.reps
            cases = build(ph, planner, n, terms)
            print(f"[{scale}/{ph}] {len(cases)} cases, {reps} repetitions, seed {a.seed}")
            await run_phase(phase=ph, cases=cases, scale=scale, outdir=out, reps=reps, warm=a.warm, seed=a.seed,
                            time_limit_s=a.time_limit_min * 60 if a.time_limit_min else None, canary=canary,
                            prewarm_rounds=a.prewarm_rounds)
    finally:
        await canary.stop()
        await client.close()


async def cmd_load_test(a) -> None:
    from .load import DEFAULT_RATES, run_load

    out = run_dir(a.label)
    rates = [float(x) for x in a.rates.split(",")] if a.rates else DEFAULT_RATES
    canary = Canary(uri(), a.scale, out / f"canary_{a.scale}.jsonl")
    await canary.start()
    try:
        await run_load(uri(), a.scale, SCALES[a.scale], out, a.container, rates, a.duration, a.seed, a.cooldown, [], canary)
    finally:
        await canary.stop()


async def cmd_limits(a) -> None:
    from .planner import Opts, search_stage

    client = _client()
    coll = client[DB][coll_name(a.scale)]
    res = {}
    for size in (1000, 1023, 1024):
        stage = search_stage(a.scale, "frequent", [oid(i) for i in range(size)], Opts())
        try:
            await (await coll.aggregate([stage, {"$limit": 1}])).to_list()
            res[str(size)] = "ok"
        except Exception as e:  # noqa: BLE001
            res[str(size)] = f"{type(e).__name__}: {str(e)[:120]}"
    (run_dir(a.label) / f"limits_{a.scale}.json").write_text(json.dumps(res, indent=1))
    print(res)
    await client.close()


async def cmd_selftest(a) -> None:
    from .selftest import run

    await run(a.scale, SCALES[a.scale])


def cmd_env(a) -> None:
    out = run_dir(a.label)
    env = environment(a.container, out / "environment.json", a.disk_path)
    print(json.dumps({k: env[k] for k in ("date_utc", "nproc", "docker_version", "mongod_version", "python", "pymongo")}, indent=1))


def cmd_m0(a) -> None:
    from .m0 import run

    asyncio.run(run(run_dir(a.label), a.docs, a.reps))


def cmd_project(a) -> None:
    from .analyze import project_disk

    res = project_disk([Path(p) for p in a.runs], a.safety, a.min_free_gb, a.target)
    print(json.dumps(res, indent=2))
    if a.out:
        Path(a.out).write_text(json.dumps(res, indent=2))
    gh = os.environ.get("GITHUB_OUTPUT")
    if gh:
        with open(gh, "a") as f:
            f.write(f"attempt={'true' if res.get('attempt') else 'false'}\n")


def cmd_analyze(a) -> None:
    from .analyze import analyze

    text = analyze([Path(p) for p in a.runs], a.noise_factor, a.scale)
    Path(a.out).write_text(text, encoding="utf-8")
    print(f"wrote {a.out} ({len(text.splitlines())} lines)")


def main() -> None:
    ap = argparse.ArgumentParser(prog="nbench")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, scale=True):
        p.add_argument("--label", required=True, help="results subdirectory, e.g. run1 or smoke-1")
        if scale:
            p.add_argument("--scale", required=True)
        p.add_argument("--container", default="")
        p.add_argument("--disk-path", default="/")

    p = sub.add_parser("load", help="load data, build indexes, record disk use")
    common(p)
    p.set_defaults(fn=cmd_load, aio=True)
    p = sub.add_parser("run", help="run measurement phases (randomized blocks)")
    common(p)
    p.add_argument("--phases", default="all", help="comma list of: calibration,layouts,count_facets,tuning,selectivity,prefilter,noise")
    p.add_argument("--reps", type=int, default=30)
    p.add_argument("--noise-reps", type=int, default=0, help="repetitions for the noise phase (default: --reps; the brief asks 100 or more)")
    p.add_argument("--warm", type=int, default=1, help="warm-up calls on each switch to a new case")
    p.add_argument("--prewarm-rounds", type=int, default=1)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--terms", default="frequent,medium")
    p.add_argument("--time-limit-min", type=float, default=0)
    p.set_defaults(fn=cmd_run, aio=True)
    p = sub.add_parser("load-test", help="open-loop load test (Q1), stateful: run last")
    common(p)
    p.add_argument("--rates", default="", help="comma list of queries per second (default 25,50,100,150,200,300,400)")
    p.add_argument("--duration", type=float, default=30)
    p.add_argument("--cooldown", type=float, default=5)
    p.add_argument("--seed", type=int, default=1)
    p.set_defaults(fn=cmd_load_test, aio=True)
    p = sub.add_parser("limits", help="1,023 / 1,024 values in an `in` clause on _id")
    common(p)
    p.set_defaults(fn=cmd_limits, aio=True)
    p = sub.add_parser("selftest", help="take every planner branch once on the smoke data, with asserts")
    p.add_argument("--scale", default="smoke")
    p.set_defaults(fn=cmd_selftest, aio=True)
    p = sub.add_parser("env", help="write environment.json")
    common(p, scale=False)
    p.set_defaults(fn=cmd_env, aio=False)
    p = sub.add_parser("m0", help="hosted Atlas M0 tasks (skipped unless HOODIE_BENCH_ATLAS_URL is set)")
    common(p, scale=False)
    p.add_argument("--docs", type=int, default=20000)
    p.add_argument("--reps", type=int, default=15)
    p.set_defaults(fn=cmd_m0, aio=False)
    p = sub.add_parser("project", help="decide whether 10M is attempted from the 100k and 1M disk measurements")
    p.add_argument("--runs", nargs="+", required=True, help="result directories holding disk_100k.json and disk_1m.json")
    p.add_argument("--safety", type=float, default=1.25)
    p.add_argument("--min-free-gb", type=float, default=2.0)
    p.add_argument("--target", default="10m")
    p.add_argument("--out", default="")
    p.set_defaults(fn=cmd_project, aio=False)
    p = sub.add_parser("analyze", help="tables for the report from one or more run directories")
    p.add_argument("--runs", nargs="+", required=True)
    p.add_argument("--scale", default="", help="restrict to one scale (default: every scale found)")
    p.add_argument("--noise-factor", type=float, default=2.0, help="a window is noisy if its canary median exceeds this multiple of the run median")
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_analyze, aio=False)
    a = ap.parse_args()
    if getattr(a, "aio", False):
        asyncio.run(a.fn(a))
    else:
        a.fn(a)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
