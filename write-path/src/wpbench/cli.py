"""Command line: `wpbench run`, `wpbench env`, `wpbench analyze`."""

import argparse
import asyncio
import os
import random
import time
from pathlib import Path

from pymongo import AsyncMongoClient

from . import phases as P
from .cmdmon import CommandCounter
from .common import DB, RESULTS, SCALES, DiskLow, uri, write_json
from .envinfo import collect
from .gen import prepare
from .loadgen import ShardPool, shards_for

ALL = "clauses,shapes,semantics,counts,latency,coalesce,marker,failure"


def _floats(s: str) -> list[float]:
    return [float(x) for x in s.split(",") if x]


async def run(a: argparse.Namespace) -> int:
    run_id = a.run_id or time.strftime("%Y%m%d-%H%M%S")
    out = Path(a.results or RESULTS) / run_id
    out.mkdir(parents=True, exist_ok=True)
    seed = a.seed if a.seed is not None else random.SystemRandom().randrange(1, 10**9)
    smoke = a.scale == "smoke"
    names = [x for x in a.phases.split(",") if x]
    ctx = P.Ctx(uri=uri(), label=a.scale, out=out, seed=seed, smoke=smoke, reps=a.reps, rtts=_floats(a.rtts),
                rates=[int(x) for x in a.rates.split(",")], duration_s=a.duration, warm_s=a.warm, container=a.container)
    ctx.counter = CommandCounter()
    ctx.client = AsyncMongoClient(ctx.uri, event_listeners=[ctx.counter], maxPoolSize=100)
    # The hosted Atlas phase talks only to the cluster named by HOODIE_BENCH_ATLAS_URL, so a run that has nothing
    # but that phase must not need (or inspect) the local engine container.
    only_m0 = names == ["m0"]
    try:
        if only_m0:
            engine = {"mongod": "not used: hosted Atlas M0 phase only"}
            write_json(out / "environment.json", collect(None, engine))
        else:
            info = await ctx.client.admin.command("buildInfo")
            engine = {"mongod": info.get("version")}
            write_json(out / "environment.json", collect(a.container, engine))
        write_json(out / "meta.json", {"run_id": run_id, "seed": seed, "scale": a.scale, "smoke": smoke,
                                       "documents": SCALES[a.scale], "phases": names, "reps": a.reps, "rtts": ctx.rtts,
                                       "rates": ctx.rates, "duration_s": a.duration, "warm_s": a.warm,
                                       "marker_minutes": a.marker_minutes, "fail_pct": a.fail_pct,
                                       "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
        print(f"run {run_id} seed {seed} scale {a.scale}", flush=True)
        needs_data = [n for n in names if n != "m0"]
        if needs_data and not a.no_load:
            res = await prepare(ctx.client, a.scale)
            write_json(out / "load.json", res)
        if any(n in names for n in ("latency", "coalesce", "marker")):
            ctx.pool = ShardPool(shards_for(max(ctx.rates)))
        for n in names:
            print(f"== phase {n}", flush=True)
            if n == "clauses":
                await P.phase_clauses(ctx)
            elif n == "shapes":
                await P.phase_shapes(ctx, [int(x) for x in a.batch_sizes.split(",")])
            elif n == "semantics":
                await P.phase_semantics(ctx)
            elif n == "counts":
                await P.phase_counts(ctx, [int(x) for x in a.count_sizes.split(",")])
            elif n == "latency":
                await P.phase_latency(ctx)
            elif n == "coalesce":
                await P.phase_coalesce(ctx)
            elif n == "marker":
                await P.phase_marker(ctx, a.marker_minutes, a.fail_pct)
            elif n == "failure":
                await P.phase_failure(ctx, a.failure_timeout, a.failure_reps)
            elif n == "m0":
                url = os.environ.get("HOODIE_BENCH_ATLAS_URL")
                if not url:
                    print("HOODIE_BENCH_ATLAS_URL not set: skipping the hosted Atlas (M0) phase", flush=True)
                    write_json(out / "m0" / "skipped.json", {"skipped": "HOODIE_BENCH_ATLAS_URL not set"})
                else:
                    await P.phase_m0(ctx, url)
            else:
                raise SystemExit(f"unknown phase {n}")
        return 0
    except DiskLow as e:
        print(f"STOPPED CLEANLY: {e}", flush=True)
        write_json(out / "stopped.json", {"reason": str(e)})
        return 0
    finally:
        if ctx.pool:
            ctx.pool.close()
        P.stop_proxies(ctx)
        await ctx.client.close()


def main() -> None:
    ap = argparse.ArgumentParser(prog="wpbench")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run phases against the database in WPBENCH_MONGO_URI")
    r.add_argument("--scale", choices=list(SCALES), default="smoke")
    r.add_argument("--phases", default=ALL, help=f"comma list, from: {ALL},m0")
    r.add_argument("--run-id")
    r.add_argument("--results", help="results root (default write-path/results)")
    r.add_argument("--seed", type=int, help="shuffle seed (logged); default random")
    r.add_argument("--reps", type=int, default=5)
    r.add_argument("--rtts", default="0,1,5", help="simulated round-trip times in ms")
    r.add_argument("--rates", default="10,100,500", help="writes per second")
    r.add_argument("--duration", type=float, default=30.0, help="seconds per measured case run")
    r.add_argument("--warm", type=float, default=5.0, help="warm-up seconds inside each case run (kept, excluded)")
    r.add_argument("--batch-sizes", default="1,10,100,500,1000,5000")
    r.add_argument("--count-sizes", default="1,10,100,1000")
    r.add_argument("--marker-minutes", type=float, default=60.0)
    r.add_argument("--fail-pct", type=float, default=0.01)
    r.add_argument("--failure-timeout", type=float, default=10.0)
    r.add_argument("--failure-reps", type=int, default=5)
    r.add_argument("--container", help="Docker container name (environment record; engine pause in the failure phase)")
    r.add_argument("--no-load", action="store_true")
    e = sub.add_parser("env", help="print the environment record")
    e.add_argument("--container")
    an = sub.add_parser("analyze", help="build report tables from one or more run directories")
    an.add_argument("runs", nargs="+")
    an.add_argument("--out", default=str(RESULTS / "analysis"))
    a = ap.parse_args()
    if a.cmd == "run":
        raise SystemExit(asyncio.run(run(a)))
    if a.cmd == "env":
        import json

        print(json.dumps(collect(a.container), indent=2, default=str))
    else:
        from .analyze import analyze

        analyze([Path(x) for x in a.runs], Path(a.out))


if __name__ == "__main__":
    main()
