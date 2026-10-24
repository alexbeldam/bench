"""Real pytest runs: cost per repeat of pytest-repeat against an in-process loop, and a check that pytest-randomly shuffles as modelled."""
from __future__ import annotations

import os
import random
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from .core import REG, Ctx, run_one, selected
from .exp import dump, header, seed_or_new

HERE = Path(__file__).resolve().parents[2]
COST_TESTS = ["rng-1pct", "rng-0.1pct", "c-sort", "c-json", "c-gather", "c-order-stable", "c-regex", "settle-eq-fixed", "c-s01"]


def pytest_run(args: list[str], ids: list[str] | None = None) -> tuple[float, dict[str, str]]:
    """Runs pytest_suite with FL2P_ONLY=ids. Returns the wall time and the outcome of each test case (names as junit reports them)."""
    with tempfile.TemporaryDirectory() as td:
        xml = Path(td) / "r.xml"
        cmd = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", f"--junitxml={xml}", "--tb=no", *args]
        env = {**os.environ, "FL2P_ONLY": ",".join(ids or [])}
        t0 = time.perf_counter()
        subprocess.run(cmd, cwd=HERE, capture_output=True, text=True, env=env)  # noqa: S603
        wall = time.perf_counter() - t0
        out: dict[str, str] = {}
        if xml.exists():
            for tc in ET.parse(xml).getroot().iter("testcase"):
                out[tc.get("name", "")] = "fail" if (tc.find("failure") is not None or tc.find("error") is not None) else "pass"
        return wall, out


def inproc_loop(names: list[str], n: int) -> float:
    ctx = Ctx()
    t0 = time.perf_counter()
    for _ in range(n):
        for nm in names:
            run_one(REG[nm], ctx)
    return time.perf_counter() - t0


def cmd_pytestcost(a: Any) -> None:
    seed = seed_or_new(a.seed)
    k = COST_TESTS
    plan = [(m, n, r) for m in ("pytest-repeat-function", "pytest-repeat-session", "inproc-loop", "pytest-plain") for n in a.ns for r in range(a.reps)
            if not (m == "pytest-plain" and n != a.ns[0])]
    random.Random(seed).shuffle(plan)
    rows = []
    for mech, n, r in plan:
        if mech == "inproc-loop":
            wall = inproc_loop(COST_TESTS, n)
        elif mech == "pytest-plain":
            wall, out = pytest_run(["-p", "no:randomly"], k)
            if len(out) != len(k):
                raise RuntimeError(f"pytest ran {len(out)} test cases, expected {len(k)}")
        else:
            scope = mech.rsplit("-", 1)[1]
            wall, out = pytest_run(["-p", "no:randomly", f"--count={n}", f"--repeat-scope={scope}"], k)
            if len(out) != n * len(k):
                raise RuntimeError(f"pytest ran {len(out)} test cases, expected {n * len(k)}: the cost row would be invalid")
        rows.append({"mech": mech, "n": n, "rep": r, "wall_s": wall})
        print(f"{mech} n={n} rep={r} {wall:.2f}s", flush=True)
    dump(a.out, {**header("pytestcost", seed, tests=COST_TESTS), "rows": rows})


def cmd_pytestcheck(a: Any) -> None:
    """Default order once, then random order with `a.seeds` seeds: which victims fail. Compared with the model in analyze."""
    seed = seed_or_new(a.seed)
    rs = random.Random(seed)
    k = ["order-dep-hidden", "order-dep-polluter", "order-dep-exposed", "c-sort", "c-json"]
    rows = []
    wall, out = pytest_run(["-p", "no:randomly"], k)
    rows.append({"order": "default", "seed": None, "res": out})
    for _ in range(a.seeds):
        s = rs.randrange(10**6)
        wall, out = pytest_run([f"--randomly-seed={s}"], k)
        rows.append({"order": "random", "seed": s, "res": out})
    for scope in ("function", "session"):
        wall, out = pytest_run(["-p", "no:randomly", "--count=4", f"--repeat-scope={scope}"], ["order-dep-hidden", "order-dep-polluter", "order-dep-exposed"])
        rows.append({"order": f"repeat-{scope}", "seed": None, "res": out})
    dump(a.out, {**header("pytestcheck", seed, k=k), "rows": rows})
    hid = sum(r["res"].get("test_planted[order-dep-hidden]") == "fail" for r in rows if r["order"] == "random")
    exp = sum(r["res"].get("test_planted[order-dep-exposed]") == "fail" for r in rows if r["order"] == "random")
    print(f"pytest-randomly over {a.seeds} seeds: hidden failed {hid}, exposed failed {exp}; default order: {rows[0]['res']}")
