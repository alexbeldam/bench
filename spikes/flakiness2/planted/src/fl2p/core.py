"""Registry of planted tests, the in-process runner and the experiment arms.

A planted test is an async callable `fn(ctx)` that raises on failure. Each carries a label (kind, truth, engine, variant).
`truth` is the per-run failure probability in natural mode when it is known by construction or analytically, the string
"measured" when it is only known from a large-n run, and 0.0 for tests that must never fail.
"""
from __future__ import annotations

import asyncio
import os
import random
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

REG: dict[str, "T"] = {}


@dataclass
class T:
    name: str
    kind: str  # race, rate, delay, timing, order-dep, control, helper, real-race
    truth: Any  # float | "measured"
    engine: str  # none, inmem, real
    variant: str  # buggy, fixed, control, helper
    fn: Callable[..., Any]
    forced: bool = False  # has an injection mode (strategy S3)
    cost: str = "micro"  # micro (no env), env (builds an in-memory env), slow (real engine)
    note: str = ""

    def label(self) -> dict[str, Any]:
        return {"name": self.name, "kind": self.kind, "truth": self.truth, "engine": self.engine, "variant": self.variant,
                "forced": self.forced, "cost": self.cost, "note": self.note}


def reg(name: str, kind: str, truth: Any, engine: str, variant: str, fn: Callable[..., Any], **kw: Any) -> T:
    assert name not in REG, name
    t = T(name, kind, truth, engine, variant, fn, **kw)
    REG[name] = t
    return t


@dataclass
class Ctx:
    forced: bool = False
    state: dict[str, Any] = field(default_factory=dict)  # process-wide state shared by the tests of one run (order-dependence)
    val: dict[str, float] = field(default_factory=dict)  # numbers a test records about its own run (stored with the pool)

    def urand(self) -> float:
        """One uniform draw from a generator seeded from os.urandom, fresh for every call."""
        return random.Random(os.urandom(16)).random()

    def rng(self) -> random.Random:
        return random.Random(os.urandom(16))


@dataclass
class Res:
    ok: bool
    dur: float
    err: str = ""
    val: dict[str, float] = field(default_factory=dict)


_LOOP: asyncio.AbstractEventLoop | None = None


def loop() -> asyncio.AbstractEventLoop:
    global _LOOP
    if _LOOP is None or _LOOP.is_closed():
        _LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(_LOOP)
    return _LOOP


def run_one(t: T, ctx: Ctx) -> Res:
    t0 = time.perf_counter()
    try:
        loop().run_until_complete(t.fn(ctx))
        return Res(True, time.perf_counter() - t0, "", dict(ctx.val))
    except BaseException as e:  # noqa: BLE001
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise
        return Res(False, time.perf_counter() - t0, f"{type(e).__name__}: {str(e)[:100]}", dict(ctx.val))


def selected(sel: str | None = None, engine: tuple[str, ...] = ("none", "inmem")) -> list[T]:
    """Registry order is the default order. `sel` is a comma list of names, kinds or variants."""
    ts = [t for t in REG.values() if t.engine in engine]
    if sel and sel != "all":
        keys = set(sel.split(","))
        ts = [t for t in ts if t.name in keys or t.kind in keys or t.variant in keys]
    return ts


# ---- arms: the environment condition of a run ---------------------------------------------------------------------


def ncpu() -> int:
    try:
        return len(os.sched_getaffinity(0))  # type: ignore[attr-defined]
    except AttributeError:
        return os.cpu_count() or 1


@contextmanager
def arm(name: str):
    """base | cpus2 | stress<L> | cpus2+stress<L> | jitter.

    cpus2 pins this process (and stress-ng) to two cores: an emulation of a 2 vCPU runner on a 4 vCPU one, not the real thing.
    stress<L> starts stress-ng with one cpu worker per allowed core at L percent load. jitter starts a thread that holds the GIL
    for 0 to 3 ms at random intervals (scheduling perturbation between the loop and worker threads)."""
    procs: list[subprocess.Popen] = []
    old_aff = None
    jit: dict[str, Any] = {"on": False}
    parts = [p for p in name.split("+") if p != "base"]
    try:
        for p in parts:
            if p == "cpus2":
                if hasattr(os, "sched_setaffinity"):
                    old_aff = os.sched_getaffinity(0)
                    os.sched_setaffinity(0, set(sorted(old_aff)[:2]))
                else:
                    print("cpus2: sched_setaffinity not available on this platform, arm is a no-op", file=sys.stderr)
        for p in parts:
            if p.startswith("stress"):
                load = int(p[6:])
                cmd = ["stress-ng", "--cpu", str(ncpu()), "--cpu-load", str(load), "--timeout", "3h"]
                try:
                    procs.append(subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))  # noqa: S603
                except FileNotFoundError:
                    print("stress-ng not found: arm runs WITHOUT noise", file=sys.stderr)
                time.sleep(1.0)
            if p == "jitter":
                jit["on"] = True
                rnd = random.Random(os.urandom(8))

                def hog() -> None:
                    # a thread that holds the GIL for 0 to 3 ms and sleeps 0 to 2 ms, so the loop thread and any worker thread
                    # compete for the interpreter at random moments (a scheduling perturbation, not CPU load)
                    while jit["on"]:
                        end = time.perf_counter() + rnd.random() * 0.003
                        while time.perf_counter() < end:
                            pass
                        time.sleep(rnd.random() * 0.002)

                th = threading.Thread(target=hog, daemon=True)
                th.start()
                jit["th"] = th
        yield
    finally:
        jit["on"] = False
        if jit.get("th") is not None:
            jit["th"].join(2)
        for pr in procs:
            pr.terminate()
        for pr in procs:
            try:
                pr.wait(10)
            except subprocess.TimeoutExpired:
                pr.kill()
        if old_aff is not None:
            os.sched_setaffinity(0, old_aff)


def machine() -> dict[str, Any]:
    import importlib.metadata as md
    import platform

    m: dict[str, Any] = {"python": platform.python_version(), "platform": platform.platform(), "nproc": ncpu(),
                         "runner_image": f"{os.environ.get('ImageOS', '')} {os.environ.get('ImageVersion', '')}".strip(),
                         "sha": os.environ.get("GITHUB_SHA", "")}
    try:
        for ln in open("/proc/cpuinfo", encoding="utf8"):
            if ln.startswith("model name"):
                m["cpu"] = ln.split(":", 1)[1].strip()
                break
    except OSError:
        m["cpu"] = platform.processor()
    for k in ("FL2P_ENGINE_DIGEST", "FL2P_SETUP_S", "FL2P_ENGINE_START_S"):
        if k in os.environ:
            m[k.lower()] = os.environ[k]
    pk = {}
    for p in ("pytest", "pytest-randomly", "pytest-repeat", "hypothesis", "pymongo", "beanie"):
        try:
            pk[p] = md.version(p)
        except md.PackageNotFoundError:
            pass
    m["pkg"] = pk
    return m
