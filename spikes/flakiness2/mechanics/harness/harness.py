"""Pytest selection and repetition tools on a tiny project.

Usage: harness.py <python> <outdir> <phase>. Phase 1 to 3; phase N restores the
caches saved by phase N-1 of the same occasion and applies the edits of phases 2
and 3 to the checkout before measuring.
"""
import hashlib
import json
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import sys
import time

py, out = sys.argv[1], pathlib.Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
here = pathlib.Path(__file__).resolve().parent
proj = here.parent / "proj"
phase = int(sys.argv[3])
rows = out / "results.jsonl"
log = out / "pytest.log"
NR = ["-p", "no:randomly"]
NC = ["-p", "no:cacheprovider"]
SUM = re.compile(r"(\d+) (passed|failed|deselected|skipped|errors?|xfailed|xpassed)")


def edit():
    if phase >= 2:
        (proj / "src/mod_b.py").write_text("def calc(x):\n    return x * 2 + 0\n")
        t = "def test_new{}():\n    time.sleep(0.02)\n    assert True\n"
        (proj / "tests/test_new.py").write_text("import time\n\n\n" + t.format(0) + "\n\n" + t.format(1))
    if phase >= 3:
        (proj / "data/defs.json").write_text('{"limit": 11}\n')


def clean(cwd, *names):
    for n in names:
        for p in pathlib.Path(cwd).glob(n):
            shutil.rmtree(p, ignore_errors=True) if p.is_dir() else p.unlink()


def run(label, args, fail="0", cwd=proj, reps=1, **extra):
    env = dict(os.environ, MECH_FAIL=fail)
    env.pop("PYTEST_ADDOPTS", None)
    for i in range(reps):
        t = time.perf_counter()
        p = subprocess.run([py, "-m", "pytest", *args], cwd=cwd, env=env, capture_output=True, text=True)
        wall = time.perf_counter() - t
        txt = p.stdout + p.stderr
        with log.open("a") as f:
            f.write(f"\n##### {label} rep{i} {' '.join(args)} fail={fail} rc={p.returncode}\n{txt[-6000:]}\n")
        lines = [x for x in txt.splitlines() if " in " in x and (SUM.search(x) or "no tests ran" in x)]
        counts = {k: int(v) for v, k in SUM.findall(lines[-1])} if lines else {}
        if lines and "no tests ran" in lines[-1]:
            counts["no_tests_ran"] = 1
        ids = re.findall(r"^(\S+::\S+)", txt, re.M)
        seed = re.search(r"randomly-seed=(\d+)", txt)
        r = dict(phase=phase, label=label, rep=i, wall=round(wall, 3), rc=p.returncode, counts=counts,
                 first=ids[:4], order=hashlib.md5("\n".join(ids).encode()).hexdigest()[:8] if ids else None,
                 seed=seed.group(1) if seed else None, **extra)
        with rows.open("a") as f:
            f.write(json.dumps(r) + "\n")
        print(label, r["wall"], r["rc"], counts, flush=True)
    return r


edit()
(out / "meta.json").write_text(json.dumps(dict(phase=phase, cache_matched=os.environ.get("CACHE_MATCHED"),
                                               had_pytest_cache=(proj / ".pytest_cache").exists(),
                                               had_testmondata=any(proj.glob(".testmondata*")))))
if phase == 1:
    clean(proj, ".pytest_cache", ".testmondata*")

run("baseline", NR + NC + ["-q"], reps=3)
run("collect_only", NR + NC + ["-q", "--co"], reps=3)

# lf / ff / nf
if phase == 1:
    run("lf_no_cache", NR + ["--lf", "-q"])
    run("nf_no_cache", NR + ["--nf", "-v"])
elif phase == 2:
    run("nf", NR + ["--nf", "-v"], fail="1")
    run("ff", NR + ["--ff", "-v"], fail="1")
    run("lf", NR + ["--lf", "-q"])
    run("lf_after_pass", NR + ["--lf", "-q"])

# testmon
tm = NR + ["--testmon", "-q"]
if phase == 1:
    for i in range(3):
        d = out / f"tm{i}"
        shutil.copytree(proj, d, ignore=shutil.ignore_patterns(".testmondata*", ".pytest_cache", "__pycache__"))
        run("testmon_cold_copy", tm, cwd=d, rep_dir=i)
        shutil.rmtree(d, ignore_errors=True)
    run("testmon_build", tm)
    run("testmon_nochange", tm)
elif phase == 2:
    run("testmon_after_src_change", tm)
    run("testmon_nochange", tm)
else:
    run("testmon_after_json_change", tm)
    run("full_after_json_change", NR + NC + ["-q"])

# random order and seed replay
if phase != 3:
    run("rand_seed_a1", ["-v", "--randomly-seed=1234"] + NC)
    run("rand_seed_a2", ["-v", "--randomly-seed=1234"] + NC)
    run("rand_seed_b", ["-v", "--randomly-seed=4321"] + NC)
    r = run("rand_default", ["-v"])
    run("rand_last", ["-v", "--randomly-seed=last"], seed_of_default=r["seed"])

    # repeat tools: cost per repeat
    for n in (1, 10, 50):
        run(f"repeat_{n}", NR + NC + ["-q", "--count", str(n)], reps=3)
        run(f"flakefinder_{n}", NR + NC + ["-q", "--flake-finder", "--flake-runs", str(n)], reps=3)

    # xdist startup
    run("xdist_none", NR + NC + ["-q"], reps=3)
    for n in ("0", "1", "2", "4", "auto"):
        run(f"xdist_n{n}", NR + NC + ["-q", "-n", n], reps=3)

if phase == 1:
    run("seed_failure", NR + ["-q"], fail="1")
