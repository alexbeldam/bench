"""Drives the pull request scenarios and records API states.

orch.py setup | occasion <label> | teardown. Needs git and gh on PATH, run from a
clone whose origin is the repository under test. Branch names start with fl2-mech-.
"""
import json
import pathlib
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

R = "alexbeldam/bench"
ROOT = pathlib.Path(__file__).resolve().parents[1]
CLONE = ROOT.parents[2]
WT = CLONE.parent / "wt"
RES = ROOT / "results"
BASES = {"": "fl2-mech-base", "a": "fl2-mech-base-a", "b": "fl2-mech-base-b", "c": "fl2-mech-base-c"}
lock = threading.Lock()


def now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def sh(args, cwd=None, check=True):
    for _ in range(4):
        p = subprocess.run(args, cwd=cwd, capture_output=True, text=True, encoding="utf-8")
        if not p.returncode or "error connecting" not in p.stderr:
            break
        time.sleep(5)
    if check and p.returncode:
        raise RuntimeError(f"{args}: {p.stderr}")
    return p


def gh(*a, check=True):
    return sh(["gh", *a], check=check)


def gj(path, *a):
    return json.loads(gh("api", path, *a).stdout)


def api(method, path, body=None):
    args = ["gh", "api", "-X", method, path]
    if body is not None:
        args += ["--input", "-"]
    for _ in range(4):
        p = subprocess.run(args, input=json.dumps(body) if body is not None else None, capture_output=True, text=True, encoding="utf-8")
        if not p.returncode or "error connecting" not in p.stderr:
            break
        time.sleep(5)
    return dict(rc=p.returncode, out=p.stdout[:1500], err=p.stderr[:1500])


def wt(branch, base=BASES[""]):
    with lock:
        d = WT / branch
        sh(["git", "fetch", "-q", "origin", base], cwd=CLONE)
        sh(["git", "worktree", "add", "-q", "-B", branch, str(d), f"origin/{base}"], cwd=CLONE)
    return d


def commit(d, files, msg):
    for rel, txt in files.items():
        p = d / "spikes/flakiness2/mechanics" / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(txt, newline="\n")
    sh(["git", "add", "-A"], cwd=d)
    sh(["git", "commit", "-q", "--allow-empty", "-m", msg], cwd=d)


def push(d, branch):
    sh(["git", "push", "-q", "origin", f"{branch}:{branch}"], cwd=d)


def open_pr(branch, base, title):
    u = gh("pr", "create", "--head", branch, "--base", base, "--title", title, "--body", "Mechanics experiment, closed after the run.").stdout.strip()
    return int(u.rsplit("/", 1)[1])


def snap(n):
    for _ in range(6):
        p = gj(f"repos/{R}/pulls/{n}")
        if p["mergeable_state"] != "unknown":
            break
        time.sleep(3)
    cr = gj(f"repos/{R}/commits/{p['head']['sha']}/check-runs")["check_runs"]
    return dict(t=now(), state=p["state"], merged=p["merged"], mergeable=p["mergeable"], mstate=p["mergeable_state"],
                sha=p["head"]["sha"][:7], checks=sorted((c["name"], c["status"], c["conclusion"]) for c in cr))


def settle(n, secs):
    """Poll until every check completed (and at least one exists) or secs elapsed."""
    t0 = time.time()
    s = snap(n)
    while time.time() - t0 < secs:
        if s["checks"] and all(c[1] == "completed" for c in s["checks"]):
            time.sleep(8)
            return snap(n)
        time.sleep(12)
        s = snap(n)
    return s


def merge(n):
    return api("PUT", f"repos/{R}/pulls/{n}/merge", {"merge_method": "merge"})


def env_of(run_id):
    p = gh("run", "view", str(run_id), "--log", check=False).stdout
    return [json.loads(l.split("MECH_ENV ", 1)[1]) for l in p.splitlines() if "MECH_ENV {" in l][:1]


def src(name):
    return {f"proj/src/extra_{name}.py": f"def f():\n    return '{name}'\n"}


def scen_paths(occ, res):
    out = res["paths"] = {}
    d = wt(f"fl2-mech-{occ}-a-docs", BASES["a"])
    commit(d, {f"docs/{occ}-a.md": "docs only\n"}, "Docs only change")
    push(d, f"fl2-mech-{occ}-a-docs")
    n = open_pr(f"fl2-mech-{occ}-a-docs", BASES["a"], f"{occ} paths docs only")
    d2 = wt(f"fl2-mech-{occ}-a-code", BASES["a"])
    commit(d2, src(f"{occ}_a"), "Code change")
    push(d2, f"fl2-mech-{occ}-a-code")
    m = open_pr(f"fl2-mech-{occ}-a-code", BASES["a"], f"{occ} paths code")
    out["docs_pr"], out["code_pr"] = n, m
    out["code"] = settle(m, 240)
    out["code_merge"] = merge(m)
    out["docs_samples"] = [snap(n)]
    for _ in range(4):
        time.sleep(60)
        out["docs_samples"].append(snap(n))
    out["docs_merge_attempt"] = merge(n)
    out["docs_final"] = snap(n)


def scen_gate(occ, res):
    out = res["gate"] = {}
    kinds = {"docs": {f"docs/{occ}-b.md": "docs only\n"},
             "code": src(f"{occ}_b"),
             "fail": {f"proj/src/extra_{occ}_fail.py": "# FAIL_MARK\n"}}
    prs = {}
    for k, files in kinds.items():
        br = f"fl2-mech-{occ}-b-{k}"
        d = wt(br, BASES["b"])
        commit(d, files, f"{k} change")
        push(d, br)
        prs[k] = open_pr(br, BASES["b"], f"{occ} gate {k}")
    for k, n in prs.items():
        out[k] = dict(pr=n, final=settle(n, 300))
    for k in ("docs", "code", "fail"):
        out[k]["merge"] = merge(prs[k])
        out[k]["after"] = snap(prs[k])


def scen_auto(occ, res):
    out = res["auto"] = {}
    prs = {}
    for k in ("c1", "c2"):
        br = f"fl2-mech-{occ}-{k}"
        d = wt(br, BASES["c"])
        commit(d, src(f"{occ}_{k}"), f"{k} change")
        push(d, br)
        prs[k] = open_pr(br, BASES["c"], f"{occ} auto {k}")
        p = gh("pr", "merge", str(prs[k]), "--auto", "--merge", check=False)
        out[f"{k}_automerge"] = dict(rc=p.returncode, out=p.stdout[:300], err=p.stderr[:300], at=now())
    out["prs"] = prs
    out["series"] = []
    updated = False
    t0 = time.time()
    while time.time() - t0 < 720:
        row = {k: snap(n) for k, n in prs.items()}
        out["series"].append({k: dict(t=v["t"], state=v["state"], merged=v["merged"], mstate=v["mstate"],
                                      checks=[(c[0], c[2] or c[1]) for c in v["checks"]]) for k, v in row.items()})
        if all(v["merged"] for v in row.values()):
            break
        behind = [k for k, v in row.items() if v["mstate"] == "behind" and not v["merged"]]
        if behind and not updated:
            k = behind[0]
            out["update_branch"] = dict(pr=prs[k], at=now(), **api("PUT", f"repos/{R}/pulls/{prs[k]}/update-branch", {}))
            updated = True
        time.sleep(15)
    out["final"] = {k: snap(n) for k, n in prs.items()}


def scen_conc(occ, res):
    out = res["conc"] = {}
    bx, by = f"fl2-mech-{occ}-x", f"fl2-mech-{occ}-y"
    d = wt(bx)
    files = {f"docs/{occ}-x1.md": "x\n", f"docs/{occ}-x2.md": "x\n", f"proj/tests/test_{occ}_x.py": "def test_x():\n    assert True\n",
             f"proj/data/{occ}_x.json": "{}\n", **src(f"{occ}_x")}
    commit(d, files, "First commit")
    t_push1 = now()
    push(d, bx)
    nx = open_pr(bx, BASES[""], f"{occ} conc x")
    commit(d, {f"docs/{occ}-x3.md": "second\n"}, "Second commit")
    push(d, bx)
    t_push2 = now()
    time.sleep(10)
    dy = wt(by)
    commit(dy, {f"docs/{occ}-y.md": "y\n", **src(f"{occ}_y")}, "Single commit")
    push(dy, by)
    ny = open_pr(by, BASES[""], f"{occ} conc y")
    out.update(prs=dict(x=nx, y=ny), push_times=dict(x1=t_push1, x2=t_push2))
    t0 = time.time()
    while time.time() - t0 < 420:
        time.sleep(15)
        runs = []
        for b in (bx, by):
            runs += json.loads(gh("run", "list", "--branch", b, "-L", "30", "--json", "databaseId,workflowName,headBranch,event,status,conclusion,createdAt,updatedAt,headSha").stdout)
        if len(runs) >= 8 and all(r["status"] == "completed" for r in runs):
            break
    out["runs"] = [dict(id=r["databaseId"], wf=r["workflowName"], br=r["headBranch"].replace(f"fl2-mech-{occ}-", ""), sha=r["headSha"][:7],
                        status=r["status"], concl=r["conclusion"], created=r["createdAt"], updated=r["updatedAt"]) for r in runs]
    out["diff"] = []
    for r in out["runs"]:
        if r["wf"] == "spike-fl2-mech-diff":
            p = gh("run", "view", str(r["id"]), "--log", check=False).stdout
            out["diff"].append(dict(id=r["id"], br=r["br"], sha=r["sha"],
                                    data=[json.loads(l.split("MECH_DIFF ", 1)[1]) for l in p.splitlines() if "MECH_DIFF {" in l][:1],
                                    env=[json.loads(l.split("MECH_ENV ", 1)[1]) for l in p.splitlines() if "MECH_ENV {" in l][:1]))


def scen_mq(occ, res):
    out = res["mq"] = {}
    body = {"name": f"fl2-mech-mq-{occ}", "target": "branch", "enforcement": "active",
            "conditions": {"ref_name": {"include": ["refs/heads/fl2-mech-mq-unused"], "exclude": []}},
            "rules": [{"type": "merge_queue", "parameters": {
                "check_response_timeout_minutes": 60, "grouping_strategy": "ALLGREEN", "max_entries_to_build": 5,
                "max_entries_to_merge": 5, "merge_method": "MERGE", "min_entries_to_merge": 1, "min_entries_to_merge_wait_minutes": 5}}]}
    out["create_merge_queue_ruleset"] = api("POST", f"repos/{R}/rulesets", body)
    try:
        rid = json.loads(out["create_merge_queue_ruleset"]["out"]).get("id")
    except Exception:
        rid = None
    if rid:
        out["cleanup"] = api("DELETE", f"repos/{R}/rulesets/{rid}")
    repo = gj(f"repos/{R}")
    out["repo"] = {k: repo.get(k) for k in ("visibility", "allow_auto_merge", "allow_merge_commit", "allow_squash_merge", "allow_rebase_merge", "delete_branch_on_merge")}
    out["owner_type"] = repo["owner"]["type"]
    out["rules_on_base_c"] = json.loads(gh("api", f"repos/{R}/rules/branches/{BASES['c']}", check=False).stdout or "null")


def scen_pytest(occ, res):
    out = res["pytest"] = {"runs": []}
    br = f"fl2-mech-run-{occ}"
    d = wt(br)
    steps = [
        (1, {}),
        (2, {"proj/src/mod_b.py": "def calc(x):\n    return x * 2 + 0\n",
             "proj/tests/test_new.py": "import time\n\n\ndef test_new0():\n    time.sleep(0.02)\n    assert True\n\n\ndef test_new1():\n    time.sleep(0.02)\n    assert True\n"}),
        (3, {"proj/data/defs.json": '{"limit": 11}\n'}),
    ]
    for ph, files in steps:
        files = dict(files, PHASE=f"{ph}\n")
        commit(d, files, f"Phase {ph}")
        sha = sh(["git", "rev-parse", "HEAD"], cwd=d).stdout.strip()
        push(d, br)
        t0 = time.time()
        rid = None
        while time.time() - t0 < 1500:
            time.sleep(20)
            rs = json.loads(gh("run", "list", "--branch", br, "--workflow", "spike-fl2-mech-pytest.yml", "-L", "10", "--json", "databaseId,headSha,status,conclusion").stdout)
            m = [r for r in rs if r["headSha"] == sha]
            if m and m[0]["status"] == "completed":
                rid = m[0]["databaseId"]
                out["runs"].append(dict(phase=ph, id=rid, conclusion=m[0]["conclusion"], secs=round(time.time() - t0)))
                break
        if rid is None:
            out["runs"].append(dict(phase=ph, id=None, conclusion="timeout"))
            break
        dl = RES / f"occ-{occ}" / f"p{ph}"
        for _ in range(3):
            if gh("run", "download", str(rid), "-D", str(dl), check=False).returncode == 0:
                break
            time.sleep(5)
        out["runs"][-1]["env"] = env_of(rid)
        if out["runs"][-1]["conclusion"] != "success":
            break


def occasion(occ):
    RES.mkdir(exist_ok=True)
    res = {"occasion": occ, "start_utc": datetime.now(timezone.utc).isoformat()}
    ts = []
    for f in (scen_paths, scen_gate, scen_auto, scen_conc, scen_mq, scen_pytest):
        def go(f=f):
            try:
                f(occ, res)
            except Exception as e:
                res[f.__name__] = {"error": repr(e)[:800]}
        t = threading.Thread(target=go)
        t.start()
        ts.append(t)
    for t in ts:
        t.join()
    res["end_utc"] = datetime.now(timezone.utc).isoformat()
    (RES / f"occ-{occ}.json").write_text(json.dumps(res, indent=1))
    print("done", occ)


def ruleset(name, base, ctx, strict):
    return {"name": name, "target": "branch", "enforcement": "active",
            "conditions": {"ref_name": {"include": [f"refs/heads/{base}"], "exclude": []}},
            "rules": [{"type": "required_status_checks", "parameters": {
                "strict_required_status_checks_policy": strict, "required_status_checks": [{"context": ctx}]}}]}


def setup():
    log = {}
    sha = sh(["git", "rev-parse", "HEAD"], cwd=CLONE).stdout.strip()
    for b in BASES.values():
        sh(["git", "push", "-q", "origin", f"{sha}:refs/heads/{b}"], cwd=CLONE)
    log["bases"] = {b: sha for b in BASES.values()}
    log["allow_auto_merge_before"] = gj(f"repos/{R}")["allow_auto_merge"]
    log["patch_auto_merge"] = api("PATCH", f"repos/{R}", {"allow_auto_merge": True})
    log["rulesets"] = {}
    for name, base, ctx, strict in [("fl2-mech-a", BASES["a"], "paths-check", False), ("fl2-mech-b", BASES["b"], "gate", False), ("fl2-mech-c", BASES["c"], "gate", True)]:
        log["rulesets"][name] = api("POST", f"repos/{R}/rulesets", ruleset(name, base, ctx, strict))
    RES.mkdir(exist_ok=True)
    (RES / "settings-log.json").write_text(json.dumps(log, indent=1))
    print(json.dumps(log, indent=1))


def teardown():
    log = {}
    for pr in json.loads(gh("pr", "list", "--state", "open", "-L", "200", "--json", "number,headRefName").stdout):
        if pr["headRefName"].startswith("fl2-mech-"):
            gh("pr", "close", str(pr["number"]), check=False)
    for r in gj(f"repos/{R}/rulesets"):
        if r["name"].startswith("fl2-mech-"):
            log[f"delete_ruleset_{r['name']}"] = api("DELETE", f"repos/{R}/rulesets/{r['id']}")
    log["patch_auto_merge"] = api("PATCH", f"repos/{R}", {"allow_auto_merge": False})
    refs = gh("api", f"repos/{R}/git/matching-refs/heads/fl2-mech-", "--paginate", "--jq", ".[].ref").stdout.split()
    for ref in refs:
        log[f"del_{ref}"] = api("DELETE", f"repos/{R}/git/{ref}")["rc"]
    RES.mkdir(exist_ok=True)
    (RES / "teardown-log.json").write_text(json.dumps(log, indent=1))
    print(json.dumps(log, indent=1))


if __name__ == "__main__":
    {"setup": setup, "teardown": teardown}.get(sys.argv[1], lambda: occasion(sys.argv[2]))()
