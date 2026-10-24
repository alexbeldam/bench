"""Opens the pull request scenarios and records API states, without waiting.

orch.py setup | create <label> | collect <label> | merge <label> | teardown.
Needs git and gh on PATH, run from a clone whose origin is the repository under
test and whose HEAD is pushed. Branch names start with fl2-mech-. `create`
returns as soon as the pull requests and the pytest dispatch exist; `collect`
takes one snapshot of their current state and can be repeated later.
"""
import json
import pathlib
import subprocess
import sys
import time
from datetime import datetime, timezone

R = "alexbeldam/bench"
ROOT = pathlib.Path(__file__).resolve().parents[1]
CLONE = ROOT.parents[2]
WT = CLONE.parent / "wt"
RES = CLONE.parent / "spikes/flakiness-strategy-2/runs/mech"
BASES = {"": "fl2-mech-base", "a": "fl2-mech-base-a", "b": "fl2-mech-base-b", "c": "fl2-mech-base-c"}


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    return dict(rc=p.returncode, out=p.stdout[:1500], err=p.stderr[:1500], at=now())


def wt(branch, base):
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


def new_pr(occ, tag, base, files, title):
    br = f"fl2-mech-{occ}-{tag}"
    d = wt(br, base)
    commit(d, files, f"{tag} change")
    push(d, br)
    return open_pr(br, base, f"{occ} {title}")


def snap(n):
    for _ in range(6):
        p = gj(f"repos/{R}/pulls/{n}")
        if p["mergeable_state"] != "unknown":
            break
        time.sleep(3)
    cr = gj(f"repos/{R}/commits/{p['head']['sha']}/check-runs")["check_runs"]
    return dict(t=now(), state=p["state"], merged=p["merged"], mergeable=p["mergeable"], mstate=p["mergeable_state"],
                sha=p["head"]["sha"][:7], checks=sorted((c["name"], c["status"], c["conclusion"]) for c in cr))


def merge(n):
    return api("PUT", f"repos/{R}/pulls/{n}/merge", {"merge_method": "merge"})


def src(name):
    return {f"proj/src/extra_{name}.py": f"def f():\n    return '{name}'\n"}


def load(occ):
    p = RES / f"occ-{occ}.json"
    return json.loads(p.read_text()) if p.exists() else {"occasion": occ}


def save(occ, res):
    RES.mkdir(parents=True, exist_ok=True)
    (RES / f"occ-{occ}.json").write_text(json.dumps(res, indent=1))


def create(occ):
    res = {"occasion": occ, "created": now()}
    res["paths"] = dict(docs=new_pr(occ, "a-docs", BASES["a"], {f"docs/{occ}-a.md": "docs only\n"}, "paths docs only"),
                        code=new_pr(occ, "a-code", BASES["a"], src(f"{occ}_a"), "paths code"))
    res["gate"] = dict(docs=new_pr(occ, "b-docs", BASES["b"], {f"docs/{occ}-b.md": "docs only\n"}, "gate docs"),
                       code=new_pr(occ, "b-code", BASES["b"], src(f"{occ}_b"), "gate code"),
                       fail=new_pr(occ, "b-fail", BASES["b"], {f"proj/src/extra_{occ}_fail.py": "# FAIL_MARK\n"}, "gate fail"))
    auto = {}
    for k in ("c1", "c2"):
        n = new_pr(occ, k, BASES["c"], src(f"{occ}_{k}"), f"auto {k}")
        p = gh("pr", "merge", str(n), "--auto", "--merge", check=False)
        auto[k] = dict(pr=n, rc=p.returncode, out=p.stdout[:300], err=p.stderr[:300], at=now())
    res["auto"] = auto
    bx = f"fl2-mech-{occ}-x"
    d = wt(bx, BASES[""])
    commit(d, {f"docs/{occ}-x1.md": "x\n", f"docs/{occ}-x2.md": "x\n", f"proj/tests/test_{occ}_x.py": "def test_x():\n    assert True\n",
               f"proj/data/{occ}_x.json": "{}\n", **src(f"{occ}_x")}, "First commit")
    t1 = now()
    push(d, bx)
    nx = open_pr(bx, BASES[""], f"{occ} conc x")
    commit(d, {f"docs/{occ}-x3.md": "second\n"}, "Second commit")
    push(d, bx)
    t2 = now()
    ny = new_pr(occ, "y", BASES[""], {f"docs/{occ}-y.md": "y\n", **src(f"{occ}_y")}, "conc y")
    res["conc"] = dict(x=nx, y=ny, push_x1=t1, push_x2=t2)
    res["mq"] = scen_mq(occ)
    p = gh("workflow", "run", "spike-fl2-mech-pytest.yml", "--ref", "main", "-f", f"occasion={occ}", "-f", "phase=1", check=False)
    res["pytest_dispatch"] = dict(rc=p.returncode, out=p.stdout[:300], err=p.stderr[:300], at=now())
    save(occ, res)
    print(json.dumps(res, indent=1))


def scen_mq(occ):
    out = {}
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
    return out


def runs_of(branch):
    return json.loads(gh("run", "list", "--branch", branch, "-L", "30", "--json", "databaseId,workflowName,event,status,conclusion,createdAt,updatedAt,headSha").stdout)


def collect(occ):
    res = load(occ)
    s = dict(at=now())
    s["paths"] = {k: snap(n) for k, n in res["paths"].items()}
    s["gate"] = {k: snap(n) for k, n in res["gate"].items()}
    s["auto"] = {k: snap(v["pr"]) for k, v in res["auto"].items()}
    behind = [k for k, v in s["auto"].items() if v["mstate"] == "behind" and not v["merged"]]
    if behind and not any("update_branch" in x for x in res.get("collects", [])):
        k = behind[0]
        n = res["auto"][k]["pr"]
        s["update_branch"] = dict(pr=n, **api("PUT", f"repos/{R}/pulls/{n}/update-branch", {}))
    s["conc_runs"] = {}
    for k in ("x", "y"):
        s["conc_runs"][k] = [dict(id=r["databaseId"], wf=r["workflowName"], sha=r["headSha"][:7], status=r["status"], concl=r["conclusion"],
                                  created=r["createdAt"], updated=r["updatedAt"]) for r in runs_of(f"fl2-mech-{occ}-{k}")]
    s["pytest_runs"] = json.loads(gh("run", "list", "--workflow", "spike-fl2-mech-pytest.yml", "-L", "20", "--json", "databaseId,status,conclusion,displayTitle,createdAt").stdout)
    res.setdefault("collects", []).append(s)
    save(occ, res)
    print(json.dumps(s, indent=1))


def merge_attempts(occ):
    """Run once, after collect: tries to merge the paths and gate pull requests."""
    res = load(occ)
    out = {"at": now()}
    for grp in ("paths", "gate"):
        for k, n in res[grp].items():
            out[f"{grp}_{k}"] = dict(pr=n, **merge(n))
    res["merge_attempts"] = out
    save(occ, res)
    print(json.dumps(out, indent=1))


def ruleset(name, base, ctx, strict):
    return {"name": name, "target": "branch", "enforcement": "active",
            "conditions": {"ref_name": {"include": [f"refs/heads/{base}"], "exclude": []}},
            "rules": [{"type": "required_status_checks", "parameters": {
                "strict_required_status_checks_policy": strict, "required_status_checks": [{"context": ctx}]}}]}


def setup():
    log = {}
    sha = sh(["git", "rev-parse", "HEAD"], cwd=CLONE).stdout.strip()
    for b in BASES.values():
        sh(["git", "push", "-q", "-f", "origin", f"{sha}:refs/heads/{b}"], cwd=CLONE)
    log["bases"] = {b: sha for b in BASES.values()}
    log["allow_auto_merge_before"] = gj(f"repos/{R}")["allow_auto_merge"]
    log["patch_auto_merge"] = api("PATCH", f"repos/{R}", {"allow_auto_merge": True})
    log["rulesets"] = {}
    for name, base, ctx, strict in [("fl2-mech-a", BASES["a"], "paths-check", False), ("fl2-mech-b", BASES["b"], "gate", False), ("fl2-mech-c", BASES["c"], "gate", True)]:
        log["rulesets"][name] = api("POST", f"repos/{R}/rulesets", ruleset(name, base, ctx, strict))
    RES.mkdir(parents=True, exist_ok=True)
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
    RES.mkdir(parents=True, exist_ok=True)
    (RES / "teardown-log.json").write_text(json.dumps(log, indent=1))
    print(json.dumps(log, indent=1))


if __name__ == "__main__":
    c = sys.argv[1]
    {"setup": setup, "teardown": teardown}.get(c, lambda: {"create": create, "collect": collect, "merge": merge_attempts}[c](sys.argv[2]))()
