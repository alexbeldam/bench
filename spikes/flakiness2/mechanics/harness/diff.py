import json
import os
import subprocess
import time

pr = os.environ["PR"]
base = os.environ["BASE"]
repo = os.environ["GITHUB_REPOSITORY"]


def run(cmd):
    t = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True)
    return time.perf_counter() - t, p.returncode, sorted(p.stdout.split())


out = {"pr": pr, "base": base}
res = {}
for name, cmd in [
    ("gh_pr_diff", ["gh", "pr", "diff", pr, "--name-only"]),
    ("gh_api_files", ["gh", "api", f"repos/{repo}/pulls/{pr}/files", "--paginate", "--jq", ".[].filename"]),
]:
    ts = []
    for _ in range(3):
        t, rc, files = run(cmd)
        ts.append(round(t, 3))
    out[name] = {"secs": ts, "rc": rc, "n": len(files)}
    res[name] = files

# merge ref checked out at depth 1: deepen by one, diff first parent against head
t1, rc1, _ = run(["git", "fetch", "--no-tags", "--deepen=1", "origin"])
t2, rc2, files = run(["git", "diff", "--name-only", "HEAD^1", "HEAD"])
out["git_deepen1"] = {"fetch_secs": round(t1, 3), "diff_secs": round(t2, 3), "rc": [rc1, rc2], "n": len(files)}
res["git_deepen1"] = files

# full history plus a three-dot diff against the base branch
t1, rc1, _ = run(["git", "fetch", "--no-tags", "--unshallow", "origin"])
t3, rc3, _ = run(["git", "fetch", "--no-tags", "origin", f"{base}:refs/remotes/origin/{base}"])
ts = []
for _ in range(3):
    t, rc, files = run(["git", "diff", "--name-only", f"origin/{base}...HEAD"])
    ts.append(round(t, 3))
out["git_threedot"] = {"unshallow_secs": round(t1, 3), "fetch_base_secs": round(t3, 3), "diff_secs": ts, "rc": rc, "n": len(files)}
res["git_threedot"] = files
out["same_lists"] = len({tuple(v) for v in res.values()}) == 1
out["files"] = res["gh_pr_diff"]
print("MECH_DIFF " + json.dumps(out))
