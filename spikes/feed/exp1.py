"""Q1: streams per process. One database-level stream versus one stream per collection, E entities, P processes."""
import sys, time, json
from common import *
import asyncio


def run(mode, E, P, rate=500, dur=20, noise=True):
    colls = [f"e{i}" for i in range(E)]
    asyncio.run(prep(colls))
    base = asyncio.run(opencur())
    cs = [spawn("cons1.py", mode, ",".join(colls)) for _ in range(P)]
    for p in cs:
        wait_line(p, "READY")
    st0 = asyncio.run(opencur())
    until = time.time() + dur + 8
    for p in cs:
        go(p, until)
    w = spawn("writer.py", "--colls", ",".join(colls), "--rate", rate, "--dur", dur, "--ndocs", 200,
              *(["--noise", "noise"] if noise else []))
    cpu = []
    time.sleep(dur / 2)
    cpu.append(dstats())
    wr = json.loads(wait_line(w, "DONE", dur + 30))
    res = [json.loads(wait_line(p, "RESULT", 30)) for p in cs]
    for p in cs + [w]:
        p.wait()
    return {"mode": mode, "E": E, "P": P, "rate": rate, "noise": noise, "writer": wr, "mongod_cpu_pct_mem": cpu, "consumers": res,
            "open_cursors_with_consumers": st0, "open_cursors_baseline": base}


async def prep(colls):
    c = client()
    await seed(c, colls + ["noise"], 200)
    await asyncio.sleep(0.5)


async def opencur():
    c = client()
    return (await c.admin.command("serverStatus"))["metrics"]["cursor"]["open"]["total"]


if __name__ == "__main__":
    out = []
    cfg = [("percoll", 3, 1), ("db", 3, 1), ("percoll", 10, 1), ("db", 10, 1), ("percoll", 3, 4), ("db", 3, 4)]
    if len(sys.argv) > 1:
        cfg = [("percoll", 3, 4), ("db", 3, 4), ("percoll", 10, 4), ("db", 10, 4)]
    for mode, E, P in cfg:
        r = run(mode, E, P)
        print(json.dumps(r)[:1500], flush=True)
        out.append(r)
    save("exp1" if len(sys.argv) == 1 else "exp1b", out)
