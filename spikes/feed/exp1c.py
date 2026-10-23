import exp1, json
from common import save
out=[]
for i in range(2):
    for m in ("percoll","db"):
        r = exp1.run(m,10,4); out.append(r)
        c=r["consumers"]
        print(m, [x["events"] for x in c], c[0]["lat"], r["writer"]["rate_actual"], r["writer"]["lag_p95_ms"], r["open_cursors_with_consumers"]-r["open_cursors_baseline"], flush=True)
save("exp1c", out)
