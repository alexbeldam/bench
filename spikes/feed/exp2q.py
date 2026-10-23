"""Q2 on a quiet server: open latency of a stream at the write's operation time, with nothing else writing."""
import asyncio, json, time
from common import *
from exp2 import hpipe, ev_t


async def main():
    c = client(); db = c[DB]
    await seed(c, ["hot"], 1)
    op, fresh, miss, wait_empty = [], [], 0, []
    for i in range(300):
        rev = mk_rev(9, i)
        async with c.start_session() as s:
            await db.hot.update_one({"_id": 0}, {"$set": {REV: rev}}, session=s)
            ts = s.operation_time
        t0 = time.perf_counter()
        st = await db.watch(hpipe(), start_at_operation_time=ts, max_await_time_ms=100)
        t1 = time.perf_counter()
        ev = None
        while ev is None and time.perf_counter() - t0 < 5:
            ev = await st.try_next()
        if ev is None or ev_t(ev)[1] != rev:
            miss += 1
        op.append((time.perf_counter() - t0) * 1e6)
        await st.close()
        t0 = time.perf_counter()  # a plain stream (no start time), established by the first empty read
        st = await db.watch(hpipe(), max_await_time_ms=100)
        await st.try_next()
        fresh.append((time.perf_counter() - t0) * 1e6)
        await st.close()
    out = {"trials": 300, "missed": miss, "open_at_optime_to_own_event_ms": lat(op), "plain_open_and_first_read_ms(includes up to 100 ms await)": lat(fresh)}
    print(json.dumps(out)); save("exp2_quiet", out)

asyncio.run(main())
