"""A TCP proxy that adds a fixed round-trip delay, to simulate a networked database.

Every chunk of bytes is held for rtt/2 in each direction, so one request/response pair gains `rtt_ms` in total.
It runs as its own process (`python -m wpbench.netproxy`) so that it does not share the client's event loop.
The delay is applied from the arrival time of each chunk, so concurrent traffic is not serialized by it.
"""

import argparse
import asyncio
import time


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay: float) -> None:
    queue: asyncio.Queue = asyncio.Queue()

    async def pump() -> None:
        try:
            while True:
                data = await reader.read(65536)
                await queue.put((time.monotonic() + delay, data))
                if not data:
                    return
        except (ConnectionError, asyncio.CancelledError):
            await queue.put((0.0, b""))

    task = asyncio.create_task(pump())
    try:
        while True:
            due, data = await queue.get()
            if not data:
                break
            wait = due - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            writer.write(data)
            await writer.drain()
    except ConnectionError:
        pass
    finally:
        task.cancel()
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass


async def serve(listen: int, target_host: str, target_port: int, rtt_ms: float) -> None:
    delay = rtt_ms / 2000.0

    async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            tr, tw = await asyncio.open_connection(target_host, target_port)
        except OSError:
            w.close()
            return
        await asyncio.gather(_pipe(r, tw, delay), _pipe(tr, w, delay))

    server = await asyncio.start_server(handle, "127.0.0.1", listen)
    async with server:
        await server.serve_forever()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, required=True)
    ap.add_argument("--target", default="127.0.0.1:27103")
    ap.add_argument("--rtt", type=float, required=True, help="added round-trip time in ms")
    a = ap.parse_args()
    host, port = a.target.rsplit(":", 1)
    asyncio.run(serve(a.listen, host, int(port), a.rtt))


if __name__ == "__main__":
    main()
