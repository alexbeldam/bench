"""A tiny TCP proxy that can sever every connection, or refuse new ones, to test stream resume."""
import asyncio


class Proxy:
    def __init__(self, target_port, listen_port=0):
        self.t, self.lp, self.conns, self.paused, self.server = target_port, listen_port, set(), False, None

    async def start(self):
        self.server = await asyncio.start_server(self._h, "127.0.0.1", self.lp)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _pipe(self, r, w):
        try:
            while data := await r.read(65536):
                w.write(data)
                await w.drain()
        except Exception:
            pass
        finally:
            w.close()

    async def _h(self, r, w):
        if self.paused:
            w.close()
            return
        try:
            tr, tw = await asyncio.open_connection("127.0.0.1", self.t)
        except Exception:
            w.close()
            return
        self.conns.update([w, tw])
        await asyncio.gather(self._pipe(r, tw), self._pipe(tr, w))
        self.conns.difference_update([w, tw])

    def sever(self):
        for w in list(self.conns):
            w.transport.abort()
        self.conns.clear()
