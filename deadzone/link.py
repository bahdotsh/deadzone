"""Rehearsal only: radio range, on one machine.

Two services on a laptop are always "in range" of each other over
loopback. This is a TCP pass-through between them that can be cut and
joined, so the rehearsal can walk the phone away from the truck and back
while both keep running, as two devices would. On real hardware there is
no such thing: Bluetooth range does it.
"""

from __future__ import annotations

import asyncio


class Link:
    def __init__(self, port: int, target_host: str, target_port: int):
        self.port = port
        self.target = (target_host, target_port)
        self.up = True
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.port)

    async def close(self) -> None:
        self.cut()
        if self._server:
            self._server.close()

    def cut(self) -> None:
        self.up = False
        for writer in list(self._writers):
            writer.close()

    def join(self) -> None:
        self.up = True

    async def set(self, up: bool) -> None:
        self.join() if up else self.cut()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if not self.up:
            writer.close()
            return
        try:
            far_reader, far_writer = await asyncio.open_connection(*self.target)
        except OSError:
            writer.close()
            return
        pair = (writer, far_writer)
        self._writers.update(pair)

        async def pipe(source: asyncio.StreamReader, sink: asyncio.StreamWriter) -> None:
            try:
                while data := await source.read(65536):
                    sink.write(data)
                    await sink.drain()
            except (OSError, asyncio.IncompleteReadError):
                pass

        try:
            await asyncio.wait(
                [asyncio.create_task(pipe(reader, far_writer)), asyncio.create_task(pipe(far_reader, writer))],
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for w in pair:
                self._writers.discard(w)
                w.close()
