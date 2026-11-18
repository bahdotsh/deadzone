"""The tracker unit: it logs readings whether or not anyone is listening,
and hands the backlog to a collector it trusts when one comes near.

What holds:

- **A reading is on disk before anything else happens to it.** The sampler
  writes to the log and only then wakes the hand-off.
- **A reading is held until a collector says it has stored it.** The cursor
  moves on an ``ack`` and on nothing else, and never past what was sent to
  the collector that acknowledged. A batch lost in the air goes again.
- **Who may collect is the unit's decision, by address.** The address is
  what the encrypted session authenticates. The role in a hello is a
  claim, so a stranger that says "collector" is refused and gets nothing.
- **One hand-off at a time.** With two trusted collectors nearby, each
  reading goes to one of them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from .mesh import Mesh, Peer, plural
from .route import Sensors
from .rpc import RpcError
from .store import UnitLog
from .web import Feed, Hub

log = logging.getLogger("deadzone.unit")

# About 2.5 KiB of JSON a batch, well inside one message.
BATCH = 40
ACK_WAIT = 8.0
MAX_BACKOFF = 15.0
IDLE_WAIT = 5.0


class Unit:
    def __init__(self, mesh: Mesh, home: Path, interval: float = 2.0, sensors: Sensors | None = None,
                 allow: list[str] | None = None, pair: bool = False):
        self.mesh = mesh
        self.home = home
        self.interval = interval
        self.sensors = sensors or Sensors()
        self.pairing = pair
        self.hub = Hub()
        self.feed = Feed(self.hub)
        self.log = UnitLog(home / "unit.db")
        self._allow_file = home / "allow.json"
        self.allow: set[str] = set(self._load_allow()) | set(allow or [])
        self._save_allow()
        self._new = asyncio.Event()
        self._acked = asyncio.Event()
        self._handoff = asyncio.Lock()
        self._waiting: tuple[str, int] | None = None  # (collector, highest seq sent to it)
        self._drains: dict[str, asyncio.Task[None]] = {}
        self._refused: set[str] = set()
        self._sampler: asyncio.Task[None] | None = None

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        self.mesh.on_hello = self._on_hello
        self.mesh.on_lost = self._on_lost
        self.mesh.on_message = self._on_message
        await self.mesh.start()
        self._sampler = asyncio.create_task(self._sample())

    async def close(self) -> None:
        tasks = [t for t in (self._sampler, *self._drains.values()) if t]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.mesh.close()
        self.log.close()

    def _load_allow(self) -> list[str]:
        try:
            saved = json.loads(self._allow_file.read_text())
        except (OSError, ValueError):
            return []
        return [a for a in saved if isinstance(a, str)] if isinstance(saved, list) else []

    def _save_allow(self) -> None:
        self._allow_file.write_text(json.dumps(sorted(self.allow), indent=2) + "\n")

    def _say(self, text: str) -> None:
        self.feed.say(text)

    # --------------------------------------------------------------- sampling

    async def _sample(self) -> None:
        while True:
            lat, lon, temp = self.sensors.read(self.log.last * self.interval)
            self.log.append(time.time(), lat, lon, temp)
            self._new.set()
            self.hub.notify()
            await asyncio.sleep(self.interval)

    # --------------------------------------------------------------- hand-off

    async def _on_hello(self, peer: Peer) -> None:
        if peer.role != "collector":
            return
        if peer.address not in self.allow and self.pairing:
            self.allow.add(peer.address)
            self._save_allow()
            self.pairing = False
            self._say(f"Paired with {peer.name or peer.address[:12]}")
        if peer.address not in self.allow:
            if peer.address not in self._refused:
                self._refused.add(peer.address)
                self._say(f"Refused {peer.name or peer.address[:12]}: not a paired collector")
                self.mesh.spawn(self._refuse(peer.address))
            return
        running = self._drains.get(peer.address)
        if running is None or running.done():
            self._say(f"{peer.name or peer.address[:12]} is in range")
            self._drains[peer.address] = asyncio.create_task(self._drain(peer))

    async def _refuse(self, address: str) -> None:
        try:
            await self.mesh.send(address, {"t": "refused"})
        except (RpcError, ConnectionError):
            pass

    async def _on_lost(self, peer: Peer) -> None:
        self._refused.discard(peer.address)
        if peer.address in self.allow:
            self._say(f"{peer.name or peer.address[:12]} went out of range")
        self._new.set()  # wake a drain that is waiting, so it sees the peer is gone

    async def _on_message(self, peer: Peer, message: dict[str, Any]) -> None:
        if message["t"] != "ack" or peer.address not in self.allow:
            return
        upto = message.get("upto")
        waiting = self._waiting
        if not isinstance(upto, int) or isinstance(upto, bool) or waiting is None or waiting[0] != peer.address:
            return
        if upto > waiting[1]:
            return  # an ack for more than this collector was sent
        before = self.log.handed
        self.log.mark_handed(upto)
        moved = self.log.handed - before
        if moved > 0:
            who = peer.name or peer.address[:12]
            self.feed.count(f"handed:{peer.address}", moved, lambda n: f"Handed {plural(n, 'reading')} to {who}")
        if upto == waiting[1]:
            self._acked.set()

    async def _drain(self, peer: Peer) -> None:
        backoff = 1.0
        while peer.nearby and peer.address in self.allow:
            sent = False
            async with self._handoff:
                rows = self.log.held(BATCH)
                if rows:
                    self._waiting = (peer.address, rows[-1][0])
                    self._acked.clear()
                    try:
                        await self.mesh.send(peer.address, {"t": "batch", "rows": rows})
                        await asyncio.wait_for(self._acked.wait(), ACK_WAIT)
                        sent = True
                    except (asyncio.TimeoutError, RpcError, ConnectionError):
                        pass
                    finally:
                        self._waiting = None
            if sent:
                backoff = 1.0
                continue
            if rows:  # no ack: the collector may be at the edge of range
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            self._new.clear()
            try:
                await asyncio.wait_for(self._new.wait(), IDLE_WAIT)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------ page

    def snapshot(self) -> dict[str, Any]:
        logged, handed = self.log.last, self.log.handed
        return {
            "role": "unit",
            "name": self.mesh.name,
            "address": self.mesh.address,
            "interval": self.interval,
            "logged": logged,
            "handed": handed,
            "held": logged - handed,
            "latest": self.log.latest(),
            "pairing": self.pairing,
            "collectors": [
                {"name": p.name or p.address[:12], "nearby": p.nearby, "paired": p.address in self.allow}
                for p in self.mesh.peers.values()
                if p.role == "collector"
            ],
            "feed": self.feed.items(),
            "now": time.time(),
        }
