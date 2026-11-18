"""The collector: the phone that walks past.

It finds units over the SDK, stores what a unit hands it, and uploads to
the map when it has an uplink. It is the recipient of the readings, then
an ordinary HTTP client of the map. It is not a relay: nothing passes
through it that it did not first store.

What holds:

- **Stored before acknowledged.** The batch is committed to the phone's
  own database, then the unit is told. The unit forgets a reading only
  after that, so a phone that dies mid-batch costs nothing.
- **Readings are filed under the sender's address**, which the encrypted
  session authenticates. A device can add readings to its own name only.
- **A row stays "to upload" until the map answers for it.** With the
  uplink off, or the map unreachable, the phone keeps carrying.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Awaitable, Callable

from .mesh import Mesh, Peer, plural
from .rpc import RpcError
from .store import CollectorStore, Row
from .web import Feed, Hub

log = logging.getLogger("deadzone.collector")

MAX_BATCH_ROWS = 200
UPLOAD_ROWS = 200
UPLOAD_TIMEOUT = 5.0
MAX_BACKOFF = 10.0


def parse_rows(value: Any) -> list[Row] | None:
    """A batch's rows, or None if any of it is not a reading."""
    if not isinstance(value, list) or not 0 < len(value) <= MAX_BATCH_ROWS:
        return None
    rows: list[Row] = []
    for item in value:
        if not isinstance(item, list) or len(item) != 5:
            return None
        seq, *numbers = item
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
            return None
        if any(isinstance(n, bool) or not isinstance(n, (int, float)) or n != n for n in numbers):
            return None
        rows.append((seq, *(float(n) for n in numbers)))  # type: ignore[arg-type]
    return rows


class Collector:
    def __init__(self, mesh: Mesh, home: Path, cloud_url: str | None = None, online: bool = True):
        self.mesh = mesh
        self.home = home
        self.cloud_url = cloud_url.rstrip("/") if cloud_url else None
        self.online = online
        self.hub = Hub()
        self.feed = Feed(self.hub)
        self.store = CollectorStore(home / "collector.db")
        self.refused: set[str] = set()
        self._found: set[str] = set()
        # Rehearsal only: the switch that walks the phone in and out of range.
        self.in_range: bool | None = None
        self.set_range: Callable[[bool], Awaitable[None]] | None = None
        self._wake = asyncio.Event()
        self._uploader: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.mesh.on_hello = self._on_hello
        self.mesh.on_lost = self._on_lost
        self.mesh.on_message = self._on_message
        await self.mesh.start()
        self._uploader = asyncio.create_task(self._upload())

    async def close(self) -> None:
        if self._uploader:
            self._uploader.cancel()
            await asyncio.gather(self._uploader, return_exceptions=True)
        await self.mesh.close()
        self.store.close()

    def _say(self, text: str) -> None:
        self.feed.say(text)

    # ------------------------------------------------------------------ mesh

    async def _on_hello(self, peer: Peer) -> None:
        if peer.role == "unit" and peer.address not in self._found:
            self._found.add(peer.address)
            self._say(f"Found {peer.name or peer.address[:12]} nearby")

    async def _on_lost(self, peer: Peer) -> None:
        self.refused.discard(peer.address)
        self._found.discard(peer.address)
        if peer.role == "unit":
            self._say(f"Lost {peer.name or peer.address[:12]}")

    async def _on_message(self, peer: Peer, message: dict[str, Any]) -> None:
        if message["t"] == "refused":
            if peer.address not in self.refused:
                self.refused.add(peer.address)
                self._say(f"{peer.name or 'A tracker'} refused this phone: it is not paired")
            return
        if message["t"] != "batch":
            return
        rows = parse_rows(message.get("rows"))
        if rows is None:
            return
        new = self.store.add(peer.address, rows)  # on disk before the ack
        try:
            await self.mesh.send(peer.address, {"t": "ack", "upto": rows[-1][0]})
        except (RpcError, ConnectionError):
            pass  # the unit sends the batch again; storing it twice is a no-op
        if new:
            who = peer.name or peer.address[:12]
            self.feed.count(f"pulled:{peer.address}", new, lambda n: f"Pulled {plural(n, 'reading')} from {who}")
            self._wake.set()

    # ---------------------------------------------------------------- uplink

    async def act(self, action: dict[str, Any]) -> None:
        if isinstance(action.get("online"), bool) and action["online"] != self.online:
            self.online = action["online"]
            self._say("Uplink on" if self.online else "Uplink off: carrying")
            self._wake.set()
        if isinstance(action.get("range"), bool) and self.set_range is not None:
            await self.set_range(action["range"])
            self.in_range = action["range"]
            self._say("Walked into range" if self.in_range else "Walked out of range")

    async def _upload(self) -> None:
        backoff = 1.0
        while True:
            batch = self.store.to_upload(UPLOAD_ROWS) if self.online and self.cloud_url else None
            if batch is None:
                self._wake.clear()
                await self._wake.wait()
                continue
            unit, rows = batch
            peer = self.mesh.peers.get(unit)
            payload = {
                "collector": self.mesh.name,
                "unit": unit,
                "unit_name": peer.name if peer and peer.name else unit[:12],
                "rows": rows,
            }
            try:
                await asyncio.to_thread(self._post, payload)
            except (OSError, urllib.error.URLError) as exc:
                log.info("upload failed, will retry: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            backoff = 1.0
            self.store.mark_uploaded(unit, [row[0] for row in rows])
            self.feed.count("uploaded", len(rows), lambda n: f"Uploaded {plural(n, 'reading')} to the map")

    def _post(self, payload: dict[str, Any]) -> None:
        request = urllib.request.Request(
            f"{self.cloud_url}/ingest", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=UPLOAD_TIMEOUT) as response:
            if response.status != 200:
                raise OSError(f"the map answered {response.status}")

    # ------------------------------------------------------------------ page

    def snapshot(self) -> dict[str, Any]:
        counts = self.store.counts()
        units = []
        for address in {*counts, *(a for a, p in self.mesh.peers.items() if p.role == "unit")}:
            peer = self.mesh.peers.get(address)
            stored, uploaded = counts.get(address, (0, 0))
            units.append({
                "name": peer.name if peer and peer.name else address[:12],
                "nearby": bool(peer and peer.nearby),
                "refused": address in self.refused,
                "stored": stored,
                "uploaded": uploaded,
            })
        units.sort(key=lambda u: u["name"])
        stored = sum(u["stored"] for u in units)
        uploaded = sum(u["uploaded"] for u in units)
        return {
            "role": "collector",
            "name": self.mesh.name,
            "address": self.mesh.address,
            "online": self.online,
            "has_map": self.cloud_url is not None,
            "in_range": self.in_range,
            "units": units,
            "stored": stored,
            "uploaded": uploaded,
            "carrying": stored - uploaded,
            "feed": self.feed.items(),
            "now": time.time(),
        }
