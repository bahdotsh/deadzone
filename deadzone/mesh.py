"""Units and collectors nearby, over the SDK.

The SDK finds nearby devices and forms an MLS session with each one by
itself. On top of that, each node says hello with its name and its role
("unit" or "collector"); after that, everything travels as JSON envelopes
inside ordinary 1:1 SDK messages:

    {"dz": 1, "t": "<kind>", ...}

A hello proves nothing about who may collect: the unit decides that from
the sender's address, which the session authenticates, never from the role
a hello claims.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from .rpc import LocalApi, RpcError

logger = logging.getLogger(__name__)

VERSION = 1
MAX_MESSAGE_CHARS = 32 * 1024
# How long a hello waits for the session before sending anyway: a queued
# message makes the engine retry a key exchange that was lost.
SESSION_WAIT = 6.0
MAX_NAME = 24
ROLES = ("unit", "collector")
SEEN_LIMIT = 4096


@dataclass
class Peer:
    address: str
    name: str | None = None
    role: str | None = None
    nearby: bool = True
    hello_sent: bool = False


def clean_name(name: Any) -> str | None:
    if not isinstance(name, str):
        return None
    name = " ".join(name.split())[:MAX_NAME]
    return name or None


def plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


class Mesh:
    def __init__(self, api: LocalApi, name: str, role: str):
        if role not in ROLES:
            raise ValueError(f"role must be one of {ROLES}")
        self.api = api
        self.name = name
        self.role = role
        self.address: str | None = None
        self.peers: dict[str, Peer] = {}
        self.on_hello: Callable[[Peer], Awaitable[None]] | None = None
        self.on_lost: Callable[[Peer], Awaitable[None]] | None = None
        self.on_message: Callable[[Peer, dict[str, Any]], Awaitable[None]] | None = None
        self._seen: dict[str, None] = {}
        self._tasks: set[asyncio.Task[Any]] = set()

    async def start(self) -> None:
        self.api.on_event = self._on_event
        hello = await self.api.connect()
        self.address = hello.get("local_address")
        await self.api.call("subscribe", {"types": "all"})
        try:
            topology = await self.api.call("get_topology")
        except RpcError:
            topology = {}
        for node in (topology or {}).get("nodes", []):
            address = node.get("id") or node.get("peer_id") if isinstance(node, dict) else None
            if isinstance(address, str) and address.startswith("off1") and address != self.address:
                self._touch(address)

    async def close(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.api.close()

    async def send(self, address: str, message: dict[str, Any]) -> None:
        content = json.dumps({"dz": VERSION, **message}, separators=(",", ":"))
        await self.api.call("send_message", {"recipient": address, "content": content, "priority": "High"})

    def spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ------------------------------------------------------------------ peers

    def _touch(self, address: str) -> Peer:
        peer = self.peers.get(address)
        if peer is None:
            peer = self.peers[address] = Peer(address)
        peer.nearby = True
        if not peer.hello_sent:
            peer.hello_sent = True
            self.spawn(self._send_hello(address, want_reply=True))
        return peer

    async def _send_hello(self, address: str, want_reply: bool) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + SESSION_WAIT
        while loop.time() < deadline:
            try:
                if await self.api.call("get_establishment_state", {"peer_id": address}) == "SessionConfirmed":
                    break
            except (RpcError, ConnectionError):
                pass
            await asyncio.sleep(0.3)
        try:
            await self.send(address, {"t": "hello", "name": self.name, "role": self.role, "want_reply": want_reply})
        except (RpcError, ConnectionError) as exc:
            logger.info("hello to %s not sent yet: %s", address[:12], exc)
            peer = self.peers.get(address)
            if peer:
                peer.hello_sent = False

    # ----------------------------------------------------------------- events

    async def _on_event(self, event: dict[str, Any]) -> None:
        kind = event.get("type")
        if kind in ("neighbor_discovered", "secure_session_established"):
            address = event.get("peer_id")
            if isinstance(address, str) and address != self.address:
                self._touch(address)
        elif kind == "neighbor_lost":
            peer = self.peers.get(event.get("peer_id", ""))
            if peer:
                peer.nearby = False
                peer.hello_sent = False
                if self.on_lost:
                    await self.on_lost(peer)
        elif kind == "message_received":
            await self._on_message(event)

    async def _on_message(self, event: dict[str, Any]) -> None:
        message_id = event.get("message_id")
        sender = event.get("sender")
        if not message_id or message_id in self._seen or not isinstance(sender, str) or sender == self.address:
            return  # delivery after a reconnect is at least once
        self._seen[message_id] = None
        if len(self._seen) > SEEN_LIMIT:
            self._seen.pop(next(iter(self._seen)))
        content = event.get("content")
        if not isinstance(content, str) or len(content) > MAX_MESSAGE_CHARS:
            return
        try:
            message = json.loads(content)
        except (ValueError, RecursionError):
            return
        if not isinstance(message, dict) or message.get("dz") != VERSION or not isinstance(message.get("t"), str):
            return  # not ours: another app on the same mesh
        peer = self.peers.get(sender) or self._touch(sender)
        peer.nearby = True
        if message["t"] == "hello":
            name = clean_name(message.get("name"))
            if name and peer.name is None:
                peer.name = name  # the first name an address gives stands
            if message.get("role") in ROLES:
                peer.role = message["role"]
            if message.get("want_reply"):
                self.spawn(self._send_hello(sender, want_reply=False))
            if self.on_hello:
                await self.on_hello(peer)
            return
        if self.on_message:
            await self.on_message(peer, message)
