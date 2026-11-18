"""One small page server, used by the unit, the collector and the map.

Each of them is a snapshot (a dict) that changes. The page gets every
snapshot over server-sent events at ``/state`` and sends button presses to
``/action``. A page is a display and a few switches: nothing that moves
readings goes through here.
"""

from __future__ import annotations

import asyncio
import json
import time
from importlib import resources
from typing import Any, AsyncIterator, Awaitable, Callable

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import BaseRoute, Route

KEEPALIVE = 15.0
# A burst of changes (a backlog arriving) becomes a few pushes, not hundreds.
MIN_PUSH_GAP = 0.25
MAX_ACTION_BYTES = 4096

TYPES = {"html": "text/html; charset=utf-8", "css": "text/css; charset=utf-8", "js": "text/javascript; charset=utf-8"}


class Feed:
    """The "what happened" list. A run of the same counted event ("handed
    1 reading", once a second) is one line with a running total; any other
    line ends the run."""

    def __init__(self, hub: "Hub", limit: int = 30) -> None:
        self.hub = hub
        self.limit = limit
        self._items: list[dict[str, Any]] = []

    def say(self, text: str) -> None:
        self._push({"ts": time.time(), "text": text, "key": None})

    def count(self, key: str, n: int, text: Callable[[int], str]) -> None:
        """Adds ``n`` to the running line for ``key``; ``text`` words the total."""
        for i, item in enumerate(self._items):
            if item["key"] is None:
                break
            if item["key"] == key:
                del self._items[i]
                n += item["n"]
                break
        self._push({"ts": time.time(), "text": text(n), "key": key, "n": n})

    def _push(self, item: dict[str, Any]) -> None:
        self._items.insert(0, item)
        del self._items[self.limit:]
        self.hub.notify()

    def items(self) -> list[tuple[float, str]]:
        return [(item["ts"], item["text"]) for item in self._items]


class Hub:
    """Wakes every open page when the snapshot has changed."""

    def __init__(self) -> None:
        self.version = 0
        self._event = asyncio.Event()

    def notify(self) -> None:
        self.version += 1
        self._event.set()
        self._event = asyncio.Event()

    async def wait(self, timeout: float) -> None:
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except asyncio.TimeoutError:
            pass


def asset(name: str) -> Response:
    data = resources.files("deadzone").joinpath("web", name).read_bytes()
    return Response(data, media_type=TYPES[name.rsplit(".", 1)[-1]], headers={"Cache-Control": "no-store"})


def make_app(
    page: str,
    snapshot: Callable[[], dict[str, Any]],
    hub: Hub,
    action: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    extra: list[BaseRoute] | None = None,
) -> Starlette:
    async def index(_: Request) -> Response:
        return asset(page)

    async def base(request: Request) -> Response:
        return asset("base." + request.path_params["kind"])

    async def state(_: Request) -> Response:
        async def stream() -> AsyncIterator[str]:
            seen = -1
            while True:
                if hub.version != seen:
                    seen = hub.version
                    yield f"data: {json.dumps(snapshot(), separators=(',', ':'))}\n\n"
                    await asyncio.sleep(MIN_PUSH_GAP)
                else:
                    yield ": keepalive\n\n"
                if hub.version == seen:
                    await hub.wait(KEEPALIVE)

        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-store"})

    async def act(request: Request) -> Response:
        if action is None:
            return JSONResponse({"error": "this page has no switches"}, status_code=404)
        body = await request.body()
        if len(body) > MAX_ACTION_BYTES:
            return JSONResponse({"error": "too large"}, status_code=413)
        try:
            payload = json.loads(body)
        except ValueError:
            payload = None
        if not isinstance(payload, dict):
            return JSONResponse({"error": "expected a JSON object"}, status_code=400)
        await action(payload)
        return JSONResponse({"ok": True})

    routes: list[BaseRoute] = [
        Route("/", index),
        Route("/base.{kind:str}", base),
        Route("/state", state),
        Route("/action", act, methods=["POST"]),
    ]
    return Starlette(routes=routes + (extra or []))


def make_server(app: Starlette, port: int, host: str = "127.0.0.1") -> uvicorn.Server:
    config = uvicorn.Config(app, host=host, port=port, log_level="warning", timeout_graceful_shutdown=1)
    return uvicorn.Server(config)
