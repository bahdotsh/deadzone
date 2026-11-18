"""The map: what the office sees.

An ordinary web service with no SDK in it. Phones upload to ``/ingest``
over plain HTTP when they have an uplink, and the page draws every reading
where the truck was when it took it, with how long the reading waited
before it arrived. It stands in for whatever backend a fleet already has.

A reading is keyed by (unit, seq), so an upload sent twice counts once.
State is in memory: the map starts empty each run.
"""

from __future__ import annotations

import json
import time
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .collector import parse_rows
from .mesh import clean_name
from .route import ROAD
from .web import Hub, asset, make_app

MAX_INGEST_BYTES = 256 * 1024
MAX_POINTS_SHOWN = 2000
# Uploads closer together than this are one arrival.
ARRIVAL_RUN = 5.0


class Cloud:
    def __init__(self) -> None:
        self.hub = Hub()
        # unit address -> {"name", "points": {seq: [seq, ts, lat, lon, temp, arrived]}}
        self.units: dict[str, dict[str, Any]] = {}
        self.arrivals: list[dict[str, Any]] = []

    def ingest(self, payload: Any) -> int | None:
        """Adds an upload; returns how many readings were new, or None if it is malformed."""
        if not isinstance(payload, dict):
            return None
        unit = payload.get("unit")
        rows = parse_rows(payload.get("rows"))
        if not isinstance(unit, str) or not unit.startswith("off1") or len(unit) > 128 or rows is None:
            return None
        entry = self.units.setdefault(unit, {"name": clean_name(payload.get("unit_name")) or unit[:12], "points": {}})
        now = time.time()
        fresh = [row for row in rows if row[0] not in entry["points"]]
        for row in fresh:
            entry["points"][row[0]] = [*row, now]
        if fresh:
            via = clean_name(payload.get("collector")) or "a phone"
            waited = now - min(row[1] for row in fresh)
            latest = self.arrivals[0] if self.arrivals else None
            if latest and latest["unit"] == entry["name"] and latest["via"] == via and now - latest["at"] < ARRIVAL_RUN:
                # One line for a stream of uploads, not one a second.
                latest.update(at=now, count=latest["count"] + len(fresh), oldest_wait=max(latest["oldest_wait"], waited))
            else:
                self.arrivals.insert(0, {"at": now, "unit": entry["name"], "count": len(fresh), "via": via,
                                         "oldest_wait": waited})
                del self.arrivals[12:]
            self.hub.notify()
        return len(fresh)

    def snapshot(self) -> dict[str, Any]:
        return {
            "role": "map",
            "road": ROAD,
            "units": [
                {
                    "name": entry["name"],
                    "count": len(entry["points"]),
                    "points": [entry["points"][seq] for seq in sorted(entry["points"])[-MAX_POINTS_SHOWN:]],
                }
                for entry in self.units.values()
            ],
            "arrivals": self.arrivals,
            "now": time.time(),
        }

    def app(self) -> Starlette:
        async def ingest(request: Request) -> Response:
            body = await request.body()
            if len(body) > MAX_INGEST_BYTES:
                return JSONResponse({"error": "too large"}, status_code=413)
            try:
                payload = json.loads(body)
            except ValueError:
                payload = None
            new = self.ingest(payload)
            if new is None:
                return JSONResponse({"error": "not an upload"}, status_code=400)
            return JSONResponse({"new": new})

        async def stage(_: Request) -> Response:
            return asset("stage.html")

        extra = [Route("/ingest", ingest, methods=["POST"]), Route("/stage", stage)]
        return make_app("map.html", self.snapshot, self.hub, extra=extra)
