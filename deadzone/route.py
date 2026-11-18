"""The simulated sensors: a refrigerated truck on a hill road.

There is no GPS or thermometer on a laptop, so the unit reads these. The
road is a closed loop of made-up waypoints; the truck drives it at a
steady speed and the cold box drifts around 4 C, with a slow warm spell
so the chart has something to show. A real unit replaces ``Sensors.read``
and nothing else.
"""

from __future__ import annotations

import math
import random

# (lat, lon), a loop. Invented coordinates, not a real road.
ROAD: list[tuple[float, float]] = [
    (31.1040, 77.1730), (31.1105, 77.1862), (31.1218, 77.1934), (31.1296, 77.2071),
    (31.1389, 77.2120), (31.1502, 77.2236), (31.1560, 77.2391), (31.1541, 77.2548),
    (31.1463, 77.2660), (31.1350, 77.2702), (31.1247, 77.2643), (31.1170, 77.2519),
    (31.1082, 77.2466), (31.0991, 77.2338), (31.0946, 77.2190), (31.0952, 77.2031),
    (31.0987, 77.1874),
]


def _lengths() -> tuple[list[float], float]:
    legs = [math.dist(ROAD[i], ROAD[(i + 1) % len(ROAD)]) for i in range(len(ROAD))]
    return legs, sum(legs)


LEGS, LOOP = _lengths()


def position(fraction: float) -> tuple[float, float]:
    """The point a given fraction of the way round the loop."""
    left = (fraction % 1.0) * LOOP
    for i, leg in enumerate(LEGS):
        if left <= leg:
            a, b = ROAD[i], ROAD[(i + 1) % len(ROAD)]
            t = left / leg if leg else 0.0
            return a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t
        left -= leg
    return ROAD[0]


class Sensors:
    def __init__(self, lap_seconds: float = 240.0, seed: int = 7):
        self.lap_seconds = lap_seconds
        self._random = random.Random(seed)

    def read(self, elapsed: float) -> tuple[float, float, float]:
        """(lat, lon, temp in C) after ``elapsed`` seconds on the road."""
        lat, lon = position(elapsed / self.lap_seconds)
        warm_spell = 2.6 * max(0.0, math.sin(elapsed / self.lap_seconds * math.tau - 1.2))
        temp = 4.0 + warm_spell + self._random.uniform(-0.25, 0.25)
        return round(lat, 6), round(lon, 6), round(temp, 2)
