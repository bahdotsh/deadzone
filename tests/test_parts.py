"""The parts on their own, with no SDK service: fast."""

from __future__ import annotations

import asyncio
from pathlib import Path

from deadzone.cloud import Cloud
from deadzone.collector import parse_rows
from deadzone.mesh import Peer
from deadzone.route import ROAD, Sensors, position
from deadzone.store import CollectorStore, UnitLog
from deadzone.unit import Unit
from deadzone.web import Feed, Hub

COLLECTOR = "off1collector"
STRANGER = "off1stranger"


class FakeMesh:
    name, address = "truck", "off1truck"

    def __init__(self):
        self.peers: dict[str, Peer] = {}
        self.sent: list[tuple[str, dict]] = []
        self.on_hello = self.on_lost = self.on_message = None

    async def start(self): ...
    async def close(self): ...

    def spawn(self, coro):
        asyncio.ensure_future(coro)

    async def send(self, address, message):
        self.sent.append((address, message))

    def of(self, kind):
        return [(a, m) for a, m in self.sent if m["t"] == kind]


async def settle(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, "not in time"
        await asyncio.sleep(0.01)


def test_the_unit_log_holds_until_marked(tmp_path: Path):
    log = UnitLog(tmp_path / "u.db")
    for i in range(5):
        log.append(float(i), 1.0, 2.0, 4.0)
    assert [r[0] for r in log.held(3)] == [1, 2, 3]
    log.mark_handed(3)
    log.mark_handed(2)  # the cursor never moves back
    log.mark_handed(99)  # nor past what was logged
    assert log.handed == 5 and log.held(10) == []
    log.close()
    assert UnitLog(tmp_path / "u.db").handed == 5  # and it is on disk


def test_the_collector_store_takes_a_batch_once(tmp_path: Path):
    store = CollectorStore(tmp_path / "c.db")
    rows = [(1, 1.0, 0.0, 0.0, 4.0), (2, 2.0, 0.0, 0.0, 4.0)]
    assert store.add("off1a", rows) == 2 and store.add("off1a", rows) == 0
    unit, pending = store.to_upload(10)
    assert unit == "off1a" and len(pending) == 2
    store.mark_uploaded("off1a", [1])
    assert store.counts() == {"off1a": (2, 1)}


def test_a_batch_that_is_not_readings_is_refused():
    good = [[1, 1.0, 2.0, 3.0, 4.0]]
    assert parse_rows(good) == [(1, 1.0, 2.0, 3.0, 4.0)]
    for bad in (None, [], [[1, 2, 3]], [[0, 1, 2, 3, 4]], [[True, 1, 2, 3, 4]], [["1", 1, 2, 3, 4]],
                [[1, "x", 2, 3, 4]], [[1, float("nan"), 2, 3, 4]], good * 201):
        assert parse_rows(bad) is None


def test_the_map_counts_an_upload_once():
    cloud = Cloud()
    upload = {"unit": "off1truck", "unit_name": "Truck 7", "collector": "Phone", "rows": [[1, 1.0, 31.1, 77.2, 4.0]]}
    assert cloud.ingest(upload) == 1 and cloud.ingest(upload) == 0
    assert cloud.ingest({"unit": "nope", "rows": upload["rows"]}) is None
    snapshot = cloud.snapshot()
    assert snapshot["units"][0]["count"] == 1 and snapshot["arrivals"][0]["via"] == "Phone"


def test_the_feed_folds_a_run_into_one_line():
    feed = Feed(Hub())
    words = lambda n: f"Handed {n}"
    feed.count("handed", 1, words)
    feed.count("handed", 2, words)
    feed.say("Phone went out of range")
    feed.count("handed", 5, words)
    assert [text for _, text in feed.items()] == ["Handed 5", "Phone went out of range", "Handed 3"]


def test_the_truck_drives_the_whole_loop():
    assert position(0.0) == ROAD[0] and position(1.0) == ROAD[0]
    lat, lon, temp = Sensors().read(30.0)
    assert 31.0 < lat < 31.2 and 77.1 < lon < 77.3 and 3.0 < temp < 8.0


async def test_only_a_paired_collector_is_handed_readings(tmp_path: Path):
    mesh = FakeMesh()
    unit = Unit(mesh, tmp_path, interval=0.01, allow=[COLLECTOR])
    await unit.start()
    try:
        stranger = mesh.peers[STRANGER] = Peer(STRANGER, "Stranger", "collector")
        await mesh.on_hello(stranger)
        await settle(lambda: mesh.of("refused"))
        await asyncio.sleep(0.1)
        assert not mesh.of("batch") and unit.log.handed == 0

        phone = mesh.peers[COLLECTOR] = Peer(COLLECTOR, "Phone", "collector")
        await mesh.on_hello(phone)
        await settle(lambda: mesh.of("batch"))
        address, batch = mesh.of("batch")[0]
        upto = batch["rows"][-1][0]
        assert address == COLLECTOR

        # An ack from the wrong phone, or for more than was sent, moves nothing.
        await mesh.on_message(stranger, {"t": "ack", "upto": upto})
        await mesh.on_message(phone, {"t": "ack", "upto": upto + 500})
        assert unit.log.handed == 0
        await mesh.on_message(phone, {"t": "ack", "upto": upto})
        assert unit.log.handed == upto
        assert all(a == COLLECTOR for a, _ in mesh.of("batch"))
    finally:
        await unit.close()


async def test_pairing_trusts_the_first_collector_and_remembers_it(tmp_path: Path):
    mesh = FakeMesh()
    unit = Unit(mesh, tmp_path, interval=0.01, pair=True)
    await unit.start()
    try:
        first = mesh.peers[COLLECTOR] = Peer(COLLECTOR, "Phone", "collector")
        await mesh.on_hello(first)
        second = mesh.peers[STRANGER] = Peer(STRANGER, "Stranger", "collector")
        await mesh.on_hello(second)
        assert unit.allow == {COLLECTOR} and not unit.pairing
    finally:
        await unit.close()
    assert Unit(FakeMesh(), tmp_path).allow == {COLLECTOR}
