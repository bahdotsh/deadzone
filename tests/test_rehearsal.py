"""Three real SDK services on this machine: the whole story.

A truck logs, its paired phone pulls while in range, the truck keeps
logging while the phone is away, the backlog crosses when it returns, a
stranger gets nothing, and the map fills only once the phone has an uplink.

Takes about a minute. Skip it with DEADZONE_FAST=1.
"""

from __future__ import annotations

import asyncio
import os
import socket
from pathlib import Path

import pytest

from deadzone.cloud import Cloud
from deadzone.collector import Collector
from deadzone.link import Link
from deadzone.mesh import Mesh
from deadzone.rpc import LocalApi
from deadzone.service import APP_ID, ServiceSpec
from deadzone.unit import Unit
from deadzone.web import make_server

pytestmark = pytest.mark.skipif(bool(os.environ.get("DEADZONE_FAST")), reason="DEADZONE_FAST is set")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def until(predicate, what: str, timeout: float = 60.0, every: float = 0.1):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"not in time: {what}")
        await asyncio.sleep(every)


def stored(collector: Collector) -> int:
    return sum(total for total, _ in collector.store.counts().values())


async def test_the_backlog_crosses_when_the_phone_returns(tmp_path: Path):
    truck_port, phone_port, map_port = free_port(), free_port(), free_port()
    specs = {
        "unit": ServiceSpec(tmp_path / "unit", transport="lan", listen=f"127.0.0.1:{truck_port}"),
        "phone": ServiceSpec(tmp_path / "phone", transport="lan", peers=[f"127.0.0.1:{phone_port}"]),
        "stranger": ServiceSpec(tmp_path / "stranger", transport="lan", peers=[f"127.0.0.1:{truck_port}"]),
    }
    link = Link(phone_port, "127.0.0.1", truck_port)
    await link.start()
    cloud = Cloud()
    server = make_server(cloud.app(), map_port)
    serving = asyncio.create_task(server.serve())
    procs = [spec.spawn() for spec in specs.values()]
    nodes = []

    def mesh(key: str, role: str) -> Mesh:
        return Mesh(LocalApi(str(specs[key].socket_path), APP_ID), key, role)

    try:
        map_url = f"http://127.0.0.1:{map_port}"
        phone = Collector(mesh("phone", "collector"), specs["phone"].home, cloud_url=map_url, online=False)
        stranger = Collector(mesh("stranger", "collector"), specs["stranger"].home, cloud_url=map_url)
        nodes += [phone, stranger]
        await asyncio.gather(phone.start(), stranger.start())
        unit = Unit(mesh("unit", "unit"), specs["unit"].home, interval=0.2, allow=[phone.mesh.address])
        nodes.append(unit)
        await unit.start()

        # In range: the phone pulls as the truck logs.
        await until(lambda: stored(phone) >= 10, "the phone pulls while in range")
        await until(lambda: unit.log.handed >= 10, "the truck hears the acknowledgement")

        # Out of range: the truck keeps logging and holds what it logs.
        link.cut()
        await asyncio.sleep(1.0)
        carried = stored(phone)
        await until(lambda: unit.log.last - unit.log.handed >= 60, "the truck holds a backlog")
        assert stored(phone) == carried

        # Back in range: the backlog crosses, with nothing missing.
        link.join()
        await until(lambda: unit.log.last - unit.log.handed <= 3, "the backlog drains", timeout=90)
        seqs = [row[0] for row in phone.store.db.execute("SELECT seq FROM readings ORDER BY seq")]
        assert seqs == list(range(1, len(seqs) + 1)) and len(seqs) >= 70

        # The stranger was in range the whole time and holds nothing.
        await until(lambda: stranger.refused, "the stranger is refused")
        assert stored(stranger) == 0

        # No uplink so far: the map is empty. With one, it fills.
        assert cloud.units == {}
        await phone.act({"online": True})
        await until(lambda: phone.snapshot()["carrying"] <= 3, "the phone uploads")
        points = next(iter(cloud.units.values()))["points"]
        assert len(points) >= 70 and sorted(points) == list(range(1, len(points) + 1))
    finally:
        for node in nodes:
            try:
                await node.close()
            except Exception:
                pass
        await link.close()
        server.should_exit = True
        await asyncio.gather(serving, return_exceptions=True)
        for proc in procs:
            proc.terminate()
        for proc in procs:
            await asyncio.to_thread(proc.wait, 20)
