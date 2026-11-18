"""deadzone unit | collector | map | rehearse

    deadzone rehearse                         everything on this machine, over loopback
    deadzone map                              the map the phones upload to
    deadzone unit --name truck-7 --pair       a tracker; trusts the first collector it meets
    deadzone collector --name phone --map http://HOST:7800
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import shutil
import signal
import socket
import subprocess
import sys
import webbrowser
from pathlib import Path
from typing import Any

import uvicorn

from .cloud import Cloud
from .collector import Collector
from .link import Link
from .mesh import Mesh, clean_name
from .rpc import LocalApi
from .service import APP_ID, ServiceSpec, default_home
from .unit import Unit
from .web import make_app, make_server

log = logging.getLogger("deadzone")

# What a node home may hold. --fresh deletes a home only when it holds
# nothing else, so a mistyped --home never takes someone's files with it.
HOME_ENTRIES = {"config.json", "store.key", "keys", "state", "service.log", "service.log.1", "allow.json",
                "unit.db", "unit.db-wal", "unit.db-shm", "collector.db", "collector.db-wal", "collector.db-shm"}


def wipe_home(home: Path) -> None:
    if not home.exists():
        return
    stray = [p.name for p in home.iterdir() if p.name not in HOME_ENTRIES]
    if stray:
        raise SystemExit(f"--fresh: {home} holds other files ({', '.join(stray[:3])}); not deleting it")
    shutil.rmtree(home)


def port_free(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
        except OSError:
            return False
    return True


def need_ports(*ports: int) -> None:
    for port in ports:
        if not port_free(port):
            raise SystemExit(f"port {port} is already in use; pick another")


def service_died(spec: ServiceSpec) -> str:
    path = spec.home / "service.log"
    try:
        tail = path.read_text(errors="replace").strip().splitlines()[-8:]
    except OSError:
        tail = []
    return "\n".join([f"the SDK service stopped while starting. The end of {path}:", ""] + ["    " + t for t in tail])


async def start_watching(starting: Any, specs: list[ServiceSpec], procs: list[subprocess.Popen[bytes]]) -> None:
    """Awaits a start, but stops at once if an SDK service dies, with the
    end of its log, instead of waiting for a socket that never comes."""
    task = asyncio.ensure_future(starting)
    while not task.done():
        for spec, proc in zip(specs, procs):
            if proc.poll() is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise SystemExit(service_died(spec))
        await asyncio.wait([task], timeout=0.3)
    task.result()


async def serve_until_stopped(servers: list[uvicorn.Server]) -> None:
    serving = [asyncio.create_task(s.serve()) for s in servers]
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await asyncio.wait([*serving, asyncio.create_task(stop.wait())], return_when=asyncio.FIRST_COMPLETED)
    log.info("stopping")
    for server in servers:
        server.should_exit = True
    await asyncio.gather(*serving, return_exceptions=True)


def stop_services(procs: list[subprocess.Popen[bytes]]) -> None:
    for proc in procs:
        proc.terminate()
    for proc in procs:
        try:
            proc.wait(20)
        except subprocess.TimeoutExpired:
            proc.kill()


def spec_from(args: argparse.Namespace, name: str) -> ServiceSpec:
    home = Path(args.home).expanduser() if args.home else default_home(name)
    if args.fresh:
        wipe_home(home)
    return ServiceSpec(home, transport=args.transport, listen=args.listen, peers=args.peer,
                       lan_discovery=not args.no_lan_discovery)


# ---------------------------------------------------------------- one node


async def run_node(args: argparse.Namespace, role: str) -> None:
    name = clean_name(args.name)
    if not name:
        raise SystemExit("give a name with --name")
    need_ports(args.port)
    spec = spec_from(args, name)
    log.info("%s: starting the SDK service (%s), log %s", name, spec.transport, spec.home / "service.log")
    procs = [spec.spawn()]
    node: Unit | Collector | None = None
    try:
        mesh = Mesh(LocalApi(str(spec.socket_path), APP_ID), name, role)
        if role == "unit":
            node = Unit(mesh, spec.home, interval=args.interval, allow=args.allow, pair=args.pair)
            app = make_app("unit.html", node.snapshot, node.hub)
        else:
            node = Collector(mesh, spec.home, cloud_url=args.map, online=not args.offline)
            app = make_app("phone.html", node.snapshot, node.hub, action=node.act)
        await start_watching(node.start(), [spec], procs)
        log.info("%s is %s", name, mesh.address)
        log.info("its page: http://127.0.0.1:%d/", args.port)
        if role == "unit" and not node.allow and not args.pair:
            log.info("no collector is paired: start again with --pair, or --allow <collector address>")
        if not args.no_browser:
            webbrowser.open(f"http://127.0.0.1:{args.port}/")
        await serve_until_stopped([make_server(app, args.port)])
    finally:
        if node is not None:
            try:
                await node.close()
            except Exception:
                pass
        stop_services(procs)


def cmd_unit(args: argparse.Namespace) -> int:
    asyncio.run(run_node(args, "unit"))
    return 0


def cmd_collector(args: argparse.Namespace) -> int:
    asyncio.run(run_node(args, "collector"))
    return 0


def cmd_map(args: argparse.Namespace) -> int:
    need_ports(args.port)
    log.info("the map: http://127.0.0.1:%d/   (phones upload to http://<this machine>:%d)", args.port, args.port)
    if not args.no_browser:
        webbrowser.open(f"http://127.0.0.1:{args.port}/")

    async def run() -> None:
        await serve_until_stopped([make_server(Cloud().app(), args.port, args.host)])

    asyncio.run(run())
    return 0


# ---------------------------------------------------------------- rehearsal


async def run_rehearsal(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    map_port, unit_port, phone_port, stranger_port = (args.port + i for i in range(4))
    truck_link, phone_link = args.link_port, args.link_port + 1
    need_ports(map_port, unit_port, phone_port, stranger_port, truck_link, phone_link)
    names = {"unit": "Truck 7", "phone": "Driver's phone", "stranger": "Stranger's phone"}
    homes = {key: root / f".{key}" for key in names}
    if args.fresh:
        for home in homes.values():
            wipe_home(home)
    # The truck listens. The paired phone reaches it through the cuttable
    # link (its radio range); the stranger is always in range.
    specs = {
        "unit": ServiceSpec(homes["unit"], transport="lan", listen=f"127.0.0.1:{truck_link}"),
        "phone": ServiceSpec(homes["phone"], transport="lan", peers=[f"127.0.0.1:{phone_link}"]),
        "stranger": ServiceSpec(homes["stranger"], transport="lan", peers=[f"127.0.0.1:{truck_link}"]),
    }
    link = Link(phone_link, "127.0.0.1", truck_link)
    if args.start_away:
        link.cut()
    await link.start()
    procs: list[subprocess.Popen[bytes]] = []
    nodes: list[Unit | Collector] = []
    order = list(specs.values())
    try:
        procs.append(specs["unit"].spawn())
        for key in ("phone", "stranger"):
            procs.append(specs[key].spawn())
        map_url = f"http://127.0.0.1:{map_port}"

        def mesh(key: str, role: str) -> Mesh:
            return Mesh(LocalApi(str(specs[key].socket_path), APP_ID), names[key], role)

        phone = Collector(mesh("phone", "collector"), homes["phone"], cloud_url=map_url, online=not args.start_offline)
        phone.in_range, phone.set_range = link.up, link.set
        stranger = Collector(mesh("stranger", "collector"), homes["stranger"], cloud_url=map_url)
        nodes += [phone, stranger]
        await start_watching(asyncio.gather(phone.start(), stranger.start()), order, procs)
        # Pairing: the truck trusts the driver's phone by address, and nobody else.
        unit = Unit(mesh("unit", "unit"), homes["unit"], interval=args.interval, allow=[phone.mesh.address or ""])
        nodes.append(unit)
        await start_watching(unit.start(), order, procs)
        servers = [
            make_server(Cloud().app(), map_port),
            make_server(make_app("unit.html", unit.snapshot, unit.hub), unit_port),
            make_server(make_app("phone.html", phone.snapshot, phone.hub, action=phone.act), phone_port),
            make_server(make_app("phone.html", stranger.snapshot, stranger.hub, action=stranger.act), stranger_port),
        ]
        stage = f"{map_url}/stage?unit={unit_port}&phone={phone_port}&stranger={stranger_port}"
        log.info("the stage (all four at once): %s", stage)
        log.info("  the map %s   the truck :%d   the phone :%d   the stranger :%d",
                 map_url, unit_port, phone_port, stranger_port)
        if args.open:
            webbrowser.open(stage)
        await serve_until_stopped(servers)
    finally:
        for node in nodes:
            try:
                await node.close()
            except Exception:
                pass
        await link.close()
        stop_services(procs)


def cmd_rehearse(args: argparse.Namespace) -> int:
    asyncio.run(run_rehearsal(args))
    return 0


def add_node_options(parser: argparse.ArgumentParser, port: int) -> None:
    parser.add_argument("--name", required=True, help="what this device is called")
    parser.add_argument("--port", type=int, default=port, help=f"loopback port for its page (default {port})")
    parser.add_argument("--transport", choices=["lan", "ble", "both"], default="ble",
                        help="ble: Bluetooth (default); lan: the same network")
    parser.add_argument("--listen", help="lan: HOST:PORT to accept other devices on")
    parser.add_argument("--peer", action="append", default=[], help="lan: HOST:PORT of another device (repeatable)")
    parser.add_argument("--no-lan-discovery", action="store_true", help="lan: find devices only through --peer")
    parser.add_argument("--home", help="where its keys and data live (default ~/.deadzone/<name>)")
    parser.add_argument("--fresh", action="store_true", help="start with a new identity and no data")
    parser.add_argument("--no-browser", action="store_true", help="do not open the page")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="deadzone", description="A tracker that logs with no signal and hands "
                                     "its backlog to a phone that comes near.")
    sub = parser.add_subparsers(dest="command", required=True)

    unit = sub.add_parser("unit", help="a tracker unit")
    add_node_options(unit, 7801)
    unit.add_argument("--interval", type=float, default=2.0, help="seconds between readings (default 2)")
    unit.add_argument("--allow", action="append", default=[], help="a collector's address to trust (repeatable)")
    unit.add_argument("--pair", action="store_true", help="trust the first collector that comes near, and keep it")
    unit.set_defaults(fn=cmd_unit)

    collector = sub.add_parser("collector", help="the phone that collects from units")
    add_node_options(collector, 7802)
    collector.add_argument("--map", help="the map's address, such as http://192.168.1.15:7800")
    collector.add_argument("--offline", action="store_true", help="start with the uplink off")
    collector.set_defaults(fn=cmd_collector)

    cloud = sub.add_parser("map", help="the map that phones upload to")
    cloud.add_argument("--port", type=int, default=7800)
    cloud.add_argument("--host", default="127.0.0.1", help="0.0.0.0 to take uploads from other machines")
    cloud.add_argument("--no-browser", action="store_true")
    cloud.set_defaults(fn=cmd_map)

    rehearse = sub.add_parser("rehearse", help="a truck, its phone, a stranger and the map on this machine")
    rehearse.add_argument("--root", default="./rehearsal", help="where the devices' homes go")
    rehearse.add_argument("--port", type=int, default=7800, help="the map's port; the pages take the next three")
    rehearse.add_argument("--link-port", type=int, default=7920, help="first loopback link port")
    rehearse.add_argument("--interval", type=float, default=1.0, help="seconds between readings (default 1)")
    rehearse.add_argument("--start-away", action="store_true", help="begin with the phone out of range")
    rehearse.add_argument("--start-offline", action="store_true", help="begin with the phone's uplink off")
    rehearse.add_argument("--fresh", action="store_true", help="new identities and no data")
    rehearse.add_argument("--open", action="store_true", help="open the stage page")
    rehearse.set_defaults(fn=cmd_rehearse)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
