"""The two durable stores, both SQLite.

``UnitLog`` is the tracker's own log. A reading is written here before
anything else happens to it, and it stays "held" until a collector has
acknowledged it. The hand-off cursor only moves forward on that
acknowledgement, so a batch lost in the air is sent again.

``CollectorStore`` is what the phone carries. A batch is committed here
before it is acknowledged (the unit forgets nothing the phone has not
stored), and a row stays "to upload" until the map has answered for it.

A row is ``(seq, ts, lat, lon, temp)``: seq is the unit's own counter,
ts the unit's clock in seconds.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

Row = tuple[int, float, float, float, float]


def _open(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    return db


class UnitLog:
    def __init__(self, path: Path):
        self.db = _open(path)
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS readings(
                seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, lat REAL, lon REAL, temp REAL);
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value INTEGER);
            INSERT OR IGNORE INTO meta VALUES('handed', 0);
            """
        )
        self.db.commit()

    def append(self, ts: float, lat: float, lon: float, temp: float) -> int:
        cur = self.db.execute("INSERT INTO readings(ts, lat, lon, temp) VALUES(?,?,?,?)", (ts, lat, lon, temp))
        self.db.commit()
        return int(cur.lastrowid or 0)

    @property
    def handed(self) -> int:
        return int(self.db.execute("SELECT value FROM meta WHERE key='handed'").fetchone()[0])

    @property
    def last(self) -> int:
        return int(self.db.execute("SELECT COALESCE(MAX(seq), 0) FROM readings").fetchone()[0])

    def held(self, limit: int) -> list[Row]:
        """The oldest readings no collector has acknowledged yet."""
        return self.db.execute(
            "SELECT seq, ts, lat, lon, temp FROM readings WHERE seq > ? ORDER BY seq LIMIT ?", (self.handed, limit)
        ).fetchall()

    def latest(self) -> Row | None:
        return self.db.execute("SELECT seq, ts, lat, lon, temp FROM readings ORDER BY seq DESC LIMIT 1").fetchone()

    def mark_handed(self, upto: int) -> None:
        self.db.execute("UPDATE meta SET value = MAX(value, ?) WHERE key='handed'", (min(upto, self.last),))
        self.db.commit()

    def close(self) -> None:
        self.db.close()


class CollectorStore:
    def __init__(self, path: Path):
        self.db = _open(path)
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS readings(
                unit TEXT, seq INTEGER, ts REAL, lat REAL, lon REAL, temp REAL,
                uploaded INTEGER DEFAULT 0, PRIMARY KEY(unit, seq));
            CREATE INDEX IF NOT EXISTS to_upload ON readings(uploaded, unit, seq);
            """
        )
        self.db.commit()

    def add(self, unit: str, rows: list[Row]) -> int:
        """Stores a batch; a row already held is left as it is. Returns how many were new."""
        before = self.db.total_changes
        self.db.executemany(
            "INSERT OR IGNORE INTO readings(unit, seq, ts, lat, lon, temp) VALUES(?,?,?,?,?,?)",
            [(unit, *row) for row in rows],
        )
        self.db.commit()
        return self.db.total_changes - before

    def to_upload(self, limit: int) -> tuple[str, list[Row]] | None:
        """The oldest rows the map has not confirmed, for one unit."""
        first = self.db.execute("SELECT unit FROM readings WHERE uploaded=0 ORDER BY ts LIMIT 1").fetchone()
        if first is None:
            return None
        rows = self.db.execute(
            "SELECT seq, ts, lat, lon, temp FROM readings WHERE uploaded=0 AND unit=? ORDER BY seq LIMIT ?",
            (first[0], limit),
        ).fetchall()
        return first[0], rows

    def mark_uploaded(self, unit: str, seqs: list[int]) -> None:
        self.db.executemany("UPDATE readings SET uploaded=1 WHERE unit=? AND seq=?", [(unit, s) for s in seqs])
        self.db.commit()

    def counts(self) -> dict[str, tuple[int, int]]:
        """unit address -> (stored, uploaded)."""
        return {
            unit: (int(total), int(done or 0))
            for unit, total, done in self.db.execute("SELECT unit, COUNT(*), SUM(uploaded) FROM readings GROUP BY unit")
        }

    def close(self) -> None:
        self.db.close()
