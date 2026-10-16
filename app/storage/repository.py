"""Persistence (doc §9.1). Prototype: SQLite. Production: raw logs in S3, events in OpenSearch,
incidents and RCA versions in PostgreSQL. The repository interface is the seam where that swap happens.

Idempotency (doc §10.2): events are keyed by a stable content-derived event_id and inserted with
INSERT OR IGNORE, so replaying the same logs never duplicates events, signals or incidents."""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from ..schemas import Incident, LogEvent, ParseErrorRecord, RCARecord, Signal

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY, epoch REAL NOT NULL, ingested_at TEXT, cluster_id TEXT, node_id TEXT,
    source_type TEXT, severity TEXT, event_type TEXT, message TEXT, attributes TEXT, source_file TEXT,
    source_offset INTEGER, parser_version TEXT, raw TEXT);
CREATE INDEX IF NOT EXISTS ix_events_node_time ON events(cluster_id, node_id, epoch);
CREATE INDEX IF NOT EXISTS ix_events_type_time ON events(event_type, epoch);
CREATE TABLE IF NOT EXISTS parse_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT, cluster_id TEXT, node_id TEXT, source_type TEXT, source_file TEXT,
    source_offset INTEGER, raw TEXT, reason TEXT, parser_version TEXT, ingested_at TEXT,
    UNIQUE(source_file, source_offset, raw));
CREATE TABLE IF NOT EXISTS checkpoints (path TEXT PRIMARY KEY, file_id TEXT, offset INTEGER, updated TEXT);
CREATE TABLE IF NOT EXISTS signals (signal_id TEXT PRIMARY KEY, cluster_id TEXT, data TEXT);
CREATE TABLE IF NOT EXISTS incidents (
    incident_id TEXT PRIMARY KEY, cluster_id TEXT, severity TEXT, status TEXT, first_seen TEXT, last_seen TEXT,
    data TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS rca_reports (
    incident_id TEXT, version INTEGER, status TEXT, created_at TEXT, data TEXT, PRIMARY KEY (incident_id, version));
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, updated TEXT);
"""

EVENT_COLS = ["event_id", "epoch", "ingested_at", "cluster_id", "node_id", "source_type", "severity", "event_type",
              "message", "attributes", "source_file", "source_offset", "parser_version", "raw"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self.conn.close()

    # ------------------------------------------------------------------------------------------ events
    def insert_events(self, events: Iterable[LogEvent]) -> int:
        rows = [(e.event_id, e.timestamp.timestamp(), e.ingested_at.isoformat(), e.cluster_id, e.node_id, e.source_type,
                 e.severity, e.event_type, e.message, json.dumps(e.attributes), e.source_file, e.source_offset,
                 e.parser_version, e.raw) for e in events]
        if not rows:
            return 0
        with self._lock, self.conn:
            before = self.conn.total_changes
            # only constant column names and "?" placeholders are interpolated; values are bound parameters
            sql = f"INSERT OR IGNORE INTO events ({','.join(EVENT_COLS)}) VALUES ({','.join('?' * len(EVENT_COLS))})"  # noqa: S608
            self.conn.executemany(sql, rows)
            return self.conn.total_changes - before

    def events(self, cluster_id: str, since_epoch: float = 0, until_epoch: float | None = None,
               node_id: str | None = None, event_type: str | None = None, limit: int | None = None) -> list[LogEvent]:
        q, p = "SELECT * FROM events WHERE cluster_id=? AND epoch>=?", [cluster_id, since_epoch]
        if until_epoch is not None:
            q += " AND epoch<=?"
            p.append(until_epoch)
        if node_id:
            q += " AND node_id=?"
            p.append(node_id)
        if event_type:
            q += " AND event_type=?"
            p.append(event_type)
        q += " ORDER BY epoch, event_id"
        if limit:
            q += f" LIMIT {int(limit)}"
        with self._lock:
            return [_row_to_event(r) for r in self.conn.execute(q, p).fetchall()]

    def events_by_ids(self, ids: list[str]) -> list[LogEvent]:
        out = []
        with self._lock:
            for i in range(0, len(ids), 500):
                chunk = ids[i:i + 500]
                out += [_row_to_event(r) for r in self.conn.execute(
                    f"SELECT * FROM events WHERE event_id IN ({','.join('?' * len(chunk))})", chunk)]  # noqa: S608 - placeholders only
        return sorted(out, key=lambda e: (e.timestamp, e.event_id))

    def event_stats(self, cluster_id: str) -> dict:
        with self._lock:
            r = self.conn.execute("SELECT MIN(epoch), MAX(epoch), COUNT(*) FROM events WHERE cluster_id=?", (cluster_id,)).fetchone()
        return {"min_epoch": r[0], "max_epoch": r[1], "count": r[2]}

    def last_seen_by_source(self, cluster_id: str) -> dict[tuple[str, str], float]:
        with self._lock:
            rows = self.conn.execute("SELECT node_id, source_type, MAX(epoch) FROM events WHERE cluster_id=? "
                                     "GROUP BY node_id, source_type", (cluster_id,)).fetchall()
        return {(r[0], r[1]): r[2] for r in rows}

    # ------------------------------------------------------------------------------------------ parse errors
    def insert_parse_errors(self, errs: Iterable[ParseErrorRecord]) -> int:
        rows = [(e.cluster_id, e.node_id, e.source_type, e.source_file, e.source_offset, e.raw, e.reason,
                 e.parser_version, e.ingested_at.isoformat()) for e in errs]
        if not rows:
            return 0
        with self._lock, self.conn:
            before = self.conn.total_changes
            self.conn.executemany("INSERT OR IGNORE INTO parse_errors (cluster_id,node_id,source_type,source_file,"
                                  "source_offset,raw,reason,parser_version,ingested_at) VALUES (?,?,?,?,?,?,?,?,?)", rows)
            return self.conn.total_changes - before

    def parse_errors(self, cluster_id: str, limit: int = 200) -> tuple[int, list[dict]]:
        with self._lock:
            n = self.conn.execute("SELECT COUNT(*) FROM parse_errors WHERE cluster_id=?", (cluster_id,)).fetchone()[0]
            rows = self.conn.execute("SELECT * FROM parse_errors WHERE cluster_id=? ORDER BY id DESC LIMIT ?",
                                     (cluster_id, limit)).fetchall()
        return n, [dict(r) for r in rows]

    # ------------------------------------------------------------------------------------------ checkpoints
    def checkpoint(self, path: str) -> tuple[Optional[str], int]:
        with self._lock:
            r = self.conn.execute("SELECT file_id, offset FROM checkpoints WHERE path=?", (path,)).fetchone()
        return (r[0], r[1]) if r else (None, 0)

    def set_checkpoint(self, path: str, file_id: str, offset: int):
        with self._lock, self.conn:
            self.conn.execute("INSERT INTO checkpoints VALUES (?,?,?,?) ON CONFLICT(path) DO UPDATE SET "
                              "file_id=excluded.file_id, offset=excluded.offset, updated=excluded.updated",
                              (path, file_id, offset, _now()))

    # ------------------------------------------------------------------------------------------ signals
    def replace_signals(self, cluster_id: str, signals: list[Signal]):
        with self._lock, self.conn:
            self.conn.execute("DELETE FROM signals WHERE cluster_id=?", (cluster_id,))
            self.conn.executemany("INSERT INTO signals VALUES (?,?,?)",
                                  [(s.signal_id, cluster_id, s.model_dump_json()) for s in signals])

    def signals(self, cluster_id: str) -> list[Signal]:
        with self._lock:
            rows = self.conn.execute("SELECT data FROM signals WHERE cluster_id=?", (cluster_id,)).fetchall()
        return sorted((Signal.model_validate_json(r[0]) for r in rows), key=lambda s: (s.start, s.signal_id))

    # ------------------------------------------------------------------------------------------ incidents
    def upsert_incident(self, inc: Incident) -> bool:
        """Insert or update by stable incident_id. RCA status/version are owned by the RCA service and kept.
        Returns True when the incident is new."""
        with self._lock, self.conn:
            cur = self.conn.execute("SELECT data, created_at FROM incidents WHERE incident_id=?", (inc.incident_id,)).fetchone()
            if cur:
                old = Incident.model_validate_json(cur[0])
                inc.rca_status, inc.rca_version = old.rca_status, old.rca_version
            self.conn.execute(
                "INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(incident_id) DO UPDATE SET "
                "severity=excluded.severity, status=excluded.status, first_seen=excluded.first_seen, "
                "last_seen=excluded.last_seen, data=excluded.data, updated_at=excluded.updated_at",
                (inc.incident_id, inc.cluster_id, inc.severity, inc.status, inc.first_seen.isoformat(),
                 inc.last_seen.isoformat(), inc.model_dump_json(), cur[1] if cur else _now(), _now()))
            return cur is None

    def set_rca_state(self, incident_id: str, status: str, version: int | None = None):
        with self._lock, self.conn:
            r = self.conn.execute("SELECT data FROM incidents WHERE incident_id=?", (incident_id,)).fetchone()
            if not r:
                return
            inc = Incident.model_validate_json(r[0])
            inc.rca_status = status
            if version is not None:
                inc.rca_version = version
            self.conn.execute("UPDATE incidents SET data=?, updated_at=? WHERE incident_id=?",
                              (inc.model_dump_json(), _now(), incident_id))

    def incidents(self, cluster_id: str, since: str | None = None, until: str | None = None, node: str | None = None,
                  severity: str | None = None, status: str | None = None) -> list[Incident]:
        q, p = "SELECT data FROM incidents WHERE cluster_id=?", [cluster_id]
        if since:
            q += " AND last_seen>=?"
            p.append(since)
        if until:
            q += " AND first_seen<=?"
            p.append(until)
        if severity:
            q += " AND severity=?"
            p.append(severity)
        if status:
            q += " AND status=?"
            p.append(status)
        with self._lock:
            out = [Incident.model_validate_json(r[0]) for r in self.conn.execute(q, p).fetchall()]
        if node:
            out = [i for i in out if node in i.nodes]
        rank = {"critical": 0, "warning": 1, "info": 2}
        return sorted(out, key=lambda i: (i.status != "open", rank[i.severity], -i.first_seen.timestamp()))

    def incident(self, incident_id: str) -> Optional[Incident]:
        with self._lock:
            r = self.conn.execute("SELECT data FROM incidents WHERE incident_id=?", (incident_id,)).fetchone()
        return Incident.model_validate_json(r[0]) if r else None

    # ------------------------------------------------------------------------------------------ RCA versions
    def add_rca(self, rec: RCARecord) -> int:
        with self._lock, self.conn:
            v = self.conn.execute("SELECT COALESCE(MAX(version),0) FROM rca_reports WHERE incident_id=?",
                                  (rec.incident_id,)).fetchone()[0] + 1
            rec.version = v
            self.conn.execute("INSERT INTO rca_reports VALUES (?,?,?,?,?)",
                              (rec.incident_id, v, rec.status, rec.created_at.isoformat(), rec.model_dump_json()))
        self.set_rca_state(rec.incident_id, rec.status, v)
        return v

    def rca(self, incident_id: str, version: int | None = None) -> Optional[RCARecord]:
        with self._lock:
            if version is None:
                r = self.conn.execute("SELECT data FROM rca_reports WHERE incident_id=? ORDER BY version DESC LIMIT 1",
                                      (incident_id,)).fetchone()
            else:
                r = self.conn.execute("SELECT data FROM rca_reports WHERE incident_id=? AND version=?",
                                      (incident_id, version)).fetchone()
        return RCARecord.model_validate_json(r[0]) if r else None

    def rca_versions(self, incident_id: str) -> list[dict]:
        with self._lock:
            rows = self.conn.execute("SELECT data FROM rca_reports WHERE incident_id=? ORDER BY version",
                                     (incident_id,)).fetchall()
        out = []
        for r in rows:
            rec = RCARecord.model_validate_json(r[0])
            out.append({"version": rec.version, "status": rec.status, "source": rec.source, "model": rec.model,
                        "prompt_version": rec.prompt_version, "trigger": rec.trigger, "created_at": rec.created_at,
                        "reason": rec.reason, "evidence_hash": rec.evidence_hash})
        return out

    # ------------------------------------------------------------------------------------------ key/value snapshots
    def put(self, key: str, value):
        with self._lock, self.conn:
            self.conn.execute("INSERT INTO kv VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
                              "updated=excluded.updated", (key, json.dumps(value, default=str), _now()))

    def get(self, key: str, default=None):
        with self._lock:
            r = self.conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(r[0]) if r else default


def _row_to_event(r: sqlite3.Row) -> LogEvent:
    d = dict(r)
    d["timestamp"] = datetime.fromtimestamp(d.pop("epoch"), tz=timezone.utc)
    d["attributes"] = json.loads(d["attributes"] or "{}")
    return LogEvent(**d)
