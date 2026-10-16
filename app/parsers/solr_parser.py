"""Solr 8/9 log parser (default log4j2 layout of solr.log).

    2026-10-15 10:18:02.421 WARN  (qtp1489193907-213) [c:products s:shard1 r:core_node3 x:products_shard1_replica_n2 t:null-41871] o.a.s.c.S.SlowRequest slow: ...

* Multiline records (Java stack traces, "Caused by:") are folded into the preceding record.
* A line that starts like a timestamp but does not match the layout (e.g. a partial write), or a
  continuation line with no preceding record, becomes a ParseErrorRecord - never silently dropped.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

from ..redaction import redact_secrets
from ..schemas import LogEvent, ParseErrorRecord

PARSER_VERSION = "solr-1.1"

HEADER = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[.,]\d{3})\s+"
    r"(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|SEVERE)\s+"
    r"\((?P<thread>[^)]*)\)\s+"
    r"\[(?P<ctx>[^\]]*)\]\s+"
    r"(?P<logger>\S+)\s?(?P<msg>.*)$")
LOOKS_LIKE_RECORD = re.compile(r"^\d{4}-\d{2}-\d{2}")
CONTINUATION = re.compile(r"^(\s|at\s|Caused by:|Suppressed:|\.\.\.\s)")
CTX = re.compile(r"\b([csrxt]):(\S*)")
QTIME = re.compile(r"\bQTime=(\d+)")
STATUS = re.compile(r"\bstatus=(\d+)")
HITS = re.compile(r"\bhits=(\d+)")
PATH = re.compile(r"\bpath=(\S+)")
PARAMS = re.compile(r"\bparams=\{([^}]*)\}")
UPDATE_TAIL = re.compile(r"\}\s+(\d+)\s+(\d+)\s*$")
EXC = re.compile(r"\b((?:[a-z]\w*\.)+[A-Z]\w*(?:Exception|Error))\b")
LEVELS = {"WARNING": "WARN", "SEVERE": "ERROR", "FATAL": "ERROR"}


def event_id(cluster_id: str, node_id: str, source: str, first_line: str) -> str:
    """Stable identity from content, so a replay or a re-read after rotation deduplicates."""
    return hashlib.sha1(f"{cluster_id}|{node_id}|{source}|{first_line}".encode(), usedforsecurity=False).hexdigest()[:24]


def _ts(s: str) -> datetime:
    # Assumption (doc §12): Solr writes UTC timestamps (Solr default -Duser.timezone=UTC)
    return datetime.strptime(s.replace(",", ".").replace("T", " "), "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)


def _build(m: re.Match, line: str, cluster_id: str, node_id: str, file: str, off: int, now: datetime) -> LogEvent:
    ctx = dict(CTX.findall(m["ctx"]))
    msg = redact_secrets(m["msg"])
    logger = m["logger"]
    attrs = {"logger": logger, "thread": m["thread"]}
    for k, name in (("c", "collection"), ("s", "shard"), ("r", "replica"), ("x", "core")):
        if ctx.get(k):
            attrs[name] = ctx[k]
    etype = "solr_log"
    if logger.endswith("S.Request") or logger.endswith("SolrCore.Request"):
        etype = "solr_request"
    elif logger.endswith("SlowRequest"):
        etype = "solr_slow_request"
    elif logger.endswith("LogUpdateProcessorFactory"):
        etype = "solr_update"
    if etype != "solr_log":
        if p := PATH.search(msg):
            attrs["path"] = p[1]
        if p := PARAMS.search(msg):
            attrs["params"] = p[1]
        if q := QTIME.search(msg):
            attrs["qtime_ms"] = int(q[1])
        if s := STATUS.search(msg):
            attrs["status"] = int(s[1])
        if h := HITS.search(msg):
            attrs["hits"] = int(h[1])
        if etype == "solr_update" and "qtime_ms" not in attrs and (u := UPDATE_TAIL.search(msg)):
            attrs["status"], attrs["qtime_ms"] = int(u[1]), int(u[2])
    if x := EXC.search(msg):
        attrs["exception_class"] = x[1]
    return LogEvent(event_id=event_id(cluster_id, node_id, "solr", line), timestamp=_ts(m["ts"]), ingested_at=now,
                    cluster_id=cluster_id, node_id=node_id, source_type="solr",
                    severity=LEVELS.get(m["level"], m["level"]), event_type=etype, message=msg, attributes=attrs,
                    source_file=file, source_offset=off, parser_version=PARSER_VERSION, raw=redact_secrets(line))


def parse_lines(lines: list[tuple[int, str]], cluster_id: str, node_id: str, file: str,
                now: datetime | None = None) -> tuple[list[LogEvent], list[ParseErrorRecord]]:
    """lines: (byte_offset, text). Returns (events, parse_errors)."""
    now = now or datetime.now(timezone.utc)
    events: list[LogEvent] = []
    errors: list[ParseErrorRecord] = []

    def err(off, raw, reason):
        errors.append(ParseErrorRecord(cluster_id=cluster_id, node_id=node_id, source_type="solr", source_file=file,
                                       source_offset=off, raw=redact_secrets(raw[:2000]), reason=reason,
                                       parser_version=PARSER_VERSION, ingested_at=now))

    for off, text in lines:
        line = text.rstrip("\r\n")
        if not line.strip():
            continue
        m = HEADER.match(line)
        if m:
            events.append(_build(m, line, cluster_id, node_id, file, off, now))
            continue
        if LOOKS_LIKE_RECORD.match(line):
            err(off, line, "record does not match the Solr log layout (truncated or unexpected format)")
        elif events and CONTINUATION.match(line):
            ev = events[-1]
            ev.raw += "\n" + redact_secrets(line)
            st = ev.attributes.get("stack", "")
            ev.attributes["stack"] = (st + "\n" + line.strip()) if st else line.strip()
            if "exception_class" not in ev.attributes and (x := EXC.search(line)):
                ev.attributes["exception_class"] = x[1]
        elif events:
            err(off, line, "unrecognised line inside a record")
        else:
            err(off, line, "continuation line without a preceding record")
    return events, errors
