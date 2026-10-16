"""JVM GC log parser.

Supported formats (doc §4.3 - modern unified GC logs differ from older formats):
  * JDK 9+ unified logging (-Xlog:gc*:file=gc.log:time,uptime,level,tags), G1:
      [2026-10-15T10:38:23.234+0000][23903.235s][info][gc          ] GC(4510) Pause Full (G1 Compaction Pause) 8188M->7560M(8192M) 18234.600ms
  * JDK 8 (-XX:+PrintGCDateStamps) summary lines:
      2026-10-15T10:38:23.234+0000: 23903.235: [Full GC (Allocation Failure)  8188M->7560M(8192M), 18.2346000 secs]

Kept: stop-the-world pauses (with heap transition), concurrent-cycle durations, evacuation-failure markers.
Well-formed lines without diagnostic value (gc,heap region counts, gc,cpu, phases) are skipped and counted.
Lines that are not GC log records at all become ParseErrorRecords. Heap values are recorded only when present.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from ..schemas import LogEvent, ParseErrorRecord
from .solr_parser import event_id

PARSER_VERSION = "gc-1.1"

DECOR = re.compile(r"^((?:\[[^\]]*\])+)\s*(.*)$")
ISO_TS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{4}|Z)?$")
PAUSE = re.compile(r"GC\((?P<id>\d+)\)\s+(?P<label>Pause .+?)\s+(?P<b>\d+)M->(?P<a>\d+)M\((?P<t>\d+)M\)\s+(?P<ms>[\d.]+)ms\s*$")
CONCURRENT = re.compile(r"GC\((?P<id>\d+)\)\s+(?P<label>Concurrent [A-Za-z ]+?)\s+(?P<ms>[\d.]+)ms\s*$")
MARKER = re.compile(r"GC\((?P<id>\d+)\)\s+(?P<label>To-space exhausted|Evacuation Failure.*)$", re.I)
LEGACY = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T[\d:.]+(?:[+-]\d{4})?):\s+[\d.]+:\s+\[(?P<label>GC pause|GC remark|GC cleanup|Full GC|GC)"
    r"(?P<rest>.*?)\s+(?P<b>[\d.]+)(?P<bu>[KMG])->(?P<a>[\d.]+)(?P<au>[KMG])\((?P<t>[\d.]+)(?P<tu>[KMG])\),\s*(?P<s>[\d.]+) secs\]")
LEGACY_START = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+(?:[+-]\d{4})?:\s+[\d.]+:")
UNIT = {"K": 1 / 1024, "M": 1, "G": 1024}


def _iso(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+0000"
    if re.search(r"[+-]\d{4}$", s):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone.utc)
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)


def _classify(label: str) -> tuple[str, str]:
    groups = re.findall(r"\(([^)]*)\)", label)
    low = label.lower()
    t = ("full" if "full" in low else "mixed" if "mixed" in low else "remark" if "remark" in low
         else "cleanup" if "cleanup" in low else "young")
    causes = [g for g in groups if g not in ("Normal", "Mixed", "Concurrent Start", "Prepare Mixed", "young", "mixed")]
    cause = ", ".join(causes)
    if "Humongous" in label:
        cause = "G1 Humongous Allocation"
    return t, cause


def _event(cluster_id, node_id, file, off, now, ts, line, etype, attrs, severity="INFO") -> LogEvent:
    return LogEvent(event_id=event_id(cluster_id, node_id, "gc", line), timestamp=ts, ingested_at=now,
                    cluster_id=cluster_id, node_id=node_id, source_type="gc", severity=severity, event_type=etype,
                    message=line.strip()[:500], attributes=attrs, source_file=file, source_offset=off,
                    parser_version=PARSER_VERSION, raw=line)


def parse_lines(lines: list[tuple[int, str]], cluster_id: str, node_id: str, file: str,
                now: datetime | None = None) -> tuple[list[LogEvent], list[ParseErrorRecord], int]:
    """Returns (events, parse_errors, skipped_count)."""
    now = now or datetime.now(timezone.utc)
    events, errors, skipped = [], [], 0

    def err(off, raw, reason):
        errors.append(ParseErrorRecord(cluster_id=cluster_id, node_id=node_id, source_type="gc", source_file=file,
                                       source_offset=off, raw=raw[:2000], reason=reason,
                                       parser_version=PARSER_VERSION, ingested_at=now))

    for off, text in lines:
        line = text.rstrip("\r\n")
        if not line.strip():
            continue
        m = DECOR.match(line)
        if m:
            decos = re.findall(r"\[([^\]]*)\]", m[1])
            msg = m[2]
            ts = next((_iso(d) for d in decos if ISO_TS.match(d.strip())), None)
            if ts is None:
                err(off, line, "unified GC record without a time decorator (-Xlog ...:time is required)")
                continue
            if p := PAUSE.search(msg):
                gtype, cause = _classify(p["label"])
                ms = float(p["ms"])
                attrs = {"gc_id": int(p["id"]), "gc_type": gtype, "cause": cause, "pause_ms": ms,
                         "heap_before_mb": int(p["b"]), "heap_after_mb": int(p["a"]), "heap_capacity_mb": int(p["t"])}
                if "Evacuation Failure" in p["label"]:
                    attrs["evacuation_failure"] = True
                events.append(_event(cluster_id, node_id, file, off, now, ts, line, "gc_pause", attrs,
                                     "WARN" if ms >= 1000 or gtype == "full" else "INFO"))
            elif c := CONCURRENT.search(msg):
                events.append(_event(cluster_id, node_id, file, off, now, ts, line, "gc_concurrent",
                                     {"gc_id": int(c["id"]), "phase": c["label"], "duration_ms": float(c["ms"])}))
            elif k := MARKER.search(msg):
                events.append(_event(cluster_id, node_id, file, off, now, ts, line, "gc_evacuation_failure",
                                     {"gc_id": int(k["id"]), "marker": k["label"]}, "WARN"))
            else:
                skipped += 1
            continue
        if lg := LEGACY.match(line):
            low = lg["label"]
            gtype = ("full" if low == "Full GC" else "remark" if "remark" in low else "cleanup" if "cleanup" in low
                     else "mixed" if "(mixed)" in lg["rest"] else "young")
            cause = ", ".join(c for c in re.findall(r"\(([^)]*)\)", lg["rest"]) if c not in ("young", "mixed", "to-space exhausted"))
            ms = float(lg["s"]) * 1000
            ts = _iso(lg["ts"])
            events.append(_event(cluster_id, node_id, file, off, now, ts, line, "gc_pause", {
                "gc_type": gtype, "cause": cause, "pause_ms": ms,
                "heap_before_mb": int(float(lg["b"]) * UNIT[lg["bu"]]), "heap_after_mb": int(float(lg["a"]) * UNIT[lg["au"]]),
                "heap_capacity_mb": int(float(lg["t"]) * UNIT[lg["tu"]])}, "WARN" if ms >= 1000 or gtype == "full" else "INFO"))
            if "to-space exhausted" in line.lower():
                events.append(_event(cluster_id, node_id, file, off, now, ts, line + " #marker", "gc_evacuation_failure",
                                     {"marker": "to-space exhausted"}, "WARN"))
            continue
        if LEGACY_START.match(line):
            skipped += 1  # JDK8 detail line (PrintGCDetails) without a summary we use
            continue
        err(off, line, "not a recognised GC log record")
    return events, errors, skipped
