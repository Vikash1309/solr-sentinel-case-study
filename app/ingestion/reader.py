"""Incremental log reader (doc §4.1).

Layout: <root>/<node_id>/solr*.log and <root>/<node_id>/gc*.log, for both the collection directory
(`log_dir`) and the upload directory. Node identity = directory name (production: collector metadata).

Per file: resume from the checkpoint, read only complete lines, parse, store events idempotently,
store malformed records in the parse-error store, then advance the checkpoint. A Solr file that is
still being written keeps its last record back until the next poll, so a stack trace that is only
partly flushed is not split into a separate record.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from pathlib import Path

from ..config import Settings
from ..observability import METRICS
from ..parsers import gc_parser, solr_parser
from ..storage.repository import Repository
from . import checkpoints

ACTIVE_WRITE_S = 5  # a file modified within this many seconds may contain a partially written record
NODE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")  # starts alphanumeric, so never "." or ".."


def discover(*roots: str) -> list[tuple[str, str, Path]]:
    found = []
    for root in roots:
        r = Path(root)
        if not r.exists():
            continue
        for node_dir in sorted(p for p in r.iterdir() if p.is_dir()):
            for f in sorted(node_dir.iterdir()):
                if not f.is_file() or not f.name.endswith(".log"):
                    continue
                kind = "gc" if f.name.startswith("gc") else "solr" if f.name.startswith("solr") else None
                if kind:
                    found.append((node_dir.name, kind, f))
    return found


def _read_new(path: Path, offset: int) -> tuple[list[tuple[int, str]], int]:
    with open(path, "rb") as fh:
        fh.seek(offset)
        data = fh.read()
    end = data.rfind(b"\n")
    if end < 0:
        return [], offset
    lines, pos = [], offset
    for raw in data[: end + 1].split(b"\n")[:-1]:
        lines.append((pos, raw.decode("utf-8", errors="replace")))
        pos += len(raw) + 1
    return lines, offset + end + 1


def ingest(repo: Repository, settings: Settings) -> dict:
    stats = {"files": 0, "events_new": 0, "events_duplicate": 0, "parse_errors": 0, "skipped": 0, "bytes": 0}
    now = datetime.now(timezone.utc)
    upload_root = Path(settings.upload_dir).resolve()
    for node_id, kind, path in discover(settings.log_dir, settings.upload_dir):
        live = upload_root not in path.resolve().parents   # uploaded files are complete; live files may be mid-write
        fid, offset = checkpoints.resume_offset(repo, path)
        if path.stat().st_size == offset:
            continue
        lines, new_offset = _read_new(path, offset)
        if not lines:
            continue
        if live and kind == "solr" and time.time() - path.stat().st_mtime < ACTIVE_WRITE_S:
            # hold back the last record (header + any continuation lines) until the writer moves on
            last_header = max((i for i, (_, t) in enumerate(lines) if solr_parser.HEADER.match(t)), default=0)
            if last_header == 0:
                continue
            new_offset = lines[last_header][0]
            lines = lines[:last_header]
        rel = f"{node_id}/{path.name}"
        if kind == "solr":
            events, errors = solr_parser.parse_lines(lines, settings.cluster_id, node_id, rel, now)
            skipped = 0
        else:
            events, errors, skipped = gc_parser.parse_lines(lines, settings.cluster_id, node_id, rel, now)
        added = repo.insert_events(events)
        repo.insert_parse_errors(errors)
        checkpoints.commit(repo, path, fid, new_offset)
        stats["files"] += 1
        stats["events_new"] += added
        stats["events_duplicate"] += len(events) - added
        stats["parse_errors"] += len(errors)
        stats["skipped"] += skipped
        stats["bytes"] += new_offset - offset
        METRICS.inc("sentinel_events_ingested_total", added, source=kind)
        METRICS.inc("sentinel_events_duplicate_total", len(events) - added, source=kind)
        METRICS.inc("sentinel_parse_errors_total", len(errors), source=kind)
        METRICS.inc("sentinel_records_skipped_total", skipped, source=kind)
    return stats


def save_upload(settings: Settings, node_id: str, source_type: str, filename: str, content: bytes) -> Path:
    """Upload endpoint support (doc §9.2): store under the upload root so the normal reader ingests it."""
    if source_type not in ("solr", "gc"):
        raise ValueError("source_type must be 'solr' or 'gc'")
    if not NODE_ID.fullmatch(node_id) or ".." in node_id:
        raise ValueError("node_id must be 1-64 characters: letters, digits, '-', '_' or '.', starting with a letter or digit")
    safe_name = "".join(c for c in Path(filename).stem if c.isalnum() or c in "-_")[:48] or "upload"
    root = Path(settings.upload_dir).resolve()
    d = (root / node_id).resolve()
    if d.parent != root:  # defence in depth: the node directory must sit directly under the upload root
        raise ValueError("invalid node_id")
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{source_type}-{int(time.time())}-{safe_name}.log"
    p.write_bytes(content if content.endswith(b"\n") else content + b"\n")
    return p
