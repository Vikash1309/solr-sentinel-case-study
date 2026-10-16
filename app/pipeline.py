"""One analysis run: ingest -> detect -> correlate -> health -> persist incidents -> queue RCA (doc §3, §7.1).

Deterministic stages finish and are persisted before any RCA runs, so incidents are visible even when the
LLM is slow or down. Re-running on the same data updates the same incidents (stable identities)."""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone

from .config import Settings
from .correlation import engine
from .detection import baselines, health, rules
from .ingestion import reader
from .observability import METRICS
from .storage.repository import Repository

log = logging.getLogger("sentinel.pipeline")
_lock = threading.Lock()


def assessment_time(repo: Repository, s: Settings) -> datetime | None:
    st = repo.event_stats(s.cluster_id)
    if st["max_epoch"] is None:
        return None
    if s.clock == "wall":
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(st["max_epoch"], tz=timezone.utc)


def _queue_rca(repo: Repository, rca, incidents, sig_by_id: dict, sync_rca: bool) -> int:
    queued = 0
    for inc in incidents:
        stored = repo.incident(inc.incident_id)
        if stored and rca.needs_rca(stored, sig_by_id):
            if sync_rca:
                rca.analyze(inc.incident_id)
            elif rca.enqueue(inc.incident_id):
                queued += 1
    return queued


def run_analysis(repo: Repository, s: Settings, rca=None, do_ingest: bool = True, force: bool = True,
                 sync_rca: bool = False) -> dict:
    with _lock:
        t0 = time.time()
        stats = reader.ingest(repo, s) if do_ingest else {"events_new": 0}
        prev = repo.get("overview")
        if not force and stats.get("events_new", 0) == 0 and prev and not prev.get("empty"):
            if rca is not None:   # nothing new to analyse, but an earlier RCA may be due for a retry
                sig_by_id = {x.signal_id: x for x in repo.signals(s.cluster_id)}
                _queue_rca(repo, rca, repo.incidents(s.cluster_id), sig_by_id, sync_rca)
            return prev
        now = assessment_time(repo, s)
        n_err, _ = repo.parse_errors(s.cluster_id, limit=1)
        if now is None:
            ov = {"empty": True, "generated_at": datetime.now(timezone.utc).isoformat(), "ingest": stats,
                  "parse_errors": n_err, "log_dir": s.log_dir}
            repo.put("overview", ov)
            return ov
        events = repo.events(s.cluster_id, since_epoch=(now - timedelta(hours=s.analysis_lookback_hours)).timestamp())
        series = baselines.compute_series(events, s.metric_window_s)
        last_seen = repo.last_seen_by_source(s.cluster_id)
        signals = rules.detect(events, series, last_seen, now, s)
        incidents = engine.correlate(signals, now, s)
        ch, timeline = health.assess(series, signals, events, last_seen, now, s)

        repo.replace_signals(s.cluster_id, signals)
        new = sum(repo.upsert_incident(i) for i in incidents)
        repo.put("series", series)
        repo.put("cluster_health", ch.model_dump(mode="json"))
        repo.put("timeline", timeline)
        sig_by_id = {x.signal_id: x for x in signals}
        queued = _queue_rca(repo, rca, incidents, sig_by_id, sync_rca) if rca is not None else 0
        st = repo.event_stats(s.cluster_id)
        lag = time.time() - st["max_epoch"]
        METRICS.set("sentinel_ingest_lag_seconds", round(lag, 1))
        METRICS.observe("sentinel_analysis_duration_seconds", time.time() - t0)
        for sev in ("info", "warning", "critical"):
            for status in ("open", "resolved"):
                METRICS.set("sentinel_incidents", sum(1 for i in incidents if i.severity == sev and i.status == status),
                            severity=sev, status=status)
        ov = {
            "empty": False, "generated_at": datetime.now(timezone.utc).isoformat(),
            "assessed_at": now.isoformat(), "clock": s.clock,
            "data_from": datetime.fromtimestamp(st["min_epoch"], tz=timezone.utc).isoformat(),
            "data_to": datetime.fromtimestamp(st["max_epoch"], tz=timezone.utc).isoformat(),
            "events_total": st["count"], "events_analyzed": len(events), "ingest": stats, "parse_errors": n_err,
            "signals": len(signals), "incidents": len(incidents), "new_incidents": new,
            "open_incidents": sum(1 for i in incidents if i.status == "open"), "rca_queued": queued,
            "analysis_ms": int((time.time() - t0) * 1000), "llm": rca.client.describe() if rca else "disabled",
        }
        repo.put("overview", ov)
        log.info("analysis: +%s events, %d signals, %d incidents (%d new), cluster=%s, %d ms",
                 stats.get("events_new"), len(signals), len(incidents), new, ch.state, ov["analysis_ms"])
        return ov
