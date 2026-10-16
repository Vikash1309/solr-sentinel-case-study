"""Rolling-window metrics and robust baselines (doc §5.2).

Per node and per 1-minute bucket: request count, p50/p95 QTime, ERROR count, HTTP 5xx, explicit hard
commits, GC pause count/total/max, GC overhead %, Full GCs, humongous allocations, evacuation failures and
heap occupancy after GC. Buckets with no GC data keep heap = None (unknown), never 0.

Robust baseline:  MAD = median(|x_i - median(x)|),  robust_z = (x - median) / (1.4826 * MAD)
  * at least `min_samples` baseline points are required, otherwise no verdict
  * MAD = 0 is handled with a floor (a perfectly flat baseline must not divide by zero)
  * anomalous buckets are excluded from the baseline, so a long incident cannot become "normal"
  * an absolute floor applies too: a deviation must also be operationally meaningful
"""
from __future__ import annotations

import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Sequence

from ..schemas import LogEvent

HARD_COMMIT = re.compile(r"start commit\{.*softCommit=false")


def _pct(values: list[float], p: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return round(s[f] + (s[c] - s[f]) * (k - f), 1)


def _empty_bucket() -> dict:
    return {"qtimes": [], "requests": 0, "errors": 0, "http_5xx": 0, "updates": 0, "hard_commits": 0,
            "gc_pauses": 0, "gc_pause_total_ms": 0.0, "gc_pause_max_ms": 0.0, "full_gcs": 0, "humongous": 0,
            "evac_failures": 0, "heap_after_max_pct": None, "solr_records": 0, "gc_records": 0}


def _add_solr(m: dict, e: LogEvent) -> None:
    a = e.attributes
    m["solr_records"] += 1
    if e.severity == "ERROR":
        m["errors"] += 1
    if e.event_type == "solr_request" and str(a.get("path", "")).endswith("/select"):
        m["requests"] += 1
        if a.get("qtime_ms") is not None:
            m["qtimes"].append(a["qtime_ms"])
        if (a.get("status") or 0) >= 500:
            m["http_5xx"] += 1
    elif e.event_type == "solr_update":
        m["updates"] += 1
        if (a.get("status") or 0) >= 500:
            m["http_5xx"] += 1
    elif e.event_type == "solr_log" and HARD_COMMIT.search(e.message):
        m["hard_commits"] += 1


def _add_gc(m: dict, e: LogEvent) -> None:
    a = e.attributes
    m["gc_records"] += 1
    if e.event_type == "gc_evacuation_failure":
        m["evac_failures"] += 1
    if e.event_type != "gc_pause" or a.get("pause_ms") is None:
        return
    m["gc_pauses"] += 1
    m["gc_pause_total_ms"] += a["pause_ms"]
    m["gc_pause_max_ms"] = max(m["gc_pause_max_ms"], a["pause_ms"])
    if a.get("gc_type") == "full":
        m["full_gcs"] += 1
    if "Humongous" in (a.get("cause") or ""):
        m["humongous"] += 1
    if a.get("heap_after_mb") is not None and a.get("heap_capacity_mb") and a.get("gc_type") in ("young", "mixed", "full"):
        pct = round(100 * a["heap_after_mb"] / a["heap_capacity_mb"], 1)
        m["heap_after_max_pct"] = pct if m["heap_after_max_pct"] is None else max(m["heap_after_max_pct"], pct)


def _finish(bucket: dict | None, b: int, window_s: int) -> dict:
    """One row per minute; empty minutes get explicit zeros (and None where a value is unknown)."""
    row: dict = {"t": datetime.fromtimestamp(b, tz=timezone.utc).isoformat(), "epoch": b}
    bucket = bucket or _empty_bucket()
    q = bucket.pop("qtimes")
    row.update(bucket)
    row["p50_qtime"], row["p95_qtime"] = _pct(q, 50), _pct(q, 95)
    row["gc_overhead_pct"] = round(min(100.0, 100 * bucket["gc_pause_total_ms"] / (window_s * 1000)), 1)
    row["gc_pause_total_ms"] = round(bucket["gc_pause_total_ms"], 1)
    return row


def compute_series(events: list[LogEvent], window_s: int = 60) -> dict[str, list[dict]]:
    """Per-node, per-minute metrics on a shared time axis (every node gets every minute)."""
    raw: dict[str, dict[int, dict]] = defaultdict(lambda: defaultdict(_empty_bucket))
    buckets_seen = set()
    for e in events:
        ep = int(e.timestamp.timestamp())
        b = ep - ep % window_s
        buckets_seen.add(b)
        (_add_solr if e.source_type == "solr" else _add_gc)(raw[e.node_id][b], e)
    if not buckets_seen:
        return {}
    lo, hi = min(buckets_seen), max(buckets_seen)
    return {node: [_finish(buckets.get(b), b, window_s) for b in range(lo, hi + 1, window_s)]
            for node, buckets in raw.items()}


def robust_flags(values: Sequence[Optional[float]], window: int, min_samples: int, k: float,
                 floor: float) -> list[tuple[int, bool, Optional[tuple[float, float]]]]:
    """Return (index, is_anomalous, (baseline_median, robust_z) | None) for each value."""
    flags: list[tuple[int, bool, Optional[tuple[float, float]]]] = []
    baseline: list[Optional[float]] = []
    for i, v in enumerate(values):
        hist = [x for x in baseline[-window:] if x is not None]
        if v is None or len(hist) < min_samples:
            flags.append((i, False, None))
            baseline.append(v)
            continue
        med = statistics.median(hist)
        mad = statistics.median([abs(x - med) for x in hist])
        mad = max(mad, 0.05 * abs(med), 1.0)          # MAD = 0 safety floor
        z = (v - med) / (1.4826 * mad)
        bad = v >= floor and z >= k
        flags.append((i, bad, (round(med, 1), round(z, 1))))
        if not bad:
            baseline.append(v)
    return flags
