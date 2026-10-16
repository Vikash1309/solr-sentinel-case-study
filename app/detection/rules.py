"""Signal detection (doc §5.1, §5.2). Three layers, none of which needs an LLM:

  1. Event rules       - known Solr / ZooKeeper / JVM failure signatures on single records, grouped into
                         bursts per node (same type, < burst_gap_s apart) so one signal != one log line.
  2. Threshold rules   - per-minute metrics vs. documented operational thresholds (GC pressure, heap after
                         GC, commit storms) and telemetry freshness.
  3. Baseline rules    - robust z-score (median/MAD) for latency and unclassified error rates.

Severity is assigned here by deterministic policy (info | warning | critical)."""
from __future__ import annotations

import hashlib
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from ..config import SEVERITY_RANK, Settings
from ..schemas import LogEvent, Signal
from .baselines import robust_flags

URL_HOST = re.compile(r"https?://([\w.-]+):\d+")
EXPENSIVE = re.compile(r"rows=(\d{4,})|start=(\d{5,})|facet\.limit=-1|facet\.pivot=|group\.limit=-1")


@dataclass
class EventRule:
    type: str
    category: str
    severity: str
    title: str
    match: Callable[[LogEvent, Settings], bool]
    min_count: int | Callable[[Settings], int] = 1
    severity_fn: Optional[Callable[[list[LogEvent], Settings], str]] = None


def _text(pattern: str, types=("solr_log", "solr_request", "solr_update", "solr_slow_request")):
    rx = re.compile(pattern, re.I)
    return lambda e, s: (e.source_type == "solr" and e.event_type in types
                         and bool(rx.search(e.message) or rx.search(e.attributes.get("stack", ""))))


def _live_nodes_decreased(e: LogEvent, s: Settings) -> bool:
    m = re.search(r"Updated live nodes from ZooKeeper\.\.\. \((\d+)\) -> \((\d+)\)", e.message)
    return bool(m and int(m[2]) < int(m[1]))


def _expensive(e: LogEvent) -> bool:
    m = EXPENSIVE.search(e.attributes.get("params", "") or e.message)
    return bool(m) and not (m[1] and int(m[1]) < 5000)


def _pause(e: LogEvent) -> float:
    return e.attributes.get("pause_ms") or 0.0


def _gc_sev(evs, s):
    return "critical" if max(_pause(e) for e in evs) >= s.gc_pause_crit_ms else "warning"


EVENT_RULES: list[EventRule] = [
    EventRule("SOLR_OOM", "resource", "critical", "java.lang.OutOfMemoryError", _text(r"OutOfMemoryError")),
    EventRule("ZK_SESSION_EXPIRED", "zookeeper", "critical", "ZooKeeper session expired",
              _text(r"state:Expired|session (was )?expired")),
    EventRule("ZK_DISCONNECTED", "zookeeper", "warning", "ZooKeeper connection lost", _text(r"state:Disconnected|ConnectionLoss")),
    EventRule("NODE_LOST", "cluster", "critical", "Live node count decreased", _live_nodes_decreased),
    EventRule("LEADER_ELECTION", "replication", "warning", "Shard leader change",
              _text(r"I am the new leader|Running the leader process|Leader for \S+ changed"),
              severity_fn=lambda evs, s: "critical" if len({e.attributes.get("shard") for e in evs}) >= 3 else "warning"),
    EventRule("REPLICA_RECOVERY", "replication", "warning", "Replica recovery",
              _text(r"Starting recovery process|PeerSync Recovery was not successful|Starting Replication Recovery")),
    EventRule("RECOVERY_FAILED", "replication", "critical", "Replica recovery failed",
              _text(r"Recovery failed|successful=\[false\]|Could not publish as ACTIVE")),
    EventRule("NO_LEADER", "replication", "critical", "No registered shard leader", _text(r"No registered leader was found")),
    EventRule("REMOTE_TIMEOUT", "errors", "warning", "Inter-node request timeouts",
              _text(r"Timeout occurred while waiting response from server|SocketTimeoutException|Connection refused|Read timed out"),
              min_count=lambda s: s.remote_timeout_burst_min,
              severity_fn=lambda evs, s: "critical" if len(evs) >= 50 else "warning"),
    EventRule("SEARCHER_LIMIT", "indexing", "warning", "maxWarmingSearchers exceeded (commits rejected)",
              _text(r"exceeded limit of maxWarmingSearchers")),
    EventRule("OVERLAPPING_SEARCHERS", "indexing", "warning", "Overlapping onDeckSearchers", _text(r"Overlapping onDeckSearchers")),
    EventRule("CIRCUIT_BREAKER", "resource", "warning", "Solr circuit breaker tripped", _text(r"Circuit Breakers tripped")),
    EventRule("DISK_FULL", "resource", "critical", "Disk full", _text(r"No space left on device")),
    EventRule("TOO_MANY_OPEN_FILES", "resource", "critical", "File descriptor exhaustion", _text(r"Too many open files")),
    EventRule("EXPENSIVE_QUERIES", "query", "warning", "Expensive queries (deep paging / unbounded facets)",
              lambda e, s: e.event_type == "solr_slow_request" and _expensive(e),
              min_count=lambda s: s.expensive_query_burst_min),
    EventRule("SLOW_QUERIES", "query", "warning", "Slow queries",
              lambda e, s: e.event_type == "solr_slow_request" and not _expensive(e),
              min_count=lambda s: s.slow_query_burst_min),
    EventRule("GC_EVAC_FAILURE", "gc", "warning", "G1 evacuation failure (to-space exhausted)",
              lambda e, s: e.event_type == "gc_evacuation_failure" or bool(e.attributes.get("evacuation_failure"))),
    EventRule("GC_FULL", "gc", "warning", "Full GC (stop-the-world compaction)",
              lambda e, s: e.event_type == "gc_pause" and e.attributes.get("gc_type") == "full", severity_fn=_gc_sev),
    EventRule("GC_LONG_PAUSE", "gc", "warning", "Long GC pause",
              lambda e, s: e.event_type == "gc_pause" and e.attributes.get("gc_type") != "full" and _pause(e) >= s.gc_pause_warn_ms,
              severity_fn=_gc_sev),
    EventRule("GC_PAUSE_EXCEEDS_ZK_TIMEOUT", "gc", "critical", "GC pause longer than zkClientTimeout",
              lambda e, s: e.event_type == "gc_pause" and _pause(e) >= s.zk_client_timeout_ms),
    EventRule("GC_HUMONGOUS", "gc", "warning", "Frequent humongous allocations",
              lambda e, s: e.event_type == "gc_pause" and "Humongous" in (e.attributes.get("cause") or ""),
              min_count=lambda s: s.humongous_burst_min),
]
RULES_BY_TYPE = {r.type: r for r in EVENT_RULES}


def signal_id(*parts) -> str:
    return "S-" + hashlib.sha1("|".join(str(p) for p in parts).encode(), usedforsecurity=False).hexdigest()[:8]


def _bursts(evs: list[LogEvent], gap_s: int) -> list[list[LogEvent]]:
    out, cur = [], []
    for e in sorted(evs, key=lambda x: x.timestamp):
        if cur and (e.timestamp - cur[-1].timestamp).total_seconds() > gap_s:
            out.append(cur)
            cur = []
        cur.append(e)
    if cur:
        out.append(cur)
    return out


def _target(evs: list[LogEvent]) -> Optional[str]:
    hosts = Counter(h for e in evs for h in URL_HOST.findall(e.message + " " + e.attributes.get("stack", "")) if h != e.node_id)
    return hosts.most_common(1)[0][0] if hosts else None


def event_signals(events: list[LogEvent], s: Settings) -> tuple[list[Signal], set[str]]:
    matched: dict[tuple[str, str], list[LogEvent]] = defaultdict(list)
    classified: set[str] = set()
    for e in events:
        for r in EVENT_RULES:
            if r.match(e, s):
                matched[(r.type, e.node_id)].append(e)
                classified.add(e.event_id)
    out = []
    for (rtype, node), evs in matched.items():
        r = RULES_BY_TYPE[rtype]
        need = r.min_count(s) if callable(r.min_count) else r.min_count
        for burst in _bursts(evs, s.burst_gap_s):
            if len(burst) < need:
                continue
            sev = r.severity_fn(burst, s) if r.severity_fn else r.severity
            first = burst[0]
            start = first.timestamp - timedelta(milliseconds=_pause(first)) if _pause(first) else first.timestamp
            metrics: dict = {}
            pauses = [_pause(e) for e in burst if _pause(e)]
            if pauses:
                metrics["max_pause_ms"] = round(max(pauses), 1)
                metrics["total_pause_ms"] = round(sum(pauses), 1)
            heaps = [round(100 * e.attributes["heap_after_mb"] / e.attributes["heap_capacity_mb"], 1)
                     for e in burst if e.attributes.get("heap_after_mb") and e.attributes.get("heap_capacity_mb")]
            if heaps:
                metrics["max_heap_after_pct"] = max(heaps)
            qt = [e.attributes["qtime_ms"] for e in burst if e.attributes.get("qtime_ms")]
            if qt:
                metrics["max_qtime_ms"], metrics["median_qtime_ms"] = max(qt), int(statistics.median(qt))
            cores = sorted({e.attributes["core"] for e in burst if e.attributes.get("core")})
            if cores:
                metrics["cores"] = cores
            detail = f"{len(burst)} record(s) between {start:%H:%M:%S} and {burst[-1].timestamp:%H:%M:%S} UTC"
            if pauses:
                detail = (f"{len(burst)} pause(s) between {start:%H:%M:%S} and {burst[-1].timestamp:%H:%M:%S} UTC, "
                          f"longest {metrics['max_pause_ms']} ms" +
                          (f", heap after GC up to {metrics['max_heap_after_pct']}%" if heaps else ""))
            elif qt:
                detail += f", QTime median {metrics['median_qtime_ms']} ms / max {metrics['max_qtime_ms']} ms"
            target = None
            if rtype == "REMOTE_TIMEOUT":
                target = _target(burst)
                if target:
                    detail += f"; requests to {target} timed out"
            if rtype == "LEADER_ELECTION":
                won = sorted({m[1] for e in burst if (m := re.search(r"I am the new leader: \S+ (\S+)", e.message))})
                moved = _target(burst)
                if won:
                    metrics["became_leader_for"] = won
                    detail += f"; {node} became leader for {', '.join(won)}"
                elif moved:
                    detail += f"; observed leader moving to {moved}"
            out.append(Signal(signal_id=signal_id(rtype, node, int(start.timestamp())), cluster_id=s.cluster_id, node_id=node,
                              signal_type=rtype, category=r.category, severity=sev, start=start, end=burst[-1].timestamp,
                              count=len(burst), title=r.title, detail=detail, metrics=metrics,
                              event_ids=[e.event_id for e in burst][:300], target_node=target, method="rule"))
    return out, classified


def _runs(flags: list[tuple[int, bool]], max_gap: int):
    runs, cur, gap = [], [], 0
    for i, f in flags:
        if f:
            cur.append(i)
            gap = 0
        elif cur:
            gap += 1
            if gap > max_gap:
                runs.append(cur)
                cur, gap = [], 0
    if cur:
        runs.append(cur)
    return runs


def _metric_signal(s: Settings, node, typ, cat, title, rows, idx, sev, metrics, detail, method) -> Signal:
    start = datetime.fromtimestamp(rows[idx[0]]["epoch"], tz=timezone.utc)
    end = datetime.fromtimestamp(rows[idx[-1]]["epoch"] + s.metric_window_s - 1, tz=timezone.utc)
    return Signal(signal_id=signal_id(typ, node, int(start.timestamp())), cluster_id=s.cluster_id, node_id=node,
                  signal_type=typ, category=cat, severity=sev, start=start, end=end, count=len(idx), title=title,
                  detail=detail, metrics=metrics, method=method)


def threshold_signals(series: dict[str, list[dict]], s: Settings) -> list[Signal]:
    out = []
    for node, rows in series.items():
        fl = [(i, (r["heap_after_max_pct"] or 0) >= s.heap_after_gc_warn_pct and r["gc_pauses"] > 0) for i, r in enumerate(rows)]
        for run in _runs(fl, 2):
            peak = max(rows[i]["heap_after_max_pct"] or 0 for i in run)
            out.append(_metric_signal(s, node, "GC_HEAP_PRESSURE", "gc", "Heap occupancy after GC is high", rows, run,
                                      "critical" if peak >= s.heap_after_gc_crit_pct else "warning",
                                      {"peak_heap_after_gc_pct": peak, "threshold_pct": s.heap_after_gc_warn_pct},
                                      f"Heap after GC >= {s.heap_after_gc_warn_pct}% for {len(run)} min (peak {peak}%)", "threshold"))
        fl = [(i, r["gc_overhead_pct"] >= s.gc_overhead_warn_pct) for i, r in enumerate(rows)]
        for run in _runs(fl, 1):
            peak = max(rows[i]["gc_overhead_pct"] for i in run)
            out.append(_metric_signal(s, node, "GC_OVERHEAD", "gc", "High GC overhead", rows, run,
                                      "critical" if peak >= s.gc_overhead_crit_pct else "warning",
                                      {"peak_gc_time_pct": peak, "threshold_pct": s.gc_overhead_warn_pct},
                                      f"Stop-the-world GC time >= {s.gc_overhead_warn_pct}% of wall clock for {len(run)} min (peak {peak}%)",
                                      "threshold"))
        fl = [(i, r["hard_commits"] >= s.explicit_commits_per_min) for i, r in enumerate(rows)]
        for run in _runs(fl, 1):
            peak = max(rows[i]["hard_commits"] for i in run)
            out.append(_metric_signal(s, node, "EXCESSIVE_COMMITS", "indexing", "Excessive explicit commits", rows, run, "warning",
                                      {"peak_commits_per_min": peak, "threshold_per_min": s.explicit_commits_per_min},
                                      f"{peak} explicit hard commits/min opening new searchers (threshold {s.explicit_commits_per_min})",
                                      "threshold"))
    return out


def baseline_signals(series: dict[str, list[dict]], events: list[LogEvent], classified: set[str], s: Settings) -> list[Signal]:
    out = []
    uncls: dict[str, Counter] = defaultdict(Counter)
    for e in events:
        if e.source_type == "solr" and e.severity == "ERROR" and e.event_id not in classified:
            ep = int(e.timestamp.timestamp())
            uncls[e.node_id][ep - ep % s.metric_window_s] += 1
    for node, rows in series.items():
        p95 = [r["p95_qtime"] for r in rows]
        fl = robust_flags(p95, s.baseline_window_buckets, s.baseline_min_samples, s.mad_k, s.p95_qtime_floor_ms)
        for run in _runs([(i, f) for i, f, _ in fl], 1):
            peak = max(p95[i] for i in run)
            base = fl[run[0]][2][0]
            out.append(_metric_signal(s, node, "LATENCY_DEGRADATION", "query", "Query latency degradation (p95)", rows, run,
                                      "warning", {"peak_p95_ms": peak, "baseline_p95_ms": base},
                                      f"p95 QTime peaked at {peak} ms vs baseline {base} ms (robust z >= {s.mad_k})", "baseline"))
        errs = [uncls[node].get(r["epoch"], 0) for r in rows]
        fl = robust_flags(errs, s.baseline_window_buckets, s.baseline_min_samples, s.mad_k, s.error_burst_min)
        for run in _runs([(i, f) for i, f, _ in fl], 1):
            peak = max(errs[i] for i in run)
            out.append(_metric_signal(s, node, "ERROR_RATE_SPIKE", "errors", "Unclassified error spike", rows, run, "warning",
                                      {"peak_errors_per_min": peak},
                                      f"{peak} unclassified ERROR records/min (statistically unusual for this node)", "baseline"))
    return out


def telemetry_signals(last_seen: dict[tuple[str, str], float], now: datetime, s: Settings) -> list[Signal]:
    """Missing telemetry (doc §5.1): a telemetry incident - NOT proof that the node is down."""
    out = []
    for (node, source), last in sorted(last_seen.items()):
        age = now.timestamp() - last
        if age > s.telemetry_stale_after_s:
            start = datetime.fromtimestamp(last, tz=timezone.utc)
            out.append(Signal(signal_id=signal_id("TELEMETRY_STALE", node, source, int(last)), cluster_id=s.cluster_id,
                              node_id=node, signal_type="TELEMETRY_STALE", category="telemetry", severity="warning",
                              start=start, end=now, count=1, title=f"No {source} telemetry",
                              detail=f"No {source} log records from {node} for {int(age // 60)} min "
                                     f"(last at {start:%H:%M:%S} UTC; threshold {s.telemetry_stale_after_s // 60} min)",
                              metrics={"source_type": source, "age_s": round(age), "last_seen": start.isoformat()},
                              method="threshold"))
    return out


def detect(events: list[LogEvent], series: dict[str, list[dict]], last_seen, now: datetime, s: Settings) -> list[Signal]:
    sig, classified = event_signals(events, s)
    sig += threshold_signals(series, s)
    sig += baseline_signals(series, events, classified, s)
    sig += telemetry_signals(last_seen, now, s)
    sig.sort(key=lambda x: (x.start, -SEVERITY_RANK[x.severity], x.signal_id))
    return sig
