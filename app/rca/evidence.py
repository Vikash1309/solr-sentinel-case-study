"""Bounded evidence package for one incident (doc §7.3). The model never sees raw log history.

Every item has an ID the model must cite, and every ID resolves to stored data:
    S-xxxxxxxx   signal (triggering rule, with metrics)           -> signals table
    E<n>         representative record (deduplicated template,    -> events table (event_ids listed)
                 count, first/last seen, one redacted sample)
    M-<node>     metric summary: baseline vs during the incident  -> per-minute series
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import OrderedDict

from ..config import SEVERITY_RANK, Settings
from ..correlation.knowledge import (
    GENERIC_ALTERNATIVES,
    IMPACT_STATEMENTS,
    KNOWLEDGE_VERSION,
    LOG_ONLY_LIMITATIONS,
    PATTERN_BY_ID,
)
from ..redaction import redact_query_terms
from ..schemas import ClusterHealth, Incident, LogEvent, Signal

MASKS = [(re.compile(r"t:null-\d+"), "t:<id>"), (re.compile(r"@[0-9a-f]{6,}"), "@<hex>"), (re.compile(r"SKU-\d+"), "SKU-<n>"),
         (re.compile(r"\(\d{10,}\)"), "(<ver>)"), (re.compile(r"\b\d+(\.\d+)?(ms|M|s)?\b"), "<n>")]
IMPACT_COUNTS = {"REMOTE_TIMEOUT": "inter_node_request_timeouts", "NO_LEADER": "updates_rejected_no_leader",
                 "SEARCHER_LIMIT": "commits_rejected_max_warming_searchers", "EXPENSIVE_QUERIES": "expensive_requests",
                 "SLOW_QUERIES": "slow_requests", "LEADER_ELECTION": "leader_change_records",
                 "REPLICA_RECOVERY": "replica_recovery_records"}


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def template(msg: str) -> str:
    for rx, rep in MASKS:
        msg = rx.sub(rep, msg)
    return msg[:240]


def representative_records(events: list[LogEvent], per_signal: list[tuple[str, list[str]]], max_items: int,
                           redact: bool) -> list[dict]:
    """Deduplicate into templates; select round-robin across signals (root first) so every signal with
    records is represented within the cap, then fill remaining slots ERROR > WARN > INFO."""
    groups: "OrderedDict[tuple, dict]" = OrderedDict()
    key_of = {}
    for e in events:
        msg = redact_query_terms(e.message) if redact else e.message
        key = (e.node_id, e.source_type, e.severity or "", template(msg))
        key_of[e.event_id] = key
        g = groups.get(key)
        if g is None:
            sample = msg if len(msg) <= 320 else msg[:320] + "..."
            if e.attributes.get("stack"):
                sample += " | " + e.attributes["stack"].splitlines()[0][:160]
            g = groups[key] = {"node": e.node_id, "source": e.source_type, "level": e.severity, "template": key[3],
                               "first_seen": _iso(e.timestamp), "last_seen": _iso(e.timestamp), "count": 0,
                               "sample": sample, "signals": [], "event_ids": []}
        g["count"] += 1
        g["last_seen"] = _iso(e.timestamp)
        if len(g["event_ids"]) < 5:
            g["event_ids"].append(e.event_id)
    chosen: list[tuple] = []
    queues = []
    for sid, ids in per_signal:
        ks = []
        for i in ids:
            k = key_of.get(i)
            if k:
                if sid not in groups[k]["signals"]:
                    groups[k]["signals"].append(sid)
                if k not in ks:
                    ks.append(k)
        if ks:
            queues.append(ks)
    while queues and len(chosen) < max_items:
        nxt = []
        for q in queues:
            while q and q[0] in chosen:
                q.pop(0)
            if q and len(chosen) < max_items:
                chosen.append(q.pop(0))
            if q:
                nxt.append(q)
        queues = nxt
    rank = {"ERROR": 0, "WARN": 1, "INFO": 2}
    rest = sorted((k for k in groups if k not in chosen), key=lambda k: (rank.get(k[2], 3), groups[k]["first_seen"]))
    chosen += rest[: max(0, max_items - len(chosen))]
    items = sorted((groups[k] for k in chosen), key=lambda g: g["first_seen"])
    return [{"id": f"E{i}", **g} for i, g in enumerate(items, 1)]


def metric_summaries(series: dict[str, list[dict]], inc: Incident) -> list[dict]:
    s0, s1 = inc.first_seen.timestamp(), inc.last_seen.timestamp()
    out = []
    for node in inc.nodes:
        rows = series.get(node, [])

        def agg(rs):
            if not rs:
                return None
            p95 = [r["p95_qtime"] for r in rs if r["p95_qtime"] is not None]
            heap = [r["heap_after_max_pct"] for r in rs if r["heap_after_max_pct"] is not None]
            return {"max_heap_after_gc_pct": max(heap) if heap else None,
                    "max_gc_pause_ms": max(r["gc_pause_max_ms"] for r in rs) if any(r["gc_records"] for r in rs) else None,
                    "max_gc_overhead_pct": max(r["gc_overhead_pct"] for r in rs),
                    "full_gcs": sum(r["full_gcs"] for r in rs), "max_p95_qtime_ms": max(p95) if p95 else None,
                    "select_requests": sum(r["requests"] for r in rs), "error_records": sum(r["errors"] for r in rs),
                    "http_5xx": sum(r["http_5xx"] for r in rs), "minutes": len(rs)}
        out.append({"id": f"M-{node}", "node": node,
                    "baseline_15min_before": agg([r for r in rows if s0 - 900 <= r["epoch"] < s0 - 60]),
                    "during_incident": agg([r for r in rows if s0 - 60 <= r["epoch"] <= s1])})
    return out


def build(inc: Incident, signals: dict[str, Signal], events_by_id, series, health: ClusterHealth | None,
          s: Settings, previous: Incident | None = None) -> dict:
    members = sorted((signals[i] for i in inc.signal_ids if i in signals), key=lambda x: x.start)
    root_first = sorted(members, key=lambda x: (x.signal_id != inc.root_signal_id, x.start))
    ids = []
    for m in sorted(members, key=lambda x: -SEVERITY_RANK[x.severity]):
        ids += m.event_ids[:60]
    events = events_by_id(sorted(set(ids)))
    records = representative_records(events, [(m.signal_id, m.event_ids[:60]) for m in root_first],
                                     s.llm_max_evidence_items, s.redact_query_terms)
    counts: dict = {}
    for m in members:
        if k := IMPACT_COUNTS.get(m.signal_type):
            counts[k] = counts.get(k, 0) + m.count
    lat = {m.node_id: m.metrics.get("peak_p95_ms") for m in members if m.signal_type == "LATENCY_DEGRADATION"}
    if lat:
        counts["peak_p95_latency_ms_by_node"] = lat
    pauses = [m.metrics.get("max_pause_ms", 0) for m in members if m.category == "gc" and m.metrics.get("max_pause_ms")]
    if pauses:
        counts["longest_gc_pause_ms"] = max(pauses)
    supported = sorted({IMPACT_STATEMENTS[m.signal_type] for m in members if m.signal_type in IMPACT_STATEMENTS})
    contexts = sorted({c for m in members for c in m.metrics.get("cores", [])})
    missing = list(LOG_ONLY_LIMITATIONS)
    if health:
        all_nodes = set(health.nodes)
        relevant = [r for r in health.partial_reasons
                    if not any(n in r for n in all_nodes) or any(n in r for n in inc.nodes)]
        missing = [f"{t.source_type} telemetry from {n.node_id} is stale" for n in health.nodes.values()
                   if n.node_id in inc.nodes for t in n.telemetry if t.stale] + relevant + missing
    hyps = []
    for h in inc.hypotheses[:3]:
        k = PATTERN_BY_ID[h.pattern_id]
        hyps.append({"pattern_id": h.pattern_id, "name": h.name, "match_score": h.score, "matched_signal_types": h.matched,
                     "contradicting_signal_types": h.contradicting, "mechanism": k["mechanism"],
                     "pattern_specific_alternatives": k["alternatives"]})
    runbook = {h.pattern_id: {"remediation": PATTERN_BY_ID[h.pattern_id]["remediation"],
                              "checks": PATTERN_BY_ID[h.pattern_id]["checks"]} for h in inc.hypotheses[:2]}
    pack = {
        "incident": {"id": inc.incident_id, "title": inc.title,
                     "policy_severity": inc.severity, "policy_severity_reason": inc.severity_reason,
                     "status": inc.status, "start": _iso(inc.first_seen), "end": _iso(inc.last_seen),
                     "duration_min": round((inc.last_seen - inc.first_seen).total_seconds() / 60, 1),
                     "affected_nodes": inc.nodes, "solr_context": contexts, "signal_count": len(members),
                     "rule_engine_root_candidate": inc.root_signal_id},
        "impact_summary": {"counts": counts, "supported_impact_statements": supported},
        "cluster_context": {
            "cluster_id": s.cluster_id,
            "nodes": sorted(health.nodes) if health else inc.nodes,
            "cluster_health_now": health.state if health else "unknown",
            "shards": {k: {"leader": v["leader"], "available_replicas": v["available_replicas"],
                           "total_replicas": v["total_replicas"]} for k, v in (health.shards if health else {}).items()},
            "jvm": {"collector": "G1"}, "solr": {"zkClientTimeout_ms": s.zk_client_timeout_ms},
            "thresholds": {"gc_pause_warn_ms": s.gc_pause_warn_ms, "heap_after_gc_warn_pct": s.heap_after_gc_warn_pct,
                           "slow_query_ms": s.slow_query_ms}},
        "signals": [{"id": m.signal_id, "start": _iso(m.start), "end": _iso(m.end), "node": m.node_id, "type": m.signal_type,
                     "severity": m.severity, "title": m.title, "detail": m.detail, "metrics": m.metrics, "count": m.count,
                     "detected_by": m.method, **({"target_node": m.target_node} if m.target_node else {}),
                     **({"root_candidate": True} if m.signal_id == inc.root_signal_id else {})} for m in members],
        "relationships": [{"cause": l.cause, "effect": l.effect, "relation": l.relation, "reason": l.reason} for l in inc.links],
        "metrics": metric_summaries(series, inc),
        "records": [{k: v for k, v in r.items() if k != "event_ids"} for r in records],
        "hypotheses": hyps,
        "alternative_explanations_to_consider": GENERIC_ALTERNATIVES,
        "runbook": runbook,
        "missing_telemetry_and_limitations": missing,
        "knowledge_version": KNOWLEDGE_VERSION,
    }
    if previous:
        pack["previous_occurrence"] = {"incident_id": previous.incident_id, "title": previous.title,
                                       "start": _iso(previous.first_seen), "end": _iso(previous.last_seen)}
    pack["_record_event_ids"] = {r["id"]: r["event_ids"] for r in records}  # stripped before sending; used to resolve E-ids
    return pack


def for_model(pack: dict) -> dict:
    return {k: v for k, v in pack.items() if not k.startswith("_")}


def valid_ids(pack: dict) -> set[str]:
    return ({x["id"] for x in pack["signals"]} | {x["id"] for x in pack["records"]} | {x["id"] for x in pack["metrics"]})


def evidence_hash(inc: Incident, signals: dict[str, Signal], prompt_version: str) -> str:
    """Changes when the incident's shape, severity or status changes, or the prompt/knowledge version changes -
    not on every new log line, so an open incident is not re-analysed on every poll."""
    key = sorted((i, signals[i].severity) for i in inc.signal_ids if i in signals)
    return hashlib.sha1(json.dumps([key, inc.severity, inc.status, prompt_version, KNOWLEDGE_VERSION]).encode(), usedforsecurity=False).hexdigest()[:16]
