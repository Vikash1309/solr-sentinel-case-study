"""Event correlation and incident grouping (doc §6).

1. Signals are scoped by cluster and node; collection/shard/core context and URLs referenced in the
   records (e.g. the peer a request timed out against) are used when available.
2. A causal rule graph proposes cause -> effect links. Each link records HOW it is supported:
     observed  - the records themselves reference the relationship (a timeout names the paused node,
                 a GC pause is itself the pause that exceeded the ZooKeeper timeout, a leader change
                 follows a live-node removal reported by ZooKeeper)
     inferred  - a known mechanism plus consistent ordering and timing (heap pressure -> full GC)
     temporal  - time proximity only (latency rising while something else happens)
   Correlation is not causation (doc §6.4): links are hypotheses, ranked, with alternatives kept.
3. Connected signals form one incident; overlapping candidates merge naturally; the incident identity
   is stable (root type + node + 10-minute bucket), so re-analysis and replay update the same incident.
4. Root candidate = earliest signal without an incoming link. Patterns rank hypotheses; contradicting
   signal types are recorded.
5. Severity is decided here by deterministic policy (the LLM explains it but never overrides it).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from ..config import SEVERITY_RANK, Settings
from ..schemas import CausalLink, Hypothesis, Incident, Signal
from .knowledge import PATTERN_BY_ID, match_patterns

GC_STALL = {"GC_FULL", "GC_LONG_PAUSE", "GC_PAUSE_EXCEEDS_ZK_TIMEOUT"}
GC_PRESSURE = {"GC_HEAP_PRESSURE", "GC_OVERHEAD", "GC_HUMONGOUS"}
ZK_LOSS = {"ZK_SESSION_EXPIRED", "ZK_DISCONNECTED"}
AVAILABILITY_IMPACT = {"NO_LEADER", "NODE_LOST", "SOLR_OOM", "DISK_FULL", "RECOVERY_FAILED", "ZK_SESSION_EXPIRED",
                       "GC_PAUSE_EXCEEDS_ZK_TIMEOUT", "TOO_MANY_OPEN_FILES"}

# (cause types, effect types, scope, max lag after cause end [s], tolerance for effect-before-cause [s], relation, reason)
CAUSAL_RULES = [
    ({"EXPENSIVE_QUERIES"}, GC_PRESSURE | {"GC_EVAC_FAILURE", "CIRCUIT_BREAKER"}, "same", 300, 120, "inferred",
     "large result sets / unbounded facets allocate humongous objects"),
    ({"EXPENSIVE_QUERIES"}, {"LATENCY_DEGRADATION"}, "same", 300, 120, "temporal", "slow expensive requests raise p95 latency"),
    ({"GC_HUMONGOUS"}, {"GC_HEAP_PRESSURE", "GC_EVAC_FAILURE", "GC_FULL"}, "same", 600, 60, "inferred",
     "humongous objects are allocated straight into the old generation"),
    ({"GC_HEAP_PRESSURE"}, {"GC_EVAC_FAILURE", "GC_FULL", "GC_OVERHEAD", "GC_LONG_PAUSE", "CIRCUIT_BREAKER", "SOLR_OOM"},
     "same", 600, 60, "inferred", "no free regions left for evacuation"),
    ({"GC_EVAC_FAILURE"}, {"GC_FULL", "GC_LONG_PAUSE"}, "same", 300, 5, "observed",
     "evacuation failure followed by a full compaction in the same GC log"),
    ({"GC_FULL", "GC_LONG_PAUSE"}, {"GC_PAUSE_EXCEEDS_ZK_TIMEOUT"}, "same", 5, 5, "observed",
     "the same pause exceeded zkClientTimeout"),
    (GC_STALL | {"GC_OVERHEAD"}, {"LATENCY_DEGRADATION", "SLOW_QUERIES"}, "same", 300, 60, "temporal",
     "stop-the-world pauses stall request threads"),
    (GC_STALL, ZK_LOSS, "same", 120, 30, "inferred", "a stop-the-world pause longer than the session timeout blocks heartbeats"),
    (ZK_LOSS, {"NODE_LOST", "LEADER_ELECTION", "NO_LEADER"}, "cross", 180, 30, "observed",
     "ZooKeeper removed the live node; peers report the change"),
    (ZK_LOSS, {"REPLICA_RECOVERY"}, "same", 300, 5, "observed", "replicas recover after the session is re-established"),
    ({"NODE_LOST"}, {"LEADER_ELECTION", "NO_LEADER"}, "any", 120, 10, "observed", "a lost node triggers leader election"),
    (GC_STALL | {"GC_OVERHEAD"}, {"REMOTE_TIMEOUT"}, "target", 300, 30, "observed",
     "peers' requests to the paused node timed out (target host in the error)"),
    ({"REMOTE_TIMEOUT", "NO_LEADER"}, {"LATENCY_DEGRADATION", "ERROR_RATE_SPIKE"}, "same", 300, 60, "temporal",
     "failed sub-requests surface as errors and latency"),
    ({"REPLICA_RECOVERY"}, {"RECOVERY_FAILED"}, "same", 1800, 5, "observed", "the recovery attempt failed"),
    ({"EXCESSIVE_COMMITS"}, {"OVERLAPPING_SEARCHERS", "SEARCHER_LIMIT"}, "same", 600, 60, "observed",
     "explicit commits open searchers on the same core"),
    ({"EXCESSIVE_COMMITS"}, {"LATENCY_DEGRADATION", "SLOW_QUERIES", "GC_OVERHEAD"}, "same", 600, 60, "inferred",
     "each commit discards caches and warms a new searcher"),
    ({"OVERLAPPING_SEARCHERS"}, {"SEARCHER_LIMIT"}, "same", 600, 30, "observed", "warming searchers reach the limit"),
    ({"OVERLAPPING_SEARCHERS", "SEARCHER_LIMIT"}, {"LATENCY_DEGRADATION", "SLOW_QUERIES"}, "same", 300, 60, "temporal",
     "caches discarded on every new searcher"),
    ({"SOLR_OOM", "DISK_FULL", "TOO_MANY_OPEN_FILES"}, {"ERROR_RATE_SPIKE", "NODE_LOST", "REMOTE_TIMEOUT"}, "any", 600, 10,
     "inferred", "resource exhaustion breaks request handling"),
]


def _scope_ok(scope: str, c: Signal, e: Signal) -> bool:
    return {"same": c.node_id == e.node_id, "cross": c.node_id != e.node_id,
            "target": e.target_node == c.node_id, "any": True}[scope]


def build_links(signals: list[Signal]) -> list[CausalLink]:
    links = []
    for c in signals:
        for e in signals:
            if c.signal_id == e.signal_id:
                continue
            for causes, effects, scope, lag, tol, rel, why in CAUSAL_RULES:
                if c.signal_type in causes and e.signal_type in effects and _scope_ok(scope, c, e) and \
                        c.start - timedelta(seconds=tol) <= e.start <= c.end + timedelta(seconds=lag):
                    links.append(CausalLink(cause=c.signal_id, effect=e.signal_id, relation=rel, reason=why))
                    break
    return links


def _components(ids: list[str], links: list[CausalLink]) -> list[set[str]]:
    parent = {i: i for i in ids}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for l in links:
        parent[find(l.cause)] = find(l.effect)
    groups: dict[str, set] = {}
    for i in ids:
        groups.setdefault(find(i), set()).add(i)
    return list(groups.values())


def severity_policy(members: list[Signal]) -> tuple[str, str]:
    """Deterministic incident severity (doc §5.1, §7.6). A single non-availability signal is never critical."""
    worst = max(members, key=lambda s: (SEVERITY_RANK[s.severity], s.method != "baseline", -s.start.timestamp()))
    if worst.severity == "critical":
        crit = [s for s in members if s.severity == "critical"]
        avail = [s for s in crit if s.signal_type in AVAILABILITY_IMPACT]
        if avail:
            return "critical", f"availability-impacting signal: {avail[0].title} on {avail[0].node_id}"
        if len(members) >= 2:
            return "critical", f"critical signal ({worst.title}) with {len(members) - 1} correlated signal(s)"
        return "warning", f"single critical-level signal without availability impact ({worst.title}) - capped at warning"
    if worst.severity == "warning":
        return "warning", f"highest signal severity: warning ({worst.title})"
    return "info", "informational signals only"


def correlate(signals: list[Signal], now: datetime, s: Settings, active_window_s: int = 300) -> list[Incident]:
    by_id = {x.signal_id: x for x in signals}
    links = build_links(signals)
    incidents: list[Incident] = []
    for comp in _components(list(by_id), links):
        members = sorted((by_id[i] for i in comp), key=lambda x: (x.start, -SEVERITY_RANK[x.severity]))
        if len(members) == 1 and members[0].severity == "info":
            continue
        clinks = [l for l in links if l.cause in comp and l.effect in comp]
        has_cause = {l.effect for l in clinks}
        roots = [m for m in members if m.signal_id not in has_cause] or members
        root = roots[0]
        hyps = [Hypothesis(**h) for h in match_patterns({m.signal_type for m in members})]
        sev, reason = severity_policy(members)
        title = (f"{PATTERN_BY_ID[hyps[0].pattern_id]['short']} ({root.node_id})" if hyps
                 else f"{root.title} on {root.node_id}" + (f" (+{len(members) - 1} related)" if len(members) > 1 else ""))
        fp = hashlib.sha1(f"{s.cluster_id}|{root.signal_type}|{root.node_id}|{int(root.start.timestamp()) // 600}".encode(), usedforsecurity=False).hexdigest()[:12]
        last = max(m.end for m in members)
        incidents.append(Incident(
            incident_id="INC-" + fp[:8].upper(), fingerprint=fp, cluster_id=s.cluster_id, title=title, severity=sev,
            severity_reason=reason, status="open" if (now - last).total_seconds() <= active_window_s else "resolved",
            first_seen=members[0].start, last_seen=last, nodes=sorted({m.node_id for m in members}),
            root_signal_id=root.signal_id, signal_ids=[m.signal_id for m in members], links=clinks, hypotheses=hyps))
    chrono = sorted(incidents, key=lambda i: i.first_seen)
    for idx, inc in enumerate(chrono):
        r = by_id[inc.root_signal_id]
        for prev in reversed(chrono[:idx]):
            pr = by_id[prev.root_signal_id]
            if pr.signal_type == r.signal_type and pr.node_id == r.node_id and \
                    (inc.first_seen - prev.last_seen).total_seconds() <= s.recurrence_window_h * 3600:
                inc.recurrence_of = prev.incident_id
                inc.title = "Recurring: " + inc.title
                break
    return incidents
