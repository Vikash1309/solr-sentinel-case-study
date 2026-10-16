"""Node and cluster health (doc §5.3).

Node health (per minute and current)
    score = 100 - worst active signal (warning 15 / critical 45) - 5 per additional active signal (max 10)
                - 1.2 per % of heap-after-GC above 80 - 1 per % of GC overhead above 5 - 1 per HTTP 5xx (max 15)
    healthy >= 80, degraded >= 50, critical < 50
    UNKNOWN when telemetry is stale: if any source of a node (solr / gc) is stale the node is reported as
    unknown unless the remaining evidence already shows it degraded/critical. Missing telemetry is never
    reported as healthy, and never as "down" either.

Cluster health is NOT an average of node scores. It is derived from shard/replica availability,
leader state, redundancy and the number of unknown nodes:
    critical  a shard has no available replica, a shard had no leader in the last 5 min, or most nodes are critical
    degraded  any node degraded/critical/unknown, any replica not active, or reduced redundancy
    healthy   otherwise
    unknown   all nodes unknown
The assessment is flagged `partial` when topology or telemetry is incomplete (e.g. a shard leader was never
observed in the logs because no Solr CLUSTERSTATUS / metrics source is connected).
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from ..config import Settings
from ..schemas import ClusterHealth, HealthState, LogEvent, NodeHealth, Signal, SourceTelemetry

PENALTY = {"info": 0, "warning": 15, "critical": 45}
LINGER_S = 120
RANK = {"healthy": 0, "unknown": 1, "degraded": 2, "critical": 3}


def _state(score: float) -> HealthState:
    return "healthy" if score >= 80 else "degraded" if score >= 50 else "critical"


def _score(row: dict, active: list[Signal]) -> int:
    pen: float = (max(PENALTY[a.severity] for a in active) + min(10, 5 * (len(active) - 1))) if active else 0
    pen += max(0.0, (row.get("heap_after_max_pct") or 0) - 80) * 1.2
    pen += max(0.0, row.get("gc_overhead_pct", 0) - 5)
    pen += min(15, row.get("http_5xx", 0))
    return max(0, round(100 - pen))


def node_timeline(series: dict[str, list[dict]], signals: list[Signal], s: Settings) -> dict[str, list[dict]]:
    out = {}
    win = max(1, s.telemetry_stale_after_s // s.metric_window_s)
    for node, rows in series.items():
        ns = [x for x in signals if x.node_id == node and x.signal_type != "TELEMETRY_STALE"]
        seen = {"solr": False, "gc": False}
        tl = []
        for i, r in enumerate(rows):
            b0, b1 = r["epoch"], r["epoch"] + s.metric_window_s
            for src in seen:
                seen[src] = seen[src] or r[f"{src}_records"] > 0
            recent = rows[max(0, i - win + 1): i + 1]
            stale = [src for src in seen if seen[src] and sum(x[f"{src}_records"] for x in recent) == 0]
            active = [a for a in ns if a.start.timestamp() < b1 and a.end.timestamp() + LINGER_S >= b0]
            score = _score(r, active)
            state = _state(score)
            if stale and state == "healthy":
                state = "unknown"
            tl.append({"t": r["t"], "epoch": b0, "score": score, "state": state, "stale_sources": stale,
                       "active_signals": sorted({a.signal_type for a in active})})
        out[node] = tl
    return out


def shard_topology(events: list[LogEvent]) -> dict[str, dict]:
    shards: dict[str, dict] = {}
    for e in events:
        a = e.attributes
        if e.source_type != "solr" or not a.get("shard") or not a.get("collection"):
            continue
        sh = shards.setdefault(f"{a['collection']}/{a['shard']}", {"replicas": {}, "leader": None, "leader_changed_at": None})
        if a.get("core"):
            sh["replicas"][a["core"]] = e.node_id
        if re.search(r"I am the new leader: \S+ (\S+)", e.message):
            sh["leader"], sh["leader_changed_at"] = e.node_id, e.timestamp.isoformat()
    return shards


def replica_states(events: list[LogEvent]) -> dict[str, str]:
    state: dict[str, str] = {}
    cores: dict[str, set] = {}
    for e in events:
        if e.attributes.get("core"):
            cores.setdefault(e.node_id, set()).add(e.attributes["core"])
    for e in events:
        if e.source_type != "solr":
            continue
        if "publishing all cores" in e.message and "DOWN" in e.message:
            for c in cores.get(e.node_id, ()):
                state[c] = "down"
        core = e.attributes.get("core")
        if core:
            if "Starting recovery process" in e.message:
                state[core] = "recovering"
            elif "Finished recovery process, successful=[true]" in e.message:
                state[core] = "active"
            elif "successful=[false]" in e.message:
                state[core] = "recovery_failed"
    return state


def _telemetry(node: str, last_seen: dict[tuple[str, str], float], now: datetime,
               s: Settings) -> tuple[list[SourceTelemetry], list[str]]:
    telemetry, stale = [], []
    for src in ("solr", "gc"):
        last = last_seen.get((node, src))
        age = None if last is None else round(now.timestamp() - last, 1)
        is_stale = age is None or age > s.telemetry_stale_after_s
        telemetry.append(SourceTelemetry(source_type=src, age_s=age, stale=is_stale,
                                         last_seen=None if last is None else datetime.fromtimestamp(last, tz=timezone.utc)))
        if is_stale:
            stale.append(src)
    return telemetry, stale


def _node_state(known: dict[str, Any], stale: list[str]) -> tuple[HealthState, int | None, str | None]:
    """Missing telemetry makes a node Unknown, never healthy. A known bad state (and its score) is kept; an
    Unknown node gets no score, because a score from partial telemetry would look like a clean bill of health."""
    if len(stale) == 2:
        return "unknown", None, "no telemetry from any source within the freshness window"
    if stale:
        reason = f"{', '.join(stale)} telemetry stale - health cannot be fully assessed"
        if known["state"] in ("degraded", "critical"):
            return known["state"], known["score"], reason
        return "unknown", None, reason
    return known["state"], known["score"], None


def _node_health(node: str, rows: list[dict], series: dict[str, list[dict]], signals: list[Signal], topo: dict,
                 rstate: dict[str, str], telemetry: list[SourceTelemetry], stale: list[str], recent: datetime) -> NodeHealth:
    last5 = series[node][-5:]
    open_sigs = [x for x in signals if x.node_id == node and x.end >= recent]
    known: dict[str, Any] = rows[-1] if rows else {"score": None, "state": "unknown"}
    reasons = [f"{x.title}: {x.detail}" for x in sorted(open_sigs, key=lambda x: -PENALTY[x.severity])][:5]
    state, score, stale_reason = _node_state(known, stale)
    if stale_reason:
        reasons.insert(0, stale_reason)
    heap = next((r["heap_after_max_pct"] for r in reversed(series[node]) if r.get("heap_after_max_pct") is not None), None)
    p95s = [r["p95_qtime"] for r in last5 if r["p95_qtime"] is not None]
    no_gc, no_solr = "gc" in stale, "solr" in stale
    return NodeHealth(
        node_id=node, state=state, score=score, reasons=reasons, telemetry=telemetry,
        heap_after_gc_pct=None if no_gc else heap,
        max_gc_pause_ms_5m=None if no_gc else max((r["gc_pause_max_ms"] for r in last5), default=None),
        gc_overhead_pct_5m=None if no_gc else round(sum(r["gc_overhead_pct"] for r in last5) / max(1, len(last5)), 1),
        p95_qtime_ms_5m=None if no_solr else (max(p95s) if p95s else None),
        errors_5m=None if no_solr else sum(r["errors"] for r in last5),
        requests_5m=None if no_solr else sum(r["requests"] for r in last5),
        leader_of=sorted(k for k, v in topo.items() if v["leader"] == node),
        replicas={c: rstate.get(c, "active") for v in topo.values() for c, n in v["replicas"].items() if n == node},
        open_signals=[{"signal_id": x.signal_id, "type": x.signal_type, "severity": x.severity} for x in open_sigs])


def _shard_view(topo: dict, rstate: dict[str, str], nodes: dict[str, NodeHealth]) -> tuple[dict, list[str], list[str]]:
    """Replica availability and redundancy per shard (cluster health is not an average of node scores)."""
    view, reasons, partial = {}, [], []
    for key, sh in sorted(topo.items()):
        avail = [c for c, n in sh["replicas"].items()
                 if rstate.get(c, "active") == "active" and nodes.get(n) and nodes[n].state != "critical"]
        view[key] = {"leader": sh["leader"], "leader_changed_at": sh["leader_changed_at"],
                     "replicas": {c: {"node": n, "state": rstate.get(c, "active")} for c, n in sh["replicas"].items()},
                     "available_replicas": len(avail), "total_replicas": len(sh["replicas"])}
        if not avail:
            reasons.append(f"{key}: no available replica")
        elif len(avail) < len(sh["replicas"]):
            reasons.append(f"{key}: reduced redundancy ({len(avail)}/{len(sh['replicas'])} replicas available)")
        if sh["leader"] is None:
            partial.append(f"{key}: leader not observed in logs (no CLUSTERSTATUS source connected)")
    return view, reasons, partial


def _cluster_state(states: list[str], shard_view: dict, no_leader_recent: bool) -> HealthState:
    if states and all(st == "unknown" for st in states):
        return "unknown"
    if any(v["available_replicas"] == 0 for v in shard_view.values()) or no_leader_recent or             sum(st == "critical" for st in states) > len(states) / 2:
        return "critical"
    if any(st in ("degraded", "critical", "unknown") for st in states) or             any(v["available_replicas"] < v["total_replicas"] for v in shard_view.values()):
        return "degraded"
    return "healthy"


def _cluster_timeline(tl: dict[str, list[dict]], signals: list[Signal]) -> list[dict]:
    """Per-minute cluster state for the dashboard."""
    if not tl:
        return []
    first = next(iter(tl.values()))
    no_leader = [x for x in signals if x.signal_type == "NO_LEADER"]
    out = []
    for i, row in enumerate(first):
        row_states = [tl[nd][i]["state"] for nd in tl if i < len(tl[nd])]
        ep = row["epoch"]
        if "critical" in row_states or any(x.start.timestamp() < ep + 60 and x.end.timestamp() + LINGER_S >= ep for x in no_leader):
            st = "critical"
        elif any(r in ("degraded", "unknown") for r in row_states):
            st = "degraded"
        else:
            st = "healthy"
        out.append({"t": row["t"], "epoch": ep, "state": st})
    return out


def assess(series: dict[str, list[dict]], signals: list[Signal], events: list[LogEvent],
           last_seen: dict[tuple[str, str], float], now: datetime, s: Settings) -> tuple[ClusterHealth, dict]:
    tl = node_timeline(series, signals, s)
    topo, rstate = shard_topology(events), replica_states(events)
    recent = now - timedelta(minutes=5)
    nodes: dict[str, NodeHealth] = {}
    for node, rows in tl.items():
        telemetry, stale = _telemetry(node, last_seen, now, s)
        nodes[node] = _node_health(node, rows, series, signals, topo, rstate, telemetry, stale, recent)

    shard_view, reasons, partial = _shard_view(topo, rstate, nodes)
    no_leader_recent = any(x.signal_type == "NO_LEADER" and x.end >= recent for x in signals)
    unknown = [n.node_id for n in nodes.values() if n.state == "unknown"]
    if unknown:
        partial.append(f"telemetry incomplete for {', '.join(unknown)}")
    cstate = _cluster_state([n.state for n in nodes.values()], shard_view, no_leader_recent)
    for n in sorted(nodes.values(), key=lambda n: -RANK[n.state]):
        if n.state != "healthy":
            reasons.append(f"{n.node_id} is {n.state}" + (f" (score {n.score})" if n.score is not None else ""))
    if no_leader_recent:
        reasons.insert(0, "a shard had no registered leader in the last 5 minutes")
    ch = ClusterHealth(cluster_id=s.cluster_id, state=cstate, partial=bool(partial), partial_reasons=partial,
                       reasons=reasons, assessed_at=now, nodes=nodes, shards=shard_view)
    return ch, {"nodes": tl, "cluster": _cluster_timeline(tl, signals)}
