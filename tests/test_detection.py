from datetime import datetime, timedelta, timezone

from app.correlation.engine import severity_policy
from app.detection.baselines import robust_flags
from app.detection.rules import telemetry_signals
from app.schemas import Signal


def test_expected_signals_detected(analysed):
    s, repo = analysed
    types = {(x.signal_type, x.node_id) for x in repo.signals(s.cluster_id)}
    for t in [("GC_PAUSE_EXCEEDS_ZK_TIMEOUT", "solr-node2"), ("ZK_SESSION_EXPIRED", "solr-node2"), ("LEADER_ELECTION", "solr-node1"),
              ("NO_LEADER", "solr-node3"), ("EXPENSIVE_QUERIES", "solr-node2"), ("GC_HEAP_PRESSURE", "solr-node2"),
              ("EXCESSIVE_COMMITS", "solr-node3"), ("SEARCHER_LIMIT", "solr-node3"), ("TELEMETRY_STALE", "solr-node3")]:
        assert t in types, t


def test_benign_node_has_no_gc_alerts(analysed):
    s, repo = analysed
    assert not [x for x in repo.signals(s.cluster_id) if x.node_id == "solr-node1" and x.category == "gc"]


def test_mad_zero_is_safe_and_needs_min_samples():
    flat = [10.0] * 10 + [10.5, 400.0]
    fl = robust_flags(flat, window=30, min_samples=5, k=3.5, floor=100)
    assert not fl[10][1] and fl[11][1]                     # tiny wiggle ignored, real jump flagged
    assert not any(f for _, f, _ in robust_flags([1.0, 500.0], 30, 5, 3.5, 100))   # not enough history


def _sig(t, sev, node="n1"):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return Signal(signal_id=f"S-{t[:8].lower():0<8}", cluster_id="c", node_id=node, signal_type=t, category="gc",
                  severity=sev, start=now, end=now, count=1, title=t, detail="")


def test_single_long_gc_pause_is_not_a_critical_incident():
    sev, reason = severity_policy([_sig("GC_LONG_PAUSE", "critical")])
    assert sev == "warning" and "capped" in reason
    assert severity_policy([_sig("NO_LEADER", "critical")])[0] == "critical"


def test_missing_telemetry_is_a_signal_not_node_down():
    now = datetime(2026, 1, 1, 11, tzinfo=timezone.utc)
    from app.config import Settings
    sig = telemetry_signals({("n3", "gc"): (now - timedelta(minutes=13)).timestamp(),
                             ("n3", "solr"): (now - timedelta(seconds=5)).timestamp()}, now, Settings())
    assert len(sig) == 1 and sig[0].signal_type == "TELEMETRY_STALE" and sig[0].severity == "warning"
