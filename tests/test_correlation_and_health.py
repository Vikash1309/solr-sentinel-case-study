from app.pipeline import run_analysis
from app.schemas import ClusterHealth


def _by_pattern(repo, s, pid):
    return next(i for i in repo.incidents(s.cluster_id) if i.hypotheses and i.hypotheses[0].pattern_id == pid)


def test_incident_grouping_and_root_cause(analysed):
    s, repo = analysed
    sig = {x.signal_id: x for x in repo.signals(s.cluster_id)}
    incs = repo.incidents(s.cluster_id)
    assert len(incs) == 4
    main = _by_pattern(repo, s, "HEAP_EXHAUSTION_GC_STALL_ZK_EXPIRY")
    assert sig[main.root_signal_id].signal_type == "EXPENSIVE_QUERIES" and main.severity == "critical"
    assert set(main.nodes) == {"solr-node1", "solr-node2", "solr-node3"}
    storm = _by_pattern(repo, s, "COMMIT_STORM")
    assert storm.nodes == ["solr-node3"] and not set(storm.signal_ids) & set(main.signal_ids)   # unrelated events not merged


def test_relationships_are_labelled(analysed):
    s, repo = analysed
    sig = {x.signal_id: x for x in repo.signals(s.cluster_id)}
    main = _by_pattern(repo, s, "HEAP_EXHAUSTION_GC_STALL_ZK_EXPIRY")
    timeouts = [l for l in main.links if sig[l.effect].signal_type == "REMOTE_TIMEOUT"]
    assert timeouts and all(l.relation == "observed" and sig[l.cause].node_id == "solr-node2" for l in timeouts)
    assert {l.relation for l in main.links} == {"observed", "inferred", "temporal"}


def test_recurrence_and_stable_identity(analysed):
    s, repo = analysed
    rec = next(i for i in repo.incidents(s.cluster_id) if i.recurrence_of)
    assert rec.status == "open" and rec.recurrence_of == _by_pattern(repo, s, "HEAP_EXHAUSTION_GC_STALL_ZK_EXPIRY").incident_id
    ids = {i.incident_id for i in repo.incidents(s.cluster_id)}
    run_analysis(repo, s, None)
    assert {i.incident_id for i in repo.incidents(s.cluster_id)} == ids


def test_stale_telemetry_is_unknown_not_healthy(analysed):
    s, repo = analysed
    ch = ClusterHealth.model_validate(repo.get("cluster_health"))
    n3 = ch.nodes["solr-node3"]
    assert n3.state == "unknown" and n3.heap_after_gc_pct is None          # no GC claim without GC telemetry
    assert ch.nodes["solr-node1"].state == "healthy"


def test_cluster_health_is_not_an_average_and_is_partial(analysed):
    s, repo = analysed
    ch = ClusterHealth.model_validate(repo.get("cluster_health"))
    assert ch.state == "degraded" and ch.partial
    assert any("leader not observed" in r for r in ch.partial_reasons)
    assert ch.shards["products/shard1"]["leader"] == "solr-node1"
    tl = repo.get("timeline")["cluster"]
    assert any(c["state"] == "critical" for c in tl if c["t"][11:16] == "10:38")   # no-leader window is critical
