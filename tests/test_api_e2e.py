"""End-to-end: upload -> parse -> detect -> correlate -> persist -> API (doc §16), plus auth and rate limits."""
from conftest import make_settings
from fastapi.testclient import TestClient

from app.main import create_app

H = {"X-API-Key": "k"}


def client(tmp_path, **kw):
    s = make_settings(tmp_path, api_key="k", **kw)
    app = create_app(s, start_background=False)
    return TestClient(app), app


def test_auth_required(tmp_path):
    c, _ = client(tmp_path)
    assert c.get("/api/v1/overview").status_code == 401
    assert c.get("/api/v1/overview", headers={"X-API-Key": "wrong"}).status_code == 401
    assert c.get("/healthz").status_code == 200


def test_empty_state_then_upload_end_to_end(tmp_path):
    c, app = client(tmp_path)
    assert c.get("/api/v1/overview", headers=H).json()["empty"] is True
    log = (b"2026-10-15 10:00:00.000 INFO  (q) [   ] o.a.s.c.X started\n"
           b"2026-10-15 10:05:00.000 ERROR (q) [c:products s:shard1 r:core_node1 x:products_shard1_replica_n1 t:1] "
           b"o.a.s.u.DirectUpdateHandler2 java.io.IOException: No space left on device\n"
           b"garbage line without timestamp\n")
    r = c.post("/api/v1/logs/upload", headers=H, files={"file": ("solr.log", log)},
               data={"node_id": "solr-node1", "source_type": "solr"})
    assert r.status_code == 200 and r.json()["analysis"]["incidents"] == 1
    inc = c.get("/api/v1/incidents", headers=H).json()[0]
    assert inc["severity"] == "critical" and inc["rca"]["status"] == "pending"          # visible before any RCA
    assert c.get("/api/v1/parse-errors", headers=H).json()["total"] == 1
    app.state.ctx.rca.analyze(inc["incident_id"])
    d = c.get(f"/api/v1/incidents/{inc['incident_id']}", headers=H).json()
    assert d["rca"]["source"] == "rules" and d["rca"]["report"]["operational_impact"]["severity"] == "critical"
    assert c.post("/api/v1/logs/upload", headers=H, files={"file": ("x.log", b"x")},
                  data={"node_id": "n", "source_type": "bogus"}).status_code == 400


def test_health_filters_metrics_and_reanalyze_rate_limit(tmp_path, synthetic):
    from pathlib import Path

    from conftest import age_files
    from generate_synthetic_logs import write_all
    c, app = client(tmp_path, reanalyze_per_minute=2)
    s = app.state.ctx.settings
    write_all(s.log_dir, synthetic)
    age_files(Path(s.log_dir))
    assert c.post("/api/v1/analysis/run", headers=H).json()["incidents"] == 4
    assert c.get("/api/v1/health/cluster", headers=H).json()["state"] == "degraded"
    states = {n["node_id"]: n["state"] for n in c.get("/api/v1/health/nodes", headers=H).json()}
    assert states["solr-node3"] == "unknown"
    crit = c.get("/api/v1/incidents?severity=critical", headers=H).json()
    assert len(crit) == 1 and len(c.get("/api/v1/incidents?node=solr-node1", headers=H).json()) == 1
    iid = crit[0]["incident_id"]
    assert c.post(f"/api/v1/incidents/{iid}/reanalyze?sync=true", headers=H).json()["version"] == 1
    assert c.post(f"/api/v1/incidents/{iid}/reanalyze?sync=true", headers=H).json()["version"] == 2
    assert c.post(f"/api/v1/incidents/{iid}/reanalyze?sync=true", headers=H).status_code == 429
    assert "sentinel_events_ingested_total" in c.get("/api/v1/metrics", headers=H).text
    sig = c.get(f"/api/v1/incidents/{iid}", headers=H).json()["signals"][0]["signal_id"]
    assert c.get(f"/api/v1/signals/{sig}/events", headers=H).status_code == 200
