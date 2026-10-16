"""Production safeguards: upload safety, startup validation, model allowlist, RCA retry, readiness, logging."""
import json
import logging
import re
from pathlib import Path

import pytest
from conftest import make_settings
from fastapi.testclient import TestClient
from test_rca_validation import FakeClient, main_incident

from app.config import validate
from app.ingestion.reader import save_upload
from app.main import create_app
from app.observability import JsonFormatter
from app.rca.providers import LLMError
from app.rca.service import RCAService

H = {"X-API-Key": "k"}
GOOD_KEY = "a" * 32


def client(tmp_path, **kw):
    app = create_app(make_settings(tmp_path, api_key="k", **kw), start_background=False)
    return TestClient(app), app


# ------------------------------------------------------------------------------------------ uploads
@pytest.mark.parametrize("node_id", ["..", "../etc", "a/b", "a\\b", ".hidden", "", "x" * 65, "node..1"])
def test_upload_rejects_unsafe_node_ids(tmp_path, node_id):
    with pytest.raises(ValueError):
        save_upload(make_settings(tmp_path), node_id, "solr", "x.log", b"line\n")


def test_upload_stays_inside_the_upload_directory(tmp_path):
    s = make_settings(tmp_path)
    p = save_upload(s, "solr-node1", "solr", "../../evil.log", b"line\n")
    assert p.resolve().parent == (Path(s.upload_dir) / "solr-node1").resolve()
    assert ".." not in p.name


def test_api_rejects_path_escape_and_oversized_upload(tmp_path):
    c, _ = client(tmp_path, max_upload_mb=1)
    r = c.post("/api/v1/logs/upload", headers=H, files={"file": ("solr.log", b"x\n")},
               data={"node_id": "..", "source_type": "solr"})
    assert r.status_code == 400
    big = b"x" * (1024 * 1024 + 10)
    r = c.post("/api/v1/logs/upload", headers=H, files={"file": ("solr.log", big)},
               data={"node_id": "solr-node1", "source_type": "solr"})
    assert r.status_code == 413
    assert not any(Path(tmp_path, "uploads").rglob("*.log"))


# ------------------------------------------------------------------------------------------ startup validation
def test_production_mode_refuses_unsafe_settings(tmp_path):
    errors = validate(make_settings(tmp_path, env="production", api_key=""))
    assert any("SENTINEL_API_KEY is required" in e for e in errors)
    assert any("SENTINEL_CLOCK" in e for e in errors)
    assert any("placeholder" in e for e in validate(make_settings(tmp_path, env="production", api_key="change-me")))
    assert validate(make_settings(tmp_path, env="production", api_key=GOOD_KEY, clock="wall")) == []


def test_invalid_values_are_reported(tmp_path):
    errors = validate(make_settings(tmp_path, clock="sometimes", poll_interval_s=0, log_format="xml"))
    assert len(errors) == 3


def test_create_app_fails_fast_on_bad_configuration(tmp_path, monkeypatch):
    with pytest.raises(RuntimeError, match="SENTINEL_API_KEY"):
        create_app(make_settings(tmp_path, env="production"), start_background=False)
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    with pytest.raises(RuntimeError, match="development only"):
        create_app(make_settings(tmp_path, env="production", api_key=GOOD_KEY, clock="wall", llm_provider="gemini"),
                   start_background=False)


# ------------------------------------------------------------------------------------------ model allowlist + audit
def test_reanalyze_only_accepts_allowed_models(tmp_path, caplog):
    c, app = client(tmp_path, llm_allowed_models="approved-model")
    log = (b"2026-10-15 10:05:00.000 ERROR (q) [c:products s:shard1 r:core_node1 x:products_shard1_replica_n1 t:1] "
           b"o.a.s.u.DirectUpdateHandler2 java.io.IOException: No space left on device\n")
    c.post("/api/v1/logs/upload", headers=H, files={"file": ("solr.log", log)},
           data={"node_id": "solr-node1", "source_type": "solr"})
    iid = c.get("/api/v1/incidents", headers=H).json()[0]["incident_id"]
    with caplog.at_level(logging.INFO, logger="sentinel.audit"):
        r = c.post(f"/api/v1/incidents/{iid}/reanalyze", headers=H, json={"model": "some-expensive-model"})
        assert r.status_code == 400 and "not allowed" in r.json()["detail"]
        assert any("action=reanalyze_rejected" in m and "model_not_allowed" in m for m in caplog.messages)
        assert c.post(f"/api/v1/incidents/{iid}/reanalyze", headers=H, json={"model": "approved-model"}).status_code == 202
    line = next(m for m in caplog.messages if "action=reanalyze " in m)
    assert re.search(r"caller=key:[0-9a-f]{12} ", line) and "model=approved-model" in line   # a fingerprint, never the key


# ------------------------------------------------------------------------------------------ RCA retry with backoff
def test_provider_outage_is_retried_with_backoff_then_given_up(analysed):
    s, repo = analysed
    inc = main_incident(repo, s)
    signals = {x.signal_id: x for x in repo.signals(s.cluster_id)}
    svc = RCAService(repo, s, FakeClient([LLMError("HTTP 503")] * 10))
    s.rca_retry_base_s, s.rca_retry_max = 3600, 2
    svc.analyze(inc.incident_id)
    assert not svc.needs_rca(repo.incident(inc.incident_id), signals)          # too soon: wait for the backoff
    s.rca_retry_base_s = 0
    assert svc.needs_rca(repo.incident(inc.incident_id), signals)              # backoff elapsed: retry
    svc.analyze(inc.incident_id)
    svc.analyze(inc.incident_id)                                                # third consecutive failure
    assert not svc.needs_rca(repo.incident(inc.incident_id), signals)          # past rca_retry_max: give up


# ------------------------------------------------------------------------------------------ readiness + logging
def test_readiness_reports_503_when_analysis_fails(tmp_path):
    c, app = client(tmp_path)
    assert c.get("/readyz").status_code == 200
    app.state.ctx.poller_state["last_error"] = "database is locked"
    assert c.get("/readyz").status_code == 503
    assert c.get("/healthz").status_code == 200                                 # liveness stays up


def test_json_log_lines_are_valid_json():
    rec = logging.LogRecord("sentinel.test", logging.INFO, __file__, 1, "hello %s", ("world",), None)
    out = json.loads(JsonFormatter().format(rec))
    assert out["msg"] == "hello world" and out["level"] == "INFO" and out["logger"] == "sentinel.test"


# ------------------------------------------------------------------------------------------ bounded evidence
def test_evidence_package_caps_signals_and_keeps_ids_consistent(analysed):
    from app.rca import evidence
    s, repo = analysed
    inc = main_incident(repo, s)
    signals = {x.signal_id: x for x in repo.signals(s.cluster_id)}
    s.llm_max_signals = 3
    pack = evidence.for_model(evidence.build(inc, signals, repo.events_by_ids, repo.get("series"), None, s))
    shown = {x["id"] for x in pack["signals"]}
    assert len(shown) == 3 and inc.root_signal_id in shown
    assert pack["incident"]["signals_omitted"] == len(inc.signal_ids) - 3
    assert all(r["cause"] in shown and r["effect"] in shown for r in pack["relationships"])
    assert all(set(r["signals"]) <= shown for r in pack["records"])   # the model is never shown an ID it may not cite
