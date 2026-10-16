"""Solr Sentinel command line.

    python run.py demo        generate sample logs if missing, start the API (:8000) and the Streamlit dashboard (:8501)
    python run.py api         start the API only (background poller + RCA workers)
    python run.py dashboard   start the Streamlit dashboard only (expects the API)
    python run.py process     one synchronous ingest + analysis + RCA run, printed to the console
    python run.py simulate    empty DB, then append 5 minutes of logs every 10 s while API + dashboard run
    python run.py generate    (re)write the synthetic sample logs
    python run.py eval        evaluate detection (full, control, isolated, threshold sensitivity) and RCA providers
    python run.py reset       delete the local database and uploads
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def _settings():
    from app.config import load_settings
    return load_settings()


def _reset(s):
    for suffix in ("", "-wal", "-shm"):
        p = Path(s.db_path + suffix)
        if p.exists():
            p.unlink()
    shutil.rmtree(s.upload_dir, ignore_errors=True)


def _generate(s, simulate=False):
    cmd = [sys.executable, str(ROOT / "scripts" / "generate_synthetic_logs.py"), "--out", s.log_dir]
    if simulate:
        return subprocess.Popen(cmd + ["--simulate", "--batch-minutes", "5", "--interval", "10"])
    subprocess.run(cmd, check=True)


def _api_thread(host, port):
    import uvicorn

    from app.main import create_app
    server = uvicorn.Server(uvicorn.Config(create_app(), host=host, port=port, log_level="warning"))
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.1)
    print(f"API:        http://{host}:{port}/docs")
    return server


def _dashboard(port, host):
    env = dict(os.environ, SENTINEL_API_URL=os.getenv("SENTINEL_API_URL", "http://127.0.0.1:8000"))
    print(f"Dashboard:  http://localhost:{port}")
    return subprocess.call([sys.executable, "-m", "streamlit", "run", str(ROOT / "dashboard" / "app.py"),
                            "--server.port", str(port), "--server.headless", "true",
                            "--browser.gatherUsageStats", "false"], env=env)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["demo", "api", "dashboard", "process", "simulate", "generate", "eval", "reset"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--ui-port", type=int, default=8501)
    ap.add_argument("--models", default="rules,auto", help="eval: comma-separated provider[:model] list; 'rules' = no LLM")
    ap.add_argument("--seeds", default="1,2,3,4,5", help="eval: random seeds for the detection evaluation")
    ap.add_argument("--repeats", type=int, default=1, help="eval: analyse each incident N times per LLM (consistency)")
    ap.add_argument("--injection", action="store_true", help="eval: run the prompt-injection test against each LLM")
    ap.add_argument("--no-sensitivity", action="store_true", help="eval: skip the threshold sensitivity analysis")
    a = ap.parse_args()
    s = _settings()
    from app.observability import configure_logging
    configure_logging(s.log_level, s.log_format)

    if a.command == "reset":
        _reset(s)
        print("database and uploads removed")
    elif a.command == "generate":
        _generate(s)
    elif a.command == "eval":
        from app import evaluation
        evaluation.run([m.strip() for m in a.models.split(",") if m.strip()], [int(x) for x in a.seeds.split(",")], s,
                       repeats=max(1, a.repeats), injection=a.injection, sensitivity=not a.no_sensitivity)
        print((ROOT / "docs" / "EVALUATION.md").read_text(encoding="utf-8"))
    elif a.command == "process":
        from app.pipeline import run_analysis
        from app.rca.service import RCAService
        from app.schemas import ClusterHealth
        from app.storage.repository import Repository
        repo = Repository(s.db_path)
        rca = RCAService(repo, s)
        ov = run_analysis(repo, s, rca, sync_rca=True)
        if ov.get("empty"):
            print("No data. Run `python run.py generate` first.")
            return
        ch = ClusterHealth.model_validate(repo.get("cluster_health"))
        print(f"\nCluster {ch.state.upper()}{' (partial)' if ch.partial else ''} at {ov['assessed_at'][11:19]} UTC; "
              f"{ov['events_total']} events, {ov['parse_errors']} parse errors, LLM {ov['llm']}")
        for n in ch.nodes.values():
            print(f"  {n.node_id:<12} {n.state:<9} score={n.score}  {n.reasons[0][:90] if n.reasons else ''}")
        print()
        for i in repo.incidents(s.cluster_id):
            rec = repo.rca(i.incident_id)
            print(f"  [{i.severity:<8}] {i.incident_id} {i.status:<8} {i.first_seen:%H:%M}-{i.last_seen:%H:%M} {i.title}")
            if rec and rec.report:
                print(f"      RCA v{rec.version} ({rec.source}{', ' + rec.reason if rec.reason else ''}, "
                      f"confidence {rec.report.probable_root_cause.confidence})")
    elif a.command == "api":
        import uvicorn
        uvicorn.run("app.main:create_app", factory=True, host=a.host, port=a.port, log_level="info")
    elif a.command == "dashboard":
        sys.exit(_dashboard(a.ui_port, a.host))
    elif a.command in ("demo", "simulate"):
        gen = None
        if a.command == "simulate":
            _reset(s)
            gen = _generate(s, simulate=True)
        elif not any(Path(s.log_dir).glob("*/*.log")):
            _generate(s)
        _api_thread(a.host, a.port)
        try:
            _dashboard(a.ui_port, a.host)
        finally:
            if gen:
                gen.terminate()


if __name__ == "__main__":
    main()
