import os
import time
from datetime import timedelta
from pathlib import Path

from conftest import age_files, make_settings
from generate_synthetic_logs import START, write_all

from app.ingestion.reader import ingest, save_upload
from app.pipeline import run_analysis
from app.storage.repository import Repository


def test_incremental_ingestion_is_exactly_once(tmp_path, synthetic):
    s = make_settings(tmp_path)
    repo = Repository(s.db_path)
    write_all(s.log_dir, synthetic, until=START + timedelta(minutes=20))
    age_files(Path(s.log_dir))
    first = ingest(repo, s)["events_new"]
    write_all(s.log_dir, synthetic, until=START + timedelta(minutes=60), append_from=START + timedelta(minutes=20))
    age_files(Path(s.log_dir))
    second = ingest(repo, s)["events_new"]
    assert ingest(repo, s)["events_new"] == 0
    full = Repository(str(tmp_path / "full.db"))
    assert first + second == ingest(full, make_settings(tmp_path, db_path=str(tmp_path / "full.db")))["events_new"]


def test_replay_creates_no_duplicate_events_or_incidents(analysed):
    s, repo = analysed
    n_events, n_inc = repo.event_stats(s.cluster_id)["count"], len(repo.incidents(s.cluster_id))
    repo.conn.execute("DELETE FROM checkpoints")
    repo.conn.commit()
    ov = run_analysis(repo, s, None)
    assert ov["ingest"]["events_new"] == 0 and ov["ingest"]["events_duplicate"] > 0
    assert repo.event_stats(s.cluster_id)["count"] == n_events and len(repo.incidents(s.cluster_id)) == n_inc


def test_rotation_is_detected(env):
    s, repo = env
    ingest(repo, s)
    p = Path(s.log_dir) / "solr-node1" / "gc.log"
    p.write_text("[2026-10-15T11:00:01.000+0000][1s][info][gc          ] GC(1) Pause Young (Normal) (G1 Evacuation Pause) 100M->50M(8192M) 5.0ms\n")
    assert ingest(repo, s)["events_new"] == 1


def test_partial_record_is_held_back_while_file_is_being_written(tmp_path):
    s = make_settings(tmp_path)
    repo = Repository(s.db_path)
    d = Path(s.log_dir) / "n1"
    d.mkdir(parents=True)
    f = d / "solr.log"
    f.write_text("2026-10-15 10:00:00.000 INFO  (main) [   ] o.a.s.c.X first\n"
                 "2026-10-15 10:00:01.000 ERROR (main) [   ] o.a.s.c.X failed: java.io.IOException: x\n")
    assert ingest(repo, s)["events_new"] == 1          # last record held back (file just modified)
    with open(f, "a") as fh:
        fh.write("\tat org.Foo.bar(Foo.java:1)\n")
    os.utime(f, (time.time() - 60, time.time() - 60))  # writer moved on
    assert ingest(repo, s)["events_new"] == 1
    ev = repo.events(s.cluster_id)[-1]
    assert "org.Foo.bar" in ev.attributes["stack"]     # stack trace was not split into a separate record


def test_upload_is_ingested(tmp_path):
    s = make_settings(tmp_path)
    repo = Repository(s.db_path)
    save_upload(s, "solr-node9", "solr", "x.log",
                b"2026-10-15 10:00:00.000 ERROR (q) [   ] o.a.s.h.RequestHandlerBase java.io.IOException: No space left on device")
    assert ingest(repo, s)["events_new"] == 1 and repo.events(s.cluster_id)[0].node_id == "solr-node9"
