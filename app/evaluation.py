"""Offline evaluation against labelled scenarios (doc §11).

Detection & correlation (deterministic), over several random seeds:
  * recall of required signals, precision (signals attributable to a labelled scenario), false positives
  * incident grouping: one incident per scenario, correct top hypothesis, root signal type + node,
    affected nodes, policy severity, recurrence link, open/resolved status, Unknown node state
  * parse-error capture, replay idempotency (re-ingesting creates no new events or incidents),
    detection delay (first relevant record -> signal start)
RCA generation, per provider (reference dataset):
  * completion: LLM answer accepted (first try / after repair) vs. fallback
  * root-cause accuracy: expected concepts named, root signal cited, root node named
  * evidence integrity: every cited ID resolves; unverified numbers; severity not overridden
  * latency and tokens
Usage:  python run.py eval --models rules,gemini,ollama:qwen2.5:7b --seeds 1,2,3,4,5
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import ROOT, Settings
from .pipeline import run_analysis
from .rca.providers import LLMClient, resolve_provider
from .rca.service import RCAService
from .schemas import ClusterHealth
from .storage.repository import Repository

sys.path.insert(0, str(ROOT / "scripts"))
from generate_synthetic_logs import START, Sim, write_all  # noqa: E402

GT = ROOT / "eval" / "ground_truth.json"
SLACK = timedelta(minutes=5)


def _t(hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    return START.replace(hour=h, minute=0) + timedelta(minutes=m) if h == 11 else START.replace(hour=h, minute=m)


def load_gt() -> dict:
    return json.loads(GT.read_text(encoding="utf-8"))


def _prepare(seed: int, work: Path, base: Settings):
    logs = work / f"logs-{seed}"
    write_all(str(logs), Sim(seed).generate())
    old = time.time() - 120  # finished files: not "being written", so no record is held back
    for f in logs.rglob("*.log"):
        os.utime(f, (old, old))
    s = Settings(**{**base.__dict__, "log_dir": str(logs), "upload_dir": str(work / f"up-{seed}"),
                    "db_path": str(work / f"db-{seed}.sqlite"), "llm_provider": "none"})
    repo = Repository(s.db_path)
    run_analysis(repo, s, None)
    return s, repo


def _in_window(sig, sc) -> bool:
    return sig.node_id in sc["nodes"] and _t(sc["window"][0]) - SLACK <= sig.start <= _t(sc["window"][1]) + SLACK


def match(incidents, scenarios) -> dict:
    out = {}
    for sc in scenarios:
        w0, w1 = _t(sc["window"][0]), _t(sc["window"][1])
        best, ov = None, 0.0
        for i in incidents:
            if not set(i.nodes) & set(sc["nodes"]):
                continue
            o = (min(i.last_seen, w1) - max(i.first_seen, w0)).total_seconds()
            if o > ov:
                best, ov = i, o
        out[sc["id"]] = best
    return out


def eval_detection(seed: int, work: Path, base: Settings) -> dict:
    gt = load_gt()
    s, repo = _prepare(seed, work, base)
    sig = repo.signals(s.cluster_id)
    by = {x.signal_id: x for x in sig}
    incs = repo.incidents(s.cluster_id)
    ch = ClusterHealth.model_validate(repo.get("cluster_health"))
    found, missing, total, delays = 0, [], 0, []
    for sc in gt["scenarios"]:
        for req in sc["must_detect"]:
            total += 1
            hit = [x for x in sig if x.signal_type == req["type"] and x.node_id == req["node"] and _in_window(x, sc)]
            if hit:
                found += 1
                first_ev = min((e.timestamp for e in repo.events_by_ids(hit[0].event_ids[:1])), default=hit[0].start)
                delays.append(max(0.0, (hit[0].start - first_ev).total_seconds()))
            else:
                missing.append(f"{sc['id']}:{req['type']}@{req['node']}")
    fps = [x for x in sig if not any(_in_window(x, sc) for sc in gt["scenarios"])]
    m = match(incs, gt["scenarios"])
    checks = {}
    for sc in gt["scenarios"]:
        inc = m[sc["id"]]
        c = {"incident": inc.incident_id if inc else None}
        if inc:
            root = by[inc.root_signal_id]
            c["pattern_ok"] = bool(inc.hypotheses) and inc.hypotheses[0].pattern_id == sc["expected_pattern"]
            c["root_ok"] = root.signal_type == sc["expected_root"]["type"] and root.node_id == sc["expected_root"]["node"]
            c["nodes_ok"] = set(inc.nodes) == set(sc["nodes"])
            c["severity_ok"] = inc.severity == sc["expected_severity"]
            if "expected_recurrence_of" in sc:
                prev = m.get(sc["expected_recurrence_of"])
                c["recurrence_ok"] = bool(prev) and inc.recurrence_of == prev.incident_id
            if "expected_status" in sc:
                c["status_ok"] = inc.status == sc["expected_status"]
            for node, st in sc.get("expected_node_state", {}).items():
                c["node_state_ok"] = ch.nodes[node].state == st
        checks[sc["id"]] = c
    matched = {i.incident_id for i in m.values() if i}
    n_err, _ = repo.parse_errors(s.cluster_id)
    # replay idempotency: forget checkpoints, re-ingest everything, re-analyse
    repo.conn.execute("DELETE FROM checkpoints")
    repo.conn.commit()
    before_inc = len(repo.incidents(s.cluster_id))
    ov = run_analysis(repo, s, None)
    replay_ok = ov["ingest"]["events_new"] == 0 and len(repo.incidents(s.cluster_id)) == before_inc
    repo.close()
    return {"seed": seed, "events": ov["events_total"], "signals": len(sig), "incidents": len(incs),
            "recall": round(found / total, 3), "missing": missing, "precision": round(1 - len(fps) / max(1, len(sig)), 3),
            "false_positives": [f"{x.signal_type}@{x.node_id} {x.start:%H:%M}" for x in fps],
            "scenario_checks": checks, "unexpected_incidents": [i.incident_id for i in incs if i.incident_id not in matched],
            "parse_errors": n_err, "parse_errors_ok": n_err == gt["expected_parse_errors"], "replay_idempotent": replay_ok,
            "max_detection_delay_s": round(max(delays), 1) if delays else None, "cluster_state": ch.state}


def _client(spec: str, s: Settings) -> LLMClient:
    if spec == "rules":
        return LLMClient(s, provider="none")
    provider, _, model = spec.partition(":")
    return LLMClient(s, provider=provider, model=model or None)


def eval_rca(models: list[str], work: Path, base: Settings, seed: int = 42) -> list[dict]:
    gt = load_gt()
    s, repo = _prepare(seed, work, base)
    m = match(repo.incidents(s.cluster_id), gt["scenarios"])
    rows = []
    for spec in models:
        client = _client(spec, s)
        if spec != "rules" and not client.enabled:
            rows.append({"model": spec, "skipped": f"provider '{resolve_provider(spec.partition(':')[0])}' not configured"})
            continue
        svc = RCAService(repo, s, client)
        per = []
        for sc in gt["scenarios"]:
            inc = m[sc["id"]]
            if not inc:
                continue
            t0 = time.time()
            rec = svc.analyze(inc.incident_id, trigger="reanalyze")
            rc = rec.report.probable_root_cause
            text = (rc.hypothesis + " " + rec.report.incident_summary).lower()
            att = [a for a in rec.attempts if "validation" in a]
            per.append({
                "scenario": sc["id"], "source": rec.source, "reason": rec.reason,
                "first_try_valid": bool(att) and att[0]["validation"]["valid"], "valid": rec.validation.get("valid", False),
                "concepts_ok": all(any(k in text for k in group) for group in sc["root_cause_keywords"]),
                "root_cited": inc.root_signal_id in rc.supporting_evidence_ids,
                "root_node_named": sc["expected_root"]["node"] in text,
                "severity_kept": rec.report.operational_impact.severity == inc.severity,
                "has_diagnostic_first": bool(rec.report.recommended_remediation) and any(
                    r.type == "diagnostic" for r in rec.report.recommended_remediation),
                "unverified_numbers": len(rec.validation.get("unverified_numbers", [])),
                "confidence": rc.confidence, "latency_s": round(time.time() - t0, 1),
                "tokens_in": sum((a.get("usage") or {}).get("input_tokens") or 0 for a in rec.attempts),
                "tokens_out": sum((a.get("usage") or {}).get("output_tokens") or 0 for a in rec.attempts),
                "hypothesis": rc.hypothesis[:400]})
        n = max(1, len(per))
        rows.append({"model": "rules (deterministic)" if spec == "rules" else client.describe(), "n": len(per),
                     "llm_answers": sum(p["source"] == "llm" for p in per),
                     "first_try_valid": sum(p["first_try_valid"] for p in per),
                     "fallbacks": None if spec == "rules" else sum(p["source"] == "rules" for p in per),
                     "root_cause_accuracy": round(sum(p["concepts_ok"] for p in per) / n, 2),
                     "root_cited": round(sum(p["root_cited"] for p in per) / n, 2),
                     "root_node_named": round(sum(p["root_node_named"] for p in per) / n, 2),
                     "severity_kept": round(sum(p["severity_kept"] for p in per) / n, 2),
                     "unverified_numbers": sum(p["unverified_numbers"] for p in per),
                     "avg_latency_s": round(sum(p["latency_s"] for p in per) / n, 1),
                     "tokens": f"{sum(p['tokens_in'] for p in per):,}/{sum(p['tokens_out'] for p in per):,}",
                     "per_incident": per})
    repo.close()
    return rows


def report_md(det: list[dict], rca: list[dict], generated_at: datetime | None = None) -> str:
    when = generated_at or datetime.now(timezone.utc)
    L = ["# Evaluation", "",
         f"Generated {when:%Y-%m-%d %H:%M UTC} by `python run.py eval`. The expected answers are in "
         "[eval/ground_truth.json](../eval/ground_truth.json): 4 planted problems and 4 malformed records. "
         "Raw results: `eval/results/latest.json`.", "",
         "## 1. Detection", "",
         "Each seed regenerates the logs with different background noise (traffic, latency, GC timing). "
         "The planted problems stay the same.", "",
         "| Seed | Events | Signals | Recall | Precision | Incidents | Scenario checks | Unexpected incidents | Parse errors captured | Replay idempotent | Cluster |",
         "|---|---|---|---|---|---|---|---|---|---|---|"]
    for d in det:
        ch = [v for c in d["scenario_checks"].values() for k, v in c.items() if k.endswith("_ok")]
        L.append(f"| {d['seed']} | {d['events']:,} | {d['signals']} | {d['recall']:.0%} | {d['precision']:.0%} | {d['incidents']} | "
                 f"{sum(ch)}/{len(ch)} | {len(d['unexpected_incidents'])} | {d['parse_errors']} {'✓' if d['parse_errors_ok'] else '✗'} | "
                 f"{'✓' if d['replay_idempotent'] else '✗'} | {d['cluster_state']} |")
    miss = sorted({x for d in det for x in d["missing"]})
    fps = sorted({x for d in det for x in d["false_positives"]})
    failed = sorted({f"{sid}.{k}" for d in det for sid, c in d["scenario_checks"].items() for k, v in c.items()
                     if k.endswith("_ok") and not v})
    L += ["", "*Scenario checks* cover, for each planted problem: the top explanation, the root signal and node, the "
          "affected nodes and the severity, plus the repeat link and open status for S3 and the Unknown node for S4. "
          "*Replay idempotent* means checkpoints were deleted and every log re-read, with no new events or incidents.", "",
          f"Missed signals: {', '.join(miss) or 'none'}.  ",
          f"False alarms: {', '.join(fps) or 'none'}.  ",
          f"Failed checks: {', '.join(failed) or 'none'}.", "",
          "## 2. Root-cause answers", "",
          "Each provider explains the same 4 incidents (seed 42).", "",
          "| Provider | Incidents | LLM answers | Valid 1st try | Fallbacks | Root-cause accuracy | Root signal cited | Root node named | Severity kept | Unverified numbers | Avg latency | Tokens in/out |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rca:
        if "skipped" in r:
            L.append(f"| {r['model']} | – | – | – | – | – | – | – | – | – | – | skipped: {r['skipped']} |")
            continue
        L.append(f"| {r['model']} | {r['n']} | {r['llm_answers']} | {r['first_try_valid']} | "
                 f"{'n/a' if r['fallbacks'] is None else r['fallbacks']} | {r['root_cause_accuracy']:.0%} | {r['root_cited']:.0%} | "
                 f"{r['root_node_named']:.0%} | {r['severity_kept']:.0%} | {r['unverified_numbers']} | {r['avg_latency_s']} s | {r['tokens']} |")
    L += ["", "The rules row is the baseline. An LLM is worth using only if it matches the baseline on accuracy and "
          "evidence, and adds what rules can't: the mechanism, alternatives, and fixes specific to the incident. "
          "*Root-cause accuracy* means the explanation names the expected concepts (for example expensive queries, "
          "GC and heap, ZooKeeper). A fallback is an LLM call that failed and was replaced by the rules-based report.", "",
          "### The explanations", ""]
    for r in rca:
        for p in r.get("per_incident", []):
            L.append(f"- **{r['model']} / {p['scenario']}** ({p['source']}, confidence {p['confidence']}): {p['hypothesis']}")
    return "\n".join(L) + "\n"


def run(models: list[str], seeds: list[int], base: Settings) -> dict:
    work = Path(tempfile.mkdtemp(prefix="sentinel-eval-"))
    try:
        det = []
        for sd in seeds:
            print(f"[eval] detection seed {sd}", flush=True)
            det.append(eval_detection(sd, work, base))
        print(f"[eval] RCA: {', '.join(models)}", flush=True)
        rca = eval_rca(models, work, base)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    res = {"generated_at": datetime.now(timezone.utc).isoformat(), "detection": det, "rca": rca}
    (ROOT / "eval" / "results").mkdir(parents=True, exist_ok=True)
    (ROOT / "eval" / "results" / "latest.json").write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "EVALUATION.md").write_text(report_md(det, rca), encoding="utf-8")
    return res
