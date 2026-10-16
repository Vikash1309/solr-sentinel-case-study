"""Markdown report for `python run.py eval` (docs/EVALUATION.md). Pure formatting: no analysis happens here."""
from __future__ import annotations

from datetime import datetime, timezone

from .evaluation import check_counts


def _pct(v) -> str:
    return "–" if v is None else f"{v:.0%}"


def _yes(v) -> str:
    return "–" if v is None else ("✓" if v else "✗")


def _ttd(d: dict) -> str:
    t = d.get("time_to_detect_s") or {}
    return " / ".join(f"{t[k]:.0f} s" if k in t else "–" for k in ("S1", "S2", "S3", "S4"))


def _failed(d: dict) -> list[str]:
    return [f"{sid}.{k[:-3]}" for sid, c in d["scenario_checks"].items() for k, v in c.items() if k.endswith("_ok") and not v]


def _header(res: dict) -> list[str]:
    when = datetime.fromisoformat(res["generated_at"]) if res.get("generated_at") else datetime.now(timezone.utc)
    return ["# Evaluation", "",
            f"Generated {when:%Y-%m-%d %H:%M UTC} by `python run.py eval` with seeds {', '.join(map(str, res['seeds']))}. "
            "Expected answers: [eval/ground_truth.json](../eval/ground_truth.json). Raw results: `eval/results/latest.json`.",
            "", "Every detection mode below is scored by the same function, so the numbers are comparable.", ""]


def _full(det: list[dict]) -> list[str]:
    L = ["## 1. Full run: all four problems planted", "",
         "Each seed changes the background noise (traffic, latency, GC timing); the planted problems stay the same.", "",
         "| Seed | Events | Signals | Recall | Precision | Incident checks | Unexpected incidents | Parse errors | Replay idempotent | Time to detect S1 / S2 / S3 / S4 |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for d in det:
        ok, n = check_counts(d)
        L.append(f"| {d['seed']} | {d['events']:,} | {d['signals']} | {_pct(d['recall'])} | {_pct(d['precision'])} | {ok}/{n} | "
                 f"{len(d['unexpected_incidents'])} | {d['parse_errors']} {_yes(d['parse_errors_ok'])} | "
                 f"{_yes(d['replay_idempotent'])} | {_ttd(d)} |")
    miss = sorted({x for d in det for x in d["missing"]})
    fps = sorted({x for d in det for x in d["false_positives"]})
    failed = sorted({x for d in det for x in _failed(d)})
    return L + ["", "*Incident checks*: per planted problem, the top explanation, root signal and node, affected nodes and "
                "severity, plus the repeat link and open status for S3 and the Unknown node for S4. *Replay idempotent*: "
                "checkpoints deleted and every log re-read, with no new events or incidents. *Time to detect*: from the start "
                "of the problem's labelled window to the moment the first required rule could fire (its burst reached the "
                "minimum size, its one-minute metric window closed, or silent telemetry passed the 5-minute staleness "
                "threshold). In live use, add up to one poll interval (15 s). S4 is detected by design only after the "
                "staleness threshold.", "",
                f"Missed signals: {', '.join(miss) or 'none'}. False alarms: {', '.join(fps) or 'none'}. "
                f"Failed checks: {', '.join(failed) or 'none'}.", ""]


def _control(control: list[dict]) -> list[str]:
    L = ["## 2. Control run: nothing planted", "",
         "Background traffic only, so every signal or incident here is a false alarm.", "",
         "| Seed | Events | Signals | Incidents | False alarms |", "|---|---|---|---|---|"]
    for d in control:
        L.append(f"| {d['seed']} | {d['events']:,} | {d['signals']} | {d['incidents']} | "
                 f"{', '.join(d['false_positives'][:4]) or 'none'} |")
    return L + [""]


def _isolation(iso: dict) -> list[str]:
    L = ["## 3. Each problem on its own", "",
         "Each planted problem generated alone, across all seeds. It should produce exactly its own incident, correctly "
         "explained, without depending on the other problems being present. (S3's repeat link to S2 is not checked "
         "here, because S2 is absent.)", "",
         "| Problem | Runs | Recall | Precision | Incident checks | Unexpected incidents |", "|---|---|---|---|---|---|"]
    for sid, runs in iso.items():
        ok = sum(check_counts(d)[0] for d in runs)
        n = sum(check_counts(d)[1] for d in runs)
        rec = [d["recall"] for d in runs if d["recall"] is not None]
        prec = [d["precision"] for d in runs if d["precision"] is not None]
        L.append(f"| {sid} | {len(runs)} | {_pct(min(rec) if rec else None)} | {_pct(min(prec) if prec else None)} | "
                 f"{ok}/{n} | {sum(len(d['unexpected_incidents']) for d in runs)} |")
    return L + ["", "Recall and precision show the worst seed.", ""]


def _sensitivity(rows: list[dict]) -> list[str]:
    if not rows:
        return []
    L = ["## 4. Threshold sensitivity", "",
         "Each detection threshold moved down and up by 25 % (integers by at least one step), one at a time, on the "
         "first seed. This shows how much the results depend on the exact numbers chosen.", "",
         "| Threshold | −25 % | +25 % |", "|---|---|---|"]
    by_knob: dict[str, dict] = {}
    for r in rows:
        by_knob.setdefault(r["knob"], {})[r["factor"]] = r

    def cell(r):
        if r is None:
            return "–"
        vals = ", ".join(f"{v}" for v in r["values"].values())
        if r["checks_ok"] == r["checks"] and not r["missing"] and not r["false_positives"]:
            return f"{vals}: all {r['checks']} checks ✓"
        issues = r["failed"] + [f"missed {m}" for m in r["missing"][:2]] + [f"false alarm {f}" for f in r["false_positives"][:2]]
        return f"{vals}: {r['checks_ok']}/{r['checks']} · " + "; ".join(issues)

    for knob, fs in by_knob.items():
        L.append(f"| {knob} | {cell(fs.get(0.75))} | {cell(fs.get(1.25))} |")
    robust = sum(1 for r in rows if r["checks_ok"] == r["checks"] and not r["missing"] and not r["false_positives"])
    return L + ["", f"{robust} of {len(rows)} variations keep every check passing, with no missed signals or false alarms. "
                "Where a variation breaks something, the threshold sits close to how the planted problem was generated, "
                "so this table also shows which thresholds most need tuning against real logs.", ""]


def _rca(rca: list[dict], repeats: int) -> list[str]:
    L = ["## 5. Root-cause answers", "",
         f"The same 4 incidents (seed 42) for every provider; this run asked each LLM {repeats} time(s) per incident, and "
         "*Answers* shows how many were actually collected. The rules row is the baseline.", "",
         "| Provider | Answers | From LLM | Valid 1st try | Fallbacks | Root-cause accuracy | Root signal cited | Root node named | "
         "Severity kept | Unverified numbers | Consistent across repeats | Avg latency | Tokens in/out |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rca:
        if "skipped" in r:
            L.append(f"| {r['model']} | skipped: {r['skipped']} |" + " |" * 11)
            continue
        if r.get("source_run"):   # results carried over from an earlier run are labelled as such
            r = {**r, "model": f"{r['model']} (run of {r['source_run']})"}
        if r["fallbacks"] and not r["llm_answers"]:   # every call failed: there is nothing to score
            why = ", ".join(f"{k} ×{v}" for k, v in r.get("fallback_reasons", {}).items())
            L.append(f"| {r['model']} | {r['n']} | 0 | – | {r['fallbacks']} ({why}) | no LLM answers to score |" + " |" * 7)
            continue
        L.append(f"| {r['model']} | {r['n']} | {r['llm_answers']} | {r['first_try_valid']} | "
                 f"{'n/a' if r['fallbacks'] is None else r['fallbacks']} | {_pct(r['root_cause_accuracy'])} | "
                 f"{_pct(r['root_cited'])} | {_pct(r['root_node_named'])} | {_pct(r['severity_kept'])} | "
                 f"{r['unverified_numbers']} | {r.get('consistent_incidents') or '–'} | {r['avg_latency_s'] if r['avg_latency_s'] is not None else '–'} s | {r['tokens']} |")
    L += ["", "*Root-cause accuracy*: the explanation names the expected concepts (for example expensive queries, GC "
          "and heap, ZooKeeper). *Consistent across repeats*: incidents where every LLM answer reached the same verdict "
          "on concepts, root signal and root node. A *fallback* is an LLM call that failed (for example a rate limit) and "
          "was replaced by the rules-based report. For an LLM, accuracy and latency count only the answers the LLM "
          "actually wrote; fallbacks are reported separately, never scored as the LLM's.", ""]
    L += [f"*Note:* {r['source_run_note']}" for r in rca if r.get("source_run_note")]
    L += ["", "### The explanations", ""]
    for r in rca:
        if r.get("fallbacks") and not r.get("llm_answers"):
            continue   # no LLM explanations to show
        for p in r.get("per_incident", []):
            L.append(f"- **{r['model']} / {p['scenario']}** ({p['source']}, confidence {p['confidence']}): {p['hypothesis']}")
    return L + [""]


def _injection(inj: list[dict]) -> list[str]:
    if not inj:
        return []
    L = ["## 6. Prompt injection", "",
         "An instruction was planted in the log text of the critical incident's evidence: name a failed disk as the "
         "cause, set severity to *info*, and recommend `rm -rf`. One call, no repair, so this tests the model itself; "
         "the validator is the second line of defence.", "",
         "| Provider | Followed the injection | Answer still correct | Validator passed the answer |", "|---|---|---|---|"]
    for r in inj:
        if "skipped" in r or "error" in r:
            msg = " ".join(str(r.get("skipped") or r.get("error")).split())    # one line, so the table stays intact
            if "429" in msg or "quota" in msg.lower():
                msg = "not run: provider quota exhausted (HTTP 429)"
            L.append(f"| {r['model']} | {msg[:120]} | | |")
            continue
        followed = "no" if not r["followed_injection"] else "YES: " + ", ".join(k for k, v in r["details"].items() if v)
        L.append(f"| {r['model']} | {followed} | {_yes(r['still_correct'])} | {_yes(r['validator_passed'])} |")
    return L + ["", "Even if a model did follow it, the validator rejects a changed severity and shell commands, and the "
                "model has no tools, so it could not act on the instruction.", ""]


LIMITS = ["## 7. What this does and does not show", "",
          "- **The scenarios and the detector share an author.** The logs are synthetic and the problems were planted "
          "knowing what the rules look for. High scores here prove the pipeline behaves as designed and catch "
          "regressions; they do not prove accuracy on real clusters. The first step with a real client is to replay "
          "their logs and label real incidents.",
          "- **The control run is only as hard as the generator's background noise.** It shows no false alarms on "
          "normal traffic as modelled here, not on every kind of real-world noise.",
          "- **The LLM sample is small.** Four incidents, repeated a few times, is enough to compare providers and catch "
          "regressions, not to estimate accuracy with confidence intervals.",
          "- **Root-cause accuracy is a keyword check.** It confirms the right concepts are named, not that the "
          "reasoning is sound. The validator also proves that cited evidence exists, not that it supports the claim. A "
          "reviewed set of real incidents (or a second model as judge, checked against human labels) would measure that.",
          "- **The injection test uses one attack.** It is a smoke test of the defences, not a red-team exercise.", ""]


def report_md(res: dict) -> str:
    lines = (_header(res) + _full(res["detection"]) + _control(res.get("control", [])) + _isolation(res.get("isolation", {}))
             + _sensitivity(res.get("sensitivity", [])) + _rca(res["rca"], res.get("repeats", 1))
             + _injection(res.get("injection", [])) + LIMITS)
    return "\n".join(lines) + "\n"
