"""Deterministic RCA in the same §7.4 contract. Used when the LLM is disabled, unavailable, below the
severity threshold, or its output fails validation twice - so an incident always has an evidence-backed
explanation and the dashboard never blocks on the model (doc §10.2 "fail safely")."""
from __future__ import annotations

from collections import defaultdict, deque

from ..correlation.knowledge import PATTERN_BY_ID
from ..schemas import AlternativeHypothesis, Confidence, Incident, OperationalImpact, RCAReport, Remediation, RootCause, Signal

SYMPTOMS = {"LATENCY_DEGRADATION", "REMOTE_TIMEOUT", "NO_LEADER", "LEADER_ELECTION", "REPLICA_RECOVERY", "NODE_LOST",
            "ERROR_RATE_SPIKE", "SLOW_QUERIES", "SEARCHER_LIMIT", "CIRCUIT_BREAKER", "TELEMETRY_STALE"}


def _chain(inc: Incident, sig: dict[str, Signal]) -> list[Signal]:
    out = defaultdict(list)
    for l in inc.links:
        out[l.cause].append(l.effect)
    seen, order, q = set(), [], deque([inc.root_signal_id])
    while q:
        x = q.popleft()
        if x in seen or x not in sig:
            continue
        seen.add(x)
        order.append(sig[x])
        q.extend(out[x])
    return sorted(order, key=lambda s: s.start)


def build(inc: Incident, sig: dict[str, Signal], pack: dict, reason: str) -> RCAReport:
    members = [sig[i] for i in inc.signal_ids if i in sig]
    root = sig[inc.root_signal_id]
    top = PATTERN_BY_ID.get(inc.hypotheses[0].pattern_id) if inc.hypotheses else None
    contributing = [h for h in inc.hypotheses[1:] if h.score >= 0.75 and not h.contradicting]
    others = [h for h in inc.hypotheses[1:] if h not in contributing][:2]
    root_records = [r["id"] for r in pack["records"] if root.signal_id in r.get("signals", [])][:3]
    observed = any(l.relation == "observed" for l in inc.links)
    confidence: Confidence = "medium" if (top and inc.hypotheses[0].score >= 0.7 and observed) else "low"
    if top:
        hyp = (f"{top['name']}. Earliest triggering condition: '{root.title}' on {root.node_id} at "
               f"{root.start:%H:%M:%S} UTC ({root.detail}).")
        if contributing:
            hyp += " Contributing pattern(s): " + "; ".join(h.name for h in contributing) + "."
    else:
        hyp = f"Inconclusive. Earliest signal: {root.title} on {root.node_id} ({root.detail})."
    contra_types = set(inc.hypotheses[0].contradicting) if inc.hypotheses else set()
    contra = [m.signal_id for m in members if m.signal_type in contra_types]
    chain = _chain(inc, sig)
    symptoms = [f"{m.start:%H:%M:%S} {m.node_id}: {m.title} - {m.detail}" for m in chain if m.signal_type in SYMPTOMS][:6]
    statements = pack["impact_summary"]["supported_impact_statements"]
    counts = pack["impact_summary"]["counts"]
    count_txt = []
    if counts.get("inter_node_request_timeouts"):
        count_txt.append(f"{counts['inter_node_request_timeouts']} inter-node request timeouts")
    if counts.get("updates_rejected_no_leader"):
        count_txt.append(f"{counts['updates_rejected_no_leader']} updates rejected without a shard leader")
    if counts.get("commits_rejected_max_warming_searchers"):
        count_txt.append(f"{counts['commits_rejected_max_warming_searchers']} commits rejected")
    business = (" ".join(statements) + (" Observed: " + "; ".join(count_txt) + "." if count_txt else "") +
                " Customer or revenue impact cannot be derived from logs and must be confirmed with request-volume metrics.")
    rem = []
    if top:
        for r in top["remediation"]:
            rem.append(Remediation(action=r["action"], priority=r["priority"], type=r["type"], evidence_ids=[root.signal_id],
                                   requires_human_approval=r["requires_human_approval"]))
        for h in contributing:
            r = next((x for x in PATTERN_BY_ID[h.pattern_id]["remediation"] if x["type"] != "diagnostic"), None)
            if r and r["action"] not in {x.action for x in rem}:
                rem.append(Remediation(action=r["action"], priority=r["priority"], type=r["type"],
                                       evidence_ids=[root.signal_id], requires_human_approval=True))
    else:
        rem.append(Remediation(action="Review the raw records around the earliest signal and collect host metrics",
                               priority="high", type="diagnostic", evidence_ids=[root.signal_id], requires_human_approval=False))
    alts = [AlternativeHypothesis(hypothesis=h.name, assessment=f"lower pattern match ({h.score})" +
                                  (f"; contradicted by {', '.join(h.contradicting)}" if h.contradicting else ""))
            for h in others]
    if top:
        alts += [AlternativeHypothesis(hypothesis=a, assessment="not excluded by the available logs; see missing evidence")
                 for a in top["alternatives"]]
    return RCAReport(
        incident_summary=(f"{inc.title}: {len(members)} correlated signals on {', '.join(inc.nodes)} between "
                          f"{inc.first_seen:%H:%M} and {inc.last_seen:%H:%M} UTC. Severity {inc.severity} by policy "
                          f"({inc.severity_reason})." +
                          (f" Recurrence of {inc.recurrence_of}." if inc.recurrence_of else "")),
        probable_root_cause=RootCause(hypothesis=hyp, confidence=confidence,
                                      supporting_evidence_ids=[root.signal_id] + root_records + [f"M-{root.node_id}"],
                                      contradicting_evidence_ids=contra,
                                      missing_evidence=pack["missing_telemetry_and_limitations"][:6]),
        operational_impact=OperationalImpact(severity=inc.severity, affected_nodes=inc.nodes, observed_symptoms=symptoms,
                                             potential_consequences=statements, business_impact=business),
        recommended_remediation=rem,
        alternative_hypotheses=alts,
        limitations=[f"Deterministic analysis (AI RCA not used: {reason.replace('_', ' ')})",
                     "Confidence is qualitative and not calibrated against labelled incidents",
                     "Relationships are hypotheses: observed/inferred/temporal labels show how each is supported"],
    )
