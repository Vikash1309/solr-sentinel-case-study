"""RCA validation (doc §7.6). The application, not the prompt, is the security and quality boundary.

Hard errors (trigger one bounded repair attempt, then a safe fallback):
  * output is not valid JSON / does not match the §7.4 contract
  * a cited evidence ID does not exist in the evidence package (fake citation)
  * no supporting evidence for a medium/high-confidence root cause
  * operational_impact.severity differs from the deterministic policy severity (severity override)
  * an affected node that is not part of the cluster
  * remediation containing shell commands or destructive operations (unsafe commands)

Enforced corrections (policy wins, recorded as warnings):
  * mitigation/corrective actions always require human approval

Soft warnings (shown in the UI, lower trust):
  * numbers in the prose that do not appear in the evidence (allowing ms<->s and rounding)
  * no diagnostic action before disruptive ones
"""
from __future__ import annotations

import json
import re

from pydantic import ValidationError

from ..schemas import RCAReport
from .evidence import valid_ids

ID_RX = re.compile(r"\b(S-[0-9a-f]{8}|E\d{1,3}|M-[A-Za-z0-9_.-]+)\b")
TIME_RX = re.compile(r"\b\d{4}-\d{2}-\d{2}(T[\d:.]+Z?)?|\b\d{1,2}:\d{2}(:\d{2})?(\.\d+)?\b")
NUM_RX = re.compile(r"(?<![\w.:-])(\d+(?:[.,]\d+)?)")
UNSAFE = re.compile(r"(rm\s+-rf|kill\s+-9|\bsudo\b|systemctl\s+(stop|restart)|DELETEREPLICA|DELETESHARD|action=DELETE|"
                    r"drop\s+collection|curl\s+-X\s*(DELETE|POST)|\bshutdown\b|\breboot\b|`[^`]{3,}`)", re.I)


def extract_json(text: str) -> dict:
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
    i, j = t.find("{"), t.rfind("}")
    if i < 0 or j < 0:
        raise ValueError("no JSON object in the model output")
    return json.loads(t[i:j + 1])


def _strings(obj, skip=("supporting_evidence_ids", "contradicting_evidence_ids", "evidence_ids", "affected_nodes")):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k not in skip:
                yield from _strings(v, skip)
    elif isinstance(obj, list):
        for v in obj:
            yield from _strings(v, skip)
    elif isinstance(obj, str):
        yield obj


def _pack_numbers(pack: dict) -> list[float]:
    return [float(x) for x in re.findall(r"\d+(?:\.\d+)?", TIME_RX.sub(" ", json.dumps(pack)))]


def _known(v: float, nums: list[float]) -> bool:
    if any(abs(v - n) <= max(0.5, 0.01 * abs(n)) for n in nums):
        return True
    return any(n and abs(c - n) <= 0.015 * abs(n) for c in (v * 1000, v * 1024, v * 60) for n in nums)


def _cited_ids(report: RCAReport) -> set[str]:
    """Every evidence ID the answer relies on: the citation lists, plus IDs mentioned in the free text."""
    rc = report.probable_root_cause
    cited = set(rc.supporting_evidence_ids) | set(rc.contradicting_evidence_ids)
    for r in report.recommended_remediation:
        cited |= set(r.evidence_ids)
    for a in report.alternative_hypotheses:
        cited |= set(a.evidence_ids)
    for txt in _strings(report.model_dump(), skip=()):
        cited |= set(ID_RX.findall(txt))
    return cited


def _evidence_errors(report: RCAReport, cited: set[str], ids: set[str]) -> list[str]:
    errors = []
    fake = sorted(c for c in cited if c not in ids)
    if fake:
        errors.append(f"cites evidence IDs that do not exist: {', '.join(fake[:10])}")
    if report.probable_root_cause.confidence != "low" and not report.probable_root_cause.supporting_evidence_ids:
        errors.append("a medium/high-confidence root cause must cite supporting_evidence_ids")
    return errors


def _policy_errors(report: RCAReport, pack: dict) -> list[str]:
    errors = []
    policy = pack["incident"]["policy_severity"]
    if report.operational_impact.severity != policy:
        errors.append(f"operational_impact.severity must equal the policy severity '{policy}' "
                      f"(got '{report.operational_impact.severity}')")
    nodes = set(pack["cluster_context"]["nodes"]) | set(pack["incident"]["affected_nodes"])
    unknown_nodes = [n for n in report.operational_impact.affected_nodes if n not in nodes]
    if unknown_nodes:
        errors.append(f"affected_nodes contains nodes not in the cluster: {', '.join(unknown_nodes)}")
    return errors


def _remediation_checks(report: RCAReport) -> tuple[list[str], list[str], list[str]]:
    """Unsafe actions are errors; missing approvals are corrected (policy wins); no diagnostic step is a warning."""
    errors, corrections, warnings = [], [], []
    for i, r in enumerate(report.recommended_remediation):
        if UNSAFE.search(r.action):
            errors.append(f"remediation[{i}] contains a command or destructive operation; describe the action instead")
        if r.type in ("mitigation", "corrective") and not r.requires_human_approval:
            r.requires_human_approval = True
            corrections.append(f"remediation[{i}] ({r.type}) set to require human approval")
    if report.recommended_remediation and not any(r.type == "diagnostic" for r in report.recommended_remediation):
        warnings.append("no diagnostic action recommended before disruptive changes")
    return errors, corrections, warnings


def _unverified_numbers(report: RCAReport, pack: dict) -> list[str]:
    nums = _pack_numbers(pack)
    unverified = []
    for txt in _strings(report.model_dump()):
        clean = TIME_RX.sub(" ", ID_RX.sub(" ", txt))
        for m in NUM_RX.findall(clean):
            v = float(m.replace(",", ""))
            if v <= 10 and v.is_integer():
                continue  # small counts ("2 shards") are not checked
            if not _known(v, nums):
                unverified.append(m)
    return sorted(set(unverified))


def validate(data: dict, pack: dict) -> tuple[RCAReport | None, dict]:
    try:
        report = RCAReport.model_validate(data)
    except ValidationError as e:
        msgs = [f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()[:8]]
        return None, {"valid": False, "errors": ["output does not match the RCA contract: " + "; ".join(msgs)],
                      "warnings": [], "corrections": [], "cited_ids": [], "unverified_numbers": []}
    ids = valid_ids(pack)
    cited = _cited_ids(report)
    remediation_errors, corrections, warnings = _remediation_checks(report)
    errors = _evidence_errors(report, cited, ids) + _policy_errors(report, pack) + remediation_errors
    unverified = _unverified_numbers(report, pack)
    if unverified:
        warnings.append(f"numbers not found in the evidence: {', '.join(unverified[:10])}")
    return report, {"valid": not errors, "errors": errors, "warnings": warnings + corrections, "corrections": corrections,
                    "cited_ids": sorted(cited & ids), "unverified_numbers": unverified}
