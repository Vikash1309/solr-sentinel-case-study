"""Canonical data contracts (doc §4.2, §5.3, §6, §7.4).

Vocabulary (doc §5.3): a *signal* is an event or metric violation; an *incident* is a group of related
signals; *health* is the current assessment of a node or the cluster."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

Severity = Literal["info", "warning", "critical"]
HealthState = Literal["healthy", "degraded", "critical", "unknown"]
Relation = Literal["observed", "inferred", "temporal"]
Confidence = Literal["low", "medium", "high"]
Trigger = Literal["auto", "reanalyze"]
RCAState = Literal["pending", "completed", "fallback", "failed"]


# ---------------------------------------------------------------------------------------------- ingestion
class LogEvent(BaseModel):
    """One normalized log record. Unknown values stay unknown (absent / None), never 0."""
    event_id: str                      # stable content hash -> idempotent replay / deduplication
    timestamp: datetime                # UTC
    ingested_at: datetime
    cluster_id: str
    node_id: str
    source_type: Literal["solr", "gc"]
    severity: Optional[str] = None     # source log level (INFO/WARN/ERROR) when present
    event_type: str                    # normalized: solr_request, solr_slow_request, solr_update, solr_log, gc_pause, ...
    message: str
    attributes: dict[str, Any] = Field(default_factory=dict)
    source_file: Optional[str] = None
    source_offset: Optional[int] = None
    parser_version: str = "1.0"
    raw: str = ""                      # original record (incl. folded stack-trace lines) for forensics

    def attr(self, key: str, default=None):
        return self.attributes.get(key, default)


class ParseErrorRecord(BaseModel):
    cluster_id: str
    node_id: str
    source_type: str
    source_file: str
    source_offset: int
    raw: str
    reason: str
    parser_version: str
    ingested_at: datetime


# ---------------------------------------------------------------------------------------------- detection
class Signal(BaseModel):
    signal_id: str
    cluster_id: str
    node_id: str
    signal_type: str
    category: str                      # gc | zookeeper | replication | query | indexing | resource | errors | telemetry | cluster
    severity: Severity
    start: datetime
    end: datetime
    count: int
    title: str
    detail: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    event_ids: list[str] = Field(default_factory=list)
    target_node: Optional[str] = None  # node referenced by the evidence (e.g. a timed-out peer)
    method: Literal["rule", "threshold", "baseline"] = "rule"


class CausalLink(BaseModel):
    cause: str
    effect: str
    relation: Relation                 # observed: the log itself references the relationship
                                       # inferred: known mechanism + consistent timing
                                       # temporal: time proximity only
    reason: str


class Hypothesis(BaseModel):
    pattern_id: str
    name: str
    score: float
    matched: list[str]
    contradicting: list[str]


class Incident(BaseModel):
    incident_id: str
    fingerprint: str
    cluster_id: str
    title: str
    severity: Severity                 # deterministic policy - the LLM never overrides it
    severity_reason: str
    status: Literal["open", "resolved"]
    first_seen: datetime
    last_seen: datetime
    nodes: list[str]
    root_signal_id: str
    signal_ids: list[str]
    links: list[CausalLink]
    hypotheses: list[Hypothesis]
    recurrence_of: Optional[str] = None
    rca_status: RCAState = "pending"
    rca_version: int = 0


# ---------------------------------------------------------------------------------------------- health
class SourceTelemetry(BaseModel):
    source_type: str
    last_seen: Optional[datetime]
    age_s: Optional[float]
    stale: bool


class NodeHealth(BaseModel):
    node_id: str
    state: HealthState
    score: Optional[int]               # None when unknown
    reasons: list[str]
    telemetry: list[SourceTelemetry]
    heap_after_gc_pct: Optional[float] = None
    max_gc_pause_ms_5m: Optional[float] = None
    gc_overhead_pct_5m: Optional[float] = None
    p95_qtime_ms_5m: Optional[float] = None
    errors_5m: Optional[int] = None
    requests_5m: Optional[int] = None
    leader_of: list[str] = Field(default_factory=list)
    replicas: dict[str, str] = Field(default_factory=dict)
    open_signals: list[dict] = Field(default_factory=list)


class ClusterHealth(BaseModel):
    cluster_id: str
    state: HealthState
    partial: bool                      # assessment based on incomplete topology/telemetry
    partial_reasons: list[str]
    reasons: list[str]
    assessed_at: datetime              # "now" used for the assessment
    nodes: dict[str, NodeHealth]
    shards: dict[str, dict]


# ---------------------------------------------------------------------------------------------- RCA output contract (doc §7.4)
class RootCause(BaseModel):
    hypothesis: str
    confidence: Confidence
    supporting_evidence_ids: list[str]
    contradicting_evidence_ids: list[str] = Field(default_factory=list)
    missing_evidence: list[str] = Field(default_factory=list)


class OperationalImpact(BaseModel):
    severity: Severity
    affected_nodes: list[str]
    observed_symptoms: list[str]
    potential_consequences: list[str]
    business_impact: str = ""          # extension: the case study asks for business impact explicitly


class Remediation(BaseModel):
    action: str
    priority: Literal["low", "medium", "high"]
    type: Literal["diagnostic", "mitigation", "corrective"]
    evidence_ids: list[str] = Field(default_factory=list)
    requires_human_approval: bool = True


class AlternativeHypothesis(BaseModel):
    hypothesis: str
    assessment: str                    # why it is less likely / what would confirm it
    evidence_ids: list[str] = Field(default_factory=list)


class RCAReport(BaseModel):
    incident_summary: str
    probable_root_cause: RootCause
    operational_impact: OperationalImpact
    recommended_remediation: list[Remediation]
    alternative_hypotheses: list[AlternativeHypothesis] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)


class RCARecord(BaseModel):
    incident_id: str
    version: int
    status: Literal["completed", "fallback", "failed"]
    source: Literal["llm", "rules"]
    reason: Optional[str] = None       # why the fallback was used
    trigger: Trigger = "auto"
    model: Optional[str] = None
    prompt_version: str
    evidence_hash: str
    created_at: datetime
    report: Optional[RCAReport] = None
    validation: dict = Field(default_factory=dict)
    evidence: dict = Field(default_factory=dict)
    attempts: list[dict] = Field(default_factory=list)
