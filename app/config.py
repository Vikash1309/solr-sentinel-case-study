"""Settings. Every threshold used by detection, correlation and health is configurable here, so it can be
tuned per cluster (doc §5.1, §12) without touching code. Override any field with an environment variable
named SENTINEL_<FIELD_NAME_IN_UPPER_CASE>; LLM settings also accept the short names in .env.example."""
from __future__ import annotations

import os
from dataclasses import dataclass, fields
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SEVERITIES = ("info", "warning", "critical")
SEVERITY_RANK = {s: i for i, s in enumerate(SEVERITIES)}


ENVIRONMENTS = ("development", "production")
DEV_ONLY_PROVIDERS = ("gemini", "groq", "openrouter", "ollama")   # free tiers / local: not for production data
PLACEHOLDER_KEYS = ("change-me", "changeme", "secret", "password")


@dataclass
class Settings:
    # ------------------------------------------------------------------ deployment
    # production = refuse to start with unsafe settings (see validate()). Env: SENTINEL_ENV
    env: str = "development"
    log_level: str = "INFO"
    log_format: str = "text"                                  # text | json (one JSON object per line, for log shipping)

    # ------------------------------------------------------------------ identity & paths
    cluster_id: str = "solrcloud-prod-1"
    log_dir: str = str(ROOT / "sample_data" / "logs")        # <log_dir>/<node_id>/{solr*.log,gc*.log}
    upload_dir: str = str(ROOT / "data" / "uploads")
    db_path: str = str(ROOT / "data" / "sentinel.db")

    # ------------------------------------------------------------------ time
    # "data": now = newest event timestamp (replayed / sample logs). "wall": now = wall clock (live collection).
    clock: str = "data"
    analysis_lookback_hours: int = 24
    metric_window_s: int = 60

    # ------------------------------------------------------------------ cluster facts (production: Solr APIs / CMDB)
    zk_client_timeout_ms: int = 15000
    telemetry_stale_after_s: int = 300                    # no records from a source for this long -> stale

    # ------------------------------------------------------------------ detection thresholds
    burst_gap_s: int = 120
    gc_pause_warn_ms: float = 1000
    gc_pause_crit_ms: float = 5000
    gc_overhead_warn_pct: float = 10
    gc_overhead_crit_pct: float = 25
    heap_after_gc_warn_pct: float = 85
    heap_after_gc_crit_pct: float = 92
    humongous_burst_min: int = 5
    slow_query_ms: int = 1000
    slow_query_burst_min: int = 5
    expensive_query_burst_min: int = 3
    explicit_commits_per_min: int = 6
    remote_timeout_burst_min: int = 2
    p95_qtime_floor_ms: float = 500
    error_burst_min: int = 5
    baseline_window_buckets: int = 30
    baseline_min_samples: int = 5
    mad_k: float = 3.5

    # ------------------------------------------------------------------ correlation
    recurrence_window_h: int = 6

    # ------------------------------------------------------------------ RCA / LLM
    llm_provider: str = os.getenv("LLM_PROVIDER", "auto")    # auto|bedrock|gateway|vllm|anthropic|gemini|groq|openrouter|openai|ollama|none
    llm_model: str = os.getenv("LLM_MODEL", "")               # fast tier ("" = provider preset)
    llm_model_strong: str = os.getenv("LLM_MODEL_STRONG", "") # strong tier for critical incidents
    llm_base_url: str = os.getenv("LLM_BASE_URL", "")
    llm_timeout_s: int = 90
    llm_min_severity: str = "warning"                         # below this: deterministic RCA only (cost control)
    llm_strong_min_severity: str = "critical"
    llm_max_evidence_items: int = 25                           # representative log records sent to the model
    llm_max_signals: int = 40                                  # signals sent to the model (root cause always kept)
    rca_workers: int = 1                                       # bounded RCA concurrency
    rca_retry_base_s: int = 300                                # provider outage: retry after 5, 10, 20, 40 min ...
    rca_retry_max: int = 4                                     # ... then keep the rules-based RCA
    llm_allowed_models: str = ""                               # extra models re-analysis may request (comma-separated)
    redact_query_terms: bool = True

    # ------------------------------------------------------------------ API
    api_key: str = os.getenv("SENTINEL_API_KEY", "")          # empty = auth disabled (local dev only)
    reanalyze_per_minute: int = 5
    max_upload_mb: int = 50
    poll_interval_s: int = 15


def load_settings(**overrides) -> Settings:
    s = Settings()
    for f in fields(s):
        env = os.getenv("SENTINEL_" + f.name.upper())
        if env is not None:
            cur = getattr(s, f.name)
            try:
                value = env.lower() not in ("0", "false", "no", "") if isinstance(cur, bool) else type(cur)(env)
            except ValueError:
                raise ValueError(f"SENTINEL_{f.name.upper()}={env!r} is not a valid {type(cur).__name__}") from None
            setattr(s, f.name, value)
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def validate(s: Settings) -> list[str]:
    """Problems that must stop the service from starting. Production mode adds the safety rules."""
    errors = []
    choices = {"env": ENVIRONMENTS, "clock": ("data", "wall"), "log_format": ("text", "json"),
               "log_level": ("DEBUG", "INFO", "WARNING", "ERROR"),
               "llm_min_severity": SEVERITIES, "llm_strong_min_severity": SEVERITIES}
    for name, allowed in choices.items():
        if getattr(s, name) not in allowed:
            errors.append(f"{name}={getattr(s, name)!r} must be one of {', '.join(allowed)}")
    for name in ("poll_interval_s", "rca_workers", "rca_retry_base_s", "max_upload_mb", "reanalyze_per_minute",
                 "llm_timeout_s", "llm_max_evidence_items", "llm_max_signals", "analysis_lookback_hours", "metric_window_s"):
        if getattr(s, name) <= 0:
            errors.append(f"{name} must be greater than 0")
    if s.env == "production":
        if not s.api_key:
            errors.append("SENTINEL_API_KEY is required in production (API authentication would be off)")
        elif s.api_key.lower() in PLACEHOLDER_KEYS or len(s.api_key) < 24:
            errors.append("SENTINEL_API_KEY must be a random secret of at least 24 characters, not a placeholder")
        if s.clock != "wall":
            errors.append("SENTINEL_CLOCK must be 'wall' in production ('data' is for replaying sample logs)")
    return errors
