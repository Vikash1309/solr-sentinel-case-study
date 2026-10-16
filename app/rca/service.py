"""RCA service (doc §7.1, §10.2):

    incident -> bounded evidence package -> provider (severity-routed model) -> schema + evidence + policy
    validation -> one bounded repair attempt -> persist as a new RCA *version* (never a duplicate incident)
    -> or fail safely to the deterministic RCA with the reason recorded.

RCAs run on a bounded background queue so detection, incident persistence and the dashboard never wait
on the model. Every record stores model, prompt version, evidence hash, validation result, token usage
and latency (auditability)."""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone

from ..config import SEVERITY_RANK, Settings
from ..observability import METRICS
from ..schemas import ClusterHealth, Incident, RCARecord, Signal
from ..storage.repository import Repository
from . import evidence, fallback, prompts, validation
from .providers import LLMClient, LLMError

log = logging.getLogger("sentinel.rca")
PRICE_IN = float(os.getenv("LLM_PRICE_IN_PER_1K", "0"))
PRICE_OUT = float(os.getenv("LLM_PRICE_OUT_PER_1K", "0"))


class RCAService:
    def __init__(self, repo: Repository, settings: Settings, client: LLMClient | None = None):
        self.repo, self.s = repo, settings
        self.client = client or LLMClient(settings)
        self.q: queue.Queue = queue.Queue()
        self._queued: set[str] = set()
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []

    # ------------------------------------------------------------------------------------------ queue
    def start_workers(self):
        for i in range(max(1, self.s.rca_workers)):
            t = threading.Thread(target=self._work, daemon=True, name=f"rca-worker-{i}")
            t.start()
            self._workers.append(t)

    def enqueue(self, incident_id: str, trigger: str = "auto", model: str | None = None, prompt_version: str | None = None):
        with self._lock:
            if incident_id in self._queued:
                return False
            self._queued.add(incident_id)
        self.repo.set_rca_state(incident_id, "pending")
        self.q.put((incident_id, trigger, model, prompt_version))
        METRICS.set("sentinel_rca_queue_depth", self.q.qsize())
        return True

    def _work(self):
        while True:
            iid, trigger, model, pv = self.q.get()
            try:
                self.analyze(iid, trigger=trigger, model=model, prompt_version=pv)
            except Exception:
                log.exception("RCA failed for %s", iid)
                self.repo.set_rca_state(iid, "failed")
                METRICS.inc("sentinel_rca_total", outcome="error")
            finally:
                with self._lock:
                    self._queued.discard(iid)
                METRICS.set("sentinel_rca_queue_depth", self.q.qsize())
                self.q.task_done()

    def drain(self, timeout: float = 600):
        """Wait until the queue is empty (CLI / tests)."""
        t0 = time.time()
        while (self.q.unfinished_tasks and time.time() - t0 < timeout):
            time.sleep(0.05)

    # ------------------------------------------------------------------------------------------ policy
    def needs_rca(self, inc: Incident, signals: dict[str, Signal]) -> bool:
        latest = self.repo.rca(inc.incident_id)
        if latest is None:
            return True
        if latest.evidence_hash != evidence.evidence_hash(inc, signals, latest.prompt_version):
            return True
        if latest.source != "rules" or not self._llm_eligible(inc):
            return False
        # a provider became available after a deterministic-only analysis
        if latest.reason == "llm_disabled":
            return True
        # the provider was down (rate limit, 5xx, network): try again later, with exponential backoff
        if latest.reason == "llm_unavailable":
            return self._retry_due(inc.incident_id, latest)
        return False

    def _retry_due(self, incident_id: str, latest: RCARecord) -> bool:
        """Retry after base, 2x base, 4x base ... up to rca_retry_max consecutive failures on the same evidence."""
        failures = 0
        for v in reversed(self.repo.rca_versions(incident_id)):
            if v["source"] != "rules" or v["reason"] != "llm_unavailable" or v["evidence_hash"] != latest.evidence_hash:
                break
            failures += 1
        if failures > self.s.rca_retry_max:
            return False
        wait = self.s.rca_retry_base_s * 2 ** (failures - 1)
        due = (datetime.now(timezone.utc) - latest.created_at).total_seconds() >= wait
        if due:
            log.info("retrying RCA for %s after %d provider failure(s)", incident_id, failures)
        return due

    def _llm_eligible(self, inc: Incident) -> bool:
        return self.client.enabled and SEVERITY_RANK[inc.severity] >= SEVERITY_RANK[self.s.llm_min_severity]

    def allowed_models(self) -> set[str]:
        """Models a caller may request on re-analysis: the configured tiers plus an explicit allowlist."""
        extra = {m.strip() for m in self.s.llm_allowed_models.split(",") if m.strip()}
        return ({self.client.model, self.client.model_strong} | extra) - {""}

    # ------------------------------------------------------------------------------------------ analysis
    def analyze(self, incident_id: str, trigger: str = "auto", model: str | None = None,
                prompt_version: str | None = None) -> RCARecord:
        t0 = time.time()
        inc = self.repo.incident(incident_id)
        if inc is None:
            raise KeyError(incident_id)
        pv = prompt_version or prompts.DEFAULT_PROMPT
        if pv not in prompts.PROMPTS:
            raise ValueError(f"unknown prompt version {pv}")
        signals = {x.signal_id: x for x in self.repo.signals(self.s.cluster_id)}
        series = self.repo.get("series", {}) or {}
        h = self.repo.get("cluster_health")
        health = ClusterHealth.model_validate(h) if h else None
        prev = self.repo.incident(inc.recurrence_of) if inc.recurrence_of else None
        pack = evidence.build(inc, signals, self.repo.events_by_ids, series, health, self.s, prev)
        model_pack = evidence.for_model(pack)
        rec = RCARecord(incident_id=inc.incident_id, version=0, status="fallback", source="rules", trigger=trigger,
                        prompt_version=pv, evidence_hash=evidence.evidence_hash(inc, signals, pv),
                        created_at=datetime.now(timezone.utc), evidence=pack)
        if self._llm_eligible(inc):
            chosen = model or self.client.model_for(inc.severity)
            messages = [{"role": "user", "content": prompts.user_message(model_pack)}]
            for _ in range(2):                                   # first attempt + one bounded repair
                try:
                    text, usage = self.client.complete(prompts.PROMPTS[pv], messages, model=chosen)
                except LLMError as e:
                    rec.attempts.append({"error": str(e)[:500]})
                    rec.reason = "llm_unavailable"
                    break
                METRICS.inc("sentinel_llm_tokens_total", usage.get("input_tokens") or 0, direction="input")
                METRICS.inc("sentinel_llm_tokens_total", usage.get("output_tokens") or 0, direction="output")
                METRICS.inc("sentinel_llm_cost_usd_total", (usage.get("input_tokens") or 0) / 1000 * PRICE_IN +
                            (usage.get("output_tokens") or 0) / 1000 * PRICE_OUT)
                try:
                    report, check = validation.validate(validation.extract_json(text), model_pack)
                except (ValueError, TypeError) as e:
                    report, check = None, {"valid": False, "errors": [f"unparseable output: {e}"], "warnings": []}
                rec.attempts.append({"usage": usage, "validation": check})
                if check["valid"]:
                    rec.status, rec.source, rec.report, rec.validation = "completed", "llm", report, check
                    rec.model, rec.reason = self.client.describe(chosen), None
                    break
                rec.reason = "llm_output_invalid"
                messages += [{"role": "assistant", "content": text},
                             {"role": "user", "content": prompts.repair_message(check["errors"])}]
        else:
            rec.reason = "llm_disabled" if not self.client.enabled else "below_llm_severity_threshold"
        if rec.source == "rules":
            report = fallback.build(inc, signals, pack, rec.reason or "llm_disabled")
            _, check = validation.validate(report.model_dump(), model_pack)
            rec.report, rec.validation = report, check
        version = self.repo.add_rca(rec)
        METRICS.inc("sentinel_rca_total", outcome=f"{rec.source}:{rec.reason or 'ok'}")
        METRICS.observe("sentinel_rca_latency_seconds", time.time() - t0, source=rec.source)
        log.info("RCA %s v%d via %s (%s)", inc.incident_id, version, rec.source, rec.reason or "ok")
        return rec
