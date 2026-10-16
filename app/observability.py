"""Self-observability of the platform (doc §2.2, §11.3): ingestion volume and lag, parse failures,
processing latency, RCA queue depth, RCA outcomes, latency, tokens and model cost.
Exposed in Prometheus text format at GET /api/v1/metrics (no extra dependency).

Logging is configured here too, so it works however the app is started (uvicorn, run.py, tests)."""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """One JSON object per line, for log shippers (Fluent Bit, CloudWatch, Datadog ...)."""

    def format(self, record: logging.LogRecord) -> str:
        out = {"ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
               "level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, ensure_ascii=False)


class _SentinelHandler(logging.StreamHandler):
    """Marker type, so reconfiguring replaces only our own handler and leaves others (e.g. test capture) alone."""


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    """Send application logs (and uvicorn's) to stderr in one consistent format. Safe to call more than once."""
    handler = _SentinelHandler()
    handler.setFormatter(JsonFormatter() if fmt == "json" else
                         logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers = [h for h in root.handlers if not isinstance(h, _SentinelHandler)] + [handler]
    root.setLevel(level)
    if fmt == "json":   # uvicorn installs its own text handlers; route its logs through ours instead
        for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
            lg = logging.getLogger(name)
            lg.handlers, lg.propagate = [], True


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counters: dict[tuple, float] = defaultdict(float)
        self.gauges: dict[tuple, float] = {}
        self.summaries: dict[tuple, list[float]] = defaultdict(lambda: [0.0, 0.0])  # sum, count
        self.help = {
            "sentinel_events_ingested_total": "Normalized events stored (after deduplication)",
            "sentinel_events_duplicate_total": "Events skipped as duplicates (replay / re-read)",
            "sentinel_parse_errors_total": "Records that could not be parsed (kept in the parse-error store)",
            "sentinel_records_skipped_total": "Well-formed records without diagnostic value (e.g. GC heap region lines)",
            "sentinel_ingest_lag_seconds": "Wall-clock now minus newest event timestamp",
            "sentinel_analysis_duration_seconds": "Detection + correlation + health cycle duration",
            "sentinel_rca_queue_depth": "Incidents waiting for RCA",
            "sentinel_rca_total": "RCA attempts by outcome",
            "sentinel_rca_latency_seconds": "End-to-end RCA latency",
            "sentinel_llm_tokens_total": "LLM tokens by direction",
            "sentinel_llm_cost_usd_total": "Estimated LLM cost (configured price per 1k tokens)",
            "sentinel_incidents": "Incidents by status and severity",
        }

    def inc(self, name: str, value: float = 1, **labels):
        with self._lock:
            self.counters[(name, tuple(sorted(labels.items())))] += value

    def set(self, name: str, value: float, **labels):
        with self._lock:
            self.gauges[(name, tuple(sorted(labels.items())))] = value

    def observe(self, name: str, value: float, **labels):
        with self._lock:
            s = self.summaries[(name, tuple(sorted(labels.items())))]
            s[0] += value
            s[1] += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {"counters": {f"{n}{dict(l)}": v for (n, l), v in self.counters.items()},
                    "gauges": {f"{n}{dict(l)}": v for (n, l), v in self.gauges.items()}}

    def render_prometheus(self) -> str:
        def lbl(labels):
            return "{" + ",".join(f'{k}="{v}"' for k, v in labels) + "}" if labels else ""
        out, seen = [], set()
        with self._lock:
            items = ([(n, l, v, "counter") for (n, l), v in self.counters.items()] +
                     [(n, l, v, "gauge") for (n, l), v in self.gauges.items()])
            for n, l, v, kind in sorted(items, key=lambda x: (x[0], x[1])):
                if n not in seen:
                    seen.add(n)
                    out.append(f"# HELP {n} {self.help.get(n, n)}")
                    out.append(f"# TYPE {n} {kind}")
                out.append(f"{n}{lbl(l)} {v}")
            for (n, l), (s, c) in sorted(self.summaries.items()):
                if n not in seen:
                    seen.add(n)
                    out.append(f"# HELP {n} {self.help.get(n, n)}")
                    out.append(f"# TYPE {n} summary")
                out.append(f"{n}_sum{lbl(l)} {s}")
                out.append(f"{n}_count{lbl(l)} {c}")
        return "\n".join(out) + "\n"


METRICS = Metrics()


class Timer:
    def __init__(self, name: str, **labels):
        self.name, self.labels = name, labels

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.elapsed = time.perf_counter() - self.t0
        METRICS.observe(self.name, self.elapsed, **self.labels)
