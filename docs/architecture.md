# Architecture

![Architecture](architecture.svg)

## How data flows

Every 15 seconds the API runs one analysis cycle:

1. **Ingest.** Read new bytes from each node's `solr.log` and `gc.log`. A checkpoint per file (file identity plus
   byte offset) means nothing is read twice, rotated files are picked up, and a line still being written waits
   for the next cycle. Uploaded files go through the same path.
2. **Parse.** Turn Solr and GC lines into one common event format. Multi-line stack traces stay together.
   Credentials are redacted before anything is stored. Lines that can't be parsed go to a parse-error store
   with the reason, instead of being dropped.
3. **Detect.** Rules, thresholds and statistical baselines turn events into *signals*, each with a severity.
   See [detection.md](detection.md).
4. **Correlate.** Link signals by cause and effect, across nodes, into *incidents*. Each incident gets a likely
   root cause, a severity set by fixed rules, and a link to any earlier occurrence.
5. **Assess health.** Mark each node Healthy, Degraded, Critical or Unknown, and judge the cluster from replica
   and leader availability.
6. **Explain.** Each new incident is queued for root-cause analysis. A worker builds an evidence package, asks
   the LLM, validates the answer and stores it as a new version. If that fails, it stores a rules-based report.
   See [ai-design.md](ai-design.md).

Incidents and health are saved **before** the LLM is called, and the LLM runs on a separate queue. A slow or
broken LLM therefore never delays detection or the dashboard.

Every event has an ID derived from its content, and incidents have stable IDs. Re-reading all the logs from
scratch creates no duplicates; the evaluation checks this on every run.

## Technology choices

| Layer | Prototype | Why | Production |
|---|---|---|---|
| Language | Python | Good for parsing, rules and LLM calls; one language end to end | Same |
| Collection | File reader with checkpoints, plus an upload endpoint | Works with no extra infrastructure | Filebeat or OpenTelemetry Collector on each node |
| Transport | In-process | Enough for one machine | Kafka, keyed by cluster and node; at-least-once delivery |
| Storage | SQLite | Zero setup, real SQL, transactional | OpenSearch for events, PostgreSQL for incidents and RCA versions, S3 for raw logs |
| API | FastAPI + Pydantic | Typed models, automatic docs, async-friendly | Same, behind an authenticated gateway |
| Dashboard | Streamlit | A usable UI in very little code | React, or the client's existing observability UI |
| LLM | Provider adapter | No lock-in; swap models without code changes | Claude on Amazon Bedrock in the client's account, or vLLM if air-gapped |
| Metrics and health | `/api/v1/metrics` (Prometheus), `/healthz` (liveness), `/readyz` (readiness), JSON logs | Standard, scrapeable, works with any orchestrator | Prometheus and Grafana alerts on lag, parse errors, queue depth, LLM cost |

Two choices left out on purpose:

* **No vector database as the main store.** Incident analysis depends on exact timestamps, numbers, ordering and
  node IDs, which a structured store handles better. Embeddings could help later for searching past incidents.
* **No agent framework (such as LangGraph).** One structured LLM call with validation works. Extra stages
  would be added only if the evaluation shows they improve answers.

## Where things live

| Part | Code |
|---|---|
| File reading, checkpoints | `app/ingestion/reader.py`, `checkpoints.py` |
| Common event format, RCA report format | `app/schemas.py` (`LogEvent`, `RCAReport`) |
| Solr and GC parsers | `app/parsers/solr_parser.py`, `gc_parser.py` |
| Rules and baselines | `app/detection/rules.py`, `baselines.py` |
| Node and cluster health | `app/detection/health.py` |
| Correlation, severity, known patterns | `app/correlation/engine.py`, `knowledge.py` |
| Evidence, prompt, providers, validation, fallback | `app/rca/evidence.py`, `prompts.py`, `providers.py`, `validation.py`, `fallback.py` |
| RCA queue | `app/rca/service.py` |
| Analysis cycle | `app/pipeline.py` |
| Storage | `app/storage/repository.py` |
| API, metrics | `app/api/routes.py`, `app/main.py`, `app/observability.py` |
| Redaction | `app/redaction.py` |
| Dashboard | `dashboard/app.py` |
| Evaluation | `app/evaluation.py`, `eval/ground_truth.json` |
