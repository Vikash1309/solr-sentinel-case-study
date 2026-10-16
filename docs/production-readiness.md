# Production readiness

What is hardened today, how to run it in production, and what is still prototype-grade, with the fix for each.

The short version: the **code and container are built to production standards**. The **storage and scaling
model is still single-node**: SQLite, one API instance. That's the main piece of work before real production
traffic, and the design for it already exists ([architecture.md](architecture.md)).

## Hardened today

### Security

| Control | How |
|---|---|
| Authentication | API key on every `/api/v1` endpoint, compared in constant time. **Production mode refuses to start** without a key, or with a placeholder or short key |
| Audit trail | Every upload, analysis run and re-analysis, **including rejected attempts**, is logged with a key fingerprint (never the key), client address and parameters (`sentinel.audit` logger) |
| Upload safety | `node_id` is checked against a strict allowlist, and the resolved path must stay inside the upload folder. An upload whose declared size is over the limit is refused before its body is read; a chunked upload is cut off as soon as it passes the limit. Set the same limit at the load balancer too |
| Cost control | Re-analysis can only request models on an allowlist (`SENTINEL_LLM_ALLOWED_MODELS`). It is rate limited per caller. Incidents below *warning* never call the LLM |
| LLM safety | The model is read-only with no tools, sees only a redacted evidence package, and every answer is validated. See [ai-design.md](ai-design.md) |
| Data protection | Credentials and tokens are redacted before storage. Search terms are redacted before any LLM call. Development-only LLM providers (free tiers, local models) are **refused in production mode** |
| Secrets | Read from environment variables only, never from files baked into the image. `.env` is excluded from the image and from git, and CI scans every commit for leaked secrets |

### Container

| Control | How |
|---|---|
| Least privilege | Runs as an unprivileged user (uid 10001). Read-only root filesystem, all Linux capabilities dropped, `no-new-privileges` |
| Reproducible builds | Base image pinned by digest. Every dependency pinned to an exact version **with a hash** (`requirements.lock`), so a tampered package fails the build |
| Small surface | Test tools live in a separate `test` stage and are not in the production image. The build context excludes `.env`, git history and docs |
| Health | `/healthz` (liveness, always 200 while the process is up) and `/readyz` (503 while analysis is failing or stale), plus Docker health checks for both services |
| Resource limits | CPU and memory limits on both services |

### Reliability

| Control | How |
|---|---|
| LLM outages | Calls retry with backoff inside the request. If the provider is still down, the incident gets the rules-based report and the AI analysis is **retried later**: after 5, 10, 20 and 40 minutes, then it stops |
| No duplicates | Event and incident IDs are derived from content, so re-reading logs after a crash never duplicates anything |
| Isolation | Incidents are saved before any LLM call, and the LLM runs on its own queue. A slow model never blocks detection |
| Fail fast | Every setting is validated at startup, and a bad value stops the service with a clear message instead of failing later |

### Operations

| Control | How |
|---|---|
| Logs | `SENTINEL_LOG_FORMAT=json` writes one JSON object per line (app and web server), ready for CloudWatch, Datadog or Fluent Bit |
| Metrics | `/api/v1/metrics` in Prometheus format: ingest volume and lag, parse errors, analysis time, RCA queue depth, outcomes, latency, tokens and cost |
| CI | Every push: secret scan, lint (including security rules and a complexity cap), type check (mypy), the full test suite, dependency vulnerability audit, tests inside the image, a non-root check, a check that production mode rejects unsafe settings, and an image vulnerability scan |
| Code health | Type-checked with mypy (no errors). No function exceeds a cyclomatic complexity of 15, enforced by lint. Every GitHub Action is pinned to a commit SHA |

## Running in production

The same image runs everywhere. Production differs only in configuration:

| Setting | Production value | Why |
|---|---|---|
| `SENTINEL_ENV` | `production` | Turns on the startup safety checks |
| `SENTINEL_API_KEY` | ≥ 24 random characters, from a secrets manager | Required in production |
| `SENTINEL_CLOCK` | `wall` | `data` is only for replaying sample logs |
| `SENTINEL_LOG_FORMAT` | `json` | Structured logs for the log platform |
| `SENTINEL_LOG_DIR` | Mounted directory of shipped logs | Instead of the sample data |
| `LLM_PROVIDER` | `bedrock` (IAM role, no keys), `gateway`, `vllm`, `anthropic` or `openai` | Free tiers and local models are refused |

On AWS, which matches the design's choice of Claude on Bedrock:
- **Compute:** ECS Fargate or EKS.
- **Dashboard access:** behind an ALB with OIDC sign-in (company SSO).
- **Secrets:** Secrets Manager.
- **Logs:** CloudWatch.
- **Metrics:** Managed Prometheus.
- **Model access:** an IAM role for Bedrock, so there are no LLM keys at all.

## Still prototype-grade

These are deliberate scope limits for the case study, not oversights. Each has a known fix.

| Gap | Risk today | Fix |
|---|---|---|
| SQLite on a local volume | One node only; no high availability; backups are manual | PostgreSQL for incidents and RCA versions, OpenSearch for events, S3 for raw logs. Only `app/storage/repository.py` changes |
| One API instance runs the poller and the RCA workers | Two replicas would analyse the same data twice | Split into an API service (scales freely) and a worker service with a single leader, or partition work by cluster |
| RCA queue is in memory | On restart, incidents never analysed are re-queued automatically, but a queued *manual* re-analysis is lost | Persist the queue: a database table, or SQS |
| Dashboard has no sign-in of its own | Anyone who can reach it can use it | Never expose it directly: put it behind SSO (ALB OIDC or oauth2-proxy) |
| One shared API key, no roles | No per-person accountability or read-only access | OIDC/JWT from the company identity provider, with viewer and operator roles. The audit log already records every action |
| Rate limits live in each process | Not shared across replicas | Enforce at the API gateway |
| Each cycle re-analyses the last 24 h (≈ 1.5 s for 19k events) | Too slow at high volume | Incremental processing on a stream (Kafka) with event-time windows |
| One cluster per deployment | Several clusters need several deployments | Add `cluster_id` to every query and partition by cluster |
| No schema migrations | Schema changes need manual care | Alembic migrations, introduced with PostgreSQL |
| Bedrock client (`boto3`) not in the lockfile | The production LLM needs one extra dependency | Add `boto3` to `requirements.txt` and regenerate the lock |
