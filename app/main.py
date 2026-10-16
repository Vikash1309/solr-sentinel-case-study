"""FastAPI application: API v1, background log poller and bounded RCA workers.

Run:  uvicorn --factory app.main:create_app   (or `python run.py api`)

The app is built by a factory, so importing this module has no side effects (no database, no threads)."""
from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .api.routes import RateLimiter, router
from .config import DEV_ONLY_PROVIDERS, Settings, load_settings, validate
from .observability import configure_logging
from .pipeline import run_analysis
from .rca.service import RCAService
from .storage.repository import Repository

log = logging.getLogger("sentinel")


@dataclass
class AppContext:
    settings: Settings
    repo: Repository
    rca: RCAService
    limiter: RateLimiter
    poller_state: dict = field(default_factory=dict)


def _poll(ctx: AppContext):
    first = True
    while True:
        try:
            t0 = time.time()
            run_analysis(ctx.repo, ctx.settings, ctx.rca, force=first)
            first = False
            ctx.poller_state.update(last_cycle_at=time.time(), last_cycle_ms=int((time.time() - t0) * 1000), last_error=None)
        except Exception as e:  # keep polling; surface the error in /healthz and the dashboard
            log.exception("analysis cycle failed")
            ctx.poller_state.update(last_error=str(e)[:300])
        time.sleep(ctx.settings.poll_interval_s)


def create_app(settings: Settings | None = None, start_background: bool = True) -> FastAPI:
    s = settings or load_settings()
    configure_logging(s.log_level, s.log_format)
    errors = validate(s)
    if s.env == "production":
        from .rca.providers import resolve_provider
        provider = resolve_provider(s.llm_provider)
        if provider in DEV_ONLY_PROVIDERS:
            errors.append(f"LLM provider '{provider}' is for development only; use bedrock, gateway, vllm, "
                          "anthropic or openai in production (or 'none' for rules-only RCA)")
    if errors:
        raise RuntimeError("invalid configuration:\n  - " + "\n  - ".join(errors))
    repo = Repository(s.db_path)
    ctx = AppContext(settings=s, repo=repo, rca=RCAService(repo, s), limiter=RateLimiter(s.reanalyze_per_minute),
                     poller_state={"last_cycle_at": None, "last_cycle_ms": None, "last_error": None,
                                   "interval_s": s.poll_interval_s, "running": start_background})

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_background:
            ctx.rca.start_workers()
            threading.Thread(target=_poll, args=(ctx,), daemon=True, name="poller").start()
            if not s.api_key:
                log.warning("SENTINEL_API_KEY is not set: API authentication is DISABLED (local development only)")
            log.info("Solr Sentinel started: env=%s cluster=%s llm=%s auth=%s", s.env, s.cluster_id,
                     ctx.rca.client.describe(), "on" if s.api_key else "OFF")
        yield

    app = FastAPI(title="Solr Sentinel", version="2.0",
                  description="AI-powered Solr observability & root cause analysis", lifespan=lifespan)
    app.state.ctx = ctx
    app.include_router(router)

    upload_limit = s.max_upload_mb * 1024 * 1024 + 64 * 1024        # plus multipart overhead

    @app.middleware("http")
    async def refuse_oversized_uploads(request, call_next):
        """Reject a too-large upload from its Content-Length, before the framework reads the body.
        Production should set the same limit at the load balancer as well."""
        if request.method == "POST" and request.url.path.endswith("/logs/upload"):
            declared = request.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > upload_limit:
                return JSONResponse({"detail": f"file too large ({s.max_upload_mb} MB limit)"}, status_code=413)
        return await call_next(request)

    def platform_status() -> dict:
        st = ctx.poller_state
        age = None if not st.get("last_cycle_at") else round(time.time() - st["last_cycle_at"], 1)
        ok = not st.get("last_error") and (not st.get("running") or (age is not None and age < 4 * s.poll_interval_s))
        return {"status": "ok" if ok else "degraded", "seconds_since_last_cycle": age, "last_error": st.get("last_error"),
                "rca_queue_depth": ctx.rca.q.qsize(), "llm": ctx.rca.client.describe()}

    @app.get("/healthz", tags=["platform"])
    def healthz():
        """Liveness: the process is up and serving. Always 200, so a slow analysis cycle never causes a restart."""
        return platform_status()

    @app.get("/readyz", tags=["platform"])
    def readyz():
        """Readiness: 503 while analysis is failing or stale, so a load balancer can route around this instance."""
        body = platform_status()
        return JSONResponse(body, status_code=200 if body["status"] == "ok" else 503)

    return app
