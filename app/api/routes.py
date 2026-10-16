"""REST API v1 (doc §9.2). Authentication by API key (header X-API-Key) when SENTINEL_API_KEY is set;
re-analysis is rate limited and creates a new RCA version, never a duplicate incident."""
from __future__ import annotations

import hashlib
import hmac
import logging
import threading
import time
from collections import defaultdict, deque
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from ..ingestion.reader import save_upload
from ..observability import METRICS
from ..pipeline import run_analysis
from ..schemas import ClusterHealth

audit = logging.getLogger("sentinel.audit")
UPLOAD_CHUNK = 1024 * 1024


def get_ctx(request: Request):
    return request.app.state.ctx


def require_key(request: Request, x_api_key: Optional[str] = Header(default=None)):
    expected = request.app.state.ctx.settings.api_key
    if expected and not (x_api_key and hmac.compare_digest(x_api_key, expected)):
        raise HTTPException(status_code=401, detail="missing or invalid API key")
    return x_api_key or "anonymous"


def caller_id(key: str) -> str:
    """Stable, non-reversible caller identity for audit logs (never log the key itself)."""
    return "anonymous" if key == "anonymous" else "key:" + hashlib.sha256(key.encode()).hexdigest()[:12]


def _audit(request: Request, key: str, action: str, **details):
    audit.info("audit action=%s caller=%s client=%s %s", action, caller_id(key),
               request.client.host if request.client else "-",
               " ".join(f"{k}={v}" for k, v in details.items()))


class RateLimiter:
    def __init__(self, per_minute: int):
        self.per_minute, self.hits, self.lock = per_minute, defaultdict(deque), threading.Lock()

    def check(self, key: str):
        now = time.time()
        with self.lock:
            q = self.hits[key]
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) >= self.per_minute:
                raise HTTPException(status_code=429, detail="re-analysis rate limit exceeded; try again later")
            q.append(now)


router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_key)])


class ReanalyzeRequest(BaseModel):
    model: Optional[str] = None
    prompt_version: Optional[str] = None


def _rca_summary(ctx, inc) -> dict:
    rec = ctx.repo.rca(inc.incident_id)
    return {"status": inc.rca_status, "version": inc.rca_version,
            "source": rec.source if rec else None, "model": rec.model if rec else None,
            "reason": rec.reason if rec else None, "created_at": rec.created_at if rec else None,
            "confidence": rec.report.probable_root_cause.confidence if rec and rec.report else None,
            "summary": rec.report.incident_summary if rec and rec.report else None}


@router.get("/overview")
def overview(ctx=Depends(get_ctx)):
    ov = ctx.repo.get("overview") or {"empty": True}
    n, _ = ctx.repo.parse_errors(ctx.settings.cluster_id, 1)
    return {**ov, "parse_errors": n, "llm": ctx.rca.client.describe(), "llm_enabled": ctx.rca.client.enabled,
            "rca_queue_depth": ctx.rca.q.qsize(), "auth_enabled": bool(ctx.settings.api_key),
            "poller": ctx.poller_state}


@router.post("/logs/upload")
async def upload(request: Request, file: UploadFile = File(...), node_id: str = Form(...), source_type: str = Form(...),
                 key: str = Depends(require_key), ctx=Depends(get_ctx)):
    # Oversized requests with a Content-Length are refused by middleware before the body is read (app/main.py).
    # This loop is the backstop for chunked uploads: it stops as soon as the limit is passed.
    limit = ctx.settings.max_upload_mb * 1024 * 1024
    chunks, size = [], 0
    while chunk := await file.read(UPLOAD_CHUNK):
        size += len(chunk)
        if size > limit:
            _audit(request, key, "upload_rejected", node=node_id, reason="too_large")
            raise HTTPException(413, f"file too large ({ctx.settings.max_upload_mb} MB limit)")
        chunks.append(chunk)
    try:
        path = save_upload(ctx.settings, node_id, source_type, file.filename or "upload.log", b"".join(chunks))
    except ValueError as e:
        _audit(request, key, "upload_rejected", node=repr(node_id)[:80], reason="invalid_input")
        raise HTTPException(400, str(e)) from None
    _audit(request, key, "upload", node=node_id, source=source_type, bytes=size, stored_as=path.name)
    # analysis is CPU-bound and takes seconds: run it off the event loop so health checks and other requests keep flowing
    ov = await run_in_threadpool(run_analysis, ctx.repo, ctx.settings, ctx.rca)
    return {"stored_as": path.name, "node_id": node_id, "source_type": source_type, "analysis": ov}


@router.post("/analysis/run")
def analysis_run(request: Request, key: str = Depends(require_key), ctx=Depends(get_ctx)):
    _audit(request, key, "analysis_run")
    return run_analysis(ctx.repo, ctx.settings, ctx.rca)


@router.get("/health/cluster")
def health_cluster(ctx=Depends(get_ctx)):
    h = ctx.repo.get("cluster_health")
    if not h:
        return {"state": "unknown", "partial": True, "partial_reasons": ["no data ingested yet"], "nodes": {}, "shards": {}}
    return h


@router.get("/health/nodes")
def health_nodes(ctx=Depends(get_ctx)):
    h = ctx.repo.get("cluster_health")
    return list(ClusterHealth.model_validate(h).nodes.values()) if h else []


@router.get("/health/timeline")
def health_timeline(ctx=Depends(get_ctx)):
    return ctx.repo.get("timeline", {"nodes": {}, "cluster": []})


@router.get("/incidents")
def incidents(since: Optional[str] = None, until: Optional[str] = None, node: Optional[str] = None,
              severity: Optional[str] = None, status: Optional[str] = None, ctx=Depends(get_ctx)):
    out = []
    sig = {x.signal_id: x for x in ctx.repo.signals(ctx.settings.cluster_id)}
    for inc in ctx.repo.incidents(ctx.settings.cluster_id, since, until, node, severity, status):
        d = inc.model_dump(mode="json", exclude={"links"})
        d["root_signal"] = sig[inc.root_signal_id].model_dump(mode="json", exclude={"event_ids"}) if inc.root_signal_id in sig else None
        d["rca"] = _rca_summary(ctx, inc)
        out.append(d)
    return out


@router.get("/incidents/{incident_id}")
def incident(incident_id: str, ctx=Depends(get_ctx)):
    inc = ctx.repo.incident(incident_id)
    if not inc:
        raise HTTPException(404, "incident not found")
    sig = {x.signal_id: x for x in ctx.repo.signals(ctx.settings.cluster_id)}
    rec = ctx.repo.rca(incident_id)
    return {"incident": inc.model_dump(mode="json"),
            "signals": [sig[i].model_dump(mode="json") for i in inc.signal_ids if i in sig],
            "rca": rec.model_dump(mode="json") if rec else None,
            "rca_versions": ctx.repo.rca_versions(incident_id)}


@router.get("/incidents/{incident_id}/rca")
def incident_rca(incident_id: str, version: Optional[int] = None, ctx=Depends(get_ctx)):
    rec = ctx.repo.rca(incident_id, version)
    if not rec:
        raise HTTPException(404, "no RCA for this incident/version")
    return rec.model_dump(mode="json")


@router.post("/incidents/{incident_id}/reanalyze", status_code=202)
def reanalyze(request: Request, incident_id: str, body: ReanalyzeRequest | None = None, sync: bool = False,
              key: str = Depends(require_key), ctx=Depends(get_ctx)):
    if not ctx.repo.incident(incident_id):
        raise HTTPException(404, "incident not found")
    ctx.limiter.check(key)
    body = body or ReanalyzeRequest()
    from ..rca.prompts import PROMPTS
    if body.prompt_version and body.prompt_version not in PROMPTS:
        raise HTTPException(400, f"unknown prompt_version; available: {sorted(PROMPTS)}")
    allowed = ctx.rca.allowed_models()
    if body.model and body.model not in allowed:   # stops callers picking an arbitrary (expensive) model
        _audit(request, key, "reanalyze_rejected", incident=incident_id, model=repr(body.model)[:80], reason="model_not_allowed")
        raise HTTPException(400, f"model not allowed; allowed: {sorted(allowed)}")
    _audit(request, key, "reanalyze", incident=incident_id, model=body.model or "default",
           prompt=body.prompt_version or "default", sync=sync)
    if sync:
        rec = ctx.rca.analyze(incident_id, trigger="reanalyze", model=body.model, prompt_version=body.prompt_version)
        return {"queued": False, "version": rec.version, "status": rec.status, "source": rec.source}
    queued = ctx.rca.enqueue(incident_id, trigger="reanalyze", model=body.model, prompt_version=body.prompt_version)
    return {"queued": queued, "detail": "re-analysis queued" if queued else "already queued"}


@router.get("/signals/{signal_id}/events")
def signal_events(signal_id: str, limit: int = Query(50, le=500), ctx=Depends(get_ctx)):
    sig = next((x for x in ctx.repo.signals(ctx.settings.cluster_id) if x.signal_id == signal_id), None)
    if not sig:
        raise HTTPException(404, "signal not found")
    if sig.event_ids:
        evs = ctx.repo.events_by_ids(sig.event_ids[:limit])
    else:  # metric/baseline signal: show WARN/ERROR records of that node within its window
        evs = [e for e in ctx.repo.events(ctx.settings.cluster_id, sig.start.timestamp(), sig.end.timestamp(), sig.node_id)
               if e.severity in ("WARN", "ERROR")][:limit]
    return [e.model_dump(mode="json") for e in evs]


@router.get("/events")
def events(node: Optional[str] = None, event_type: Optional[str] = None, since: Optional[float] = None,
           until: Optional[float] = None, ids: Optional[str] = None, limit: int = Query(200, le=2000), ctx=Depends(get_ctx)):
    if ids:
        return [e.model_dump(mode="json") for e in ctx.repo.events_by_ids(ids.split(",")[:limit])]
    return [e.model_dump(mode="json") for e in
            ctx.repo.events(ctx.settings.cluster_id, since or 0, until, node, event_type, limit)]


@router.get("/parse-errors")
def parse_errors(limit: int = Query(100, le=1000), ctx=Depends(get_ctx)):
    n, rows = ctx.repo.parse_errors(ctx.settings.cluster_id, limit)
    return {"total": n, "items": rows}


@router.get("/node-metrics")
def node_metrics(ctx=Depends(get_ctx)):
    return ctx.repo.get("series", {})


@router.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return METRICS.render_prometheus()
