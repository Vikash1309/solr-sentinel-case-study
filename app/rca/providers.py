"""LLM provider adapter (doc §3.1, §7.2): the application does not depend on one vendor.

  provider     protocol             credentials                       default model                 use
  ----------   ------------------   -------------------------------   ---------------------------   --------------------------------
  bedrock      AWS Bedrock Converse AWS credential chain + AWS_REGION BEDROCK_MODEL_ID(_STRONG)       PRODUCTION default (Claude Sonnet)
  gateway      OpenAI-compatible    LLM_API_KEY                       aliases rca-fast / rca-strong PRODUCTION via LiteLLM gateway
  vllm         OpenAI-compatible    LLM_API_KEY (optional)            LLM_MODEL                      PRODUCTION on-prem
  anthropic    Anthropic Messages   ANTHROPIC_API_KEY                 claude-sonnet-5-5             direct API
  gemini       OpenAI-compatible    GEMINI_API_KEY                    gemini-3.8-flash              development (free tier)
  groq         OpenAI-compatible    GROQ_API_KEY                      llama-3.3-70b-versatile       development (free tier)
  openrouter   OpenAI-compatible    OPENROUTER_API_KEY                ...:free                      development
  openai       OpenAI-compatible    OPENAI_API_KEY                    gpt-4o-mini                   direct API
  ollama       Ollama native API    -                                 qwen2.5:14b                   development, fully offline
  none         -                    -                                 -                             deterministic RCA only

LLM_PROVIDER=auto picks the first provider whose key is set. Severity routing: incidents at or above
llm_strong_min_severity use the strong model. Retries with exponential backoff (honouring Retry-After) on
429 / 5xx / network errors; JSON mode is dropped automatically for endpoints that reject it.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

from ..config import SEVERITY_RANK
from ..config import Settings as Config

PRESETS = {
    "bedrock": {"kind": "bedrock", "base": "", "key_env": None, "model": "", "key_optional": True},
    "anthropic": {"kind": "anthropic", "base": "https://api.anthropic.com", "key_env": "ANTHROPIC_API_KEY", "model": "claude-sonnet-5-5"},
    "gemini": {"kind": "openai", "base": "https://generativelanguage.googleapis.com/v1beta/openai", "key_env": "GEMINI_API_KEY", "model": "gemini-3.8-flash"},
    "groq": {"kind": "openai", "base": "https://api.groq.com/openai/v1", "key_env": "GROQ_API_KEY", "model": "llama-3.3-70b-versatile"},
    "openrouter": {"kind": "openai", "base": "https://openrouter.ai/api/v1", "key_env": "OPENROUTER_API_KEY", "model": "meta-llama/llama-3.3-70b-instruct:free"},
    "openai": {"kind": "openai", "base": "https://api.openai.com/v1", "key_env": "OPENAI_API_KEY", "model": "gpt-4o-mini"},
    "ollama": {"kind": "ollama", "base": "http://localhost:11434", "key_env": None, "model": "qwen2.5:14b"},
    "gateway": {"kind": "openai", "base": "http://localhost:4000/v1", "key_env": "LLM_API_KEY", "model": "rca-fast",
                "model_strong": "rca-strong", "key_optional": True},
    "vllm": {"kind": "openai", "base": "http://localhost:8000/v1", "key_env": "LLM_API_KEY", "model": "",
             "key_optional": True},
}
AUTO_ORDER = ["anthropic", "gemini", "groq", "openrouter", "openai"]
RETRYABLE = {408, 409, 429, 500, 502, 503, 504, 529}


class LLMError(Exception):
    pass


class _HTTPError(Exception):
    def __init__(self, code, body, retry_after=None):
        super().__init__(f"HTTP {code}: {body[:300]}")
        self.code, self.body, self.retry_after = code, body, retry_after


def resolve_provider(name: str) -> str:
    name = (name or "auto").lower()
    if name != "auto":
        return name
    for p in AUTO_ORDER:
        if os.getenv(PRESETS[p]["key_env"]):
            return p
    return "none"


class LLMClient:
    def __init__(self, cfg: Config, provider: str | None = None, model: str | None = None, base_url: str | None = None,
                 max_retries: int = 3):
        self.cfg = cfg
        self.provider = resolve_provider(provider or cfg.llm_provider)
        preset = PRESETS.get(self.provider)
        self.kind = preset["kind"] if preset else None
        if self.provider == "bedrock":
            preset = dict(preset, model=os.getenv("BEDROCK_MODEL_ID", ""), model_strong=os.getenv("BEDROCK_MODEL_ID_STRONG", ""))
        self.model = model or cfg.llm_model or (preset["model"] if preset else "")
        # strong tier for high-severity incidents; defaults to the same model when no tier is configured
        self.model_strong = (model or cfg.llm_model_strong or (preset or {}).get("model_strong") or self.model)
        self.key_optional = bool(preset and preset.get("key_optional"))
        self.base = (base_url or cfg.llm_base_url or (preset["base"] if preset else "")).rstrip("/")
        self.key = os.getenv(preset["key_env"], "") if preset and preset["key_env"] else ""
        self.max_retries = max_retries
        self.json_mode = True

    @property
    def enabled(self) -> bool:
        if self.kind is None:
            return False
        if self.kind == "bedrock":
            try:
                import boto3  # noqa: F401
            except ImportError:
                return False
            return bool(self.model)
        if self.kind == "ollama" or self.key_optional:
            return bool(self.model)
        return bool(self.key)

    def describe(self, model: str | None = None) -> str:
        if not self.enabled:
            return "disabled"
        m = model or (self.model if self.model == self.model_strong else f"{self.model}|{self.model_strong}")
        return f"{self.provider}:{m}"

    def model_for(self, severity: str) -> str:
        """Severity routing: strong model for serious incidents, fast/cheap model for the rest."""
        return self.model_strong if SEVERITY_RANK[severity] >= SEVERITY_RANK[self.cfg.llm_strong_min_severity] else self.model

    # ------------------------------------------------------------------ transport
    def _post(self, url: str, body: dict, headers: dict) -> dict:
        req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.cfg.llm_timeout_s) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            ra = e.headers.get("retry-after") if e.headers else None
            raise _HTTPError(e.code, e.read().decode(errors="replace"), ra) from e

    def _with_retries(self, fn):
        delay, last = 2.0, None
        for attempt in range(self.max_retries + 1):
            try:
                return fn()
            except _HTTPError as e:
                last = e
                if e.code == 400 and self.json_mode and ("response_format" in e.body or "json" in e.body.lower()):
                    self.json_mode = False  # endpoint/model does not support JSON mode: retry without it
                    continue
                if e.code not in RETRYABLE or attempt == self.max_retries:
                    raise LLMError(str(e)) from e
                wait = min(30.0, float(e.retry_after)) if (e.retry_after or "").replace(".", "").isdigit() else delay
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last = e
                if attempt == self.max_retries:
                    raise LLMError(f"network error: {e}") from e
                wait = delay
            time.sleep(wait)
            delay *= 2
        raise LLMError(str(last))

    def _bedrock(self, system: str, messages: list[dict], model: str, max_tokens: int) -> tuple[str, dict]:
        """AWS Bedrock Converse API: data stays in the client's AWS account/region; IAM + CloudTrail audit."""
        import boto3
        from botocore.config import Config as BotoConfig
        from botocore.exceptions import BotoCoreError, ClientError
        client = boto3.client("bedrock-runtime", region_name=os.getenv("AWS_REGION"),
                              config=BotoConfig(read_timeout=self.cfg.llm_timeout_s,
                                                retries={"max_attempts": self.max_retries + 1, "mode": "adaptive"}))
        try:
            r = client.converse(modelId=model, system=[{"text": system}],
                                messages=[{"role": m["role"], "content": [{"text": m["content"]}]} for m in messages],
                                inferenceConfig={"temperature": 0, "maxTokens": max_tokens})
        except (BotoCoreError, ClientError) as e:
            raise LLMError(f"bedrock: {e}") from e
        text = "".join(c.get("text", "") for c in r["output"]["message"]["content"])
        u = r.get("usage", {})
        return text, {"input_tokens": u.get("inputTokens"), "output_tokens": u.get("outputTokens")}

    # ------------------------------------------------------------------ API
    def complete(self, system: str, messages: list[dict], max_tokens: int = 4000, model: str | None = None) -> tuple[str, dict]:
        """messages: [{"role": "user"|"assistant", "content": str}] -> (text, usage)"""
        if not self.enabled:
            raise LLMError("LLM not configured")
        model = model or self.model
        t0 = time.time()
        if self.kind == "bedrock":
            text, usage = self._bedrock(system, messages, model, max_tokens)
            usage["latency_ms"] = int((time.time() - t0) * 1000)
            usage["model"] = model
            return text, usage
        if self.kind == "anthropic":
            def call():
                return self._post(f"{self.base}/v1/messages",
                                  {"model": model, "max_tokens": max_tokens, "temperature": 0, "system": system,
                                   "messages": messages},
                                  {"x-api-key": self.key, "anthropic-version": "2023-06-01", "content-type": "application/json"})
            data = self._with_retries(call)
            text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
            u = data.get("usage", {})
            usage = {"input_tokens": u.get("input_tokens"), "output_tokens": u.get("output_tokens")}
        elif self.kind == "ollama":
            def call():
                body = {"model": model, "stream": False, "messages": [{"role": "system", "content": system}] + messages,
                        # the evidence pack is up to ~10k tokens: raise Ollama's small default context window
                        "options": {"temperature": 0, "num_ctx": 16384}}
                if self.json_mode:
                    body["format"] = "json"
                return self._post(f"{self.base}/api/chat", body, {"content-type": "application/json"})
            data = self._with_retries(call)
            text = data.get("message", {}).get("content", "")
            usage = {"input_tokens": data.get("prompt_eval_count"), "output_tokens": data.get("eval_count")}
        else:
            def call():
                body = {"model": model, "temperature": 0, "max_tokens": max_tokens,
                        "messages": [{"role": "system", "content": system}] + messages}
                if self.json_mode:
                    body["response_format"] = {"type": "json_object"}
                return self._post(f"{self.base}/chat/completions", body,
                                  {"Authorization": f"Bearer {self.key or 'none'}", "content-type": "application/json"})
            data = self._with_retries(call)
            try:
                text = data["choices"][0]["message"]["content"] or ""
            except (KeyError, IndexError, TypeError) as e:
                raise LLMError(f"unexpected response shape: {str(data)[:200]}") from e
            u = data.get("usage") or {}
            usage = {"input_tokens": u.get("prompt_tokens"), "output_tokens": u.get("completion_tokens")}
        usage["latency_ms"] = int((time.time() - t0) * 1000)
        usage["model"] = model
        return text, usage
