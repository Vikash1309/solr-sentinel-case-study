from app.config import Settings
from app.rca import providers as pv


def test_retry_on_429_and_json_mode_fallback(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "k")
    monkeypatch.setattr(pv.time, "sleep", lambda s: None)
    c = pv.LLMClient(Settings(llm_provider="groq"))
    calls = []

    def fake_post(url, body, headers):
        calls.append(body)
        if len(calls) == 1:
            raise pv._HTTPError(429, "rate limited", "1")
        if len(calls) == 2:
            raise pv._HTTPError(400, "response_format is not supported")
        return {"choices": [{"message": {"content": "{}"}}], "usage": {"prompt_tokens": 5, "completion_tokens": 2}}

    monkeypatch.setattr(c, "_post", fake_post)
    text, usage = c.complete("sys", [{"role": "user", "content": "hi"}], model="m")
    assert text == "{}" and usage["input_tokens"] == 5 and usage["model"] == "m"
    assert "response_format" in calls[1] and "response_format" not in calls[2]


def test_provider_selection_and_severity_routing(monkeypatch):
    for k in ("ANTHROPIC_API_KEY", "GEMINI_API_KEY", "GROQ_API_KEY", "OPENROUTER_API_KEY", "OPENAI_API_KEY", "LLM_API_KEY"):
        monkeypatch.delenv(k, raising=False)
    assert pv.LLMClient(Settings(llm_provider="auto")).describe() == "disabled"
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    assert pv.LLMClient(Settings(llm_provider="auto")).provider == "gemini"
    g = pv.LLMClient(Settings(llm_provider="gateway", llm_model="", llm_model_strong=""))   # ignore any LLM_MODEL in the environment
    assert g.enabled and g.model_for("critical") == "rca-strong" and g.model_for("warning") == "rca-fast"
    assert not pv.LLMClient(Settings(llm_provider="vllm", llm_model="")).enabled          # vLLM needs an explicit model name
    monkeypatch.delenv("BEDROCK_MODEL_ID", raising=False)
    assert not pv.LLMClient(Settings(llm_provider="bedrock")).enabled       # needs boto3 and model IDs
