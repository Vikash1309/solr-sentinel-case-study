"""Redaction (doc §7.6, §10.3).

* redact_secrets  - applied by the parsers BEFORE storage: credentials, tokens, basic-auth in URLs.
* redact_query_terms - applied BEFORE any external model call: end-user search terms in q / fq
  (potential PII) are removed while the query *shape* (rows, start, facets) that the RCA needs is kept.
"""
from __future__ import annotations

import re

_SECRET_PATTERNS = [
    (re.compile(r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|authorization)\s*([=:])\s*([^\s&,;}\]]+)"),
     lambda m: f"{m[1]}{m[2]}<redacted>"),
    (re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}"), lambda m: f"{m[1]} <redacted>"),
    (re.compile(r"(https?://)[^/\s:@]+:[^/\s@]+@"), lambda m: f"{m[1]}<redacted>@"),
]

_QUERY_PARAM = re.compile(r"\b(q|fq)=([^&}\s]*)")


def redact_secrets(text: str) -> str:
    for rx, rep in _SECRET_PATTERNS:
        text = rx.sub(rep, text)
    return text


def redact_query_terms(text: str) -> str:
    def rep(m):
        name, val = m[1], m[2]
        if val in ("*:*", "") or re.fullmatch(r"[\w.]+:\*", val):
            return m[0]
        head = val.split(":", 1)[0]
        field = head if ":" in val and re.fullmatch(r"[\w.]+", head) else None
        return f"{name}={field + ':' if field else ''}<redacted>"
    return _QUERY_PARAM.sub(rep, text)
