"""Structured logging with secret redaction.

Log records carry context fields (sandbox_id, worker_id, pool_id, operation_id,
cloud, provider_cluster). Values that look like secrets are never emitted.
"""

from __future__ import annotations

import contextvars
import json
import logging
import re
import sys
from typing import Any

CONTEXT_FIELDS = ("sandbox_id", "worker_id", "pool_id", "operation_id", "cloud", "provider_cluster")

_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "sandboxpilot_log_context"
)

_SECRET_KEY_RE = re.compile(
    r"(token|secret|password|passwd|authorization|api_key|apikey|credential)", re.I
)
_BEARER_RE = re.compile(r"(Bearer\s+)[A-Za-z0-9._\-]+", re.I)
_TOKEN_QS_RE = re.compile(r"([?&](?:token|sig|signature)=)[^&\s]+", re.I)


def redact_text(text: str) -> str:
    text = _BEARER_RE.sub(r"\1[REDACTED]", text)
    return _TOKEN_QS_RE.sub(r"\1[REDACTED]", text)


def redact_mapping(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in data.items():
        if _SECRET_KEY_RE.search(str(key)):
            out[key] = "[REDACTED]"
        elif isinstance(value, dict):
            out[key] = redact_mapping(value)
        elif isinstance(value, str):
            out[key] = redact_text(value)
        else:
            out[key] = value
    return out


def bind_context(**fields: Any) -> contextvars.Token[dict[str, Any]]:
    merged = {**_log_context.get({}), **{k: v for k, v in fields.items() if v is not None}}
    return _log_context.set(merged)


def reset_context(token: contextvars.Token[dict[str, Any]]) -> None:
    _log_context.reset(token)


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _log_context.get({})
        for field in CONTEXT_FIELDS:
            if not hasattr(record, field):
                setattr(record, field, ctx.get(field))
        return True


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_text(record.getMessage()),
        }
        for field in CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(redact_mapping(extra))
        if record.exc_info:
            payload["exception"] = redact_text(self.formatException(record.exc_info))
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ctx_parts = []
        for field in CONTEXT_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                ctx_parts.append(f"{field}={value}")
        ctx = f" [{' '.join(ctx_parts)}]" if ctx_parts else ""
        base = f"{self.formatTime(record, '%H:%M:%S')} {record.levelname:<7} {record.name}{ctx}: {redact_text(record.getMessage())}"
        if record.exc_info:
            base += "\n" + redact_text(self.formatException(record.exc_info))
        return base


def configure_logging(
    level: str | int = "INFO", *, json_format: bool = False, stream: Any = None
) -> None:
    root = logging.getLogger("sandboxpilot")
    root.handlers.clear()
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JSONFormatter() if json_format else TextFormatter())
    handler.addFilter(_ContextFilter())
    root.addHandler(handler)
    root.setLevel(level if isinstance(level, int) else level.upper())
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name if name.startswith("sandboxpilot") else f"sandboxpilot.{name}")
