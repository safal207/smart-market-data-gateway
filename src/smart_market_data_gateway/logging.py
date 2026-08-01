import contextvars
from datetime import UTC, datetime
import json
import logging
from typing import Any
from uuid import uuid4

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)
connection_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "connection_id", default=""
)

_SENSITIVE_FRAGMENTS = ("authorization", "token", "secret", "password", "credential", "api_key")


def _redact(value: Any, key: str = "") -> Any:
    if any(fragment in key.lower() for fragment in _SENSITIVE_FRAGMENTS):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): _redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_redact(item) for item in value]
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "severity": record.levelname,
            "service": getattr(record, "service", "gateway"),
            "event": getattr(record, "event", record.getMessage()),
            "message": record.getMessage(),
            "correlation_id": correlation_id_var.get() or None,
            "connection_id": connection_id_var.get() or None,
        }
        for field in (
            "symbol",
            "provider",
            "client_id",
            "tier",
            "stream_id",
            "retry_count",
            "duration_ms",
        ):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(_redact(payload), default=str, separators=(",", ":"))


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())


def new_correlation_id(value: str | None = None) -> str:
    correlation_id = value or str(uuid4())
    correlation_id_var.set(correlation_id)
    return correlation_id
