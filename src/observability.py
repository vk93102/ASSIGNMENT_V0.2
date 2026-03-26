from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any


_STANDARD_LOG_RECORD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
}


class JsonFormatter(logging.Formatter):
    """Newline-delimited JSON formatter.

    Includes user-provided `extra` fields on the LogRecord.
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        payload: dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Attach extras (e.g., request_id/status/tokens), keeping them JSON-safe.
        for k, v in record.__dict__.items():
            if k in _STANDARD_LOG_RECORD_ATTRS or k.startswith("_"):
                continue
            if v is None or isinstance(v, (str, int, float, bool)):
                payload[k] = v
            else:
                payload[k] = str(v)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=True)


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger.

    Uses env var LOG_LEVEL (default INFO). Keeps formatting minimal to avoid
    affecting evaluation harnesses.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level_name = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(level)

    log_format = os.getenv("LOG_FORMAT", "text").lower().strip()
    if log_format == "json":
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.propagate = False
    return logger


def safe_extra(**fields: Any) -> dict[str, Any]:
    """Return logging extra dict with JSON-safe scalars only."""
    out: dict[str, Any] = {}
    for k, v in fields.items():
        if v is None or isinstance(v, (str, int, float, bool)):
            out[k] = v
        else:
            out[k] = str(v)
    return out
