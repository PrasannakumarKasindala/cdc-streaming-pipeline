"""Structured JSON logging: one object per line to stderr.

stderr is deliberate -- reports and emitted DDL go to stdout so they stay
pipeable. Level via CDCPIPE_LOG_LEVEL (default INFO).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any


class JsonFormatter(logging.Formatter):
    _RESERVED = {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in self._RESERVED and not k.startswith("_"):
                payload[k] = v
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def get_logger(name: str = "cdcpipe") -> logging.Logger:
    logger = logging.getLogger(name)
    if getattr(logger, "_wmk_configured", False):
        return logger
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(JsonFormatter())
    logger.addHandler(h)
    logger.setLevel(os.environ.get("CDCPIPE_LOG_LEVEL", "INFO").upper())
    logger.propagate = False
    logger._wmk_configured = True  # type: ignore[attr-defined]
    return logger
