"""Structured JSON logging (hard rule #5).

Every pipeline stage emits exactly one machine-parseable log line per record
following the schema in doc 02 §6. ``stage_log`` is the single chokepoint so all
stages stay schema-consistent and the harness can assert on the log stream.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_CONFIGURED = False


def configure_logging(level: str = "INFO") -> None:
    """Configure structlog to emit one JSON object per line to stdout."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
            # The positional "event" carries the stage name; rename it to "stage"
            # so the line matches the doc 02 §6 schema with no duplicate key.
            structlog.processors.EventRenamer("stage"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED = True


def stage_log(
    *,
    stage: str,
    component: str,
    trace_id: str,
    client_id: str,
    status: str = "ok",
    duration_ms: int | None = None,
    level: str = "INFO",
    detail: dict[str, Any] | None = None,
) -> None:
    """Emit one structured stage log line per doc 02 §6.

    Field order/shape is fixed: ts, level, trace_id, client_id, stage, status,
    component, duration_ms, detail.
    """
    logger = structlog.get_logger()
    fields: dict[str, Any] = {
        "trace_id": trace_id,
        "client_id": client_id,
        "status": status,
        "component": component,
        "detail": detail or {},
    }
    if duration_ms is not None:
        fields["duration_ms"] = duration_ms

    log_method = getattr(logger, level.lower(), logger.info)
    # The positional ``stage`` becomes the renamed "stage" key (see EventRenamer).
    log_method(stage, **fields)
