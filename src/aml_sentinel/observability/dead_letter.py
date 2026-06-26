"""Dead-letter handling (Phase 8).

A worker that fails to process a message records it here instead of crashing or
losing it (doc 02 §6 rule 3: a ``status:"failed"`` produces a dead-letter
record and no partial DB write). Each worker calls :func:`record_dead_letter`
from its consume loop's exception handler, on a *fresh* session so a poisoned
transaction can't take the dead-letter write down with it.
"""

from __future__ import annotations

from typing import Any

from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import DeadLetter
from aml_sentinel.observability.logging import stage_log


def record_dead_letter(
    *,
    stage: str,
    component: str,
    topic: str,
    partition: int,
    offset: int,
    error: Exception,
    trace_id: str | None = None,
    client_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> str:
    """Persist a dead-letter row and emit the ``status:"failed"`` log line."""
    dl_id = str(ULID())
    with SessionLocal() as session:
        session.add(
            DeadLetter(
                id=dl_id,
                topic=topic,
                partition=partition,
                msg_offset=offset,
                trace_id=trace_id,
                client_id=client_id,
                stage=stage,
                error_type=type(error).__name__,
                error_msg=str(error)[:1000],
                payload=payload,
            )
        )
        session.commit()
    stage_log(
        stage=stage,
        component=component,
        trace_id=trace_id or "-",
        client_id=client_id or "-",
        status="failed",
        level="ERROR",
        detail={
            "error_type": type(error).__name__,
            "error_msg": str(error)[:200],
            "dead_letter_id": dl_id,
        },
    )
    return dl_id
