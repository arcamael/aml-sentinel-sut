"""Decision engine worker (Phase 6).

Consumes ``screening.completed``, applies the rules engine
(:mod:`aml_sentinel.decisioning.rules`), persists a ``decision`` plus an
**append-only** ``audit`` snapshot (inputs + matches + rule trace), produces
``decision.made``, and emits the ``decide`` log line.

Two invariants this worker guarantees:

* **Exactly one decision per screening** — ``decision.screening_id`` is UNIQUE;
  a redelivered ``screening.completed`` is suppressed by the idempotency key, and
  the UNIQUE constraint is the DB-level backstop.
* **Immutable, complete audit** — the snapshot captures the matches that drove
  the outcome and the full rule trace; the ``audit`` table rejects UPDATE/DELETE
  (Phase 1 trigger).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from ulid import ULID

from aml_sentinel.config import settings
from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Audit, Decision, Idempotency
from aml_sentinel.decisioning.rules import decide
from aml_sentinel.events import (
    TOPIC_DECISION_MADE,
    TOPIC_SCREENING_COMPLETED,
    EventProducer,
    make_envelope,
)
from aml_sentinel.observability.dead_letter import record_dead_letter
from aml_sentinel.observability.logging import configure_logging, stage_log
from aml_sentinel.workers.normalizer import idempotency_key

COMPONENT = "decision-engine"
CONSUMER_GROUP = "decision-engine"


@dataclass
class DecisionOutcome:
    client_id: str
    trace_id: str
    decision_id: str | None
    screening_id: str | None
    outcome: str | None
    reason_codes: list[str]
    top_match_id: str | None
    created: bool
    skipped: bool


def process_message(
    session: Session,
    producer: EventProducer,
    *,
    envelope: dict,
    topic: str,
    partition: int,
    offset: int,
) -> DecisionOutcome:
    """Decide one ``screening.completed`` envelope idempotently."""
    started = time.perf_counter()
    trace_id: str = envelope["trace_id"]
    client_id: str = envelope["client_id"]
    payload: dict = envelope["payload"]
    screening_id: str = payload["screening_id"]
    matches: list[dict[str, Any]] = payload.get("matches", [])

    key = idempotency_key(trace_id, topic, partition, offset)
    if session.get(Idempotency, key) is not None:
        stage_log(
            stage="decide",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=client_id,
            status="ok",
            detail={"idempotent_skip": True, "key": key},
        )
        return DecisionOutcome(client_id, trace_id, None, screening_id, None, [], None, False, True)

    result = decide(matches)
    decision_id = str(ULID())
    top_match_id = result.top_match.get("match_id") if result.top_match else None
    decided_at = datetime.now(UTC)

    # Immutable audit snapshot: everything needed to defend the outcome.
    snapshot = {
        "screening_id": screening_id,
        "client_id": client_id,
        "trace_id": trace_id,
        "profile_hash": payload.get("profile_hash"),
        "list_versions": payload.get("list_versions", {}),
        "matches": matches,
        "rule_trace": result.rule_trace,
        "outcome": result.outcome,
        "reason_codes": result.reason_codes,
    }

    session.add(
        Decision(
            id=decision_id,
            screening_id=screening_id,
            client_id=client_id,
            trace_id=trace_id,
            outcome=result.outcome,
            reason_codes=result.reason_codes,
            top_match_id=top_match_id,
            decided_at=decided_at,
        )
    )
    try:
        # Flush the parent decision first (FK audit → decision; no ORM
        # relationship to order the inserts). This is also where the
        # UNIQUE(screening_id) backstop fires when this screening was already
        # decided under a different message — so it must be inside the try.
        session.flush()
        session.add(
            Audit(
                id=str(ULID()),
                decision_id=decision_id,
                trace_id=trace_id,
                snapshot=snapshot,
            )
        )
        session.add(Idempotency(key=key))
        session.commit()
    except IntegrityError:
        # Either an idempotency-key race or the UNIQUE(screening_id) backstop:
        # this screening already has a decision. Exactly-once preserved.
        session.rollback()
        stage_log(
            stage="decide",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=client_id,
            status="ok",
            detail={"idempotent_skip": True, "reason": "already_decided"},
        )
        return DecisionOutcome(client_id, trace_id, None, screening_id, None, [], None, False, True)

    event_payload = {
        "decision_id": decision_id,
        "screening_id": screening_id,
        "outcome": result.outcome,
        "reason_codes": result.reason_codes,
        "top_match_id": top_match_id,
        "decided_at": decided_at.isoformat(),
    }
    out = make_envelope(
        trace_id=trace_id,
        client_id=client_id,
        event_type="decision.made",
        producer=COMPONENT,
        payload=event_payload,
    )
    producer.produce(TOPIC_DECISION_MADE, key=client_id, envelope=out)

    duration_ms = int((time.perf_counter() - started) * 1000)
    stage_log(
        stage="decide",
        component=COMPONENT,
        trace_id=trace_id,
        client_id=client_id,
        status="ok",
        duration_ms=duration_ms,
        detail={
            "decision_id": decision_id,
            "outcome": result.outcome,
            "reason_codes": result.reason_codes,
        },
    )
    return DecisionOutcome(
        client_id=client_id,
        trace_id=trace_id,
        decision_id=decision_id,
        screening_id=screening_id,
        outcome=result.outcome,
        reason_codes=result.reason_codes,
        top_match_id=top_match_id,
        created=True,
        skipped=False,
    )


def _build_consumer(group_id: str = CONSUMER_GROUP) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def main() -> None:  # pragma: no cover - long-running service entrypoint
    configure_logging()
    consumer = _build_consumer()
    consumer.subscribe([TOPIC_SCREENING_COMPLETED])
    producer = EventProducer()
    try:
        while True:
            msg: Message | None = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                stage_log(
                    stage="decide",
                    component=COMPONENT,
                    trace_id="-",
                    client_id="-",
                    status="failed",
                    level="ERROR",
                    detail={"error_type": "kafka", "error_msg": str(msg.error())},
                )
                continue

            try:
                envelope = json.loads(msg.value())
                with SessionLocal() as session:
                    process_message(
                        session,
                        producer,
                        envelope=envelope,
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
            except Exception as exc:
                record_dead_letter(
                    stage="decide",
                    component=COMPONENT,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    error=exc,
                )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":  # pragma: no cover
    main()
