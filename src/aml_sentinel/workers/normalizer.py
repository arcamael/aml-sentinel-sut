"""Normalizer worker (Phase 3).

Consumes ``client.submitted``, canonicalizes the raw KYC profile
(:mod:`aml_sentinel.matching.normalize`), persists ``normalized_profile``,
produces ``profile.normalized``, and emits the ``normalize`` log line.

Idempotency (hard rule #6, doc 02 §4.8): each message is keyed by
``trace_id:topic:partition:offset`` in the ``idempotency`` table. Redelivery of
the same message is a no-op — no duplicate ``normalized_profile`` row and no
duplicate downstream event. The unique constraint on
``(client_id, profile_hash)`` is a second, DB-level backstop.

Processing then committing the offset gives at-least-once delivery; the
idempotency key makes the effect exactly-once.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date

from confluent_kafka import Consumer, KafkaError, Message
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from ulid import ULID

from aml_sentinel.config import settings
from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Idempotency, NormalizedProfile
from aml_sentinel.events import (
    TOPIC_CLIENT_SUBMITTED,
    TOPIC_PROFILE_NORMALIZED,
    EventProducer,
    make_envelope,
)
from aml_sentinel.matching.normalize import normalize
from aml_sentinel.observability.logging import configure_logging, stage_log

COMPONENT = "normalizer"
CONSUMER_GROUP = "normalizer"


def idempotency_key(trace_id: str, topic: str, partition: int, offset: int) -> str:
    """Durable per-message dedup key (doc 02 §4.8).

    ``partition`` is included because Kafka offsets are only unique *within* a
    partition; the doc's ``trace_id:topic:offset`` shape is otherwise honored.
    """
    return f"{trace_id}:{topic}:{partition}:{offset}"


@dataclass(frozen=True)
class ProcessResult:
    """Outcome of processing one message — used by the verification harness."""

    client_id: str
    trace_id: str
    profile_hash: str | None
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
) -> ProcessResult:
    """Normalize one ``client.submitted`` envelope idempotently.

    Returns ``created=True`` on first processing, ``skipped=True`` on redelivery
    (idempotency hit) or a benign duplicate ``(client_id, profile_hash)``.
    """
    started = time.perf_counter()
    trace_id: str = envelope["trace_id"]
    client_id: str = envelope["client_id"]
    raw_kyc: dict = envelope["payload"]

    key = idempotency_key(trace_id, topic, partition, offset)
    if session.get(Idempotency, key) is not None:
        # Exact redelivery of an already-processed message: no-op.
        stage_log(
            stage="normalize",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=client_id,
            status="ok",
            detail={"idempotent_skip": True, "key": key},
        )
        return ProcessResult(client_id, trace_id, None, created=False, skipped=True)

    result = normalize(raw_kyc)

    session.add(
        NormalizedProfile(
            id=str(ULID()),
            client_id=client_id,
            trace_id=trace_id,
            profile_hash=result.profile_hash,
            canonical_name=result.canonical_name,
            name_parts=result.name_parts,
            dob_iso=date.fromisoformat(result.dob_iso) if result.dob_iso else None,
            nationality_iso2=result.nationality_iso2,
            residence_iso2=result.residence_iso2,
            document_ids=result.document_ids,
        )
    )
    session.add(Idempotency(key=key))
    try:
        # normalized_profile + idempotency row commit atomically.
        session.commit()
    except IntegrityError:
        session.rollback()
        # Backstop: this (client_id, profile_hash) already exists (e.g. a retry
        # under a different offset). Treat as already-processed; do not re-emit.
        stage_log(
            stage="normalize",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=client_id,
            status="ok",
            detail={"idempotent_skip": True, "reason": "duplicate_profile"},
        )
        return ProcessResult(client_id, trace_id, result.profile_hash, created=False, skipped=True)

    # Persisted → emit the downstream event (keyed by client_id for ordering).
    out = make_envelope(
        trace_id=trace_id,
        client_id=client_id,
        event_type="profile.normalized",
        producer=COMPONENT,
        payload=result.to_normalized_payload(),
    )
    producer.produce(TOPIC_PROFILE_NORMALIZED, key=client_id, envelope=out)

    duration_ms = int((time.perf_counter() - started) * 1000)
    stage_log(
        stage="normalize",
        component=COMPONENT,
        trace_id=trace_id,
        client_id=client_id,
        status="ok",
        duration_ms=duration_ms,
        detail={
            "profile_hash": result.profile_hash,
            "transliterated": result.transliterated,
            "fields_defaulted": result.fields_defaulted,
        },
    )
    return ProcessResult(client_id, trace_id, result.profile_hash, created=True, skipped=False)


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
    """Run the normalizer as a long-lived consumer (compose service)."""
    configure_logging()
    consumer = _build_consumer()
    consumer.subscribe([TOPIC_CLIENT_SUBMITTED])
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
                    stage="normalize",
                    component=COMPONENT,
                    trace_id="-",
                    client_id="-",
                    status="failed",
                    level="ERROR",
                    detail={"error_type": "kafka", "error_msg": str(msg.error())},
                )
                continue

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
            # At-least-once: commit offset only after the effect is durable.
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":  # pragma: no cover
    main()
