"""Screening worker (Phase 5).

Consumes ``profile.normalized``, queries the provider gateway across all three
list types, fuzzy-matches the returned candidates, persists a ``screening`` plus
its ``match`` rows, captures the per-provider ``list_versions``, produces
``screening.completed``, and emits the ``screen`` log line.

Idempotent like the normalizer (doc 02 §4.8): keyed
``trace_id:topic:partition:offset``. A re-screen triggered by reconciliation
arrives as a *new* ``profile.normalized`` message (new offset) and therefore
correctly produces a *new* screening — only literal redelivery is suppressed.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from ulid import ULID

from aml_sentinel.config import settings
from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Idempotency, Match, Screening
from aml_sentinel.events import (
    TOPIC_PROFILE_NORMALIZED,
    TOPIC_SCREENING_COMPLETED,
    EventProducer,
    make_envelope,
)
from aml_sentinel.matching.fuzzy import SCREENING_THRESHOLD, score_candidate
from aml_sentinel.observability.logging import configure_logging, stage_log
from aml_sentinel.providers.gateway import ProviderGateway
from aml_sentinel.workers.normalizer import idempotency_key

COMPONENT = "screening-worker"
CONSUMER_GROUP = "screening-worker"


@dataclass
class ScreeningResult:
    client_id: str
    trace_id: str
    screening_id: str | None
    match_count: int
    max_score: float
    cache_hits: int
    event_payload: dict[str, Any] | None
    created: bool
    skipped: bool
    matches: list[dict[str, Any]] = field(default_factory=list)


def process_message(
    session: Session,
    gateway: ProviderGateway,
    producer: EventProducer,
    *,
    envelope: dict,
    topic: str,
    partition: int,
    offset: int,
) -> ScreeningResult:
    """Screen one ``profile.normalized`` envelope idempotently."""
    started = time.perf_counter()
    trace_id: str = envelope["trace_id"]
    client_id: str = envelope["client_id"]
    payload: dict = envelope["payload"]

    key = idempotency_key(trace_id, topic, partition, offset)
    if session.get(Idempotency, key) is not None:
        stage_log(
            stage="screen",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=client_id,
            status="ok",
            detail={"idempotent_skip": True, "key": key},
        )
        return ScreeningResult(client_id, trace_id, None, 0, 0.0, 0, None, False, True)

    canonical_name: str = payload["canonical_name"]
    dob_iso: str | None = payload.get("dob_iso")
    profile_hash: str = payload["profile_hash"]

    # Query every provider; fuzzy-score every returned candidate.
    responses = gateway.screen(canonical_name=canonical_name, dob_iso=dob_iso)

    screening_id = str(ULID())
    list_versions: dict[str, str] = {}
    matches: list[dict[str, Any]] = []
    candidates_total = 0
    cache_hits = 0
    max_score = 0.0

    for response in responses.values():
        list_versions[response.provider_id] = response.list_version
        candidates_total += len(response.candidates)
        if response.cache_hit:
            cache_hits += 1
        for cand in response.candidates:
            scored = score_candidate(
                canonical_name,
                dob_iso,
                entity_name=cand.entity_name,
                aliases=cand.aliases,
                candidate_dob=cand.dob_iso,
            )
            if scored.score >= SCREENING_THRESHOLD:
                matches.append(
                    {
                        "match_id": str(ULID()),
                        "provider_id": cand.provider_id,
                        "list_type": cand.list_type,
                        "matched_name": scored.matched_name,
                        "score": scored.score,
                        "dob_match": scored.dob_match,
                        "evidence_ref": cand.entry_id,
                        # Risk attributes the decision engine needs (PEP tier,
                        # media confidence); carried on the event + audit, not in
                        # the match table.
                        "pep_tier": cand.risk_payload.get("pep_tier"),
                        "media_confidence": cand.risk_payload.get("media_confidence"),
                    }
                )
                max_score = max(max_score, scored.score)

    # Persist screening + matches + idempotency atomically. Flush the parent
    # screening first so the FK from match → screening is satisfiable (there is
    # no ORM relationship to let the unit-of-work order these inserts itself).
    session.add(
        Screening(
            id=screening_id,
            client_id=client_id,
            trace_id=trace_id,
            profile_hash=profile_hash,
            list_versions=list_versions,
            status="completed",
        )
    )
    session.flush()
    for m in matches:
        session.add(
            Match(
                id=m["match_id"],
                screening_id=screening_id,
                provider_id=m["provider_id"],
                list_type=m["list_type"],
                watchlist_entry_id=None,  # local mirror is populated in Phase 7
                matched_name=m["matched_name"],
                score=m["score"],
                dob_match=m["dob_match"],
            )
        )
    session.add(Idempotency(key=key))
    try:
        session.commit()
    except IntegrityError:
        # Idempotency-key race only (the FK is already satisfied by the flush
        # above); a concurrent consumer beat us to this exact message.
        session.rollback()
        stage_log(
            stage="screen",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=client_id,
            status="ok",
            detail={"idempotent_skip": True, "reason": "duplicate_key"},
        )
        return ScreeningResult(client_id, trace_id, None, 0, 0.0, 0, None, False, True)

    # Emit screening.completed (doc 02 §2).
    event_payload = {
        "screening_id": screening_id,
        "profile_hash": profile_hash,
        "list_versions": list_versions,
        "matches": matches,
    }
    out = make_envelope(
        trace_id=trace_id,
        client_id=client_id,
        event_type="screening.completed",
        producer=COMPONENT,
        payload=event_payload,
    )
    producer.produce(TOPIC_SCREENING_COMPLETED, key=client_id, envelope=out)

    duration_ms = int((time.perf_counter() - started) * 1000)
    stage_log(
        stage="screen",
        component=COMPONENT,
        trace_id=trace_id,
        client_id=client_id,
        status="ok",
        duration_ms=duration_ms,
        detail={
            "screening_id": screening_id,
            "providers_queried": len(responses),
            "candidates": candidates_total,
            "matches": len(matches),
            "max_score": round(max_score, 4),
            "cache_hits": cache_hits,
        },
    )
    return ScreeningResult(
        client_id=client_id,
        trace_id=trace_id,
        screening_id=screening_id,
        match_count=len(matches),
        max_score=round(max_score, 4),
        cache_hits=cache_hits,
        event_payload=event_payload,
        created=True,
        skipped=False,
        matches=matches,
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
    consumer.subscribe([TOPIC_PROFILE_NORMALIZED])
    gateway = ProviderGateway()
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
                    stage="screen",
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
                    gateway,
                    producer,
                    envelope=envelope,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        gateway.close()
        producer.close()


if __name__ == "__main__":  # pragma: no cover
    main()
