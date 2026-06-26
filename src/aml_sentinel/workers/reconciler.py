"""Reconciliation worker (Phase 7).

Consumes ``watchlist.updated``, keeps screenings fresh when a provider's list
changes, and records the status drift:

1. invalidate the gateway cache for the provider (so re-screens see the new list);
2. upsert the local ``watchlist_entry`` mirror + bump ``list_version``;
3. select clients whose *latest* screening used an older ``list_version``;
4. re-screen each of them and record ``newly_flagged`` / ``newly_cleared``;
5. write one ``reconciliation_run`` (start → finish), timed by an injectable ``Clock``.

**Implementation note (deviation from doc 02 §5).** The reconciler re-screens
*synchronously* through the shared screening + decision logic rather than
re-emitting ``profile.normalized`` for an async screening worker. This yields
exact drift counts in a single ``reconciliation_run`` and avoids double-screening
a client. The re-screen still flows through the identical code paths, so a fresh
``screening`` (referencing the new ``list_version``), ``screening.completed``,
``decision``, immutable ``audit``, and ``decision.made`` are all produced, and
the client's ``trace_id`` is reused so lineage stays intact.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime

from confluent_kafka import Consumer, KafkaError, Message
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from ulid import ULID

from aml_sentinel.config import settings
from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import (
    Idempotency,
    NormalizedProfile,
    ReconciliationRun,
    WatchlistEntry,
)
from aml_sentinel.events import TOPIC_WATCHLIST_UPDATED, EventProducer
from aml_sentinel.observability.dead_letter import record_dead_letter
from aml_sentinel.observability.logging import configure_logging, stage_log
from aml_sentinel.providers.gateway import ProviderGateway
from aml_sentinel.workers import decision as decision_worker
from aml_sentinel.workers import screening as screening_worker
from aml_sentinel.workers.normalizer import idempotency_key

COMPONENT = "reconciler"
CONSUMER_GROUP = "reconciler"

ACTIVE_RISK = ("FLAG", "ESCALATE")


class Clock:
    """Injectable clock (doc 01 §7) so reconciliation timing is deterministic."""

    def now(self) -> datetime:
        return datetime.now(UTC)


def version_key(version: str | None) -> int:
    """``"v3"`` → ``3``; tolerant of missing/odd values (sort floor)."""
    if not version:
        return 0
    try:
        return int(str(version).lstrip("vV"))
    except ValueError:
        return 0


@dataclass
class ReconcileResult:
    run_id: str | None
    provider_id: str
    new_version: str
    clients_rescreened: int
    newly_flagged: int
    newly_cleared: int
    skipped: bool


def _latest_screenings(session: Session) -> list[tuple[str, dict]]:
    """(client_id, list_versions) for each client's most recent screening."""
    rows = session.execute(
        text(
            """
            SELECT DISTINCT ON (client_id) client_id, list_versions
            FROM screening
            ORDER BY client_id, screened_at DESC
            """
        )
    ).all()
    return [(r[0], r[1]) for r in rows]


def _latest_decision_outcome(session: Session, client_id: str) -> str | None:
    row = session.execute(
        text(
            """
            SELECT outcome FROM decision
            WHERE client_id = :cid
            ORDER BY decided_at DESC
            LIMIT 1
            """
        ),
        {"cid": client_id},
    ).first()
    return row[0] if row else None


def _latest_normalized(session: Session, client_id: str) -> NormalizedProfile | None:
    return session.scalar(
        select(NormalizedProfile)
        .where(NormalizedProfile.client_id == client_id)
        .order_by(NormalizedProfile.created_at.desc())
        .limit(1)
    )


def _upsert_mirror(
    session: Session, *, list_type: str, new_version: str, change: str, entry: dict
) -> None:
    """Upsert the local watchlist_entry mirror (doc 02 §4.3)."""
    dob_iso = entry.get("dob_iso")
    dob = date.fromisoformat(dob_iso) if dob_iso else None
    existing = session.get(WatchlistEntry, entry["entry_id"])
    if existing is None:
        session.add(
            WatchlistEntry(
                id=entry["entry_id"],
                provider_id=entry["provider_id"],
                list_type=list_type,
                list_version=new_version,
                entity_name=entry["entity_name"],
                aliases=entry.get("aliases", []),
                dob_iso=dob,
                country_iso2=entry.get("country_iso2"),
                risk_payload=entry.get("risk_payload", {}),
                is_active=(change != "remove"),
            )
        )
    else:
        existing.list_version = new_version
        existing.is_active = change != "remove"


def _normalized_payload(np: NormalizedProfile, rescreen_reason: str) -> dict:
    return {
        "profile_hash": np.profile_hash,
        "canonical_name": np.canonical_name,
        "name_parts": np.name_parts,
        "dob_iso": np.dob_iso.isoformat() if np.dob_iso else None,
        "nationality_iso2": np.nationality_iso2,
        "residence_iso2": np.residence_iso2,
        "document_ids": np.document_ids,
        "rescreen_reason": rescreen_reason,
    }


def process_message(
    session: Session,
    gateway: ProviderGateway,
    producer: EventProducer,
    *,
    envelope: dict,
    topic: str,
    partition: int,
    offset: int,
    clock: Clock | None = None,
) -> ReconcileResult:
    """Reconcile one ``watchlist.updated`` envelope (idempotent)."""
    clock = clock or Clock()
    started = time.perf_counter()
    trace_id: str = envelope["trace_id"]
    payload: dict = envelope["payload"]
    provider_id: str = payload["provider_id"]
    list_type: str = payload["list_type"]
    change: str = payload["change"]
    new_version: str = payload["new_list_version"]
    entry: dict | None = payload.get("entry")

    key = idempotency_key(trace_id, topic, partition, offset)
    if session.get(Idempotency, key) is not None:
        stage_log(
            stage="reconcile",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=provider_id,
            status="ok",
            detail={"idempotent_skip": True, "key": key},
        )
        return ReconcileResult(None, provider_id, new_version, 0, 0, 0, skipped=True)

    # 1) Cache invalidation + 2) local mirror upsert + version bump.
    gateway.invalidate_provider(provider_id)
    if entry and change in ("add", "remove"):
        _upsert_mirror(
            session, list_type=list_type, new_version=new_version, change=change, entry=entry
        )

    # 3) Affected = clients whose latest screening used an older version.
    affected: list[str] = []
    old_version: str | None = None
    for client_id, list_versions in _latest_screenings(session):
        current = (list_versions or {}).get(provider_id)
        if current is not None and version_key(current) < version_key(new_version):
            affected.append(client_id)
            old_version = current

    run_id = str(ULID())
    run = ReconciliationRun(
        id=run_id,
        provider_id=provider_id,
        list_type=list_type,
        old_version=old_version,
        new_version=new_version,
        started_at=clock.now(),
    )
    session.add(run)
    session.add(Idempotency(key=key))
    session.commit()

    # 4) Re-screen each affected client through the shared screening + decision
    #    logic; tally drift against the client's prior outcome.
    newly_flagged = newly_cleared = rescreened = 0
    for idx, client_id in enumerate(affected):
        np = _latest_normalized(session, client_id)
        if np is None:
            continue
        prior_outcome = _latest_decision_outcome(session, client_id)
        rescreen_reason = f"list_update {provider_id} {new_version}"

        # Reuse the client's trace_id (lineage). Synthetic, unique message
        # identity so each reconciliation re-screen is its own idempotent event.
        rescreen_topic = f"reconcile:{run_id}"
        norm_env = {
            "trace_id": np.trace_id,
            "client_id": client_id,
            "event_type": "profile.normalized",
            "payload": _normalized_payload(np, rescreen_reason),
        }
        screen = screening_worker.process_message(
            session,
            gateway,
            producer,
            envelope=norm_env,
            topic=rescreen_topic,
            partition=0,
            offset=idx,
        )
        screening_env = {
            "trace_id": np.trace_id,
            "client_id": client_id,
            "event_type": "screening.completed",
            "payload": screen.event_payload,
        }
        outcome = decision_worker.process_message(
            session,
            producer,
            envelope=screening_env,
            topic=f"{rescreen_topic}:decide",
            partition=0,
            offset=idx,
        )
        rescreened += 1
        new_outcome = outcome.outcome
        if prior_outcome == "CLEAR" and new_outcome in ACTIVE_RISK:
            newly_flagged += 1
        elif prior_outcome in ACTIVE_RISK and new_outcome == "CLEAR":
            newly_cleared += 1

    # 5) Finalize the run.
    run.clients_rescreened = rescreened
    run.newly_flagged = newly_flagged
    run.newly_cleared = newly_cleared
    run.finished_at = clock.now()
    session.commit()

    duration_ms = int((time.perf_counter() - started) * 1000)
    stage_log(
        stage="reconcile",
        component=COMPONENT,
        trace_id=trace_id,
        client_id=provider_id,
        status="ok",
        duration_ms=duration_ms,
        detail={
            "reconciliation_run_id": run_id,
            "new_list_version": new_version,
            "clients_rescreened": rescreened,
            "newly_flagged": newly_flagged,
            "newly_cleared": newly_cleared,
        },
    )
    return ReconcileResult(
        run_id, provider_id, new_version, rescreened, newly_flagged, newly_cleared, skipped=False
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
    consumer.subscribe([TOPIC_WATCHLIST_UPDATED])
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
                    stage="reconcile",
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
                        gateway,
                        producer,
                        envelope=envelope,
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
            except Exception as exc:
                record_dead_letter(
                    stage="reconcile",
                    component=COMPONENT,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    error=exc,
                )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        gateway.close()
        producer.close()


if __name__ == "__main__":  # pragma: no cover
    main()
