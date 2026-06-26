"""Kafka topics, the shared event envelope, and a thin producer wrapper.

Every message on every topic shares the envelope defined in doc 02 §2. The
``trace_id`` rides in the envelope unchanged from ingestion to audit (hard
rule #3).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer

from aml_sentinel.config import settings

# ── Topic names (doc 02 §1) ──────────────────────────────────────────────────
TOPIC_CLIENT_SUBMITTED = "client.submitted"
TOPIC_PROFILE_NORMALIZED = "profile.normalized"
TOPIC_SCREENING_COMPLETED = "screening.completed"
TOPIC_DECISION_MADE = "decision.made"
TOPIC_WATCHLIST_UPDATED = "watchlist.updated"

SCHEMA_VERSION = 1


def make_envelope(
    *,
    trace_id: str,
    client_id: str,
    event_type: str,
    producer: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Build the canonical event envelope (doc 02 §2)."""
    return {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "produced_at": datetime.now(UTC).isoformat(),
        "producer": producer,
        "payload": payload,
    }


class EventProducer:
    """Synchronous Kafka producer wrapper.

    Keyed by ``client_id`` so all events for a client land on one partition,
    preserving per-client ordering (doc 02 §1).
    """

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers or settings.kafka_bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "client.id": "aml-sentinel",
            }
        )

    def produce(self, topic: str, key: str, envelope: dict[str, Any]) -> None:
        """Produce one envelope and block until it is acknowledged."""
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(envelope, separators=(",", ":")).encode("utf-8"),
        )
        # Flush synchronously so the caller can rely on durability before
        # returning a 201 (ingestion must not lose the event).
        self._producer.flush(10)

    def close(self) -> None:
        self._producer.flush(10)
