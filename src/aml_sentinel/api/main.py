"""AML-Sentinel Ingestion API (Phase 2).

Accepts a KYC profile, persists ``raw_profile``, and emits ``client.submitted``
with the canonical envelope. Generates a UUIDv7 ``trace_id`` when the caller does
not supply one; that ``trace_id`` is propagated unchanged downstream (hard rule #3).
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from ulid import ULID

from aml_sentinel.api.deps import close_producer, get_db, get_producer
from aml_sentinel.api.schemas import ClientCreate, ClientCreated, ClientView
from aml_sentinel.db.models import RawProfile
from aml_sentinel.events import (
    TOPIC_CLIENT_SUBMITTED,
    EventProducer,
    make_envelope,
)
from aml_sentinel.ids import uuid7
from aml_sentinel.observability.logging import configure_logging, stage_log

COMPONENT = "ingestion-api"

# ── Metrics ──────────────────────────────────────────────────────────────────
INGEST_TOTAL = Counter(
    "aml_ingest_total",
    "Ingestion requests by outcome.",
    labelnames=("status",),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    yield
    close_producer()


app = FastAPI(title="AML-Sentinel Ingestion API", version="0.1.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "component": COMPONENT}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/clients", response_model=ClientCreated, status_code=201)
def create_client(
    body: ClientCreate,
    db: Session = Depends(get_db),
    producer: EventProducer = Depends(get_producer),
) -> ClientCreated:
    started = time.perf_counter()

    # Generate identifiers when absent. trace_id is UUIDv7 (doc 01 §6).
    trace_id = body.trace_id or str(uuid7())
    client_id = body.client_id or f"cli_{ULID()}"
    kyc = body.kyc_payload()

    raw = RawProfile(
        id=str(ULID()),
        client_id=client_id,
        trace_id=trace_id,
        raw_payload=kyc,
        source="rest",
    )
    db.add(raw)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # Duplicate client_id (UNIQUE) — idempotent-ish reject, not a 500.
        stage_log(
            stage="ingest",
            component=COMPONENT,
            trace_id=trace_id,
            client_id=client_id,
            status="failed",
            level="WARNING",
            detail={"error_type": "duplicate_client_id", "error_msg": "client_id exists"},
        )
        INGEST_TOTAL.labels(status="duplicate").inc()
        raise HTTPException(status_code=409, detail="client_id already exists") from None

    # Persisted → emit the event. Payload is the raw KYC profile (doc 02 §2).
    envelope = make_envelope(
        trace_id=trace_id,
        client_id=client_id,
        event_type="client.submitted",
        producer=COMPONENT,
        payload=kyc,
    )
    producer.produce(TOPIC_CLIENT_SUBMITTED, key=client_id, envelope=envelope)

    duration_ms = int((time.perf_counter() - started) * 1000)
    INGEST_TOTAL.labels(status="ok").inc()
    stage_log(
        stage="ingest",
        component=COMPONENT,
        trace_id=trace_id,
        client_id=client_id,
        status="ok",
        duration_ms=duration_ms,
        detail={"source": "rest", "topic": TOPIC_CLIENT_SUBMITTED},
    )

    return ClientCreated(client_id=client_id, trace_id=trace_id)


@app.get("/clients/{client_id}", response_model=ClientView)
def get_client(client_id: str, db: Session = Depends(get_db)) -> RawProfile:
    raw = db.scalar(select(RawProfile).where(RawProfile.client_id == client_id))
    if raw is None:
        raise HTTPException(status_code=404, detail="client not found")
    return raw
