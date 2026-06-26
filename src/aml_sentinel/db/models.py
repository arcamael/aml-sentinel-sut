"""ORM models for the AML-Sentinel system of record.

Mirrors doc 02 §4 exactly. Foreign keys enforce lineage; the test harness uses
them to detect orphans. Every business row carries a ``trace_id`` so a client's
journey (ingest → normalize → screen → decide) is reconstructable end-to-end.

ID columns are application-supplied strings (ULID/UUIDv7) rather than DB
sequences, because the same identifiers travel on the Kafka envelopes.
"""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aml_sentinel.db.base import Base

# Enum-like value domains, enforced as CHECK constraints (kept in the DB so the
# SDET can prove the constraint, not just the application).
SOURCE_VALUES = ("rest", "kafka")
LIST_TYPE_VALUES = ("sanctions", "pep", "adverse_media")
SCREENING_STATUS_VALUES = ("completed", "failed")
OUTCOME_VALUES = ("CLEAR", "FLAG", "ESCALATE")


def _in(column: str, values: tuple[str, ...]) -> str:
    rendered = ", ".join(f"'{v}'" for v in values)
    return f"{column} IN ({rendered})"


class RawProfile(Base):
    """§4.1 — the raw KYC profile as submitted."""

    __tablename__ = "raw_profile"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    client_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (CheckConstraint(_in("source", SOURCE_VALUES), name="ck_raw_profile_source"),)


class NormalizedProfile(Base):
    """§4.2 — deterministic canonicalization of a raw profile."""

    __tablename__ = "normalized_profile"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    client_id: Mapped[str] = mapped_column(
        String, ForeignKey("raw_profile.client_id"), nullable=False
    )
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    profile_hash: Mapped[str] = mapped_column(String, nullable=False)
    canonical_name: Mapped[str] = mapped_column(String, nullable=False)
    name_parts: Mapped[dict] = mapped_column(JSONB, nullable=False)
    dob_iso: Mapped[date | None] = mapped_column(Date, nullable=True)
    nationality_iso2: Mapped[str | None] = mapped_column(String(2), nullable=True)
    residence_iso2: Mapped[str | None] = mapped_column(String(2), nullable=True)
    document_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("client_id", "profile_hash", name="uq_normalized_client_hash"),
    )


class WatchlistEntry(Base):
    """§4.3 — local mirror of provider watchlist data."""

    __tablename__ = "watchlist_entry"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    provider_id: Mapped[str] = mapped_column(String, nullable=False)
    list_type: Mapped[str] = mapped_column(String, nullable=False)
    list_version: Mapped[str] = mapped_column(String, nullable=False)
    entity_name: Mapped[str] = mapped_column(String, nullable=False)
    aliases: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    dob_iso: Mapped[date | None] = mapped_column(Date, nullable=True)
    country_iso2: Mapped[str | None] = mapped_column(String(2), nullable=True)
    risk_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(_in("list_type", LIST_TYPE_VALUES), name="ck_watchlist_list_type"),
    )


class Screening(Base):
    """§4.4 — a screening run for a client's normalized profile."""

    __tablename__ = "screening"

    id: Mapped[str] = mapped_column("screening_id", String, primary_key=True)
    client_id: Mapped[str] = mapped_column(String, nullable=False)
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    profile_hash: Mapped[str] = mapped_column(String, nullable=False)
    list_versions: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String, nullable=False)
    screened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(_in("status", SCREENING_STATUS_VALUES), name="ck_screening_status"),
    )


class Match(Base):
    """§4.5 — a scored candidate match produced by the screening worker."""

    __tablename__ = "match"

    id: Mapped[str] = mapped_column("match_id", String, primary_key=True)
    screening_id: Mapped[str] = mapped_column(
        String, ForeignKey("screening.screening_id"), nullable=False
    )
    provider_id: Mapped[str] = mapped_column(String, nullable=False)
    list_type: Mapped[str] = mapped_column(String, nullable=False)
    watchlist_entry_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("watchlist_entry.id"), nullable=True
    )
    matched_name: Mapped[str] = mapped_column(String, nullable=False)
    score: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    dob_match: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(_in("list_type", LIST_TYPE_VALUES), name="ck_match_list_type"),
        CheckConstraint("score >= 0 AND score <= 1", name="ck_match_score_range"),
    )


class Decision(Base):
    """§4.6 — the explainable CLEAR/FLAG/ESCALATE outcome (1:1 with screening)."""

    __tablename__ = "decision"

    id: Mapped[str] = mapped_column("decision_id", String, primary_key=True)
    screening_id: Mapped[str] = mapped_column(
        String, ForeignKey("screening.screening_id"), unique=True, nullable=False
    )
    client_id: Mapped[str] = mapped_column(String, nullable=False)
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    outcome: Mapped[str] = mapped_column(String, nullable=False)
    reason_codes: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    top_match_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("match.match_id"), nullable=True
    )
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (CheckConstraint(_in("outcome", OUTCOME_VALUES), name="ck_decision_outcome"),)


class Audit(Base):
    """§4.7 — append-only immutable snapshot (inputs + matches + rule trace).

    Immutability is enforced at the DB layer by a trigger created in the
    migration (rejects UPDATE and DELETE). This table is itself a test target.
    """

    __tablename__ = "audit"

    id: Mapped[str] = mapped_column("audit_id", String, primary_key=True)
    decision_id: Mapped[str] = mapped_column(
        String, ForeignKey("decision.decision_id"), nullable=False
    )
    trace_id: Mapped[str] = mapped_column(String, nullable=False)
    snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class Idempotency(Base):
    """§4.8 — consumer idempotency keys (``trace_id:topic:offset``)."""

    __tablename__ = "idempotency"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class DeadLetter(Base):
    """§Phase 8 — failed messages, so no failure is silently dropped.

    A worker that cannot process a message records it here (with the stage and
    error) instead of crashing or losing it; the dead-letter count is a
    data-quality metric and the rows are a test surface (doc 02 §6 rule 3).
    """

    __tablename__ = "dead_letter"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    topic: Mapped[str] = mapped_column(String, nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    msg_offset: Mapped[int] = mapped_column("offset", Integer, nullable=False, default=0)
    trace_id: Mapped[str | None] = mapped_column(String, nullable=True)
    client_id: Mapped[str | None] = mapped_column(String, nullable=True)
    stage: Mapped[str] = mapped_column(String, nullable=False)
    error_type: Mapped[str] = mapped_column(String, nullable=False)
    error_msg: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class ReconciliationRun(Base):
    """§4.9 — bookkeeping for a list-update reconciliation pass."""

    __tablename__ = "reconciliation_run"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    provider_id: Mapped[str] = mapped_column(String, nullable=False)
    list_type: Mapped[str] = mapped_column(String, nullable=False)
    old_version: Mapped[str | None] = mapped_column(String, nullable=True)
    new_version: Mapped[str] = mapped_column(String, nullable=False)
    clients_rescreened: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    newly_flagged: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    newly_cleared: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(_in("list_type", LIST_TYPE_VALUES), name="ck_reconciliation_list_type"),
    )
