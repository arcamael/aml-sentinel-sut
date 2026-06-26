"""Smoke seed: insert exactly one row per table, respecting FK order.

Proves the schema is wired correctly end-to-end (lineage FKs satisfiable, JSONB
columns accept payloads, the append-only audit row inserts). IDs are freshly
generated per run so the script is safely re-runnable without deleting rows —
which matters because the ``audit`` row can never be removed.

Usage:
    python -m aml_sentinel.db.seed_smoke
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import (
    Audit,
    Decision,
    Idempotency,
    Match,
    NormalizedProfile,
    RawProfile,
    ReconciliationRun,
    Screening,
    WatchlistEntry,
)


def seed() -> dict[str, str]:
    """Insert one row per table and return a map of table → primary key."""
    trace_id = str(ULID())  # one trace_id threads the client's lineage
    client_id = f"client-{ULID()}"
    profile_hash = f"hash-{ULID()}"

    raw = RawProfile(
        id=str(ULID()),
        client_id=client_id,
        trace_id=trace_id,
        raw_payload={"name": "Ivan Petrov", "dob": "1980-01-01", "nationality": "RU"},
        source="rest",
    )

    normalized = NormalizedProfile(
        id=str(ULID()),
        client_id=client_id,
        trace_id=trace_id,
        profile_hash=profile_hash,
        canonical_name="ivan petrov",
        name_parts={"first": "ivan", "last": "petrov"},
        dob_iso=date(1980, 1, 1),
        nationality_iso2="RU",
        residence_iso2="RU",
        document_ids=[{"type": "passport", "value": "X1234567"}],
    )

    watchlist = WatchlistEntry(
        id=str(ULID()),
        provider_id="world_check",
        list_type="sanctions",
        list_version="v1",
        entity_name="Ivan Petrov",
        aliases=["Ivan Petroff"],
        dob_iso=date(1980, 1, 1),
        country_iso2="RU",
        risk_payload={"program": "OFAC-SDN"},
        is_active=True,
    )

    screening = Screening(
        id=str(ULID()),
        client_id=client_id,
        trace_id=trace_id,
        profile_hash=profile_hash,
        list_versions={"world_check": "v1"},
        status="completed",
    )

    match = Match(
        id=str(ULID()),
        screening_id=screening.id,
        provider_id="world_check",
        list_type="sanctions",
        watchlist_entry_id=watchlist.id,
        matched_name="Ivan Petrov",
        score=Decimal("0.9876"),
        dob_match=True,
    )

    decision = Decision(
        id=str(ULID()),
        screening_id=screening.id,
        client_id=client_id,
        trace_id=trace_id,
        outcome="ESCALATE",
        reason_codes=["SANCTIONS_MATCH"],
        top_match_id=match.id,
    )

    audit = Audit(
        id=str(ULID()),
        decision_id=decision.id,
        trace_id=trace_id,
        snapshot={
            "inputs": {"canonical_name": "ivan petrov"},
            "matches": [{"match_id": match.id, "score": "0.9876"}],
            "rule_trace": ["SANCTIONS_MATCH >= tau -> ESCALATE"],
        },
    )

    idempotency = Idempotency(
        key=f"{trace_id}:client.submitted:0",
        processed_at=datetime.now(UTC),
    )

    reconciliation = ReconciliationRun(
        id=str(ULID()),
        provider_id="world_check",
        list_type="sanctions",
        old_version="v0",
        new_version="v1",
        clients_rescreened=1,
        newly_flagged=1,
        newly_cleared=0,
        finished_at=datetime.now(UTC),
    )

    # Insert in strict FK dependency order. We flush each row explicitly rather
    # than relying on the unit-of-work sort, because no ORM relationships() are
    # declared (the models are deliberately plain) so SQLAlchemy can't infer the
    # ordering on its own.
    ordered_rows = [
        raw,
        normalized,
        watchlist,
        screening,
        match,
        decision,
        audit,
        idempotency,
        reconciliation,
    ]

    with SessionLocal() as session:
        for row in ordered_rows:
            session.add(row)
            session.flush()
        session.commit()
        result = {
            "raw_profile": raw.id,
            "normalized_profile": normalized.id,
            "watchlist_entry": watchlist.id,
            "screening": screening.id,
            "match": match.id,
            "decision": decision.id,
            "audit": audit.id,
            "idempotency": idempotency.key,
            "reconciliation_run": reconciliation.id,
        }
    return result


def main() -> None:
    inserted = seed()
    print("✓ smoke seed inserted one row per table (FK order respected):")
    for table, pk in inserted.items():
        print(f"  {table:<22} {pk}")


if __name__ == "__main__":
    main()
