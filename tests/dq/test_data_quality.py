"""SQL data-quality monitors as tests (doc 02 §7)."""

from __future__ import annotations

import allure
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.observability import monitors
from tests.helpers import create_client, screen_and_decide

pytestmark = [pytest.mark.dq, allure.epic("AML-Sentinel"), allure.feature("Data-quality monitors")]


def _run(check):
    with SessionLocal() as s:
        return check(s)


def test_clean_client_raises_no_per_client_breach(gateway, producer, sanctions_mock):
    cid, tid, norm = create_client("Quintus Aurelius", "1985-05-05", "Italy")
    screen_and_decide(gateway, producer, cid, tid, norm, sanctions_mock["token"])
    for check in (
        monitors.check_completeness,
        monitors.check_decision_coverage,
        monitors.check_lineage,
        monitors.check_determinism,
    ):
        result = _run(check)
        assert cid not in result.breaches, f"{check.__name__} flagged a clean client"


def test_audit_immutability_holds():
    assert _run(monitors.check_audit_immutability).passed


def test_orphan_match_monitor_fires_on_injected_orphan():
    orphan_id = f"match_orphan_{ULID()}"
    with SessionLocal() as s:
        s.execute(text("SET LOCAL session_replication_role = replica"))
        s.execute(
            text(
                "INSERT INTO match (match_id, screening_id, provider_id, list_type, "
                "matched_name, score, dob_match, created_at) VALUES "
                "(:id, 'nope', 'world_check', 'sanctions', 'Orphan', 0.99, false, now())"
            ),
            {"id": orphan_id},
        )
        s.commit()
    try:
        result = _run(monitors.check_orphan_match)
        assert not result.passed
        assert orphan_id in result.breaches
    finally:
        with SessionLocal() as s:
            s.execute(text("DELETE FROM match WHERE match_id = :id"), {"id": orphan_id})
            s.commit()


def test_completeness_monitor_fires_on_missing_normalized():
    cid = f"cli_incomplete_{ULID()}"
    with SessionLocal() as s:
        s.execute(
            text(
                "INSERT INTO raw_profile "
                "(id, client_id, trace_id, raw_payload, source, created_at) "
                "VALUES (:id, :cid, :tid, '{}', 'rest', now())"
            ),
            {"id": str(ULID()), "cid": cid, "tid": str(ULID())},
        )
        s.commit()
    try:
        result = _run(monitors.check_completeness)
        assert not result.passed
        assert cid in result.breaches
    finally:
        with SessionLocal() as s:
            s.execute(text("DELETE FROM raw_profile WHERE client_id = :cid"), {"cid": cid})
            s.commit()


def test_audit_update_is_rejected_by_db():
    """The trigger itself is a test target — a raw UPDATE must be refused."""
    with SessionLocal() as s:
        audit_id = s.execute(text("SELECT audit_id FROM audit LIMIT 1")).first()
        if audit_id is None:
            pytest.skip("no audit rows to probe")
        with pytest.raises(IntegrityError):
            s.execute(
                text("UPDATE audit SET trace_id = 'tamper' WHERE audit_id = :id"),
                {"id": audit_id[0]},
            )
            s.commit()
        s.rollback()
