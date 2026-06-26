"""E2E — ingest → normalize → screen → decide; DB, events, and logs agree."""

from __future__ import annotations

import allure
import pytest
from sqlalchemy import func, select

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Audit, Decision, Match, Screening
from tests.helpers import create_client, screen_and_decide

pytestmark = [pytest.mark.e2e, allure.epic("AML-Sentinel"), allure.feature("End-to-end pipeline")]


def test_sanctioned_client_escalates_with_consistent_lineage(gateway, producer, sanctions_mock):
    token = sanctions_mock["token"]
    cid, tid, norm = create_client("Ivan Petrov", "1972-03-14", "Russia")
    screen, decision = screen_and_decide(gateway, producer, cid, tid, norm, token)

    allure.attach(str(screen.event_payload), "screening.completed")
    with SessionLocal() as s:
        db_matches = s.scalar(
            select(func.count()).select_from(Match).where(Match.screening_id == screen.screening_id)
        )
        db_decisions = s.scalar(
            select(func.count())
            .select_from(Decision)
            .where(Decision.screening_id == screen.screening_id)
        )
        screening = s.get(Screening, screen.screening_id)
        audit = s.scalar(select(Audit).where(Audit.decision_id == decision.decision_id))

    # Outcome + 1:1 coverage.
    assert decision.outcome == "ESCALATE"
    assert decision.reason_codes == ["SANCTIONS_MATCH"]
    assert db_decisions == 1

    # DB == event == log (screen.match_count is exactly screen.detail.matches).
    assert screen.match_count == db_matches == len(screen.event_payload["matches"])

    # Audit cites the driving match; list_versions captured.
    assert any(m["list_type"] == "sanctions" for m in audit.snapshot["matches"])
    assert any(t["rule"] == "SANCTIONS_MATCH" for t in audit.snapshot["rule_trace"])
    assert sanctions_mock["provider_id"] in screening.list_versions

    # trace_id propagated unchanged across every row (lineage).
    with SessionLocal() as s:
        rows = s.execute(select(Screening.trace_id).where(Screening.client_id == cid)).all()
    assert all(r[0] == tid for r in rows)
    assert audit.snapshot["trace_id"] == tid


def test_non_listed_client_is_cleared(gateway, producer, sanctions_mock):
    token = sanctions_mock["token"]
    cid, tid, norm = create_client("Aurelia Nightingale", "1991-04-23", "Spain")
    screen, decision = screen_and_decide(gateway, producer, cid, tid, norm, token)
    assert screen.match_count == 0
    assert decision.outcome == "CLEAR"
    assert decision.reason_codes == ["NO_MATCH"]
