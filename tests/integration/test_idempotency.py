"""Integration — consumer idempotency: redelivery creates no duplicate rows."""

from __future__ import annotations

import allure
import pytest
from sqlalchemy import func, select

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Decision, Screening
from aml_sentinel.workers import decision as decision_worker
from aml_sentinel.workers import screening as screening_worker
from tests.helpers import create_client, screen_and_decide

pytestmark = [pytest.mark.integration, allure.epic("AML-Sentinel"), allure.feature("Idempotency")]


def _count(model, client_id):
    with SessionLocal() as s:
        return s.scalar(select(func.count()).select_from(model).where(model.client_id == client_id))


def test_screening_redelivery_creates_no_duplicate(gateway, producer, sanctions_mock):
    token = sanctions_mock["token"]
    cid, tid, norm = create_client("Ivan Petrov", "1972-03-14", "Russia")
    screen, _ = screen_and_decide(gateway, producer, cid, tid, norm, token, offset=0)
    assert screen.created

    # Re-deliver the exact same message identity → idempotent skip.
    norm_env = {
        "trace_id": tid,
        "client_id": cid,
        "event_type": "profile.normalized",
        "payload": norm.to_normalized_payload(),
    }
    with SessionLocal() as s:
        again = screening_worker.process_message(
            s, gateway, producer, envelope=norm_env, topic=f"t:{token}", partition=0, offset=0
        )
    assert again.skipped
    assert _count(Screening, cid) == 1


def test_decision_redelivery_and_unique_backstop(gateway, producer, sanctions_mock):
    token = sanctions_mock["token"]
    cid, tid, norm = create_client("Viktor Ivanov", "1965-08-21", "Russia")
    screen, outcome = screen_and_decide(gateway, producer, cid, tid, norm, token, offset=0)
    assert outcome.created

    sc_env = {
        "trace_id": tid,
        "client_id": cid,
        "event_type": "screening.completed",
        "payload": screen.event_payload,
    }
    # Same key → idempotent skip; new key but same screening_id → UNIQUE backstop.
    with SessionLocal() as s:
        same = decision_worker.process_message(
            s, producer, envelope=sc_env, topic=f"t:{token}:d", partition=0, offset=0
        )
    with SessionLocal() as s:
        retry = decision_worker.process_message(
            s, producer, envelope=sc_env, topic=f"t:{token}:d", partition=0, offset=99
        )
    assert same.skipped and retry.skipped
    assert _count(Decision, cid) == 1
