"""Reconciliation — watchlist updates re-screen affected clients (Phase 7)."""

from __future__ import annotations

import uuid

import allure
import httpx
import pytest
from sqlalchemy import text

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.workers import reconciler as recon
from tests.helpers import create_client, screen_and_decide

pytestmark = [
    pytest.mark.reconciliation,
    allure.epic("AML-Sentinel"),
    allure.feature("Reconciliation"),
]


def _latest_version(client_id: str, provider_id: str) -> str | None:
    with SessionLocal() as s:
        row = s.execute(
            text(
                "SELECT list_versions FROM screening WHERE client_id=:c "
                "ORDER BY screened_at DESC LIMIT 1"
            ),
            {"c": client_id},
        ).first()
    return (row[0] or {}).get(provider_id) if row else None


def _latest_outcome(client_id: str) -> str | None:
    with SessionLocal() as s:
        return recon._latest_decision_outcome(s, client_id)


def _reconcile(gateway, producer, sm, *, change, new_version, entry=None):
    event = {
        "trace_id": str(uuid.uuid7()),
        "client_id": sm["provider_id"],
        "event_type": "watchlist.updated",
        "payload": {
            "provider_id": sm["provider_id"],
            "list_type": "sanctions",
            "change": change,
            "entry": entry,
            "new_list_version": new_version,
        },
    }
    body = {"change": change, "new_list_version": new_version}
    if entry is not None:
        body["entry"] = entry
    httpx.post(f"{sm['base_url']}/_control/list", json=body, timeout=3.0)
    with SessionLocal() as s:
        return recon.process_message(
            s,
            gateway,
            producer,
            envelope=event,
            topic="watchlist.updated",
            partition=0,
            offset=0,
        )


def test_add_match_flips_clear_client_to_escalate(gateway, producer, sanctions_mock):
    sm = sanctions_mock
    cid, tid, norm = create_client("Gregor Volkonsky", "1976-09-12", "Russia")
    _, baseline = screen_and_decide(gateway, producer, cid, tid, norm, sm["token"])
    assert baseline.outcome == "CLEAR"
    assert _latest_version(cid, sm["provider_id"]) == "v1"

    entry = {
        "entry_id": "wl_sanctions_9001",
        "provider_id": sm["provider_id"],
        "list_type": "sanctions",
        "list_version": "v2",
        "entity_name": "Gregor Volkonsky",
        "aliases": ["Grigori Volkonsky"],
        "dob_iso": "1976-09-12",
        "country_iso2": "RU",
        "risk_payload": {"program": "OFAC-SDN", "pep_tier": None, "media_confidence": None},
    }
    result = _reconcile(gateway, producer, sm, change="add", new_version="v2", entry=entry)

    allure.attach(str(result), "reconciliation_result")
    assert result.newly_flagged == 1
    assert _latest_outcome(cid) == "ESCALATE"
    assert _latest_version(cid, sm["provider_id"]) == "v2"  # no stale active screening remains


def test_version_bump_rescreens_without_false_flip(gateway, producer, sanctions_mock):
    sm = sanctions_mock
    cid, tid, norm = create_client("Theodora Marchetti", "1983-02-19", "Italy")
    _, baseline = screen_and_decide(gateway, producer, cid, tid, norm, sm["token"])
    assert baseline.outcome == "CLEAR"

    result = _reconcile(gateway, producer, sm, change="version_bump", new_version="v2")

    assert result.clients_rescreened >= 1
    assert result.newly_flagged == 0
    assert _latest_outcome(cid) == "CLEAR"
    assert _latest_version(cid, sm["provider_id"]) == "v2"
