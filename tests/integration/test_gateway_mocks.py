"""Integration — provider gateway against the mocks, incl. fault injection."""

from __future__ import annotations

import allure
import httpx
import pytest

pytestmark = [
    pytest.mark.integration,
    allure.epic("AML-Sentinel"),
    allure.feature("Provider gateway"),
]


def test_planted_sanctioned_name_returns_candidate(gateway):
    resp = gateway.query("sanctions", "Ivan Petrov", dob_iso="1972-03-14")
    ids = [c.entry_id for c in resp.candidates]
    allure.attach(str(ids[:10]), "candidate_ids")
    assert "wl_sanctions_0001" in ids
    assert resp.list_version == "v1"
    assert not resp.degraded


def test_repeat_query_is_cache_hit(gateway):
    first = gateway.query("sanctions", "Ivan Petrov", dob_iso="1972-03-14")
    second = gateway.query("sanctions", "Ivan Petrov", dob_iso="1972-03-14")
    assert not first.cache_hit
    assert second.cache_hit
    assert [c.entry_id for c in second.candidates] == [c.entry_id for c in first.candidates]


@pytest.mark.parametrize("fault", ["timeout", "500", "malformed", "empty"])
def test_provider_fault_degrades_gracefully(gateway, sanctions_mock, fault):
    httpx.post(f"{sanctions_mock['base_url']}/_control/fault", json={"type": fault}, timeout=2.0)
    with allure.step(f"query under injected fault={fault}"):
        resp = gateway.query("sanctions", "Sergei Smirnov", dob_iso="1980-01-01")
    httpx.post(f"{sanctions_mock['base_url']}/_control/fault", json={"type": "clear"}, timeout=2.0)
    assert resp.degraded
    assert resp.candidates == []
    assert resp.error is not None
