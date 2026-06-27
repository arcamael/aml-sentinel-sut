"""Dataset quality — validate the *generated data*, not just the runtime DB.

The data is the test oracle for the whole SUT, so a bad data record must be
caught (doc 04 §7). These tests assert the shipped datasets are clean and prove
the validator catches each class of injected corruption.
"""

from __future__ import annotations

import json
from pathlib import Path

import allure
import pytest

from tools.datagen import watchlists
from tools.datagen.validate import validate_all, validate_watchlists

pytestmark = [pytest.mark.dq, allure.epic("AML-Sentinel"), allure.feature("Dataset quality")]

DATA = Path(__file__).resolve().parents[2] / "data"

GOOD_SANCTION = {
    "entry_id": "wl_sanctions_0001",
    "provider_id": "world_check",
    "list_type": "sanctions",
    "list_version": "v1",
    "entity_name": "Ivan Petrov",
    "aliases": ["Ivan Petroff"],
    "dob_iso": "1972-03-14",
    "country_iso2": "RU",
    "risk_payload": {"program": "OFAC-SDN", "pep_tier": None, "media_confidence": None},
}


def _write_one(tmp_path: Path, sanction: dict) -> Path:
    wl = tmp_path / "watchlists"
    wl.mkdir(parents=True, exist_ok=True)
    (wl / "sanctions.jsonl").write_text(json.dumps(sanction) + "\n", encoding="utf-8")
    (wl / "pep.jsonl").write_text("", encoding="utf-8")
    (wl / "adverse_media.jsonl").write_text("", encoding="utf-8")
    return tmp_path


def test_shipped_datasets_have_no_violations(datasets):
    violations = validate_all(DATA)
    allure.attach("\n".join(str(v) for v in violations) or "(none)", "violations")
    assert violations == [], f"{len(violations)} dataset violations: {violations[:8]}"


@pytest.mark.parametrize(
    "mutate,expected_check",
    [
        (lambda r: r.update(aliases=["Ivan Petrovff"]), "implausible_alias"),
        (lambda r: r.update(country_iso2="RUS"), "bad_country"),
        (lambda r: r.update(dob_iso="1972-13-40"), "bad_dob"),
        (lambda r: r.update(entity_name="  "), "empty_entity_name"),
        (lambda r: r.update(entry_id="sanctions_1"), "entry_id_format"),
        (lambda r: r.update(provider_id="dow_jones"), "provider_mismatch"),
        (lambda r: r["risk_payload"].update(program=None), "missing_program"),
        (lambda r: r.update(aliases=["Ivan Petroff", "Ivan Petroff"]), "duplicate_alias"),
    ],
)
def test_validator_catches_injected_bad_record(tmp_path, mutate, expected_check):
    rec = json.loads(json.dumps(GOOD_SANCTION))  # deep copy
    mutate(rec)
    violations = validate_watchlists(_write_one(tmp_path, rec))
    checks = {v.check for v in violations}
    assert expected_check in checks, f"expected {expected_check}, got {checks}"


def test_clean_record_passes_validation(tmp_path):
    assert validate_watchlists(_write_one(tmp_path, GOOD_SANCTION)) == []


def test_watchlist_generation_is_deterministic(tmp_path):
    watchlists.generate(tmp_path / "a", seed=42)
    watchlists.generate(tmp_path / "b", seed=42)
    a = (tmp_path / "a" / "sanctions.jsonl").read_bytes()
    b = (tmp_path / "b" / "sanctions.jsonl").read_bytes()
    assert a == b, "watchlist generation is not byte-identical for the same seed"
