"""Unit — normalization rules, golden-driven (doc 04 §4.1)."""

from __future__ import annotations

import json
from pathlib import Path

import allure
import pytest

from aml_sentinel.matching.normalize import normalize, normalize_country, normalize_dob

GOLDEN = Path(__file__).resolve().parents[2] / "data" / "golden" / "normalization.jsonl"
_CASES = [json.loads(line) for line in GOLDEN.read_text(encoding="utf-8").splitlines()]


@allure.epic("AML-Sentinel")
@allure.feature("Normalization")
@pytest.mark.unit
@pytest.mark.parametrize("case", _CASES, ids=[c["expected"]["canonical_name"] for c in _CASES])
def test_normalization_matches_golden(case):
    with allure.step("normalize the raw input"):
        produced = normalize(case["input"]).to_golden_expected()
        allure.attach(json.dumps(case["input"]), "input", allure.attachment_type.JSON)
        allure.attach(json.dumps(produced), "produced", allure.attachment_type.JSON)
    assert produced == case["expected"]


@allure.epic("AML-Sentinel")
@allure.feature("Normalization")
@pytest.mark.unit
def test_profile_hash_is_deterministic():
    a = normalize({"full_name": "Ivan  Petroff", "dob": "14/03/1972", "nationality": "Russia"})
    b = normalize({"full_name": "Ivan  Petroff", "dob": "14/03/1972", "nationality": "Russia"})
    assert a.profile_hash == b.profile_hash


@allure.epic("AML-Sentinel")
@allure.feature("Normalization")
@pytest.mark.unit
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1972-03-14", "1972-03-14"),
        ("14/03/1972", "1972-03-14"),
        ("02.11.1965", "1965-11-02"),
        ("", None),
        ("not-a-date", None),
    ],
)
def test_dob_parsing(raw, expected):
    assert normalize_dob(raw) == expected


@allure.epic("AML-Sentinel")
@allure.feature("Normalization")
@pytest.mark.unit
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Russia", "RU"),
        ("russian federation", "RU"),
        ("CN", "CN"),
        ("Atlantis", None),
        (None, None),
    ],
)
def test_country_normalization(raw, expected):
    assert normalize_country(raw) == expected
