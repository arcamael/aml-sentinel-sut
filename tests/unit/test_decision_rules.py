"""Unit — decision rules engine, golden-driven (doc 04 §4.3)."""

from __future__ import annotations

import json
from pathlib import Path

import allure
import pytest

from aml_sentinel.decisioning.rules import decide

GOLDEN = Path(__file__).resolve().parents[2] / "data" / "golden" / "decisions.jsonl"
_CASES = [json.loads(line) for line in GOLDEN.read_text(encoding="utf-8").splitlines()]


@allure.epic("AML-Sentinel")
@allure.feature("Decision engine")
@pytest.mark.unit
@pytest.mark.parametrize(
    "case", _CASES, ids=[c["expected"]["outcome"] + "-" + str(i) for i, c in enumerate(_CASES)]
)
def test_decision_matches_golden(case):
    result = decide(case["matches"])
    allure.attach(json.dumps(case["matches"]), "matches", allure.attachment_type.JSON)
    assert result.outcome == case["expected"]["outcome"]
    assert result.reason_codes == case["expected"]["reason_codes"]


@allure.epic("AML-Sentinel")
@allure.feature("Decision engine")
@pytest.mark.unit
def test_precedence_escalate_over_flag():
    result = decide(
        [
            {"list_type": "adverse_media", "score": 0.9, "media_confidence": 0.9},
            {"list_type": "sanctions", "score": 0.95},
        ]
    )
    assert result.outcome == "ESCALATE"
    assert result.reason_codes == ["ADVERSE_MEDIA", "SANCTIONS_MATCH"]
    assert result.top_match["list_type"] == "sanctions"


@allure.epic("AML-Sentinel")
@allure.feature("Decision engine")
@pytest.mark.unit
def test_no_matches_is_clear():
    result = decide([])
    assert result.outcome == "CLEAR"
    assert result.reason_codes == ["NO_MATCH"]
    assert result.top_match is None
