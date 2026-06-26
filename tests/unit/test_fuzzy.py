"""Unit — fuzzy scorer, golden-driven precision/recall (doc 04 §4.2)."""

from __future__ import annotations

import json
from pathlib import Path

import allure
import pytest

from aml_sentinel.matching.fuzzy import SCREENING_THRESHOLD, score_pair

GOLDEN = Path(__file__).resolve().parents[2] / "data" / "golden" / "matching.jsonl"
_PAIRS = [json.loads(line) for line in GOLDEN.read_text(encoding="utf-8").splitlines()]


def _score(p):
    return score_pair(p["profile_name"], p["candidate_name"], p["dob_profile"], p["dob_candidate"])


@allure.epic("AML-Sentinel")
@allure.feature("Fuzzy matching")
@pytest.mark.unit
@pytest.mark.parametrize(
    "pair", _PAIRS, ids=[f"{p['profile_name']}~{p['candidate_name']}" for p in _PAIRS]
)
def test_pair_prediction_matches_expectation(pair):
    score = _score(pair)
    predicted = score >= SCREENING_THRESHOLD
    if pair["expected_match"]:
        assert score >= pair["min_score"], f"true pair scored {score:.3f}"
    assert predicted == pair["expected_match"]


@allure.epic("AML-Sentinel")
@allure.feature("Fuzzy matching")
@pytest.mark.unit
def test_precision_recall_targets():
    tp = fp = fn = 0
    for p in _PAIRS:
        predicted = _score(p) >= SCREENING_THRESHOLD
        if predicted and p["expected_match"]:
            tp += 1
        elif predicted and not p["expected_match"]:
            fp += 1
        elif not predicted and p["expected_match"]:
            fn += 1
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    allure.attach(f"precision={precision:.3f} recall={recall:.3f}", "metrics")
    assert precision >= 0.95
    assert recall >= 0.90
