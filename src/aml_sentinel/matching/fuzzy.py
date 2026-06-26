"""Fuzzy name matching + scoring (Phase 5).

Turns a (profile, candidate) pair into a ``score ∈ [0, 1]`` that the screening
worker thresholds into ``match`` rows. The scorer is deliberately small and
explainable — it is validated against ``data/golden/matching.jsonl`` (hard rule
#2: tune to the golden set, never to a single case).

Design:

* **Name similarity** uses ``rapidfuzz`` ``token_sort_ratio`` (tolerant of
  first/last name-order swaps) combined with plain ``ratio`` (catches typos and
  transliteration variants once both sides are canonicalized). We deliberately
  avoid ``token_set_ratio``/``WRatio`` because they score a substring ("Ivan" vs
  "Ivan Petrov") as a perfect match — exactly the false-positive trap the golden
  set guards against.
* **Alias handling**: a candidate is scored against its ``entity_name`` *and*
  every alias; the best wins.
* **DOB corroboration**: a *different* DOB hard-caps the score (kills
  "same common name, different person" false positives); a *matching* DOB gives
  a small boost; a *missing* DOB (e.g. sanctions entries with a partial DOB)
  leaves the name score untouched so recall is preserved.
"""

from __future__ import annotations

from dataclasses import dataclass

from rapidfuzz import fuzz

from aml_sentinel.matching.normalize import canonical_name

# Tuned against data/golden/matching.jsonl to hit precision ≥ 0.95, recall ≥ 0.90.
SCREENING_THRESHOLD = 0.82
DIFFERENT_DOB_CAP = 0.75  # a mismatched DOB can never clear the threshold
SAME_DOB_BOOST = 0.03


def name_similarity(a_canonical: str, b_canonical: str) -> float:
    """Order-tolerant name similarity in ``[0, 1]`` over two canonical names."""
    return (
        max(
            fuzz.ratio(a_canonical, b_canonical),
            fuzz.token_sort_ratio(a_canonical, b_canonical),
        )
        / 100.0
    )


def dob_relation(dob_profile: str | None, dob_candidate: str | None) -> str:
    """``same`` | ``different`` | ``unknown`` (when either DOB is absent)."""
    if dob_profile and dob_candidate:
        return "same" if dob_profile == dob_candidate else "different"
    return "unknown"


def score_pair(
    profile_name: str,
    candidate_name: str,
    dob_profile: str | None = None,
    dob_candidate: str | None = None,
) -> float:
    """Score one profile-name vs one candidate-name, with DOB corroboration."""
    base = name_similarity(canonical_name(profile_name), canonical_name(candidate_name))
    relation = dob_relation(dob_profile, dob_candidate)
    if relation == "different":
        return round(min(base, DIFFERENT_DOB_CAP), 4)
    if relation == "same":
        return round(min(1.0, base + SAME_DOB_BOOST), 4)
    return round(base, 4)


@dataclass(frozen=True)
class ScoredCandidate:
    """Best score for a candidate entity across its entity_name + aliases."""

    score: float
    matched_name: str
    dob_match: bool


def score_candidate(
    profile_name: str,
    profile_dob: str | None,
    *,
    entity_name: str,
    aliases: list[str],
    candidate_dob: str | None,
) -> ScoredCandidate:
    """Score a profile against a full candidate entity (entity_name + aliases)."""
    best_score = -1.0
    best_name = entity_name
    for name in [entity_name, *aliases]:
        s = score_pair(profile_name, name, profile_dob, candidate_dob)
        if s > best_score:
            best_score = s
            best_name = name
    return ScoredCandidate(
        score=best_score,
        matched_name=best_name,
        dob_match=dob_relation(profile_dob, candidate_dob) == "same",
    )
