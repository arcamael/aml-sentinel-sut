"""Decision rules engine (Phase 6).

A small **config-driven** rules table turns a screening's matches into an
explainable ``CLEAR`` / ``FLAG`` / ``ESCALATE`` outcome plus ``reason_codes`` and
a full rule trace (for the immutable audit snapshot). Validated against
``data/golden/decisions.jsonl`` (hard rule #2).

Rule set (doc 03 Phase 6):

* any ``sanctions`` match ≥ τ            → ESCALATE  ``SANCTIONS_MATCH``
* ``pep`` tier 1–2 (≥ τ)                 → ESCALATE  ``PEP_TIER_1_2``
* ``pep`` tier 3–4 (≥ τ)                 → FLAG      ``PEP_TIER_3_4``
* ``adverse_media`` match ≥ τ            → FLAG      ``ADVERSE_MEDIA``
* nothing                                → CLEAR     ``NO_MATCH``

Precedence is by severity: ESCALATE > FLAG > CLEAR. All triggered reason codes
are reported (sorted) so a multi-match screening is fully explained.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from aml_sentinel.matching.fuzzy import SCREENING_THRESHOLD

# The decision-level score floor equals the screening floor: every match the
# screening worker persisted is decision-active. Kept as a named constant so the
# threshold is configuration, not a magic number sprinkled through the rules.
DECISION_THRESHOLD = SCREENING_THRESHOLD

SEVERITY = {"CLEAR": 1, "FLAG": 2, "ESCALATE": 3}
NO_MATCH_CODE = "NO_MATCH"


@dataclass(frozen=True)
class Rule:
    code: str
    list_type: str
    outcome: str
    min_score: float = DECISION_THRESHOLD
    tiers: tuple[int, ...] | None = None  # PEP only

    def applies_to(self, match: dict[str, Any]) -> bool:
        if match.get("list_type") != self.list_type:
            return False
        if float(match.get("score", 0.0)) < self.min_score:
            return False
        if self.tiers is not None and match.get("pep_tier") not in self.tiers:
            return False
        return True


# Config-driven table (order is documentation only; precedence is by severity).
RULES: tuple[Rule, ...] = (
    Rule("SANCTIONS_MATCH", "sanctions", "ESCALATE"),
    Rule("PEP_TIER_1_2", "pep", "ESCALATE", tiers=(1, 2)),
    Rule("PEP_TIER_3_4", "pep", "FLAG", tiers=(3, 4)),
    Rule("ADVERSE_MEDIA", "adverse_media", "FLAG"),
)


@dataclass(frozen=True)
class DecisionResult:
    outcome: str
    reason_codes: list[str]
    top_match: dict[str, Any] | None
    rule_trace: list[dict[str, Any]] = field(default_factory=list)


def decide(matches: list[dict[str, Any]]) -> DecisionResult:
    """Apply the rules table to a screening's matches → an explainable decision."""
    triggered: list[tuple[Rule, dict[str, Any]]] = []
    for rule in RULES:
        for match in matches:
            if rule.applies_to(match):
                triggered.append((rule, match))

    if not triggered:
        return DecisionResult(
            outcome="CLEAR",
            reason_codes=[NO_MATCH_CODE],
            top_match=None,
            rule_trace=[],
        )

    reason_codes = sorted({rule.code for rule, _ in triggered})
    outcome = max((rule.outcome for rule, _ in triggered), key=lambda o: SEVERITY[o])

    # Top match = the highest-scoring match that drove the *final* outcome.
    drivers = [(rule, m) for rule, m in triggered if rule.outcome == outcome]
    top_match = max(drivers, key=lambda rm: float(rm[1].get("score", 0.0)))[1]

    rule_trace = [
        {
            "rule": rule.code,
            "list_type": rule.list_type,
            "outcome": rule.outcome,
            "score": match.get("score"),
            "pep_tier": match.get("pep_tier"),
            "match_ref": match.get("match_id") or match.get("evidence_ref"),
        }
        for rule, match in triggered
    ]
    return DecisionResult(outcome, reason_codes, top_match, rule_trace)
