"""Golden decision dataset (doc 04 §4.3) — one row per distinct rule path.

Each row is a set of matches plus the expected ``outcome`` + ``reason_codes``
(doc 04 §4.3). Hand-authored ground truth covering every rule path and the
multi-match precedence cases (ESCALATE > FLAG > CLEAR), so Phase 6's engine is
validated against an independent source of truth (hard rule #2). Output sorted
for deterministic diffs.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

# (matches, expected_outcome, expected_reason_codes)
_CASES: list[tuple[list[dict[str, Any]], str, list[str]]] = [
    # ── single rule paths ────────────────────────────────────────────────────
    ([{"list_type": "sanctions", "score": 0.93}], "ESCALATE", ["SANCTIONS_MATCH"]),
    ([{"list_type": "pep", "score": 0.90, "pep_tier": 1}], "ESCALATE", ["PEP_TIER_1_2"]),
    ([{"list_type": "pep", "score": 0.88, "pep_tier": 2}], "ESCALATE", ["PEP_TIER_1_2"]),
    ([{"list_type": "pep", "score": 0.91, "pep_tier": 3}], "FLAG", ["PEP_TIER_3_4"]),
    ([{"list_type": "pep", "score": 0.95, "pep_tier": 4}], "FLAG", ["PEP_TIER_3_4"]),
    (
        [{"list_type": "adverse_media", "score": 0.90, "media_confidence": 0.9}],
        "FLAG",
        ["ADVERSE_MEDIA"],
    ),
    ([], "CLEAR", ["NO_MATCH"]),
    # ── multi-match precedence ───────────────────────────────────────────────
    (
        [
            {"list_type": "sanctions", "score": 0.90},
            {"list_type": "pep", "score": 0.90, "pep_tier": 3},
        ],
        "ESCALATE",
        ["PEP_TIER_3_4", "SANCTIONS_MATCH"],
    ),
    (
        [
            {"list_type": "pep", "score": 0.90, "pep_tier": 3},
            {"list_type": "adverse_media", "score": 0.88, "media_confidence": 0.8},
        ],
        "FLAG",
        ["ADVERSE_MEDIA", "PEP_TIER_3_4"],
    ),
    (
        [
            {"list_type": "sanctions", "score": 0.95},
            {"list_type": "adverse_media", "score": 0.90, "media_confidence": 0.85},
        ],
        "ESCALATE",
        ["ADVERSE_MEDIA", "SANCTIONS_MATCH"],
    ),
    (
        [
            {"list_type": "pep", "score": 0.92, "pep_tier": 2},
            {"list_type": "adverse_media", "score": 0.85, "media_confidence": 0.7},
        ],
        "ESCALATE",
        ["ADVERSE_MEDIA", "PEP_TIER_1_2"],
    ),
    (
        [
            {"list_type": "pep", "score": 0.90, "pep_tier": 1},
            {"list_type": "pep", "score": 0.90, "pep_tier": 4},
        ],
        "ESCALATE",
        ["PEP_TIER_1_2", "PEP_TIER_3_4"],
    ),
]


def build_records() -> list[dict[str, Any]]:
    records = [
        {"matches": matches, "expected": {"outcome": outcome, "reason_codes": codes}}
        for (matches, outcome, codes) in _CASES
    ]
    records.sort(key=lambda r: json.dumps(r, sort_keys=True))
    return records


def _serialize(records: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(r, sort_keys=True, ensure_ascii=False) for r in records) + "\n"


def generate(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "decisions.jsonl"
    payload = _serialize(build_records())
    path.write_text(payload, encoding="utf-8", newline="\n")
    sha = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    print(f"wrote {path} ({len(_CASES)} decision cases) sha256={sha}")
    return path
