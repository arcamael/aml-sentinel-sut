"""Golden matching dataset (doc 04 §4.2) — defines the Phase 5 precision/recall target.

Balanced true/false (profile, candidate) pairs spanning exact, typo,
transliteration, alias-spelling, name-order swap, missing-DOB, plus the
false-positive traps the scorer must reject: same common name with a different
DOB, different first name with a shared surname, and substring overlaps.

Each row (doc 04 §4.2):

    {profile_name, candidate_name, dob_profile, dob_candidate, list_type,
     expected_match, min_score}

``expected_match`` is the ground truth; ``min_score`` is the lower bound a true
pair's score must clear (and that a false pair must stay under). Output is
sorted for deterministic diffs. These rows are hand-authored, not random.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

# (profile_name, candidate_name, dob_profile, dob_candidate, list_type, expected, min_score)
_PAIRS: list[tuple[str, str, str | None, str | None, str, bool, float]] = [
    # ── True matches ─────────────────────────────────────────────────────────
    ("Ivan Petrov", "Ivan Petrov", "1972-03-14", "1972-03-14", "sanctions", True, 0.82),  # exact
    ("Ivan Petroff", "Ivan Petrov", "1972-03-14", "1972-03-14", "sanctions", True, 0.82),  # typo
    ("Jon Smith", "John Smith", "1980-01-15", "1980-01-15", "pep", True, 0.82),  # typo
    ("Ivan Petrov", "Иван Петров", "1972-03-14", "1972-03-14", "sanctions", True, 0.82),  # translit
    ("Dmitri Ivanov", "Дмитрий Иванов", "1969-07-03", "1969-07-03", "sanctions", True, 0.82),
    (
        "Petrov Ivan",
        "Ivan Petrov",
        "1972-03-14",
        "1972-03-14",
        "sanctions",
        True,
        0.82,
    ),  # order swap
    ("Mensah Robert", "Robert Mensah", "1958-11-02", "1958-11-02", "pep", True, 0.82),  # order swap
    (
        "Viktor Ivanoff",
        "Viktor Ivanov",
        "1965-08-21",
        "1965-08-21",
        "sanctions",
        True,
        0.82,
    ),  # alias
    (
        "Ivan Petrov",
        "Ivan Petrov",
        "1972-03-14",
        None,
        "sanctions",
        True,
        0.82,
    ),  # candidate partial DOB
    ("Elena Volkova", "Elena Volkov", "1970-04-17", "1970-04-17", "pep", True, 0.82),  # spelling
    (
        "Robert K Mensah",
        "Robert Mensah",
        "1958-11-02",
        "1958-11-02",
        "pep",
        True,
        0.82,
    ),  # middle name
    (
        "Sergei Smirnov",
        "Sergey Smirnov",
        "1985-05-05",
        "1985-05-05",
        "sanctions",
        True,
        0.82,
    ),  # spelling
    # ── False matches: same/strong name but different DOB (the core trap) ─────
    ("Ivan Petrov", "Ivan Petrov", "1990-01-01", "1972-03-14", "sanctions", False, 0.82),
    ("Robert Mensah", "Robert Mensah", "1991-02-02", "1958-11-02", "pep", False, 0.82),
    ("Elena Volkova", "Elena Volkova", "1995-03-03", "1970-04-17", "pep", False, 0.82),
    # ── False matches: shared surname, different first name ──────────────────
    ("Sergei Petrov", "Ivan Petrov", "1972-03-14", "1972-03-14", "sanctions", False, 0.82),
    ("Anna Ivanova", "Viktor Ivanov", "1980-08-08", "1965-08-21", "sanctions", False, 0.82),
    # ── False matches: substring traps ───────────────────────────────────────
    ("Ivan", "Ivan Petrov", "1972-03-14", "1972-03-14", "sanctions", False, 0.82),
    ("Petrov", "Ivan Petrov", "1972-03-14", "1972-03-14", "sanctions", False, 0.82),
    ("Mensah", "Robert Mensah", "1958-11-02", "1958-11-02", "pep", False, 0.82),
    # ── False matches: unrelated people ──────────────────────────────────────
    ("John Smith", "Ivan Petrov", "1980-01-15", "1972-03-14", "sanctions", False, 0.82),
    ("Maria Garcia", "Elena Volkova", "1988-06-06", "1970-04-17", "pep", False, 0.82),
    ("Wei Chen", "Carlos Delgado", "1979-09-09", "1980-06-30", "adverse_media", False, 0.82),
    ("Ivan Petrova", "Ivan Sidorov", "1972-03-14", "1972-03-14", "sanctions", False, 0.82),
]


def build_records() -> list[dict[str, Any]]:
    records = [
        {
            "profile_name": p,
            "candidate_name": c,
            "dob_profile": dp,
            "dob_candidate": dc,
            "list_type": lt,
            "expected_match": exp,
            "min_score": ms,
        }
        for (p, c, dp, dc, lt, exp, ms) in _PAIRS
    ]
    records.sort(key=lambda r: json.dumps(r, sort_keys=True))
    return records


def _serialize(records: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(r, sort_keys=True, ensure_ascii=False) for r in records) + "\n"


def generate(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "matching.jsonl"
    payload = _serialize(build_records())
    path.write_text(payload, encoding="utf-8", newline="\n")
    sha = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    n_true = sum(1 for _, _, _, _, _, exp, _ in _PAIRS if exp)
    print(
        f"wrote {path} ({len(_PAIRS)} pairs: {n_true} true / {len(_PAIRS) - n_true} false) "
        f"sha256={sha}"
    )
    return path
