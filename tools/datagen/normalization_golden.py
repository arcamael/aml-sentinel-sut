"""Golden dataset generator for normalization (doc 04 §4.1).

Produces ``data/golden/normalization.jsonl``: one ``{"input", "expected"}`` per
line, where ``expected`` is computed by the *same* canonicalization the
Normalizer worker uses (:func:`aml_sentinel.matching.normalize.normalize`). The
generator and the worker therefore agree by construction — including
``profile_hash`` (doc 04 §4.1, hard rule #2: golden is the source of truth).

The input cases are hand-authored to cover every dirty/edge transformation at
least once (double spaces, mixed case, ``DD/MM/YYYY`` and ``DD.MM.YYYY`` dates,
full country names, trailing punctuation, transliteration of Cyrillic/Arabic/
Chinese, hyphenated and single-name entities, missing DOB/residence, ISO-2
pass-through, very long names). Output is sorted for deterministic, stable diffs.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from aml_sentinel.matching.normalize import normalize

# Each input mirrors the raw KYC shape (doc 04 §3). ``residence_country`` and
# ``document_ids`` are optional. Comments name the transformation each case
# exercises so the coverage intent is auditable.
_INPUT_CASES: list[dict[str, Any]] = [
    # clean baseline
    {"full_name": "John Smith", "dob": "1980-01-15", "nationality": "United Kingdom"},
    # double spaces + alternate (Latin) spelling
    {"full_name": "Ivan  Petroff", "dob": "14/03/1972", "nationality": "Russia"},
    # mixed case + trailing punctuation
    {"full_name": "  aLICE   o'Brien.  ", "dob": "1990-07-04", "nationality": "Ireland"},
    # DD.MM.YYYY date + full country name → ISO-2
    {"full_name": "Hans Müller", "dob": "02.11.1965", "nationality": "Germany"},
    # transliteration: Cyrillic → Latin
    {"full_name": "Иван Петров", "dob": "1972-03-14", "nationality": "Russian Federation"},
    # transliteration: Arabic script
    {"full_name": "محمد علي", "dob": "1985-09-09", "nationality": "Egypt"},
    # transliteration: Chinese script
    {"full_name": "习近平", "dob": "1953-06-15", "nationality": "China"},
    # hyphenated / compound surname + middle name
    {
        "full_name": "Anne-Marie van der Berg",
        "dob": "12/05/1978",
        "nationality": "Netherlands",
        "residence_country": "Belgium",
    },
    # single-name entity (mononym)
    {"full_name": "Suharto", "dob": "1921-06-08", "nationality": "Indonesia"},
    # missing DOB (defaulted) + residence present
    {
        "full_name": "Maria  Garcia",
        "dob": "",
        "nationality": "Spain",
        "residence_country": "Portugal",
    },
    # ISO-2 codes passed through unchanged
    {"full_name": "Liu Wei", "dob": "1988-12-31", "nationality": "CN", "residence_country": "US"},
    # unknown country → defaulted to null
    {"full_name": "Test Person", "dob": "2000-02-29", "nationality": "Atlantis"},
    # very long, multi-part name
    {
        "full_name": "Juan Carlos Alfonso Victor Maria de Borbon y Borbon",
        "dob": "05/01/1938",
        "nationality": "Spain",
    },
    # mixed script with Latin alias-style spacing
    {"full_name": "Дмитрий   Иванов", "dob": "03.07.1969", "nationality": "Belarus"},
    # document_ids present (should pass through into normalized payload, not hash)
    {
        "full_name": "Robert Mugabe",
        "dob": "21/02/1924",
        "nationality": "Zimbabwe",
        "document_ids": [{"type": "passport", "value": "ZW0099887"}],
    },
]


def build_records() -> list[dict[str, Any]]:
    """Compute ``expected`` for each input and return sorted golden records."""
    records = []
    for raw in _INPUT_CASES:
        result = normalize(raw)
        records.append({"input": raw, "expected": result.to_golden_expected()})
    # Deterministic order: sort by the canonical name then the full JSON.
    records.sort(key=lambda r: (r["expected"]["canonical_name"], json.dumps(r, sort_keys=True)))
    return records


def _serialize(records: list[dict[str, Any]]) -> str:
    """One compact JSON object per line, keys sorted, trailing newline."""
    lines = [json.dumps(r, sort_keys=True, ensure_ascii=False) for r in records]
    return "\n".join(lines) + "\n"


def generate(out_dir: Path) -> Path:
    """Write ``normalization.jsonl`` under ``out_dir`` and return its path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "normalization.jsonl"
    payload = _serialize(build_records())
    path.write_text(payload, encoding="utf-8", newline="\n")
    sha = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    print(f"wrote {path} ({len(build_records())} records) sha256={sha}")
    return path
