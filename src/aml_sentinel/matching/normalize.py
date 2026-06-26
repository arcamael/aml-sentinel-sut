"""Deterministic canonicalization of a raw KYC profile (Phase 3).

This is the first real data-quality surface: dirty inputs (double spaces, mixed
case, ``DD/MM/YYYY`` dates, full country names, non-Latin scripts) are mapped to
a single canonical form. The mapping is **pure and deterministic** (hard rule
#1): the same input always produces the same output, including a stable
``profile_hash``.

The data generator imports :func:`normalize` so the golden
``normalization.jsonl`` expectations and the Normalizer worker stay in
lock-step (doc 04 §4.1) — there is exactly one canonicalization in the system.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from unidecode import unidecode

from aml_sentinel.matching.countries import COUNTRY_TO_ISO2, VALID_ISO2

# Punctuation that is *not* part of a name token. Hyphen and apostrophe are kept
# (compound surnames "Müller-Schmidt", "O'Brien"); everything else becomes a
# separator so trailing/embedded punctuation collapses away.
_NON_NAME_CHARS = re.compile(r"[^\w\s'-]", re.UNICODE)
_WHITESPACE = re.compile(r"\s+")

_ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_SLASH_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")  # DD/MM/YYYY
_DOT_DATE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")  # DD.MM.YYYY


@dataclass(frozen=True)
class NormalizationResult:
    """Canonical form of a raw profile plus the metadata the worker logs."""

    canonical_name: str
    name_parts: dict[str, str]
    dob_iso: str | None
    nationality_iso2: str | None
    residence_iso2: str | None
    document_ids: list[dict[str, Any]]
    profile_hash: str
    transliterated: bool
    fields_defaulted: list[str]

    def to_normalized_payload(self) -> dict[str, Any]:
        """The ``profile.normalized`` event payload (doc 02 §2)."""
        return {
            "profile_hash": self.profile_hash,
            "canonical_name": self.canonical_name,
            "name_parts": self.name_parts,
            "dob_iso": self.dob_iso,
            "nationality_iso2": self.nationality_iso2,
            "residence_iso2": self.residence_iso2,
            "document_ids": self.document_ids,
        }

    def to_golden_expected(self) -> dict[str, Any]:
        """The shape stored as ``expected`` in golden/normalization.jsonl."""
        return {
            "canonical_name": self.canonical_name,
            "name_parts": self.name_parts,
            "dob_iso": self.dob_iso,
            "nationality_iso2": self.nationality_iso2,
            "residence_iso2": self.residence_iso2,
            "profile_hash": self.profile_hash,
        }


def transliterate(value: str) -> tuple[str, bool]:
    """Romanise non-Latin text; report whether anything actually changed.

    ``transliterated`` is True when the input contained non-ASCII characters
    (e.g. Cyrillic "Иван" → "Ivan"), which the worker surfaces in its log line.
    """
    ascii_value = unidecode(value)
    changed = ascii_value != value
    return ascii_value, changed


def canonical_name(raw_name: str) -> str:
    """Transliterate → lowercase → strip punctuation → collapse whitespace."""
    ascii_name, _ = transliterate(raw_name)
    lowered = ascii_name.lower()
    no_punct = _NON_NAME_CHARS.sub(" ", lowered)
    collapsed = _WHITESPACE.sub(" ", no_punct).strip()
    # Trim stray hyphens/apostrophes left dangling at token edges.
    tokens = [t.strip("-'") for t in collapsed.split(" ")]
    return " ".join(t for t in tokens if t)


def parse_name_parts(canonical: str) -> dict[str, str]:
    """Split a canonical name into ``first``/``last`` (+ ``middle`` if present).

    A single-token name (mononym / single-name entity, an edge category in doc
    04) is treated as a surname so it lines up with watchlist ``entity_name``
    matching downstream.
    """
    tokens = canonical.split()
    if not tokens:
        return {"first": "", "last": ""}
    if len(tokens) == 1:
        return {"first": "", "last": tokens[0]}
    parts = {"first": tokens[0], "last": tokens[-1]}
    if len(tokens) > 2:
        parts["middle"] = " ".join(tokens[1:-1])
    return parts


def normalize_dob(raw_dob: str | None) -> str | None:
    """Parse the supported DOB formats to ISO ``YYYY-MM-DD``; else ``None``.

    Supports ISO (pass-through), ``DD/MM/YYYY`` and ``DD.MM.YYYY``. Invalid or
    missing dates default to ``None`` (recorded in ``fields_defaulted``).
    """
    if raw_dob is None:
        return None
    s = str(raw_dob).strip()
    if not s:
        return None

    iso = _ISO_DATE.match(s)
    if iso:
        year, month, day = (int(g) for g in iso.groups())
    else:
        dmy = _SLASH_DATE.match(s) or _DOT_DATE.match(s)
        if not dmy:
            return None
        day, month, year = (int(g) for g in dmy.groups())

    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def normalize_country(raw_country: str | None) -> str | None:
    """Map a country name (or ISO-2 code) to ISO-3166-1 alpha-2; else ``None``."""
    if raw_country is None:
        return None
    s = str(raw_country).strip()
    if not s:
        return None
    # Already an ISO-2 code we recognise.
    if len(s) == 2 and s.upper() in VALID_ISO2:
        return s.upper()
    ascii_value, _ = transliterate(s)
    key = _WHITESPACE.sub(" ", ascii_value.lower()).strip().strip(".")
    return COUNTRY_TO_ISO2.get(key)


def compute_profile_hash(
    canonical_name_value: str,
    dob_iso: str | None,
    nationality_iso2: str | None,
) -> str:
    """Stable SHA-256 over the screening identity (name + DOB + nationality).

    Residence and document IDs are deliberately excluded: they do not change
    *who* is being screened, so re-submitting the same person with a new address
    yields the same ``profile_hash`` (supports idempotency + reconciliation).
    """
    basis = "|".join(
        [
            canonical_name_value,
            dob_iso or "",
            nationality_iso2 or "",
        ]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def normalize(raw_kyc: dict[str, Any]) -> NormalizationResult:
    """Canonicalize a raw KYC profile (doc 04 §3 shape) into the result above."""
    raw_name = str(raw_kyc.get("full_name", ""))
    canonical = canonical_name(raw_name)
    name_parts = parse_name_parts(canonical)

    _, transliterated = transliterate(raw_name)

    dob_iso = normalize_dob(raw_kyc.get("dob"))
    nationality_iso2 = normalize_country(raw_kyc.get("nationality"))
    residence_iso2 = normalize_country(raw_kyc.get("residence_country"))
    document_ids = list(raw_kyc.get("document_ids") or [])

    profile_hash = compute_profile_hash(canonical, dob_iso, nationality_iso2)

    fields_defaulted: list[str] = []
    if dob_iso is None:
        fields_defaulted.append("dob_iso")
    if nationality_iso2 is None:
        fields_defaulted.append("nationality_iso2")
    if residence_iso2 is None:
        fields_defaulted.append("residence_iso2")

    return NormalizationResult(
        canonical_name=canonical,
        name_parts=name_parts,
        dob_iso=dob_iso,
        nationality_iso2=nationality_iso2,
        residence_iso2=residence_iso2,
        document_ids=document_ids,
        profile_hash=profile_hash,
        transliterated=transliterated,
        fields_defaulted=fields_defaulted,
    )
