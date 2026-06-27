"""Dataset quality validation (doc 04 §7).

The generators produce the data the whole SUT is tested against, so the data
itself is a test surface. :func:`validate_all` runs schema, range, referential,
determinism, and *plausibility* checks over every generated file and returns the
violating records — so a bad data record is caught instead of silently flowing
through the pipeline. Wired into ``python -m tools.datagen verify`` and the
``tests/dq`` dataset-quality tests.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from aml_sentinel.matching.normalize import canonical_name, normalize

ENTRY_ID_RE = re.compile(r"^wl_(sanctions|pep|adverse_media)_\d{4}$")
ISO2_RE = re.compile(r"^[A-Z]{2}$")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

PROVIDER_FOR = {"sanctions": "world_check", "pep": "dow_jones", "adverse_media": "comply_advantage"}
FILE_FOR = {
    "sanctions": "sanctions.jsonl",
    "pep": "pep.jsonl",
    "adverse_media": "adverse_media.jsonl",
}
OUTCOMES = {"CLEAR", "FLAG", "ESCALATE"}


@dataclass(frozen=True)
class Violation:
    check: str
    locator: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.check}] {self.locator}: {self.detail}"


def _load(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _valid_date(value: str) -> bool:
    if not ISO_DATE_RE.match(value):
        return False
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _alias_is_append_artifact(entity_name: str, alias: str) -> bool:
    """True when an alias token merely *appends* letters to a full entity token.

    Real transliteration variants substitute (``Petrov`` → ``Petroff``); a pure
    append (``Ivanov`` → ``Ivanovff``, ``Singh`` → ``Singha``) is a synthetic,
    implausible artifact — not the kind of data a real watchlist holds.
    """
    entity_tokens = set(canonical_name(entity_name).split())
    for atok in canonical_name(alias).split():
        if atok in entity_tokens:
            continue
        for etok in entity_tokens:
            if atok != etok and atok.startswith(etok) and len(atok) > len(etok):
                return True
    return False


def validate_watchlists(data_dir: Path) -> list[Violation]:
    v: list[Violation] = []
    seen_ids: set[str] = set()
    for list_type, filename in FILE_FOR.items():
        path = data_dir / "watchlists" / filename
        if not path.exists():
            v.append(Violation("missing_file", filename, "watchlist file not found"))
            continue
        for rec in _load(path):
            eid = rec.get("entry_id", "?")
            loc = f"{filename}:{eid}"

            if not ENTRY_ID_RE.match(str(eid)) or f"_{list_type}_" not in str(eid):
                v.append(Violation("entry_id_format", loc, f"bad entry_id {eid!r}"))
            if eid in seen_ids:
                v.append(Violation("duplicate_entry_id", loc, "entry_id seen in another record"))
            seen_ids.add(eid)

            if rec.get("provider_id") != PROVIDER_FOR[list_type]:
                v.append(
                    Violation("provider_mismatch", loc, f"provider_id={rec.get('provider_id')}")
                )
            if rec.get("list_type") != list_type:
                v.append(Violation("list_type_mismatch", loc, f"list_type={rec.get('list_type')}"))

            name = str(rec.get("entity_name", "")).strip()
            if not name:
                v.append(Violation("empty_entity_name", loc, "entity_name blank"))

            aliases = rec.get("aliases", [])
            if not isinstance(aliases, list):
                v.append(Violation("aliases_type", loc, "aliases is not a list"))
                aliases = []
            if len(aliases) != len(set(aliases)):
                v.append(Violation("duplicate_alias", loc, "aliases contain duplicates"))
            for alias in aliases:
                if not str(alias).strip():
                    v.append(Violation("empty_alias", loc, "blank alias"))
                elif _alias_is_append_artifact(name, str(alias)):
                    v.append(
                        Violation("implausible_alias", loc, f"append-artifact alias {alias!r}")
                    )

            dob = rec.get("dob_iso")
            if dob is not None and not _valid_date(str(dob)):
                v.append(Violation("bad_dob", loc, f"dob_iso={dob!r}"))

            country = rec.get("country_iso2")
            if country is not None and not ISO2_RE.match(str(country)):
                v.append(Violation("bad_country", loc, f"country_iso2={country!r}"))

            risk = rec.get("risk_payload", {})
            if list_type == "sanctions" and not risk.get("program"):
                v.append(Violation("missing_program", loc, "sanctions entry has no program"))
            if list_type == "pep" and risk.get("pep_tier") not in (1, 2, 3, 4):
                v.append(Violation("bad_pep_tier", loc, f"pep_tier={risk.get('pep_tier')}"))
            if list_type == "adverse_media":
                conf = risk.get("media_confidence")
                if conf is None or not (0.3 <= conf <= 0.99):
                    v.append(Violation("bad_media_confidence", loc, f"media_confidence={conf}"))
    return v


def validate_goldens(data_dir: Path) -> list[Violation]:
    v: list[Violation] = []
    golden = data_dir / "golden"

    norm_path = golden / "normalization.jsonl"
    if norm_path.exists():
        for i, rec in enumerate(_load(norm_path)):
            produced = normalize(rec["input"]).to_golden_expected()
            if produced != rec["expected"]:
                v.append(
                    Violation(
                        "golden_norm_drift",
                        f"normalization.jsonl:{i}",
                        "expected does not recompute from normalize()",
                    )
                )

    match_path = golden / "matching.jsonl"
    if match_path.exists():
        for i, rec in enumerate(_load(match_path)):
            loc = f"matching.jsonl:{i}"
            if (
                not str(rec.get("profile_name", "")).strip()
                or not str(rec.get("candidate_name", "")).strip()
            ):
                v.append(Violation("golden_match_empty_name", loc, "blank name"))
            if not isinstance(rec.get("expected_match"), bool):
                v.append(Violation("golden_match_label", loc, "expected_match not bool"))
            if not (0.0 <= rec.get("min_score", -1) <= 1.0):
                v.append(
                    Violation("golden_match_min_score", loc, f"min_score={rec.get('min_score')}")
                )
            for k in ("dob_profile", "dob_candidate"):
                dob = rec.get(k)
                if dob is not None and not _valid_date(str(dob)):
                    v.append(Violation("golden_match_dob", loc, f"{k}={dob!r}"))

    dec_path = golden / "decisions.jsonl"
    if dec_path.exists():
        for i, rec in enumerate(_load(dec_path)):
            loc = f"decisions.jsonl:{i}"
            exp = rec.get("expected", {})
            if exp.get("outcome") not in OUTCOMES:
                v.append(Violation("golden_decision_outcome", loc, f"outcome={exp.get('outcome')}"))
            codes = exp.get("reason_codes")
            if not isinstance(codes, list) or not codes:
                v.append(Violation("golden_decision_codes", loc, "reason_codes empty/not-list"))
    return v


def validate_manifest(data_dir: Path) -> list[Violation]:
    v: list[Violation] = []
    man_path = data_dir / "watchlists" / "manifest.json"
    if not man_path.exists():
        return [Violation("missing_file", "manifest.json", "manifest not found")]
    man = json.loads(man_path.read_text())

    all_ids: set[str] = set()
    for list_type, filename in FILE_FOR.items():
        path = data_dir / "watchlists" / filename
        if path.exists():
            rows = _load(path)
            all_ids |= {r["entry_id"] for r in rows}
            declared = man.get("providers", {}).get(PROVIDER_FOR[list_type], {}).get("count")
            if declared is not None and declared != len(rows):
                v.append(
                    Violation("manifest_count", filename, f"declared {declared} != {len(rows)}")
                )

    for target in man.get("plant_targets", []):
        if target not in all_ids:
            v.append(
                Violation("dangling_plant_target", "manifest.json", f"{target} not in any list")
            )
    return v


def validate_all(data_dir: Path) -> list[Violation]:
    return validate_watchlists(data_dir) + validate_goldens(data_dir) + validate_manifest(data_dir)
