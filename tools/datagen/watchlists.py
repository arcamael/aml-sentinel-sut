"""Watchlist generator (doc 04 §2) — build first; everything else derives from it.

Produces the three provider lists the mocks serve, plus a ``manifest.json`` that
records per-provider counts, ``list_version``, and the ``plant_targets`` (entry
IDs that Phase 5 profiles will be derived from). Fully deterministic
(``SEED=42``, hard rule #1): a fixed ``random.Random`` over curated pools, output
sorted by ``entry_id``, so re-running yields byte-identical files.

Each record (doc 04 §2):

    {entry_id, provider_id, list_type, list_version, entity_name, aliases[],
     dob_iso, country_iso2, risk_payload{program, pep_tier, media_confidence}}
"""

from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path
from typing import Any

LIST_VERSION = "v1"

PROVIDERS = {
    "sanctions": ("world_check", "sanctions.jsonl"),
    "pep": ("dow_jones", "pep.jsonl"),
    "adverse_media": ("comply_advantage", "adverse_media.jsonl"),
}
COUNTS = {"sanctions": 200, "pep": 300, "adverse_media": 150}

# Curated anchors with stable entry IDs so the harness can query known names.
# Index 0 of each list is the canonical doc-04 example.
CURATED_SANCTIONS: list[dict[str, Any]] = [
    {
        "entity_name": "Ivan Petrov",
        "aliases": ["Ivan Petroff", "Иван Петров"],
        "dob_iso": "1972-03-14",
        "country_iso2": "RU",
        "program": "OFAC-SDN",
    },
    {
        "entity_name": "Viktor Ivanov",
        "aliases": ["Viktor Ivanoff", "V. Ivanov"],
        "dob_iso": "1965-08-21",
        "country_iso2": "RU",
        "program": "EU-CFSP",
    },
    {
        "entity_name": "محمد الأسد",  # non-Latin original, Latin alias
        "aliases": ["Mohammed Al-Assad", "Muhammad Al Asad"],
        "dob_iso": None,  # partial DOB → null (tests DOB corroboration absence)
        "country_iso2": "SY",
        "program": "UN-SC",
    },
]
CURATED_PEP: list[dict[str, Any]] = [
    {
        "entity_name": "Robert Mensah",
        "aliases": ["Bob Mensah", "Robert K. Mensah"],
        "dob_iso": "1958-11-02",
        "country_iso2": "NG",
        "pep_tier": 1,
    },
    {
        "entity_name": "Elena Volkova",
        "aliases": ["Elena Volkov", "Lena Volkova"],
        "dob_iso": "1970-04-17",
        "country_iso2": "RU",
        "pep_tier": 3,
    },
]
CURATED_MEDIA: list[dict[str, Any]] = [
    {
        "entity_name": "Carlos Delgado",
        "aliases": ["Carlos M. Delgado"],
        "dob_iso": "1980-06-30",
        "country_iso2": "MX",
        "media_confidence": 0.91,
    },
]

_FIRST_NAMES = [
    "Alexander",
    "Maria",
    "Dmitri",
    "Sofia",
    "Hassan",
    "Wei",
    "John",
    "Fatima",
    "Pavel",
    "Olga",
    "Ahmed",
    "Ling",
    "Robert",
    "Anna",
    "Sergei",
    "Yuki",
    "Carlos",
    "Isabel",
    "Viktor",
    "Natalia",
    "Omar",
    "Chen",
    "James",
    "Leila",
    "Andrei",
    "Katarina",
    "Ibrahim",
    "Mei",
    "Thomas",
    "Elena",
    "Nikolai",
    "Aisha",
    "Mikhail",
    "Tatiana",
    "Yusuf",
    "Hana",
    "Daniel",
    "Vera",
    "Igor",
    "Amira",
]
_LAST_NAMES = [
    "Petrov",
    "Ivanov",
    "Sidorov",
    "Kuznetsov",
    "Smirnov",
    "Volkov",
    "Sokolov",
    "Popov",
    "Lebedev",
    "Kozlov",
    "Novak",
    "Horvath",
    "Nguyen",
    "Wang",
    "Li",
    "Zhang",
    "Chen",
    "Al-Sayed",
    "Hassan",
    "Khan",
    "Mensah",
    "Okafor",
    "Mwangi",
    "Delgado",
    "Garcia",
    "Fernandez",
    "Rossi",
    "Muller",
    "Schmidt",
    "Kowalski",
    "Nowak",
    "Yilmaz",
    "Demir",
    "Tanaka",
    "Sato",
    "Kim",
    "Park",
    "Singh",
    "Petrova",
    "Volkova",
]
_COUNTRIES = ["RU", "UA", "BY", "KZ", "SY", "IR", "CN", "NG", "MX", "VE", "TR", "DE", "FR", "GB"]
_PROGRAMS = ["OFAC-SDN", "EU-CFSP", "UN-SC", "HMT", "OFAC-SSI"]


def _translit_surname(last: str) -> str | None:
    """A *realistic* transliteration variant of a surname, or None.

    Real variants substitute, not append: ``Petrov`` → ``Petroff`` (the trailing
    ``-v`` becomes ``-ff``), ``Ivanov`` → ``Ivanoff``. Surnames without a known
    pattern get no spelling variant rather than a synthetic artifact.
    """
    if last.endswith("v"):
        return last[:-1] + "ff"
    return None


def _alias_variants(rng: random.Random, first: str, last: str) -> list[str]:
    """Deterministic alias perturbations: order-swap, transliteration, family."""
    variants: list[str] = []
    if rng.random() < 0.5:
        variants.append(f"{last} {first}")  # name-order swap
    # Always draw so the RNG stream is independent of the surname shape.
    take_translit = rng.random() < 0.4
    translit = _translit_surname(last)
    if translit is not None and take_translit:
        variants.append(f"{first} {translit}")  # realistic spelling variant
    if rng.random() < 0.3:
        relative = rng.choice(_FIRST_NAMES)
        variants.append(f"{relative} {last}")  # family-member style
    # De-dupe while preserving order, deterministically.
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _near_dup_articles(rng: random.Random, name: str) -> list[dict[str, Any]]:
    """A base headline + near-duplicate variants (tests dedupe downstream)."""
    base = f"{name} linked to financial misconduct probe"
    variants = [
        base,
        base.replace("linked to", "tied to"),
        base.replace("probe", "investigation"),
    ]
    n = rng.randint(1, 3)
    return [{"headline": variants[i], "source": f"src_{i}"} for i in range(n)]


def _build_list(list_type: str, rng: random.Random) -> list[dict[str, Any]]:
    provider_id, _ = PROVIDERS[list_type]
    count = COUNTS[list_type]
    curated = {
        "sanctions": CURATED_SANCTIONS,
        "pep": CURATED_PEP,
        "adverse_media": CURATED_MEDIA,
    }[list_type]

    records: list[dict[str, Any]] = []
    for i in range(count):
        seq = i + 1
        entry_id = f"wl_{list_type}_{seq:04d}"
        if i < len(curated):
            c = curated[i]
            entity_name = c["entity_name"]
            aliases = list(c["aliases"])
            dob_iso = c["dob_iso"]
            country_iso2 = c["country_iso2"]
        else:
            first = rng.choice(_FIRST_NAMES)
            last = rng.choice(_LAST_NAMES)
            entity_name = f"{first} {last}"
            aliases = _alias_variants(rng, first, last)
            # ~20% of sanctions entries carry a partial (null) DOB.
            if list_type == "sanctions" and rng.random() < 0.2:
                dob_iso = None
            else:
                year = rng.randint(1945, 1995)
                month = rng.randint(1, 12)
                day = rng.randint(1, 28)
                dob_iso = f"{year:04d}-{month:02d}-{day:02d}"
            country_iso2 = rng.choice(_COUNTRIES)

        if list_type == "sanctions":
            program = c["program"] if i < len(curated) else rng.choice(_PROGRAMS)
            risk_payload: dict[str, Any] = {
                "program": program,
                "pep_tier": None,
                "media_confidence": None,
            }
        elif list_type == "pep":
            tier = c["pep_tier"] if i < len(curated) else rng.randint(1, 4)
            risk_payload = {"program": None, "pep_tier": tier, "media_confidence": None}
        else:  # adverse_media
            conf = c["media_confidence"] if i < len(curated) else round(rng.uniform(0.3, 0.99), 2)
            risk_payload = {
                "program": None,
                "pep_tier": None,
                "media_confidence": conf,
                "articles": _near_dup_articles(rng, entity_name),
            }

        records.append(
            {
                "entry_id": entry_id,
                "provider_id": provider_id,
                "list_type": list_type,
                "list_version": LIST_VERSION,
                "entity_name": entity_name,
                "aliases": aliases,
                "dob_iso": dob_iso,
                "country_iso2": country_iso2,
                "risk_payload": risk_payload,
            }
        )

    records.sort(key=lambda r: r["entry_id"])
    return records


def _serialize(records: list[dict[str, Any]]) -> str:
    lines = [json.dumps(r, sort_keys=True, ensure_ascii=False) for r in records]
    return "\n".join(lines) + "\n"


def _plant_targets(all_records: dict[str, list[dict[str, Any]]]) -> list[str]:
    """Deterministic ~15% subset of entry IDs that profiles will be derived from."""
    targets: list[str] = []
    for list_type, records in all_records.items():
        for idx, rec in enumerate(records):
            # Always include the curated anchors; sample the rest at ~1/7.
            if idx < {"sanctions": 3, "pep": 2, "adverse_media": 1}[list_type] or idx % 7 == 0:
                targets.append(rec["entry_id"])
    return sorted(targets)


def generate(out_dir: Path, seed: int = 42) -> Path:
    """Write the three list files + manifest under ``out_dir``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    all_records: dict[str, list[dict[str, Any]]] = {}
    file_shas: dict[str, str] = {}
    providers_manifest: dict[str, Any] = {}

    # Generate in a fixed list order so the shared RNG stream is deterministic.
    for list_type in ("sanctions", "pep", "adverse_media"):
        records = _build_list(list_type, rng)
        all_records[list_type] = records
        provider_id, filename = PROVIDERS[list_type]
        payload = _serialize(records)
        (out_dir / filename).write_text(payload, encoding="utf-8", newline="\n")
        sha = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        file_shas[filename] = sha
        providers_manifest[provider_id] = {
            "list_type": list_type,
            "list_version": LIST_VERSION,
            "count": len(records),
            "file": filename,
            "sha256": sha,
        }

    manifest = {
        "seed": seed,
        "list_version": LIST_VERSION,
        "providers": providers_manifest,
        "plant_targets": _plant_targets(all_records),
    }
    manifest_text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    (out_dir / "manifest.json").write_text(manifest_text, encoding="utf-8", newline="\n")

    total = sum(len(r) for r in all_records.values())
    print(
        f"wrote {total} watchlist entries to {out_dir} "
        f"(sanctions={len(all_records['sanctions'])}, pep={len(all_records['pep'])}, "
        f"adverse_media={len(all_records['adverse_media'])}); "
        f"plant_targets={len(manifest['plant_targets'])}"
    )
    for filename, sha in sorted(file_shas.items()):
        print(f"  {filename} sha256={sha}")
    return out_dir / "manifest.json"
