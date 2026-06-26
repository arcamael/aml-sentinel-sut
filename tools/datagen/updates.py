"""Reconciliation update scenarios (doc 04 §5).

Three deterministic scenario files under ``data/updates/`` that drive Phase 7:

* ``scenario_add_match.jsonl``    — add a sanctions entry matching a previously
  ``CLEAR`` client; bump v1→v2. Expected: that client → ESCALATE.
* ``scenario_remove.jsonl``       — remove an entry a client matched; bump v1→v2.
  Expected: that client → CLEAR on re-screen.
* ``scenario_version_bump.jsonl`` — version bump with no substantive change.
  Expected: re-screen runs, outcome unchanged (freshness without false flips).

Each line (doc 04 §5) carries the provider change, the watchlist ``entry`` (for
add/remove), a self-contained ``target_profile`` so the harness can create the
exact client, the ``new_list_version``, and ``expected_outcome_after``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

_SCENARIOS: dict[str, dict[str, Any]] = {
    "scenario_add_match.jsonl": {
        "provider_id": "world_check",
        "list_type": "sanctions",
        "change": "add",
        "entry": {
            "entry_id": "wl_sanctions_9001",
            "provider_id": "world_check",
            "list_type": "sanctions",
            "list_version": "v2",
            "entity_name": "Gregor Volkonsky",
            "aliases": ["Grigori Volkonsky"],
            "dob_iso": "1976-09-12",
            "country_iso2": "RU",
            "risk_payload": {"program": "OFAC-SDN", "pep_tier": None, "media_confidence": None},
        },
        "target_profile": {
            "full_name": "Gregor Volkonsky",
            "dob": "1976-09-12",
            "nationality": "Russia",
        },
        "target_client_id": "cli_recon_add_0001",
        "new_list_version": "v2",
        "expected_outcome_after": "ESCALATE",
    },
    "scenario_remove.jsonl": {
        "provider_id": "world_check",
        "list_type": "sanctions",
        "change": "remove",
        "entry": {
            "entry_id": "wl_sanctions_0001",
            "provider_id": "world_check",
            "list_type": "sanctions",
            "list_version": "v2",
            "entity_name": "Ivan Petrov",
        },
        "target_profile": {
            "full_name": "Ivan Petrov",
            "dob": "1972-03-14",
            "nationality": "Russia",
        },
        "target_client_id": "cli_recon_remove_0001",
        "new_list_version": "v2",
        "expected_outcome_after": "CLEAR",
    },
    "scenario_version_bump.jsonl": {
        "provider_id": "world_check",
        "list_type": "sanctions",
        "change": "version_bump",
        "entry": None,
        "target_profile": {
            "full_name": "Theodora Marchetti",
            "dob": "1983-02-19",
            "nationality": "Italy",
        },
        "target_client_id": "cli_recon_bump_0001",
        "new_list_version": "v2",
        "expected_outcome_after": "CLEAR",
    },
}


def generate(out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    for filename, scenario in _SCENARIOS.items():
        payload = json.dumps(scenario, sort_keys=True, ensure_ascii=False) + "\n"
        (out_dir / filename).write_text(payload, encoding="utf-8", newline="\n")
        sha = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        print(f"wrote {out_dir / filename} (change={scenario['change']}) sha256={sha}")
    return out_dir
