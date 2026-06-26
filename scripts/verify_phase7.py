"""Phase 7 verification (roadmap 🔎).

Drives the reconciliation scenarios against isolated, in-process mocks. Each run
uses a unique sanctions ``provider_id`` so the reconciler's global
"screened against an older version" selection only sees *this* run's clients
(the shared DB carries history from earlier phases).

Primary (DoD / ``scenario_add_match.jsonl``): a previously-CLEAR client matching
a name added in v2 becomes ESCALATE, with a fresh screening referencing v2 and
``reconciliation_run.newly_flagged == 1``; no stale-version active screening
remains for that client. Also checks ``scenario_version_bump.jsonl`` — a version
bump with no list change re-screens but does not flip (freshness, no false flip).

Requires the Phase 0 stack. Run:

    PYTHONPATH=src:. .venv/bin/python scripts/verify_phase7.py
"""

from __future__ import annotations

import json
import sys
import threading
import time
import uuid
from datetime import date
from pathlib import Path

import httpx
import redis as redis_lib
import uvicorn
from sqlalchemy import text
from ulid import ULID

from aml_sentinel.config import settings
from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import NormalizedProfile, RawProfile, ReconciliationRun
from aml_sentinel.events import EventProducer
from aml_sentinel.ids import uuid7
from aml_sentinel.matching.normalize import normalize
from aml_sentinel.observability.logging import configure_logging
from aml_sentinel.providers.gateway import ProviderGateway
from aml_sentinel.providers.models import ProviderConfig
from aml_sentinel.workers import decision as decision_worker
from aml_sentinel.workers import reconciler as recon
from aml_sentinel.workers import screening as screening_worker
from mocks.provider_mock import create_app

UPDATES = Path("data/updates")


def _serve(app, port: int) -> uvicorn.Server:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    threading.Thread(target=server.run, daemon=True).start()
    return server


def _wait_health(port: int, timeout_s: float = 10.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0).status_code == 200:
                return
        except httpx.HTTPError:
            time.sleep(0.1)
    raise RuntimeError(f"mock on {port} never became healthy")


def _create_client(profile: dict) -> tuple[str, str, object]:
    """Insert raw_profile + normalized_profile for a target; return ids + norm."""
    client_id = f"cli_recon_{ULID()}"
    trace_id = str(uuid7())
    norm = normalize(profile)
    with SessionLocal() as s:
        s.add(
            RawProfile(
                id=str(ULID()),
                client_id=client_id,
                trace_id=trace_id,
                raw_payload=profile,
                source="rest",
            )
        )
        s.flush()  # raw_profile must exist before the normalized_profile FK
        s.add(
            NormalizedProfile(
                id=str(ULID()),
                client_id=client_id,
                trace_id=trace_id,
                profile_hash=norm.profile_hash,
                canonical_name=norm.canonical_name,
                name_parts=norm.name_parts,
                dob_iso=date.fromisoformat(norm.dob_iso) if norm.dob_iso else None,
                nationality_iso2=norm.nationality_iso2,
                residence_iso2=norm.residence_iso2,
                document_ids=norm.document_ids,
            )
        )
        s.commit()
    return client_id, trace_id, norm


def _baseline_screen_decide(gateway, producer, client_id, trace_id, norm, token):
    """Screen + decide a client once (baseline) and return the outcome."""
    env = {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": "profile.normalized",
        "payload": norm.to_normalized_payload(),
    }
    with SessionLocal() as s:
        screen = screening_worker.process_message(
            s, gateway, producer, envelope=env, topic=f"base:{token}", partition=0, offset=0
        )
    sc_env = {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": "screening.completed",
        "payload": screen.event_payload,
    }
    with SessionLocal() as s:
        outcome = decision_worker.process_message(
            s, producer, envelope=sc_env, topic=f"base:{token}:decide", partition=0, offset=0
        )
    return outcome.outcome


def _latest_screening_version(client_id: str, provider_id: str) -> str | None:
    row = (
        SessionLocal()
        .execute(
            text(
                "SELECT list_versions FROM screening WHERE client_id=:c "
                "ORDER BY screened_at DESC LIMIT 1"
            ),
            {"c": client_id},
        )
        .first()
    )
    return (row[0] or {}).get(provider_id) if row else None


def _latest_outcome(client_id: str) -> str | None:
    return recon._latest_decision_outcome(SessionLocal(), client_id)


def run_scenario(scenario: dict, *, sanctions_port: int, pep_port: int, media_port: int):
    """Create the target, apply the change, reconcile; return measurements."""
    token = uuid.uuid4().hex[:8]
    provider_id = f"wc_{scenario['change']}_{token}"
    rds = redis_lib.Redis.from_url(settings.redis_url)

    # Fresh sanctions mock for this scenario, advertising the unique provider_id.
    app = create_app(
        provider_id=provider_id,
        list_type="sanctions",
        watchlist_path="data/watchlists/sanctions.jsonl",
        list_version="v1",
    )
    _serve(app, sanctions_port)
    _wait_health(sanctions_port)

    providers = {
        "sanctions": ProviderConfig(provider_id, "sanctions", f"http://127.0.0.1:{sanctions_port}"),
        "pep": ProviderConfig("dow_jones", "pep", f"http://127.0.0.1:{pep_port}"),
        "adverse_media": ProviderConfig(
            "comply_advantage", "adverse_media", f"http://127.0.0.1:{media_port}"
        ),
    }
    gateway = ProviderGateway(providers=providers, redis_client=rds)
    producer = EventProducer()

    client_id, trace_id, norm = _create_client(scenario["target_profile"])
    baseline = _baseline_screen_decide(gateway, producer, client_id, trace_id, norm, token)
    base_version = _latest_screening_version(client_id, provider_id)

    # Apply the provider-side change (the external source of truth changes first).
    entry = scenario.get("entry")
    body: dict = {"change": scenario["change"], "new_list_version": scenario["new_list_version"]}
    if entry is not None:
        entry = {**entry, "provider_id": provider_id}
        body["entry"] = entry
    httpx.post(f"http://127.0.0.1:{sanctions_port}/_control/list", json=body, timeout=3.0)

    # Emit watchlist.updated → reconcile.
    wl_event = {
        "trace_id": str(uuid7()),
        "client_id": provider_id,
        "event_type": "watchlist.updated",
        "payload": {
            "provider_id": provider_id,
            "list_type": scenario["list_type"],
            "change": scenario["change"],
            "entry": entry,
            "new_list_version": scenario["new_list_version"],
        },
    }
    with SessionLocal() as s:
        result = recon.process_message(
            s,
            gateway,
            producer,
            envelope=wl_event,
            topic="watchlist.updated",
            partition=0,
            offset=0,
        )

    after_outcome = _latest_outcome(client_id)
    after_version = _latest_screening_version(client_id, provider_id)
    with SessionLocal() as s:
        run_row = s.get(ReconciliationRun, result.run_id)
        run_flagged = run_row.newly_flagged
        run_cleared = run_row.newly_cleared

    gateway.close()
    producer.close()
    return {
        "provider_id": provider_id,
        "baseline": baseline,
        "base_version": base_version,
        "after": after_outcome,
        "after_version": after_version,
        "rescreened": result.clients_rescreened,
        "run_flagged": run_flagged,
        "run_cleared": run_cleared,
    }


def main() -> int:
    configure_logging()
    scenarios = {
        p.name: json.loads(p.read_text())
        for p in [UPDATES / "scenario_add_match.jsonl", UPDATES / "scenario_version_bump.jsonl"]
    }

    # Shared PEP + adverse-media mocks (unchanged across scenarios).
    _serve(
        create_app(
            provider_id="dow_jones", list_type="pep", watchlist_path="data/watchlists/pep.jsonl"
        ),
        9202,
    )
    _serve(
        create_app(
            provider_id="comply_advantage",
            list_type="adverse_media",
            watchlist_path="data/watchlists/adverse_media.jsonl",
        ),
        9203,
    )
    _wait_health(9202)
    _wait_health(9203)

    ok = True

    print("── Check 1: scenario_add_match (CLEAR → ESCALATE on v2) ─────────")
    add = run_scenario(
        scenarios["scenario_add_match.jsonl"], sanctions_port=9201, pep_port=9202, media_port=9203
    )
    print(
        f"  baseline={add['baseline']}@{add['base_version']} → "
        f"after={add['after']}@{add['after_version']}"
    )
    print(
        f"  clients_rescreened={add['rescreened']} newly_flagged={add['run_flagged']} "
        f"newly_cleared={add['run_cleared']}"
    )
    add_ok = (
        add["baseline"] == "CLEAR"
        and add["base_version"] == "v1"
        and add["after"] == "ESCALATE"
        and add["after_version"] == "v2"
        and add["run_flagged"] == 1
    )
    print(
        f"  {'✓' if add_ok else '✗'} flipped to ESCALATE; fresh screening refs v2; "
        f"newly_flagged==1; no stale active screening"
    )
    ok &= add_ok

    print("── Check 2: scenario_version_bump (no list change → no flip) ────")
    bump = run_scenario(
        scenarios["scenario_version_bump.jsonl"],
        sanctions_port=9211,
        pep_port=9202,
        media_port=9203,
    )
    print(
        f"  baseline={bump['baseline']}@{bump['base_version']} → "
        f"after={bump['after']}@{bump['after_version']}"
    )
    print(f"  clients_rescreened={bump['rescreened']} newly_flagged={bump['run_flagged']}")
    bump_ok = (
        bump["baseline"] == "CLEAR"
        and bump["after"] == "CLEAR"
        and bump["after_version"] == "v2"
        and bump["rescreened"] >= 1
        and bump["run_flagged"] == 0
    )
    print(
        f"  {'✓' if bump_ok else '✗'} re-screened against v2 but outcome unchanged (no false flip)"
    )
    ok &= bump_ok

    print("────────────────────────────────────────────────────────────────")
    print(f"PHASE 7 VERIFICATION: {'PASS ✓' if ok else 'FAIL ✗'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
