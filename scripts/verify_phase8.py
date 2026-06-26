"""Phase 8 verification (roadmap 🔎).

1. ``GET /metrics`` returns the data-quality + pipeline series.
2. Monitors pass on healthy data: a freshly-created, fully-consistent client is
   flagged by no per-client monitor; audit-immutability + match-accuracy hold.
3. Inject an orphan ``match`` → the orphan monitor fires → ``alert_sink`` records it.
4. Withhold a ``normalized_profile`` → the completeness monitor fires + alerts.

The monitor service runs in-process (TestClient); the capture-only alert sink
runs on a background uvicorn thread. All injected corruption is cleaned up after.
(The shared DB carries residue from earlier phases — e.g. the Phase-1 smoke seed
breaches ``determinism`` — so "passes on healthy data" is asserted against a
clean cohort we create here, not globally.)

Requires the Phase 0 stack. Run:

    PYTHONPATH=src:. .venv/bin/python scripts/verify_phase8.py
"""

from __future__ import annotations

import sys
import threading
import time
import uuid
from datetime import date

import httpx
import uvicorn
from fastapi.testclient import TestClient
from sqlalchemy import text
from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Decision, NormalizedProfile, RawProfile, Screening
from aml_sentinel.matching.normalize import normalize
from aml_sentinel.observability import app as monitor_app
from mocks.alert_sink.app import app as alert_app

ALERT_PORT = 9301


def _serve(app, port: int) -> None:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    threading.Thread(target=server.run, daemon=True).start()
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0).status_code == 200:
                return
        except httpx.HTTPError:
            time.sleep(0.1)
    raise RuntimeError(f"service on {port} never became healthy")


def _alerts() -> list[dict]:
    return httpx.get(f"http://127.0.0.1:{ALERT_PORT}/alerts", timeout=2.0).json()["alerts"]


def _reset_alerts() -> None:
    httpx.post(f"http://127.0.0.1:{ALERT_PORT}/_control/reset", timeout=2.0)


def _result(run_json: dict, check: str) -> dict:
    return next(r for r in run_json["results"] if r["check"] == check)


def _create_clean_client() -> str:
    """A fully-consistent client (raw→normalized→screening→decision, one trace)."""
    cid = f"cli_clean_{ULID()}"
    tid = str(uuid.uuid7())
    norm = normalize({"full_name": "Quintus Aurelius", "dob": "1985-05-05", "nationality": "Italy"})
    sid = str(ULID())
    with SessionLocal() as s:
        s.add(
            RawProfile(id=str(ULID()), client_id=cid, trace_id=tid, raw_payload={}, source="rest")
        )
        s.flush()
        s.add(
            NormalizedProfile(
                id=str(ULID()),
                client_id=cid,
                trace_id=tid,
                profile_hash=norm.profile_hash,
                canonical_name=norm.canonical_name,
                name_parts=norm.name_parts,
                dob_iso=date.fromisoformat(norm.dob_iso),
                nationality_iso2=norm.nationality_iso2,
                residence_iso2=norm.residence_iso2,
                document_ids=norm.document_ids,
            )
        )
        s.add(
            Screening(
                id=sid,
                client_id=cid,
                trace_id=tid,
                profile_hash=norm.profile_hash,
                list_versions={"world_check": "v1"},
                status="completed",
            )
        )
        s.flush()
        s.add(
            Decision(
                id=str(ULID()),
                screening_id=sid,
                client_id=cid,
                trace_id=tid,
                outcome="CLEAR",
                reason_codes=["NO_MATCH"],
                top_match_id=None,
            )
        )
        s.commit()
    return cid


def _delete_client(cid: str) -> None:
    with SessionLocal() as s:
        for table in ("decision", "screening", "normalized_profile", "raw_profile"):
            s.execute(text(f"DELETE FROM {table} WHERE client_id = :c"), {"c": cid})
        s.commit()


def main() -> int:
    _serve(alert_app, ALERT_PORT)
    client = TestClient(monitor_app.app)
    ok = True

    # ── Check 1: metrics endpoint live ───────────────────────────────────────
    print("── Check 1: GET /metrics returns the series ────────────────────")
    client.post("/monitors/run")
    body = client.get("/metrics").text
    wanted = ["aml_stage_rows", "aml_decision_outcome", "aml_match_rate", "aml_dq_breaches"]
    have = [m for m in wanted if m in body]
    c1 = have == wanted
    print(f"  series present: {have}")
    print(f"  {'✓' if c1 else '✗'} /metrics exposes the DQ + pipeline series")
    ok &= c1

    # ── Check 2: monitors pass on healthy (freshly-created) data ─────────────
    print("── Check 2: monitors pass on a clean client ────────────────────")
    clean = _create_clean_client()
    run = client.post("/monitors/run").json()
    per_client = ["completeness", "decision_coverage", "lineage", "determinism", "freshness"]
    flagged = [c for c in per_client if clean in _result(run, c)["breaches"]]
    invariants = {c: _result(run, c)["passed"] for c in ["audit_immutability", "match_accuracy"]}
    c2 = not flagged and all(invariants.values())
    print(f"  clean client flagged by: {flagged or 'none'}")
    print(f"  global invariants: {invariants}")
    print(f"  {'✓' if c2 else '✗'} healthy client raises no breach; immutability + accuracy hold")
    ok &= c2
    _delete_client(clean)

    # ── Check 3: inject orphan match → orphan monitor fires + alert ──────────
    print("── Check 3: inject orphan match → orphan_match fires + alert ────")
    orphan_id = f"match_orphan_{ULID()}"
    with SessionLocal() as s:
        # SET LOCAL bypasses the FK only within this transaction (auto-reverts),
        # so we can plant the orphan the FK normally forbids — defense-in-depth.
        s.execute(text("SET LOCAL session_replication_role = replica"))
        s.execute(
            text(
                "INSERT INTO match (match_id, screening_id, provider_id, list_type, "
                "matched_name, score, dob_match, created_at) VALUES "
                "(:id, 'screening_does_not_exist', 'world_check', 'sanctions', "
                "'Orphan', 0.99, false, now())"
            ),
            {"id": orphan_id},
        )
        s.commit()
    _reset_alerts()
    run = client.post("/monitors/run").json()
    orphan = _result(run, "orphan_match")
    alerted = any(a.get("check") == "orphan_match" for a in _alerts())
    c3 = (not orphan["passed"]) and orphan_id in orphan["breaches"] and alerted
    print(
        f"  orphan_match: passed={orphan['passed']} breach_count={orphan['breach_count']} "
        f"injected_present={orphan_id in orphan['breaches']}"
    )
    print(f"  alert_sink recorded orphan_match alert: {alerted}")
    print(f"  {'✓' if c3 else '✗'} orphan monitor fired and alerted")
    ok &= c3
    with SessionLocal() as s:
        s.execute(text("DELETE FROM match WHERE match_id = :id"), {"id": orphan_id})
        s.commit()

    # ── Check 4: missing normalized_profile → completeness fires ─────────────
    print("── Check 4: raw_profile without normalized → completeness fires ─")
    lonely = f"cli_incomplete_{ULID()}"
    with SessionLocal() as s:
        s.add(
            RawProfile(
                id=str(ULID()),
                client_id=lonely,
                trace_id=str(uuid.uuid7()),
                raw_payload={},
                source="rest",
            )
        )
        s.commit()
    _reset_alerts()
    run = client.post("/monitors/run").json()
    completeness = _result(run, "completeness")
    alerted = any(a.get("check") == "completeness" for a in _alerts())
    c4 = (not completeness["passed"]) and lonely in completeness["breaches"] and alerted
    print(
        f"  completeness: passed={completeness['passed']} "
        f"breach_count={completeness['breach_count']} present={lonely in completeness['breaches']}"
    )
    print(f"  alert_sink recorded completeness alert: {alerted}")
    print(f"  {'✓' if c4 else '✗'} completeness monitor fired and alerted")
    ok &= c4
    _delete_client(lonely)

    print("────────────────────────────────────────────────────────────────")
    print(f"PHASE 8 VERIFICATION: {'PASS ✓' if ok else 'FAIL ✗'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
