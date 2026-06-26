"""Phase 6 verification (roadmap 🔎).

1. **Golden decisions** — for every row in ``data/golden/decisions.jsonl`` the
   rules engine's ``outcome`` + ``reason_codes`` equal ``expected``.
2. **Worker round-trip** — screen → decide a real ESCALATE profile and a CLEAR
   profile, then prove:
   * exactly one ``decision`` per ``screening`` (1:1),
   * the ``decision.made`` event agrees with the row,
   * the ``audit`` snapshot contains the matches that drove the outcome + the
     rule trace,
   * a second delivery (and a same-``screening_id`` retry) creates no second
     decision (idempotency + the UNIQUE backstop).

Requires the Phase 0 stack + the three mock containers up. Run:

    PYTHONPATH=src:. .venv/bin/python scripts/verify_phase6.py
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

from sqlalchemy import func, select
from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Audit, Decision
from aml_sentinel.decisioning.rules import decide
from aml_sentinel.events import TOPIC_DECISION_MADE, TOPIC_SCREENING_COMPLETED, EventProducer
from aml_sentinel.matching.normalize import normalize
from aml_sentinel.observability.logging import configure_logging
from aml_sentinel.providers.gateway import ProviderGateway
from aml_sentinel.workers import decision as decision_worker
from aml_sentinel.workers import screening as screening_worker

GOLDEN = Path("data/golden/decisions.jsonl")


def check_golden() -> bool:
    print("── Check 1: golden decisions (outcome + reason_codes) ──────────")
    rows = [json.loads(line) for line in GOLDEN.read_text(encoding="utf-8").splitlines()]
    failures = 0
    for r in rows:
        res = decide(r["matches"])
        if (
            res.outcome != r["expected"]["outcome"]
            or res.reason_codes != r["expected"]["reason_codes"]
        ):
            failures += 1
            print(f"  ✗ {r['matches']}")
            print(f"      expected={r['expected']} got=({res.outcome}, {res.reason_codes})")
    ok = failures == 0
    print(
        f"  {'✓' if ok else '✗'} {len(rows) - failures}/{len(rows)} decision cases match expected"
    )
    return ok


def _consume_decision_made(client_id: str) -> dict | None:
    consumer = decision_worker._build_consumer(group_id=f"verify6-out-{client_id}")
    consumer.subscribe([TOPIC_DECISION_MADE])
    found = None
    idle = 0
    while idle < 8 and found is None:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            continue
        env = json.loads(msg.value())
        if env.get("client_id") == client_id:
            found = env
            break
    consumer.close()
    return found


def _screen_then_decide(gateway, producer, *, full_name, dob, nationality):
    """Run screening → decision for one profile; return the DecisionOutcome + ids."""
    client_id = f"cli_decide_{ULID()}"
    trace_id = str(uuid.uuid7())
    norm = normalize({"full_name": full_name, "dob": dob, "nationality": nationality})
    norm_env = {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": "profile.normalized",
        "payload": norm.to_normalized_payload(),
    }
    with SessionLocal() as s:
        screen = screening_worker.process_message(
            s,
            gateway,
            producer,
            envelope=norm_env,
            topic="profile.normalized",
            partition=0,
            offset=0,
        )

    screening_env = {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": "screening.completed",
        "payload": screen.event_payload,
    }
    with SessionLocal() as s:
        outcome = decision_worker.process_message(
            s,
            producer,
            envelope=screening_env,
            topic=TOPIC_SCREENING_COMPLETED,
            partition=0,
            offset=0,
        )
    return client_id, trace_id, screen, outcome, screening_env


def check_roundtrip() -> bool:
    print("── Check 2: screen → decide round-trip ─────────────────────────")
    configure_logging()
    gateway = ProviderGateway()
    producer = EventProducer()
    ok = True

    # ── ESCALATE path: a known sanctioned profile ────────────────────────────
    client_id, trace_id, screen, outcome, screening_env = _screen_then_decide(
        gateway, producer, full_name="Ivan Petrov", dob="1972-03-14", nationality="Russia"
    )
    event = _consume_decision_made(client_id)
    ev = event["payload"] if event else {}

    with SessionLocal() as s:
        decision_count = s.scalar(
            select(func.count())
            .select_from(Decision)
            .where(Decision.screening_id == screen.screening_id)
        )
        decision_row = s.get(Decision, outcome.decision_id)
        audit_row = s.scalar(select(Audit).where(Audit.decision_id == outcome.decision_id))

    audit_matches = audit_row.snapshot["matches"] if audit_row else []
    audit_sanctions = any(m["list_type"] == "sanctions" for m in audit_matches)
    audit_rule = any(
        t["rule"] == "SANCTIONS_MATCH"
        for t in (audit_row.snapshot["rule_trace"] if audit_row else [])
    )

    print(f"  [ESCALATE] client={client_id}")
    print(
        f"    outcome={outcome.outcome} reason_codes={outcome.reason_codes} "
        f"top_match_id={'set' if outcome.top_match_id else None}"
    )
    print(f"    decisions for screening = {decision_count} (expect 1)")
    print(f"    decision.made event: outcome={ev.get('outcome')} codes={ev.get('reason_codes')}")
    print(
        f"    audit snapshot: matches={len(audit_matches)} has_sanctions={audit_sanctions} "
        f"rule_trace_has_SANCTIONS_MATCH={audit_rule}"
    )

    esc_ok = (
        outcome.outcome == "ESCALATE"
        and outcome.reason_codes == ["SANCTIONS_MATCH"]
        and outcome.top_match_id is not None
        and decision_count == 1
        and decision_row is not None
        and ev.get("outcome") == "ESCALATE"
        and ev.get("reason_codes") == ["SANCTIONS_MATCH"]
        and audit_sanctions
        and audit_rule
    )
    print(f"    {'✓' if esc_ok else '✗'} ESCALATE persisted 1:1; event + audit agree")
    ok &= esc_ok

    # ── Idempotency + UNIQUE(screening_id) backstop ──────────────────────────
    with SessionLocal() as s:
        again = decision_worker.process_message(
            s,
            producer,
            envelope=screening_env,
            topic=TOPIC_SCREENING_COMPLETED,
            partition=0,
            offset=0,  # same key → idempotent
        )
    with SessionLocal() as s:
        retry = decision_worker.process_message(
            s,
            producer,
            envelope=screening_env,
            topic=TOPIC_SCREENING_COMPLETED,
            partition=0,
            offset=99,  # new key, same screening_id
        )
    with SessionLocal() as s:
        final_count = s.scalar(
            select(func.count())
            .select_from(Decision)
            .where(Decision.screening_id == screen.screening_id)
        )
    idem_ok = again.skipped and retry.skipped and final_count == 1
    print(
        f"    re-deliver skipped={again.skipped}; same-screening retry skipped={retry.skipped}; "
        f"decisions still={final_count}"
    )
    print(f"    {'✓' if idem_ok else '✗'} exactly one decision per screening (idempotent + UNIQUE)")
    ok &= idem_ok

    # ── CLEAR path: a non-listed profile ─────────────────────────────────────
    client_id2, _, screen2, outcome2, _ = _screen_then_decide(
        gateway, producer, full_name="Aurelia Nightingale", dob="1991-04-23", nationality="Spain"
    )
    with SessionLocal() as s:
        decision_count2 = s.scalar(
            select(func.count())
            .select_from(Decision)
            .where(Decision.screening_id == screen2.screening_id)
        )
    clear_ok = (
        outcome2.outcome == "CLEAR"
        and outcome2.reason_codes == ["NO_MATCH"]
        and outcome2.top_match_id is None
        and decision_count2 == 1
    )
    print(
        f"  [CLEAR] client={client_id2} matches={screen2.match_count} "
        f"outcome={outcome2.outcome} reason_codes={outcome2.reason_codes} "
        f"decisions={decision_count2}"
    )
    print(f"    {'✓' if clear_ok else '✗'} non-listed profile → CLEAR, decision coverage 1:1")
    ok &= clear_ok

    gateway.close()
    producer.close()
    return ok


def main() -> int:
    c1 = check_golden()
    c2 = check_roundtrip()
    print("────────────────────────────────────────────────────────────────")
    ok = c1 and c2
    print(f"PHASE 6 VERIFICATION: {'PASS ✓' if ok else 'FAIL ✗'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
