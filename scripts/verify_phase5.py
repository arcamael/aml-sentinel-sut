"""Phase 5 verification (roadmap 🔎).

1. **Matching accuracy** — score every pair in ``data/golden/matching.jsonl`` at
   the configured threshold and assert precision ≥ 0.95, recall ≥ 0.90, and that
   each true pair clears its ``min_score``.
2. **Worker round-trip** — screen a known sanctioned profile end-to-end through
   the gateway + (running) mocks, then assert the persisted ``match`` rows agree
   with the emitted ``screening.completed`` event and the ``screen`` log detail
   (``screen.detail.matches == COUNT(match) == len(event.matches)``).
3. **Idempotency** — re-deliver the same message → no new screening.

Requires the Phase 0 stack + the three mock containers (Phase 4) up. Run:

    PYTHONPATH=src:. .venv/bin/python scripts/verify_phase5.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from sqlalchemy import func, select
from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import Match, Screening
from aml_sentinel.events import (
    TOPIC_PROFILE_NORMALIZED,
    TOPIC_SCREENING_COMPLETED,
    EventProducer,
)
from aml_sentinel.ids import uuid7
from aml_sentinel.matching.fuzzy import SCREENING_THRESHOLD, score_pair
from aml_sentinel.observability.logging import configure_logging
from aml_sentinel.providers.gateway import ProviderGateway
from aml_sentinel.workers.screening import _build_consumer, process_message

GOLDEN = Path("data/golden/matching.jsonl")


def check_matching() -> bool:
    print("── Check 1: golden matching precision/recall ───────────────────")
    rows = [json.loads(line) for line in GOLDEN.read_text(encoding="utf-8").splitlines()]
    tp = fp = fn = tn = 0
    below_min = 0
    for r in rows:
        score = score_pair(
            r["profile_name"], r["candidate_name"], r["dob_profile"], r["dob_candidate"]
        )
        predicted = score >= SCREENING_THRESHOLD
        if predicted and r["expected_match"]:
            tp += 1
        elif predicted and not r["expected_match"]:
            fp += 1
        elif not predicted and r["expected_match"]:
            fn += 1
        else:
            tn += 1
        if r["expected_match"] and score < r["min_score"]:
            below_min += 1
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    ok = precision >= 0.95 and recall >= 0.90 and below_min == 0
    print(f"  pairs={len(rows)} tp={tp} fp={fp} fn={fn} tn={tn} threshold={SCREENING_THRESHOLD}")
    print(
        f"  precision={precision:.3f} (target ≥0.95)  recall={recall:.3f} (target ≥0.90)  "
        f"below_min={below_min}"
    )
    print(f"  {'✓' if ok else '✗'} matching meets precision/recall targets")
    return ok


def _consume_screening_completed(client_id: str) -> dict | None:
    consumer = _build_consumer(group_id=f"verify5-out-{client_id}")
    consumer.subscribe([TOPIC_SCREENING_COMPLETED])
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


def check_worker_roundtrip() -> bool:
    print("── Check 2: worker round-trip (DB == event == log) ─────────────")
    configure_logging()

    from aml_sentinel.matching.normalize import normalize

    client_id = f"cli_screen_{ULID()}"
    trace_id = str(uuid7())
    result_norm = normalize(
        {"full_name": "Ivan Petrov", "dob": "1972-03-14", "nationality": "Russia"}
    )
    envelope = {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": "profile.normalized",
        "payload": result_norm.to_normalized_payload(),
    }

    gateway = ProviderGateway()
    producer = EventProducer()
    with SessionLocal() as session:
        result = process_message(
            session,
            gateway,
            producer,
            envelope=envelope,
            topic=TOPIC_PROFILE_NORMALIZED,
            partition=0,
            offset=0,
        )

    # DB count of match rows for this screening.
    with SessionLocal() as session:
        db_count = session.scalar(
            select(func.count()).select_from(Match).where(Match.screening_id == result.screening_id)
        )
        screening = session.get(Screening, result.screening_id)

    event = _consume_screening_completed(client_id)
    event_payload = event["payload"] if event else None
    event_match_count = len(event_payload["matches"]) if event_payload else -1
    event_screening_id = event_payload["screening_id"] if event_payload else None

    sanctions_hit = any(
        m["list_type"] == "sanctions" and m["evidence_ref"] == "wl_sanctions_0001"
        for m in result.matches
    )

    print(f"  client_id   = {client_id}")
    print(f"  screening_id= {result.screening_id}")
    print(f"  list_versions = {screening.list_versions if screening else None}")
    print(
        f"  log.matches={result.match_count}  COUNT(match)={db_count}  "
        f"event.matches={event_match_count}  max_score={result.max_score}  "
        f"cache_hits={result.cache_hits}"
    )
    print(f"  planted sanctions match (wl_sanctions_0001) present: {sanctions_hit}")

    agree = (
        result.match_count == db_count == event_match_count
        and event_screening_id == result.screening_id
    )
    # Idempotency: re-deliver the same message → no new screening.
    with SessionLocal() as session:
        again = process_message(
            session,
            gateway,
            producer,
            envelope=envelope,
            topic=TOPIC_PROFILE_NORMALIZED,
            partition=0,
            offset=0,
        )
    with SessionLocal() as session:
        screening_count = session.scalar(
            select(func.count()).select_from(Screening).where(Screening.client_id == client_id)
        )
    gateway.close()
    producer.close()

    idempotent = again.skipped and screening_count == 1
    print(
        f"  re-deliver same offset → skipped={again.skipped}, "
        f"screenings_for_client={screening_count}"
    )

    ok = (
        result.created
        and result.match_count >= 1
        and sanctions_hit
        and agree
        and idempotent
        and event is not None
    )
    print(f"  {'✓' if ok else '✗'} DB == event == log; planted match found; idempotent")
    return ok


def main() -> int:
    c1 = check_matching()
    c2 = check_worker_roundtrip()
    print("────────────────────────────────────────────────────────────────")
    ok = c1 and c2
    print(f"PHASE 5 VERIFICATION: {'PASS ✓' if ok else 'FAIL ✗'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
