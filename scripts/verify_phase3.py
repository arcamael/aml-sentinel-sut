"""Phase 3 verification (roadmap 🔎).

Two checks, both printing real output:

1. **Golden normalization** — for every line in ``data/golden/normalization.jsonl``
   the Normalizer's own canonicalization reproduces ``expected`` exactly
   (canonical_name, name_parts, dob_iso, nationality_iso2, residence_iso2,
   profile_hash). Also re-asserts determinism: same input → same profile_hash.

2. **Idempotent re-consume** — produce one ``client.submitted``, consume it →
   exactly one ``normalized_profile`` row + one ``profile.normalized`` event;
   then re-read the *same partition/offset* → zero new rows (idempotency).

Requires the Phase 0 stack (postgres + redpanda) to be up. Run:

    PYTHONPATH=src .venv/bin/python scripts/verify_phase3.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import func, select, text
from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import NormalizedProfile, RawProfile
from aml_sentinel.events import (
    TOPIC_CLIENT_SUBMITTED,
    TOPIC_PROFILE_NORMALIZED,
    EventProducer,
    make_envelope,
)
from aml_sentinel.ids import uuid7
from aml_sentinel.matching.normalize import normalize
from aml_sentinel.observability.logging import configure_logging
from aml_sentinel.workers.normalizer import _build_consumer, process_message

GOLDEN = Path("data/golden/normalization.jsonl")


def check_golden() -> bool:
    """Assert every golden input normalizes to its expected output."""
    print("── Check 1: golden normalization ───────────────────────────────")
    lines = GOLDEN.read_text(encoding="utf-8").splitlines()
    failures = 0
    for i, line in enumerate(lines, start=1):
        rec = json.loads(line)
        result = normalize(rec["input"])
        produced = result.to_golden_expected()
        if produced != rec["expected"]:
            failures += 1
            print(f"  ✗ line {i}: produced != expected")
            print(f"      input    = {rec['input']}")
            print(f"      expected = {rec['expected']}")
            print(f"      produced = {produced}")
            continue
        # Determinism: recompute and confirm identical profile_hash.
        again = normalize(rec["input"])
        assert again.profile_hash == result.profile_hash, "non-deterministic hash"
    ok = failures == 0
    print(
        f"  {'✓' if ok else '✗'} {len(lines) - failures}/{len(lines)} records match "
        f"expected; profile_hash deterministic"
    )
    return ok


def _count_normalized(client_id: str) -> int:
    with SessionLocal() as s:
        return s.scalar(
            select(func.count())
            .select_from(NormalizedProfile)
            .where(NormalizedProfile.client_id == client_id)
        )


def _drain_topic(group: str) -> int:
    """Count messages currently on profile.normalized for a fresh group."""
    c = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([TOPIC_PROFILE_NORMALIZED])
    seen = 0
    idle = 0
    while idle < 4:
        msg = c.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            idle += 1
            continue
        idle = 0
        seen += 1
    c.close()
    return seen


def check_idempotency() -> bool:
    """Produce one message, consume it, then re-consume the same offset."""
    print("── Check 2: idempotent re-consume of the same offset ───────────")
    configure_logging()

    client_id = f"cli_verify_{ULID()}"
    trace_id = str(uuid7())
    kyc = {
        "full_name": "Ivan  Petroff",
        "dob": "14/03/1972",
        "nationality": "Russia",
        "residence_country": "Cyprus",
        "document_ids": [{"type": "passport", "value": "RU1234567"}],
    }

    # Mimic the Ingestion API: persist raw_profile (FK target) then emit event.
    with SessionLocal() as s:
        s.add(
            RawProfile(
                id=str(ULID()),
                client_id=client_id,
                trace_id=trace_id,
                raw_payload=kyc,
                source="rest",
            )
        )
        s.commit()

    producer = EventProducer()
    envelope = make_envelope(
        trace_id=trace_id,
        client_id=client_id,
        event_type="client.submitted",
        producer="ingestion-api",
        payload=kyc,
    )
    producer.produce(TOPIC_CLIENT_SUBMITTED, key=client_id, envelope=envelope)

    out_before = _drain_topic(f"verify-out-{client_id}-a")

    # Consume our message via a dedicated group and locate its (tp, offset).
    consumer = _build_consumer(group_id=f"verify-{client_id}")
    consumer.subscribe([TOPIC_CLIENT_SUBMITTED])
    target_msg = None
    idle = 0
    while idle < 8 and target_msg is None:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            continue
        env = json.loads(msg.value())
        if env.get("client_id") == client_id:
            target_msg = msg
            break
    assert target_msg is not None, "did not receive the produced message"

    tp = TopicPartition(target_msg.topic(), target_msg.partition(), target_msg.offset())
    env = json.loads(target_msg.value())

    # First processing.
    with SessionLocal() as s:
        r1 = process_message(
            s,
            producer,
            envelope=env,
            topic=tp.topic,
            partition=tp.partition,
            offset=tp.offset,
        )
    count_after_first = _count_normalized(client_id)
    out_after_first = _drain_topic(f"verify-out-{client_id}-b")

    # Re-deliver the EXACT same offset and process again.
    consumer.assign([tp])
    redelivered = None
    idle = 0
    while idle < 8 and redelivered is None:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            continue
        redelivered = msg
        break
    assert redelivered is not None, "re-delivery of the same offset failed"
    assert redelivered.offset() == tp.offset, "offset mismatch on re-delivery"

    with SessionLocal() as s:
        r2 = process_message(
            s,
            producer,
            envelope=json.loads(redelivered.value()),
            topic=tp.topic,
            partition=tp.partition,
            offset=tp.offset,
        )
    count_after_second = _count_normalized(client_id)
    out_after_second = _drain_topic(f"verify-out-{client_id}-c")
    consumer.close()
    producer.close()

    print(f"  client_id          = {client_id}")
    print(f"  trace_id           = {trace_id}")
    print(f"  redelivered offset = {tp.partition}:{tp.offset} (same both times)")
    print(
        f"  1st process        : created={r1.created} skipped={r1.skipped} "
        f"-> normalized rows={count_after_first}"
    )
    print(
        f"  2nd process (same) : created={r2.created} skipped={r2.skipped} "
        f"-> normalized rows={count_after_second}"
    )
    print(
        f"  profile.normalized events emitted: after_1st={out_after_first - out_before}, "
        f"after_2nd={out_after_second - out_after_first}"
    )

    # Confirm trace_id propagated unchanged onto the persisted row (hard rule #3).
    with SessionLocal() as s:
        np = s.scalar(select(NormalizedProfile).where(NormalizedProfile.client_id == client_id))
        trace_ok = np is not None and np.trace_id == trace_id

    ok = (
        r1.created
        and not r1.skipped
        and r2.skipped
        and not r2.created
        and count_after_first == 1
        and count_after_second == 1
        and (out_after_first - out_before) == 1
        and (out_after_second - out_after_first) == 0
        and trace_ok
    )
    print(f"  trace_id on normalized_profile == ingest trace_id: {trace_ok}")
    print(f"  {'✓' if ok else '✗'} redelivery created 0 new rows / 0 new events (idempotent)")
    return ok


def main() -> int:
    # Fail fast if the audit immutability / schema isn't present (smoke).
    with SessionLocal() as s:
        s.execute(text("SELECT 1"))
    c1 = check_golden()
    c2 = check_idempotency()
    print("────────────────────────────────────────────────────────────────")
    ok = c1 and c2
    print(f"PHASE 3 VERIFICATION: {'PASS ✓' if ok else 'FAIL ✗'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
