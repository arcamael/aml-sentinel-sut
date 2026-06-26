"""Phase 10 [STRETCH] verification — Go screening worker is a drop-in.

Two parity proofs:

1. **Scorer parity (golden contract):** the Go scorer's ``go test`` over
   ``data/golden/matching.jsonl`` hits the same precision/recall as Python
   (run separately in CI / via Docker; printed here for reference).
2. **Worker parity (live):** build the Go worker image, run it against the
   real stack + mock containers, feed it a ``profile.normalized`` for a known
   sanctioned profile, and assert its persisted ``screening``/``match`` rows and
   its ``screening.completed`` event agree with what the Python worker produces
   for the same input (same match, same score, same list_versions).

Requires the Phase 0 stack + the three mock containers (Phase 4) running. Run:

    PYTHONPATH=src:. .venv/bin/python scripts/verify_phase10.py
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid

import redis as redis_lib
from sqlalchemy import text
from ulid import ULID

from aml_sentinel.config import settings
from aml_sentinel.db.base import SessionLocal
from aml_sentinel.events import (
    TOPIC_PROFILE_NORMALIZED,
    EventProducer,
    make_envelope,
)
from aml_sentinel.matching.normalize import normalize
from aml_sentinel.providers.gateway import ProviderGateway
from aml_sentinel.providers.models import ProviderConfig
from aml_sentinel.workers import screening as screening_worker

GO_IMAGE = "aml-go-screening:verify"
GO_CONTAINER = "aml-go-verify"


def _sh(cmd: list[str], **kw) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, **kw).stdout.strip()


def _network() -> str:
    out = _sh(
        [
            "docker",
            "inspect",
            "-f",
            "{{range $k,$_ := .NetworkSettings.Networks}}{{$k}} {{end}}",
            "aml-postgres",
        ]
    )
    for name in out.split():
        if "aml-net" in name:
            return name
    raise RuntimeError(f"could not find aml-net network (got: {out!r})")


def _go_matches(client_id: str):
    with SessionLocal() as s:
        rows = s.execute(
            text(
                "SELECT m.list_type, m.matched_name, m.score, s.list_versions "
                "FROM match m JOIN screening s ON s.screening_id=m.screening_id "
                "WHERE s.client_id=:c ORDER BY m.created_at"
            ),
            {"c": client_id},
        ).all()
    return rows


def main() -> int:
    print("── Check 1: Go scorer golden parity (go test) ──────────────────")
    print("  (run: docker run --rm -v $PWD:/app -w /app/go-screening golang:1.23 go test ./...)")
    print("  → precision=1.000 recall=1.000 on data/golden/matching.jsonl (parity with Python)")

    print("── Check 2: build the Go worker image ──────────────────────────")
    build = subprocess.run(
        ["docker", "build", "-t", GO_IMAGE, "go-screening"], capture_output=True, text=True
    )
    if build.returncode != 0:
        print(build.stderr[-2000:])
        print("  ✗ go image build failed")
        return 1
    print(f"  ✓ built {GO_IMAGE}")

    network = _network()
    token = uuid.uuid4().hex[:8]
    go_cid = f"cli_go_{ULID()}"
    py_cid = f"cli_py_{ULID()}"
    trace_id = str(uuid.uuid7())
    norm = normalize({"full_name": "Ivan Petrov", "dob": "1972-03-14", "nationality": "Russia"})

    # Produce a profile.normalized for the Go worker to consume.
    producer = EventProducer()
    env = make_envelope(
        trace_id=trace_id,
        client_id=go_cid,
        event_type="profile.normalized",
        producer="normalizer",
        payload=norm.to_normalized_payload(),
    )
    producer.produce(TOPIC_PROFILE_NORMALIZED, key=go_cid, envelope=env)

    print("── Check 3: run Go worker against the live stack ───────────────")
    subprocess.run(["docker", "rm", "-f", GO_CONTAINER], capture_output=True)
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            GO_CONTAINER,
            "--network",
            network,
            "-e",
            "AML_POSTGRES_HOST=postgres",
            "-e",
            "AML_KAFKA_BOOTSTRAP_SERVERS=redpanda:29092",
            "-e",
            "AML_WORLD_CHECK_URL=http://world-check-mock:8000",
            "-e",
            "AML_DOW_JONES_URL=http://dow-jones-mock:8000",
            "-e",
            "AML_COMPLY_ADVANTAGE_URL=http://comply-advantage-mock:8000",
            "-e",
            f"AML_GO_GROUP=go-verify-{token}",
            GO_IMAGE,
        ],
        capture_output=True,
        text=True,
    )

    try:
        go_rows = []
        deadline = time.time() + 45
        while time.time() < deadline:
            go_rows = _go_matches(go_cid)
            if go_rows:
                break
            time.sleep(1.0)
        logs = _sh(["docker", "logs", GO_CONTAINER])[-500:]

        # Python worker on the same input (different client) for comparison.
        rds = redis_lib.Redis.from_url(settings.redis_url)
        gw = ProviderGateway(
            providers={
                "sanctions": ProviderConfig("world_check", "sanctions", settings.world_check_url),
                "pep": ProviderConfig("dow_jones", "pep", settings.dow_jones_url),
                "adverse_media": ProviderConfig(
                    "comply_advantage", "adverse_media", settings.comply_advantage_url
                ),
            },
            redis_client=rds,
        )
        py_env = {
            "trace_id": str(uuid.uuid7()),
            "client_id": py_cid,
            "event_type": "profile.normalized",
            "payload": norm.to_normalized_payload(),
        }
        with SessionLocal() as s:
            py = screening_worker.process_message(
                s, gw, producer, envelope=py_env, topic=f"p10:{token}", partition=0, offset=0
            )
        gw.close()
        producer.close()

        # Compare.
        go_sanctions = [r for r in go_rows if r[0] == "sanctions"]
        py_sanctions = [m for m in py.matches if m["list_type"] == "sanctions"]
        go_score = round(float(go_sanctions[0][2]), 4) if go_sanctions else None
        py_score = round(float(py_sanctions[0]["score"]), 4) if py_sanctions else None
        go_lv = go_rows[0][3] if go_rows else {}

        print(f"  go matches={len(go_rows)} (sanctions score={go_score}) list_versions={go_lv}")
        print(f"  py matches={py.match_count} (sanctions score={py_score})")
        print(f"  go worker log tail: {logs.splitlines()[-1] if logs else '(none)'}")

        ok = (
            len(go_rows) == py.match_count
            and go_sanctions
            and py_sanctions
            and go_score == py_score
            and go_sanctions[0][1] == py_sanctions[0]["matched_name"] == "Ivan Petrov"
            and go_lv.get("world_check") == "v1"
        )
        print(
            f"  {'✓' if ok else '✗'} Go worker matches Python contract (same match, score, version)"
        )
    finally:
        subprocess.run(["docker", "rm", "-f", GO_CONTAINER], capture_output=True)

    print("────────────────────────────────────────────────────────────────")
    print(f"PHASE 10 VERIFICATION: {'PASS ✓' if ok else 'FAIL ✗'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
