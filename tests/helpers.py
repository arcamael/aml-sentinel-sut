"""Shared test helpers (Phase 9).

Small building blocks the harness layers reuse: starting an in-process mock
provider, creating a fully-consistent client, and driving a profile through the
screening + decision logic. Kept deterministic — every gateway gets a unique
sanctions ``provider_id`` so the reconciler's global "stale version" selection
sees only that test's clients.
"""

from __future__ import annotations

import socket
import threading
import time
import uuid
from datetime import date

import httpx
import uvicorn
from ulid import ULID

from aml_sentinel.db.base import SessionLocal
from aml_sentinel.db.models import NormalizedProfile, RawProfile
from aml_sentinel.matching.normalize import normalize
from aml_sentinel.workers import decision as decision_worker
from aml_sentinel.workers import screening as screening_worker


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def serve(app, port: int) -> uvicorn.Server:
    """Run a FastAPI app on a background thread; block until it is healthy."""
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    threading.Thread(target=server.run, daemon=True).start()
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0).status_code == 200:
                return server
        except httpx.HTTPError:
            time.sleep(0.05)
    raise RuntimeError(f"service on {port} never became healthy")


def create_client(full_name: str, dob: str, nationality: str) -> tuple[str, str, object]:
    """Insert raw_profile + normalized_profile (mimics ingest+normalize)."""
    client_id = f"cli_test_{ULID()}"
    trace_id = str(uuid.uuid7())
    norm = normalize({"full_name": full_name, "dob": dob, "nationality": nationality})
    with SessionLocal() as s:
        s.add(
            RawProfile(
                id=str(ULID()),
                client_id=client_id,
                trace_id=trace_id,
                raw_payload={"full_name": full_name, "dob": dob, "nationality": nationality},
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


def screen_and_decide(gateway, producer, client_id, trace_id, norm, token, offset=0):
    """Run one screening + decision through the worker logic; return both results."""
    norm_env = {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": "profile.normalized",
        "payload": norm.to_normalized_payload(),
    }
    with SessionLocal() as s:
        screen = screening_worker.process_message(
            s, gateway, producer, envelope=norm_env, topic=f"t:{token}", partition=0, offset=offset
        )
    sc_env = {
        "trace_id": trace_id,
        "client_id": client_id,
        "event_type": "screening.completed",
        "payload": screen.event_payload,
    }
    with SessionLocal() as s:
        outcome = decision_worker.process_message(
            s, producer, envelope=sc_env, topic=f"t:{token}:d", partition=0, offset=offset
        )
    return screen, outcome
