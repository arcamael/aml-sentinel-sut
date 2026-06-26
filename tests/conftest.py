"""Session fixtures for the AML-Sentinel harness (Phase 9, doc 01 §6).

Readiness-gated: unit tests run with no infrastructure; integration/e2e/DQ/
reconciliation tests depend on ``infra`` (Postgres + Redis + Redpanda reachable)
and are skipped with a clear message if the stack is down. Mock providers run
in-process on background threads, seeded from the generated watchlists.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


# ── Deterministic datasets (no infra needed) ─────────────────────────────────
@pytest.fixture(scope="session", autouse=True)
def datasets():
    """Generate goldens / watchlists / updates if absent (deterministic)."""
    from tools.datagen import (
        decisions_golden,
        matching_golden,
        normalization_golden,
        updates,
        watchlists,
    )

    if not (DATA / "golden" / "normalization.jsonl").exists():
        normalization_golden.generate(DATA / "golden")
    if not (DATA / "golden" / "matching.jsonl").exists():
        matching_golden.generate(DATA / "golden")
    if not (DATA / "golden" / "decisions.jsonl").exists():
        decisions_golden.generate(DATA / "golden")
    if not (DATA / "watchlists" / "sanctions.jsonl").exists():
        watchlists.generate(DATA / "watchlists")
    if not (DATA / "updates" / "scenario_add_match.jsonl").exists():
        updates.generate(DATA / "updates")
    return DATA


# ── Infrastructure readiness ─────────────────────────────────────────────────
@pytest.fixture(scope="session")
def infra():
    """Skip the whole infra-dependent layer cleanly if the stack is down."""
    import redis as redis_lib
    from confluent_kafka.admin import AdminClient
    from sqlalchemy import text

    from aml_sentinel.config import settings
    from aml_sentinel.db.base import engine

    try:
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Postgres not reachable: {exc}")
    try:
        redis_lib.Redis.from_url(settings.redis_url).ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Redis not reachable: {exc}")
    try:
        md = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers}).list_topics(
            timeout=5
        )
        assert md.topics
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Redpanda not reachable: {exc}")
    return True


@pytest.fixture(scope="session")
def redis_client(infra):
    import redis as redis_lib

    from aml_sentinel.config import settings

    return redis_lib.Redis.from_url(settings.redis_url)


@pytest.fixture(scope="session")
def producer(infra):
    from aml_sentinel.events import EventProducer

    p = EventProducer()
    yield p
    p.close()


# ── Mock providers (in-process) ──────────────────────────────────────────────
@pytest.fixture(scope="session")
def shared_mocks(infra, datasets):
    """Read-only PEP + adverse-media mocks + alert sink for the session."""
    from mocks.alert_sink.app import app as alert_app
    from mocks.provider_mock import create_app
    from tests.helpers import free_port, serve

    ports = {"pep": free_port(), "adverse_media": free_port(), "alert": free_port()}
    serve(
        create_app(
            provider_id="dow_jones",
            list_type="pep",
            watchlist_path=str(DATA / "watchlists" / "pep.jsonl"),
        ),
        ports["pep"],
    )
    serve(
        create_app(
            provider_id="comply_advantage",
            list_type="adverse_media",
            watchlist_path=str(DATA / "watchlists" / "adverse_media.jsonl"),
        ),
        ports["adverse_media"],
    )
    serve(alert_app, ports["alert"])
    return ports


@pytest.fixture
def sanctions_mock(datasets):
    """A fresh sanctions mock with a unique provider_id (safe to mutate)."""
    from mocks.provider_mock import create_app
    from tests.helpers import free_port, serve

    token = uuid.uuid4().hex[:8]
    provider_id = f"world_check_{token}"
    port = free_port()
    serve(
        create_app(
            provider_id=provider_id,
            list_type="sanctions",
            watchlist_path=str(DATA / "watchlists" / "sanctions.jsonl"),
            list_version="v1",
        ),
        port,
    )
    return {
        "provider_id": provider_id,
        "port": port,
        "token": token,
        "base_url": f"http://127.0.0.1:{port}",
    }


@pytest.fixture
def gateway(infra, redis_client, sanctions_mock, shared_mocks):
    """A gateway wired to this test's fresh sanctions mock + shared PEP/media."""
    from aml_sentinel.providers.gateway import ProviderGateway
    from aml_sentinel.providers.models import ProviderConfig

    providers = {
        "sanctions": ProviderConfig(
            sanctions_mock["provider_id"], "sanctions", sanctions_mock["base_url"]
        ),
        "pep": ProviderConfig("dow_jones", "pep", f"http://127.0.0.1:{shared_mocks['pep']}"),
        "adverse_media": ProviderConfig(
            "comply_advantage", "adverse_media", f"http://127.0.0.1:{shared_mocks['adverse_media']}"
        ),
    }
    gw = ProviderGateway(providers=providers, redis_client=redis_client)
    yield gw
    gw.close()


@pytest.fixture
def db(infra):
    from aml_sentinel.db.base import SessionLocal

    s = SessionLocal()
    yield s
    s.close()
