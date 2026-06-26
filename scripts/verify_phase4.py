"""Phase 4 verification (roadmap 🔎).

Spins up the three mock providers in-process (uvicorn on background threads),
seeded from the generated watchlists, and drives the real ProviderGateway over
HTTP against them. Three checks:

1. **Planted match** — query a known sanctioned name → candidate returned with
   the correct ``list_version`` (cache miss, hits the provider).
2. **Cache hit** — repeat the identical query → served from Redis;
   ``aml_gateway_cache_hits_total`` increments and the log shows ``cache_hit``.
3. **Fault tolerance** — inject ``POST /_control/fault {timeout}`` → the gateway
   returns a *degraded* (empty) result, does not crash, and logs ``WARNING``;
   ``aml_gateway_degraded_total`` increments.

Requires Redis (Phase 0 stack). Run:

    PYTHONPATH=src .venv/bin/python scripts/verify_phase4.py
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import httpx
import redis as redis_lib
import uvicorn
from prometheus_client import REGISTRY

from aml_sentinel.config import settings
from aml_sentinel.observability.logging import configure_logging
from aml_sentinel.providers.gateway import ProviderGateway
from aml_sentinel.providers.models import ProviderConfig
from mocks.provider_mock import create_app
from tools.datagen import watchlists

PORTS = {"sanctions": 9101, "pep": 9102, "adverse_media": 9103}
FILES = {
    "sanctions": "data/watchlists/sanctions.jsonl",
    "pep": "data/watchlists/pep.jsonl",
    "adverse_media": "data/watchlists/adverse_media.jsonl",
}
PROVIDER_IDS = {"sanctions": "world_check", "pep": "dow_jones", "adverse_media": "comply_advantage"}


def _serve(app, port: int) -> uvicorn.Server:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # safe to run off the main thread
    threading.Thread(target=server.run, daemon=True).start()
    return server


def _wait_health(port: int, timeout_s: float = 10.0) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
            if r.status_code == 200:
                return r.json()
        except httpx.HTTPError:
            time.sleep(0.1)
    raise RuntimeError(f"mock on port {port} never became healthy")


def _metric(name: str, provider: str) -> float:
    val = REGISTRY.get_sample_value(name, {"provider": provider})
    return val or 0.0


def main() -> int:
    configure_logging()

    # Ensure the watchlists the mocks serve exist (deterministic regeneration).
    if not Path(FILES["sanctions"]).exists():
        watchlists.generate(Path("data/watchlists/"), seed=settings.seed)

    servers = []
    for list_type, port in PORTS.items():
        app = create_app(
            provider_id=PROVIDER_IDS[list_type],
            list_type=list_type,
            watchlist_path=FILES[list_type],
        )
        servers.append(_serve(app, port))
    health = {lt: _wait_health(port) for lt, port in PORTS.items()}
    print("── mocks up ────────────────────────────────────────────────────")
    for lt, h in health.items():
        print(
            f"  {h['provider_id']:18s} list_type={h['list_type']:13s} "
            f"list_version={h['list_version']} entries={h['entries']}"
        )

    # Gateway pointed at the in-process mocks; start from a clean Redis cache.
    rds = redis_lib.Redis.from_url(settings.redis_url)
    for key in rds.scan_iter("gw:*"):
        rds.delete(key)
    for key in rds.scan_iter("gwver:*"):
        rds.delete(key)

    providers = {
        lt: ProviderConfig(PROVIDER_IDS[lt], lt, f"http://127.0.0.1:{PORTS[lt]}") for lt in PORTS
    }
    gw = ProviderGateway(providers=providers, redis_client=rds)

    ok = True

    # ── Check 1: planted match ───────────────────────────────────────────────
    print("── Check 1: planted sanctioned name → candidate + list_version ──")
    r1 = gw.query("sanctions", "Ivan Petrov", dob_iso="1972-03-14")
    hit_ids = [c.entry_id for c in r1.candidates]
    found = "wl_sanctions_0001" in hit_ids
    c1 = found and r1.list_version == "v1" and not r1.cache_hit and not r1.degraded
    print(
        f"  candidates={len(r1.candidates)} list_version={r1.list_version} "
        f"cache_hit={r1.cache_hit} degraded={r1.degraded}"
    )
    print(f"  wl_sanctions_0001 present: {found}")
    print(f"  {'✓' if c1 else '✗'} planted match returned with correct list_version")
    ok &= c1

    # ── Check 2: cache hit on repeat ─────────────────────────────────────────
    print("── Check 2: repeat identical query → cache hit (metric increments) ")
    hits_before = _metric("aml_gateway_cache_hits_total", "world_check")
    r2 = gw.query("sanctions", "Ivan Petrov", dob_iso="1972-03-14")
    hits_after = _metric("aml_gateway_cache_hits_total", "world_check")
    c2 = (
        r2.cache_hit
        and [c.entry_id for c in r2.candidates] == hit_ids
        and (hits_after - hits_before) == 1
    )
    print(
        f"  cache_hit={r2.cache_hit} candidates={len(r2.candidates)} "
        f"cache_hits_total {hits_before:.0f} -> {hits_after:.0f}"
    )
    print(f"  {'✓' if c2 else '✗'} second query served from cache; metric +1")
    ok &= c2

    # ── Check 3: timeout fault → degraded, not a crash ───────────────────────
    print("── Check 3: inject timeout fault → degraded result + WARNING ────")
    httpx.post(
        f"http://127.0.0.1:{PORTS['sanctions']}/_control/fault",
        json={"type": "timeout"},
        timeout=2.0,
    )
    degraded_before = _metric("aml_gateway_degraded_total", "world_check")
    # Fresh, uncached name so the gateway actually calls the (faulting) provider.
    t0 = time.perf_counter()
    r3 = gw.query("sanctions", "Sergei Smirnov", dob_iso="1980-01-01")
    elapsed = time.perf_counter() - t0
    degraded_after = _metric("aml_gateway_degraded_total", "world_check")
    httpx.post(
        f"http://127.0.0.1:{PORTS['sanctions']}/_control/fault", json={"type": "clear"}, timeout=2.0
    )
    c3 = (
        r3.degraded
        and r3.candidates == []
        and r3.error is not None
        and (degraded_after - degraded_before) == 1
    )
    print(
        f"  degraded={r3.degraded} candidates={len(r3.candidates)} "
        f"error={r3.error!r} elapsed={elapsed:.2f}s"
    )
    print(f"  degraded_total {degraded_before:.0f} -> {degraded_after:.0f}")
    print(f"  {'✓' if c3 else '✗'} provider fault handled gracefully (no crash)")
    ok &= c3

    gw.close()
    for s in servers:
        s.should_exit = True

    print("────────────────────────────────────────────────────────────────")
    print(f"PHASE 4 VERIFICATION: {'PASS ✓' if ok else 'FAIL ✗'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
