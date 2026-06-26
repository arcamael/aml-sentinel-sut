"""Provider gateway (Phase 4).

A single facade over the three mock providers (doc 01 §3.1). For each query it:

1. resolves the provider's current ``list_version`` (cheaply, via ``/health``,
   cached in Redis with a short TTL);
2. checks the Redis cache keyed ``(provider_id, name_hash, list_version)``;
3. on a miss, calls ``/search`` with a **per-provider timeout**, **bounded
   retries with exponential backoff**, and a **circuit breaker**;
4. caches a clean result, or returns a **degraded** (empty) result on failure —
   never raising — and logs a ``WARNING``.

Everything is observable: Prometheus counters for requests/cache/degraded plus a
structured JSON log line per query.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import httpx
import redis as redis_lib
import structlog
from prometheus_client import Counter, Gauge

from aml_sentinel.config import settings
from aml_sentinel.providers.circuit_breaker import CircuitBreaker
from aml_sentinel.providers.models import Candidate, ProviderConfig, ProviderResponse

COMPONENT = "provider-gateway"

# ── Metrics ──────────────────────────────────────────────────────────────────
GW_REQUESTS = Counter(
    "aml_gateway_requests_total",
    "Gateway provider queries by outcome.",
    labelnames=("provider", "outcome"),
)
GW_CACHE_HITS = Counter(
    "aml_gateway_cache_hits_total", "Gateway cache hits.", labelnames=("provider",)
)
GW_CACHE_MISSES = Counter(
    "aml_gateway_cache_misses_total", "Gateway cache misses.", labelnames=("provider",)
)
GW_DEGRADED = Counter(
    "aml_gateway_degraded_total",
    "Degraded (graceful-failure) responses.",
    labelnames=("provider",),
)
GW_BREAKER_OPEN = Gauge(
    "aml_gateway_breaker_open",
    "1 when a provider circuit is open.",
    labelnames=("provider",),
)


class ProviderError(Exception):
    """A retryable provider-side failure (5xx, malformed/empty body)."""


def name_hash(canonical_name: str) -> str:
    """Stable hash used in the cache key (doc 01 §6)."""
    return hashlib.sha256(canonical_name.encode("utf-8")).hexdigest()


class ProviderGateway:
    def __init__(
        self,
        *,
        providers: dict[str, ProviderConfig] | None = None,
        redis_client: redis_lib.Redis | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._log = structlog.get_logger()
        self.providers = providers or self._default_providers()
        self._redis = redis_client if redis_client is not None else self._default_redis()
        self._client = http_client or httpx.Client(timeout=settings.provider_timeout_s)
        self._breakers = {
            list_type: CircuitBreaker(
                threshold=settings.breaker_threshold,
                cooldown_s=settings.breaker_cooldown_s,
            )
            for list_type in self.providers
        }

    @staticmethod
    def _default_providers() -> dict[str, ProviderConfig]:
        return {
            "sanctions": ProviderConfig("world_check", "sanctions", settings.world_check_url),
            "pep": ProviderConfig("dow_jones", "pep", settings.dow_jones_url),
            "adverse_media": ProviderConfig(
                "comply_advantage", "adverse_media", settings.comply_advantage_url
            ),
        }

    @staticmethod
    def _default_redis() -> redis_lib.Redis | None:
        try:
            client = redis_lib.Redis.from_url(settings.redis_url)
            client.ping()
            return client
        except Exception:  # pragma: no cover - cache is optional
            return None

    # ── public API ───────────────────────────────────────────────────────────
    def screen(
        self, *, canonical_name: str, dob_iso: str | None = None
    ) -> dict[str, ProviderResponse]:
        """Query every provider for a name; return one response per list type."""
        return {
            list_type: self.query(list_type, canonical_name, dob_iso)
            for list_type in self.providers
        }

    def query(
        self, list_type: str, canonical_name: str, dob_iso: str | None = None
    ) -> ProviderResponse:
        cfg = self.providers[list_type]
        started = time.perf_counter()

        version = self._list_version(cfg)
        nh = name_hash(canonical_name)
        cache_key = f"gw:{cfg.provider_id}:{nh}:{version}"

        cached = self._cache_get(cache_key)
        if cached is not None:
            GW_CACHE_HITS.labels(cfg.provider_id).inc()
            GW_REQUESTS.labels(cfg.provider_id, "cache_hit").inc()
            resp = ProviderResponse(
                provider_id=cfg.provider_id,
                list_type=list_type,
                list_version=cached["list_version"],
                candidates=[Candidate.from_dict(c) for c in cached["candidates"]],
                cache_hit=True,
            )
            self._log_query(cfg, resp, started, cache_hit=True)
            return resp

        GW_CACHE_MISSES.labels(cfg.provider_id).inc()
        resp = self._resilient_search(cfg, canonical_name, dob_iso, version)

        if not resp.degraded:
            self._cache_set(cache_key, resp)
            GW_REQUESTS.labels(cfg.provider_id, "ok").inc()
        self._log_query(cfg, resp, started, cache_hit=False)
        return resp

    # ── resilience ───────────────────────────────────────────────────────────
    def _resilient_search(
        self, cfg: ProviderConfig, name: str, dob: str | None, version: str
    ) -> ProviderResponse:
        breaker = self._breakers[cfg.list_type]
        GW_BREAKER_OPEN.labels(cfg.provider_id).set(0 if breaker.allow() else 1)

        if not breaker.allow():
            return self._degraded(cfg, version, "circuit_open")

        last_error: str | None = None
        for attempt in range(settings.provider_retries + 1):
            try:
                list_version, candidates = self._call_search(cfg, name, dob)
                breaker.record_success()
                GW_BREAKER_OPEN.labels(cfg.provider_id).set(0)
                return ProviderResponse(
                    provider_id=cfg.provider_id,
                    list_type=cfg.list_type,
                    list_version=list_version,
                    candidates=candidates,
                )
            except (ProviderError, httpx.TimeoutException, httpx.RequestError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < settings.provider_retries:
                    time.sleep(settings.provider_backoff_base_s * (2**attempt))

        # Exhausted retries → count one breaker failure for the whole query.
        breaker.record_failure()
        GW_BREAKER_OPEN.labels(cfg.provider_id).set(0 if breaker.allow() else 1)
        return self._degraded(cfg, version, last_error or "unknown")

    def _call_search(
        self, cfg: ProviderConfig, name: str, dob: str | None
    ) -> tuple[str, list[Candidate]]:
        params: dict[str, Any] = {"name": name, "limit": 50}
        if dob:
            params["dob"] = dob
        r = self._client.get(
            f"{cfg.base_url}/search", params=params, timeout=settings.provider_timeout_s
        )
        if r.status_code >= 500:
            raise ProviderError(f"status {r.status_code}")
        try:
            data = r.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise ProviderError(f"malformed/empty body: {exc}") from exc
        candidates = [Candidate.from_dict(c) for c in data.get("candidates", [])]
        return data.get("list_version", "unknown"), candidates

    def _degraded(self, cfg: ProviderConfig, version: str, error: str) -> ProviderResponse:
        GW_DEGRADED.labels(cfg.provider_id).inc()
        GW_REQUESTS.labels(cfg.provider_id, "degraded").inc()
        return ProviderResponse(
            provider_id=cfg.provider_id,
            list_type=cfg.list_type,
            list_version=version,
            candidates=[],
            degraded=True,
            error=error,
        )

    # ── list version (cheap, cached) ─────────────────────────────────────────
    def _list_version(self, cfg: ProviderConfig) -> str:
        vkey = f"gwver:{cfg.provider_id}"
        if self._redis is not None:
            cached = self._redis.get(vkey)
            if cached is not None:
                return cached.decode()
        try:
            r = self._client.get(f"{cfg.base_url}/health", timeout=settings.provider_timeout_s)
            version = r.json().get("list_version", "unknown")
        except Exception:
            return "unknown"
        if self._redis is not None and version != "unknown":
            self._redis.setex(vkey, settings.provider_version_ttl_s, version)
        return version

    # ── cache helpers ────────────────────────────────────────────────────────
    def _cache_get(self, key: str) -> dict[str, Any] | None:
        if self._redis is None:
            return None
        raw = self._redis.get(key)
        return json.loads(raw) if raw is not None else None

    def _cache_set(self, key: str, resp: ProviderResponse) -> None:
        if self._redis is None:
            return
        self._redis.setex(key, settings.provider_cache_ttl_s, json.dumps(resp.to_cache_dict()))

    # ── logging ──────────────────────────────────────────────────────────────
    def _log_query(
        self, cfg: ProviderConfig, resp: ProviderResponse, started: float, *, cache_hit: bool
    ) -> None:
        duration_ms = int((time.perf_counter() - started) * 1000)
        fields = dict(
            component=COMPONENT,
            provider=cfg.provider_id,
            list_type=cfg.list_type,
            list_version=resp.list_version,
            cache_hit=cache_hit,
            degraded=resp.degraded,
            candidates=len(resp.candidates),
            duration_ms=duration_ms,
        )
        if resp.degraded:
            self._log.warning("gateway_query", status="degraded", error=resp.error, **fields)
        else:
            self._log.info("gateway_query", status="ok", **fields)

    def invalidate_provider(self, provider_id: str) -> int:
        """Drop cached version + results for a provider (call on a list update).

        Without this the short-lived ``gwver`` cache could keep serving the old
        ``list_version`` (and its results) for up to its TTL after a watchlist
        change, so a reconciliation re-screen would miss the new entry.
        """
        if self._redis is None:
            return 0
        deleted = 0
        self._redis.delete(f"gwver:{provider_id}")
        for key in self._redis.scan_iter(f"gw:{provider_id}:*"):
            self._redis.delete(key)
            deleted += 1
        return deleted

    def close(self) -> None:
        self._client.close()
