"""Central configuration for AML-Sentinel.

All runtime configuration is read from the environment (12-factor) with sane
local-development defaults that match ``docker-compose.yml``. Reused by every
later phase (API, workers, providers).
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide settings, populated from env vars (prefix ``AML_``)."""

    model_config = SettingsConfigDict(
        env_prefix="AML_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Determinism (hard rule #1) ───────────────────────────────────────────
    seed: int = 42

    # ── PostgreSQL ───────────────────────────────────────────────────────────
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "aml"
    postgres_password: str = "aml_secret"
    postgres_db: str = "aml_sentinel"

    # ── Redis ────────────────────────────────────────────────────────────────
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    # ── Kafka / Redpanda ─────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"

    # ── Provider gateway + mock providers (Phase 4) ──────────────────────────
    # Base URLs default to the host-mapped mock ports; in compose the workers
    # override these with the internal service hostnames.
    world_check_url: str = "http://localhost:9101"
    dow_jones_url: str = "http://localhost:9102"
    comply_advantage_url: str = "http://localhost:9103"

    provider_timeout_s: float = 2.0
    provider_retries: int = 2  # additional attempts after the first
    provider_backoff_base_s: float = 0.05
    breaker_threshold: int = 3  # consecutive failures before opening
    breaker_cooldown_s: float = 5.0
    provider_cache_ttl_s: int = 300
    provider_version_ttl_s: int = 10

    @property
    def database_url(self) -> str:
        """SQLAlchemy / Alembic connection URL (psycopg2 driver)."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


settings = Settings()
