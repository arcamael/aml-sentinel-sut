"""Value objects for the provider gateway (Phase 4)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ProviderConfig:
    """Where to reach one mock provider and what list it serves."""

    provider_id: str
    list_type: str  # sanctions | pep | adverse_media
    base_url: str


@dataclass(frozen=True)
class Candidate:
    """A raw candidate entry returned by a provider (pre-scoring).

    Mirrors the watchlist record (doc 04 §2); the screening worker (Phase 5)
    turns these into scored ``match`` rows.
    """

    entry_id: str
    provider_id: str
    list_type: str
    list_version: str
    entity_name: str
    aliases: list[str] = field(default_factory=list)
    dob_iso: str | None = None
    country_iso2: str | None = None
    risk_payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Candidate:
        return cls(
            entry_id=d["entry_id"],
            provider_id=d["provider_id"],
            list_type=d["list_type"],
            list_version=d["list_version"],
            entity_name=d["entity_name"],
            aliases=list(d.get("aliases", [])),
            dob_iso=d.get("dob_iso"),
            country_iso2=d.get("country_iso2"),
            risk_payload=dict(d.get("risk_payload", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProviderResponse:
    """Outcome of querying one provider through the gateway.

    ``degraded=True`` means the gateway could not get a clean answer (timeout,
    5xx, malformed/empty body, or an open circuit) and is returning a safe empty
    result instead of raising — the caller must never crash on a flaky provider.
    """

    provider_id: str
    list_type: str
    list_version: str
    candidates: list[Candidate]
    cache_hit: bool = False
    degraded: bool = False
    error: str | None = None

    def to_cache_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "list_type": self.list_type,
            "list_version": self.list_version,
            "candidates": [c.to_dict() for c in self.candidates],
        }
