"""Pydantic request/response models for the Ingestion API.

Field names mirror the generated KYC profile in doc 04 §3 (``full_name``,
``residence_country``, structured ``document_ids``) so generator output can be
POSTed to ``/clients`` unchanged in later E2E phases. ``extra="ignore"`` lets
planted-label metadata (e.g. ``expected_match_entry_id``) pass through harmlessly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ClientCreate(BaseModel):
    """Inbound KYC profile (doc 04 §3). Raw/dirty values are normalized later."""

    model_config = ConfigDict(extra="ignore")

    full_name: str = Field(min_length=1)
    dob: str = Field(min_length=1)
    nationality: str = Field(min_length=1)
    residence_country: str | None = None
    document_ids: list[dict[str, Any]] = Field(default_factory=list)

    # Optional identifiers; generated server-side when absent.
    client_id: str | None = None
    trace_id: str | None = None

    def kyc_payload(self) -> dict[str, Any]:
        """The raw KYC fields persisted as ``raw_profile.raw_payload``."""
        return {
            "full_name": self.full_name,
            "dob": self.dob,
            "nationality": self.nationality,
            "residence_country": self.residence_country,
            "document_ids": self.document_ids,
        }


class ClientCreated(BaseModel):
    """201 response for a successful submission."""

    client_id: str
    trace_id: str


class ClientView(BaseModel):
    """GET /clients/{client_id} inspection view."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    client_id: str
    trace_id: str
    raw_payload: dict[str, Any]
    source: str
    created_at: datetime
