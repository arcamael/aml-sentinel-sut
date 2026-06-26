"""Shared mock-provider implementation (doc 01 §7).

A single :func:`create_app` builds a FastAPI service for any of the three
providers. It:

* loads its watchlist file at startup (``/search`` returns candidate entries),
* exposes ``/health`` (always 200, advertises ``list_version``) and ``/_state``,
* injects faults via ``POST /_control/fault`` — ``timeout`` | ``slow`` | ``500``
  | ``malformed`` | ``empty`` — so the gateway's resilience can be tested.

**Retrieval, not matching.** ``/search`` does coarse token-prefix *blocking*
(a real watchlist API's job) and returns candidates; the fuzzy *scoring* that
decides a true match is the SUT's responsibility (Phase 5). The mock embeds no
business logic (doc 01 §7 rule).
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from unidecode import unidecode

# How long each timing fault stalls. ``slow`` must stay under the gateway's
# per-provider timeout (it should still succeed); ``timeout`` must exceed it.
SLOW_DELAY_S = 0.5
TIMEOUT_DELAY_S = 10.0

FAULT_TYPES = {"none", "clear", "timeout", "slow", "500", "malformed", "empty"}


class FaultRequest(BaseModel):
    type: str
    # Apply to the next N requests; -1 (default) = until explicitly cleared.
    count: int = -1


def _block_keys(name: str) -> set[str]:
    """Coarse blocking keys for a name: 3-char token prefixes + first letters."""
    keys: set[str] = set()
    for token in unidecode(name).lower().split():
        token = token.strip("-'")
        if not token:
            continue
        keys.add(token[:3])
        keys.add(token[0])
    return keys


def _load_entries(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    entries = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            entries.append(json.loads(line))
    return entries


def create_app(
    *,
    provider_id: str,
    list_type: str,
    watchlist_path: str | os.PathLike[str],
    list_version: str = "v1",
) -> FastAPI:
    app = FastAPI(title=f"mock-{provider_id}", version="0.1.0")

    entries = _load_entries(Path(watchlist_path))
    # Pre-compute blocking keys per entry (entity_name + aliases) once.
    index: list[tuple[set[str], dict[str, Any]]] = []
    for e in entries:
        keys = _block_keys(e["entity_name"])
        for alias in e.get("aliases", []):
            keys |= _block_keys(alias)
        index.append((keys, e))

    state: dict[str, Any] = {
        "fault": {"type": "none", "remaining": 0},
        "requests": 0,
    }

    def _fault_active() -> bool:
        f = state["fault"]
        return f["type"] not in ("none", "clear") and f["remaining"] != 0

    def _consume_fault() -> str:
        f = state["fault"]
        ftype = f["type"]
        if f["remaining"] > 0:
            f["remaining"] -= 1
            if f["remaining"] == 0:
                state["fault"] = {"type": "none", "remaining": 0}
        return ftype

    @app.get("/health")
    def health() -> dict[str, Any]:
        # Health stays green regardless of injected faults: faults model flaky
        # *query* behavior, not a dead provider, so the gateway must discover
        # them mid-call rather than skip the provider up front.
        return {
            "status": "ok",
            "provider_id": provider_id,
            "list_type": list_type,
            "list_version": list_version,
            "entries": len(entries),
        }

    @app.get("/_state")
    def get_state() -> dict[str, Any]:
        return {
            "provider_id": provider_id,
            "list_type": list_type,
            "list_version": list_version,
            "entries": len(entries),
            "requests": state["requests"],
            "fault": state["fault"],
        }

    @app.post("/_control/fault")
    def set_fault(req: FaultRequest) -> dict[str, Any]:
        if req.type not in FAULT_TYPES:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"unknown fault type: {req.type}",
                    "allowed": sorted(FAULT_TYPES),
                },
            )
        if req.type in ("none", "clear"):
            state["fault"] = {"type": "none", "remaining": 0}
        else:
            state["fault"] = {"type": req.type, "remaining": req.count}
        return {"fault": state["fault"]}

    @app.get("/search")
    async def search(name: str, dob: str | None = None, limit: int = 50, offset: int = 0):
        state["requests"] += 1

        if _fault_active():
            ftype = _consume_fault()
            if ftype == "timeout":
                await asyncio.sleep(TIMEOUT_DELAY_S)  # gateway will time out first
            elif ftype == "slow":
                await asyncio.sleep(SLOW_DELAY_S)  # delayed but still answers
            elif ftype == "500":
                return JSONResponse(status_code=500, content={"error": "injected 500"})
            elif ftype == "malformed":
                return Response(content='{"candidates": [', media_type="application/json")
            elif ftype == "empty":
                return Response(content="", media_type="application/json")

        query_keys = _block_keys(name)
        matched = [e for keys, e in index if keys & query_keys]
        # Stable order so pagination is deterministic.
        matched.sort(key=lambda e: e["entry_id"])
        page = matched[offset : offset + limit]
        return {
            "provider_id": provider_id,
            "list_type": list_type,
            "list_version": list_version,
            "total": len(matched),
            "candidates": page,
        }

    return app
