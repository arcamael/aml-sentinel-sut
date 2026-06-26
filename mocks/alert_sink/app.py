"""Capture-only alert sink (doc 01 §7).

Records alert payloads so the harness can assert that data-quality breaches
fire an alert — without sending anything anywhere. ``POST /alerts`` stores;
``GET /alerts`` lists; ``GET /_state`` summarizes; ``POST /_control/reset``
clears between tests.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request

app = FastAPI(title="mock-alert-sink", version="0.1.0")

_ALERTS: list[dict[str, Any]] = []


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "component": "alert-sink"}


@app.post("/alerts")
async def receive_alert(request: Request) -> dict[str, Any]:
    payload = await request.json()
    _ALERTS.append(payload)
    return {"received": True, "count": len(_ALERTS)}


@app.get("/alerts")
def list_alerts() -> dict[str, Any]:
    return {"count": len(_ALERTS), "alerts": _ALERTS}


@app.get("/_state")
def state() -> dict[str, Any]:
    return {"count": len(_ALERTS), "checks": sorted({a.get("check", "?") for a in _ALERTS})}


@app.post("/_control/reset")
def reset() -> dict[str, Any]:
    _ALERTS.clear()
    return {"count": 0}
