"""Monitor service (Phase 8).

Exposes Prometheus ``/metrics`` and runs the data-quality monitor catalog on
demand (``POST /monitors/run``), posting any breaches to the capture-only
``alert_sink``. A scheduler (or the test harness) calls ``/monitors/run``; the
gauges it refreshes are then scraped from ``/metrics``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from aml_sentinel.config import settings
from aml_sentinel.db.base import SessionLocal
from aml_sentinel.observability.logging import configure_logging, stage_log
from aml_sentinel.observability.metrics import refresh_metrics
from aml_sentinel.observability.monitors import run_all

COMPONENT = "monitors"


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    yield


app = FastAPI(title="AML-Sentinel Monitors", version="0.1.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "component": COMPONENT}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _emit_alert(result_dict: dict) -> None:
    """Best-effort POST to the capture-only alert sink (never raises)."""
    try:
        httpx.post(f"{settings.alert_sink_url}/alerts", json=result_dict, timeout=2.0)
    except httpx.HTTPError:
        pass


@app.post("/monitors/run")
def run_monitors() -> dict:
    """Run every monitor, refresh metrics, and alert on breaches."""
    with SessionLocal() as session:
        results = run_all(session)
        refresh_metrics(session, results)

    breaches = [r for r in results if not r.passed]
    for r in breaches:
        _emit_alert({"type": "dq_breach", **r.to_dict()})

    stage_log(
        stage="monitor",
        component=COMPONENT,
        trace_id="-",
        client_id="-",
        status="ok" if not breaches else "failed",
        level="INFO" if not breaches else "WARNING",
        detail={
            "checks": len(results),
            "breached": [r.check for r in breaches],
        },
    )
    return {
        "checks": len(results),
        "passed": sum(1 for r in results if r.passed),
        "breached": [r.check for r in breaches],
        "results": [r.to_dict() for r in results],
    }
