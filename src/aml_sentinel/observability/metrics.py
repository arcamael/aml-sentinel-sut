"""Prometheus metrics for the monitor service (Phase 8).

DB-derived gauges (throughput per stage, decision mix, match-rate, dead-letter
count, reconciliation lag) plus one breach gauge per data-quality monitor. The
gateway's cache-hit-rate / degraded counters live in the worker process that
owns the gateway; these are the data-quality + pipeline-shape series.
"""

from __future__ import annotations

from prometheus_client import Gauge
from sqlalchemy import text
from sqlalchemy.orm import Session

from aml_sentinel.observability.monitors import MonitorResult

STAGE_ROWS = Gauge("aml_stage_rows", "Row count per pipeline stage.", labelnames=("stage",))
DECISION_MIX = Gauge("aml_decision_outcome", "Decisions by outcome.", labelnames=("outcome",))
MATCH_RATE = Gauge("aml_match_rate", "matches / screenings.")
DEAD_LETTERS = Gauge("aml_dead_letter_total", "Dead-letter rows.")
RECON_RUNS = Gauge("aml_reconciliation_runs_total", "Reconciliation runs recorded.")
RECON_NEWLY_FLAGGED = Gauge("aml_reconciliation_newly_flagged", "Sum of newly_flagged across runs.")
RECON_OPEN = Gauge("aml_reconciliation_open", "Reconciliation runs not yet finished (lag).")
DQ_BREACHES = Gauge("aml_dq_breaches", "Breaching rows per DQ monitor.", labelnames=("check",))
DQ_PASSED = Gauge("aml_dq_passed", "1 if the DQ monitor passes else 0.", labelnames=("check",))

_STAGE_TABLES = {
    "ingest": "raw_profile",
    "normalize": "normalized_profile",
    "screen": "screening",
    "match": "match",
    "decide": "decision",
    "audit": "audit",
}


def refresh_metrics(session: Session, monitor_results: list[MonitorResult]) -> None:
    """Recompute every gauge from the current DB state + monitor results."""
    for stage, table in _STAGE_TABLES.items():
        count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
        STAGE_ROWS.labels(stage).set(count)

    for outcome in ("CLEAR", "FLAG", "ESCALATE"):
        n = (
            session.execute(
                text("SELECT COUNT(*) FROM decision WHERE outcome = :o"), {"o": outcome}
            ).scalar()
            or 0
        )
        DECISION_MIX.labels(outcome).set(n)

    screenings = session.execute(text("SELECT COUNT(*) FROM screening")).scalar() or 0
    matches = session.execute(text("SELECT COUNT(*) FROM match")).scalar() or 0
    MATCH_RATE.set((matches / screenings) if screenings else 0.0)

    DEAD_LETTERS.set(session.execute(text("SELECT COUNT(*) FROM dead_letter")).scalar() or 0)
    RECON_RUNS.set(session.execute(text("SELECT COUNT(*) FROM reconciliation_run")).scalar() or 0)
    RECON_NEWLY_FLAGGED.set(
        session.execute(
            text("SELECT COALESCE(SUM(newly_flagged),0) FROM reconciliation_run")
        ).scalar()
        or 0
    )
    RECON_OPEN.set(
        session.execute(
            text("SELECT COUNT(*) FROM reconciliation_run WHERE finished_at IS NULL")
        ).scalar()
        or 0
    )

    for r in monitor_results:
        DQ_BREACHES.labels(r.check).set(r.breach_count)
        DQ_PASSED.labels(r.check).set(1 if r.passed else 0)
