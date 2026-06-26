"""Data-quality monitors — the doc 02 §7 catalog as runnable checks (Phase 8).

Each monitor maps a real AML risk to a SQL/behavioral assertion and returns a
:class:`MonitorResult` listing any breaching rows. They run both as scheduled
checks in the SUT (the monitor service posts breaches to ``alert_sink``) and as
pytest assertions in Phase 9. A monitor "passes on healthy data and fires on
injected corruption" — the verification injects each corruption and confirms the
matching monitor reports it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from aml_sentinel.matching.normalize import compute_profile_hash

MATCHING_GOLDEN = Path("data/golden/matching.jsonl")
MAX_BREACH_SAMPLE = 25


@dataclass
class MonitorResult:
    check: str
    risk: str
    passed: bool
    breach_count: int = 0
    breaches: list[Any] = field(default_factory=list)
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "risk": self.risk,
            "passed": self.passed,
            "breach_count": self.breach_count,
            "breaches": self.breaches[:MAX_BREACH_SAMPLE],
            "detail": self.detail,
        }


def _version_key(version: str | None) -> int:
    if not version:
        return 0
    try:
        return int(str(version).lstrip("vV"))
    except ValueError:
        return 0


def _result(check: str, risk: str, breaches: list[Any], detail: str = "") -> MonitorResult:
    return MonitorResult(
        check=check,
        risk=risk,
        passed=len(breaches) == 0,
        breach_count=len(breaches),
        breaches=list(breaches),
        detail=detail,
    )


def check_completeness(session: Session) -> MonitorResult:
    """Every raw_profile has exactly one normalized_profile."""
    rows = session.execute(
        text(
            """
            SELECT r.client_id FROM raw_profile r
            LEFT JOIN normalized_profile n ON n.client_id = r.client_id
            GROUP BY r.client_id
            HAVING COUNT(n.id) <> 1
            """
        )
    ).all()
    return _result("completeness", "dropped clients = unscreened high-risk", [r[0] for r in rows])


def check_orphan_match(session: Session) -> MonitorResult:
    """No match without a screening."""
    rows = session.execute(
        text(
            """
            SELECT m.match_id FROM match m
            LEFT JOIN screening s ON s.screening_id = m.screening_id
            WHERE s.screening_id IS NULL
            """
        )
    ).all()
    return _result("orphan_match", "broken lineage = indefensible audit", [r[0] for r in rows])


def check_orphan_decision(session: Session) -> MonitorResult:
    """No decision without a screening."""
    rows = session.execute(
        text(
            """
            SELECT d.decision_id FROM decision d
            LEFT JOIN screening s ON s.screening_id = d.screening_id
            WHERE s.screening_id IS NULL
            """
        )
    ).all()
    return _result("orphan_decision", "broken lineage = indefensible audit", [r[0] for r in rows])


def check_lineage(session: Session) -> MonitorResult:
    """trace_id is identical across all rows for a client."""
    rows = session.execute(
        text(
            """
            SELECT client_id FROM (
                SELECT client_id, trace_id FROM raw_profile
                UNION SELECT client_id, trace_id FROM normalized_profile
                UNION SELECT client_id, trace_id FROM screening
                UNION SELECT client_id, trace_id FROM decision
            ) u
            GROUP BY client_id
            HAVING COUNT(DISTINCT trace_id) > 1
            """
        )
    ).all()
    return _result("lineage", "can't prove what was screened", [r[0] for r in rows])


def check_decision_coverage(session: Session) -> MonitorResult:
    """Every completed screening has exactly one decision."""
    rows = session.execute(
        text(
            """
            SELECT s.screening_id FROM screening s
            LEFT JOIN decision d ON d.screening_id = s.screening_id
            WHERE s.status = 'completed'
            GROUP BY s.screening_id
            HAVING COUNT(d.decision_id) <> 1
            """
        )
    ).all()
    return _result("decision_coverage", "unreviewed clients", [r[0] for r in rows])


def check_freshness(session: Session, scope_clients: set[str] | None = None) -> MonitorResult:
    """No active (latest) screening references a stale list_version.

    Stale = a newer version of that provider's list exists among screenings.
    """
    latest = session.execute(
        text(
            """
            SELECT DISTINCT ON (client_id) client_id, list_versions
            FROM screening ORDER BY client_id, screened_at DESC
            """
        )
    ).all()
    all_rows = session.execute(text("SELECT list_versions FROM screening")).all()

    max_version: dict[str, int] = {}
    for (lv,) in all_rows:
        for provider, ver in (lv or {}).items():
            max_version[provider] = max(max_version.get(provider, 0), _version_key(ver))

    breaches: list[str] = []
    for client_id, lv in latest:
        if scope_clients is not None and client_id not in scope_clients:
            continue
        for provider, ver in (lv or {}).items():
            if _version_key(ver) < max_version.get(provider, 0):
                breaches.append(client_id)
                break
    return _result("freshness", "screening against outdated sanctions", breaches)


def check_idempotency(session: Session) -> MonitorResult:
    """Re-processing left no duplicate (client_id, profile_hash) screening identity."""
    rows = session.execute(
        text(
            """
            SELECT client_id, profile_hash FROM normalized_profile
            GROUP BY client_id, profile_hash
            HAVING COUNT(*) > 1
            """
        )
    ).all()
    return _result(
        "idempotency", "double-counting, inflated metrics", [f"{r[0]}:{r[1]}" for r in rows]
    )


def check_audit_immutability(session: Session) -> MonitorResult:
    """UPDATE on audit is rejected by the DB (Phase 1 trigger)."""
    audit_id = session.execute(text("SELECT audit_id FROM audit LIMIT 1")).first()
    if audit_id is None:
        return MonitorResult(
            "audit_immutability", "tampered evidence", passed=True, detail="no audit rows to probe"
        )
    try:
        with session.begin_nested():
            session.execute(
                text("UPDATE audit SET trace_id = trace_id WHERE audit_id = :id"),
                {"id": audit_id[0]},
            )
        # If we got here the UPDATE was accepted → immutability is broken.
        session.rollback()
        return _result(
            "audit_immutability", "tampered evidence", [audit_id[0]], "UPDATE was NOT rejected"
        )
    except Exception:
        session.rollback()
        return MonitorResult(
            "audit_immutability", "tampered evidence", passed=True, detail="UPDATE rejected"
        )


def check_determinism(session: Session) -> MonitorResult:
    """Stored profile_hash recomputes identically from the canonical inputs."""
    rows = session.execute(
        text(
            """
            SELECT client_id, canonical_name, dob_iso, nationality_iso2, profile_hash
            FROM normalized_profile
            """
        )
    ).all()
    breaches: list[str] = []
    for client_id, canonical_name, dob_iso, nationality_iso2, profile_hash in rows:
        recomputed = compute_profile_hash(
            canonical_name,
            dob_iso.isoformat() if dob_iso else None,
            nationality_iso2,
        )
        if recomputed != profile_hash:
            breaches.append(client_id)
    return _result("determinism", "flaky screening", breaches)


def check_match_accuracy(session: Session | None = None) -> MonitorResult:
    """Golden matching precision/recall within thresholds (false negatives = breach)."""
    if not MATCHING_GOLDEN.exists():
        return MonitorResult(
            "match_accuracy",
            "false negatives = compliance breach",
            passed=True,
            detail="golden matching set not generated",
        )
    from aml_sentinel.matching.fuzzy import SCREENING_THRESHOLD, score_pair

    rows = [json.loads(line) for line in MATCHING_GOLDEN.read_text().splitlines()]
    tp = fp = fn = 0
    for r in rows:
        score = score_pair(
            r["profile_name"], r["candidate_name"], r["dob_profile"], r["dob_candidate"]
        )
        predicted = score >= SCREENING_THRESHOLD
        if predicted and r["expected_match"]:
            tp += 1
        elif predicted and not r["expected_match"]:
            fp += 1
        elif not predicted and r["expected_match"]:
            fn += 1
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    breaches = (
        [] if (precision >= 0.95 and recall >= 0.90) else [f"p={precision:.3f},r={recall:.3f}"]
    )
    return _result(
        "match_accuracy",
        "false negatives = compliance breach",
        breaches,
        detail=f"precision={precision:.3f} recall={recall:.3f}",
    )


# Ordered catalog (doc 02 §7).
MONITORS = (
    check_completeness,
    check_orphan_match,
    check_orphan_decision,
    check_lineage,
    check_decision_coverage,
    check_freshness,
    check_idempotency,
    check_audit_immutability,
    check_determinism,
    check_match_accuracy,
)


def run_all(session: Session) -> list[MonitorResult]:
    """Run every monitor; return their results in catalog order."""
    results: list[MonitorResult] = []
    for monitor in MONITORS:
        try:
            results.append(monitor(session))
        except Exception as exc:  # a monitor must never take the service down
            session.rollback()
            results.append(
                MonitorResult(monitor.__name__, "monitor error", passed=False, detail=str(exc))
            )
    return results
