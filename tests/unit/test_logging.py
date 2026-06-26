"""Unit — structured log schema (doc 02 §6)."""

from __future__ import annotations

import json

import allure
import pytest

from aml_sentinel.observability.logging import configure_logging, stage_log


@allure.epic("AML-Sentinel")
@allure.feature("Observability")
@pytest.mark.unit
def test_stage_log_emits_required_schema(capsys):
    configure_logging()
    stage_log(
        stage="normalize",
        component="normalizer",
        trace_id="trace-1",
        client_id="cli-1",
        status="ok",
        duration_ms=12,
        detail={"profile_hash": "abc", "transliterated": False, "fields_defaulted": []},
    )
    line = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(line)
    for key in ("ts", "level", "trace_id", "client_id", "stage", "status", "component", "detail"):
        assert key in record, f"missing {key}"
    assert record["stage"] == "normalize"
    assert record["trace_id"] == "trace-1"
    assert record["detail"]["profile_hash"] == "abc"
