"""Unit checks for the post-Glue quarantine report formatter and aggregation."""

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "infra"
    / "terraform"
    / "quarantine_report_lambda"
    / "handler.py"
)


@pytest.fixture(scope="module")
def report_module(monkeypatch_module):
    monkeypatch_module.setenv("AWS_REGION", "us-east-1")
    monkeypatch_module.setenv("GLUE_JOB_NAME", "rtmsp-dev-payments-sdp")
    monkeypatch_module.setenv("GLUE_DATABASE", "rtmsp_dev_payments")
    monkeypatch_module.setenv("ATHENA_WORKGROUP", "rtmsp-dev-payments")
    monkeypatch_module.setenv("ATHENA_RESULTS", "s3://gold/athena-results/")
    monkeypatch_module.setenv("REPORT_PREFIX", "s3://glue/quarantine-reports/")
    monkeypatch_module.setenv(
        "SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:topic"
    )
    monkeypatch_module.setenv("STATE_TABLE", "quarantine-state")
    spec = importlib.util.spec_from_file_location(
        "quarantine_report_handler", MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def monkeypatch_module():
    """Environment fixture isolated from tests that use the host AWS config."""
    monkeypatch = pytest.MonkeyPatch()
    yield monkeypatch
    monkeypatch.undo()


def test_zero_rejections_produce_explicit_zero_counts(report_module, monkeypatch):
    queries = iter(
        [
            ([["bronze", "101"], ["rejected", "202"]], "snap-query"),
            (
                [["summary", "total_records", "0"], ["summary", "new_records", "0"]],
                "count-query",
            ),
        ]
    )
    statements = []

    def fake_query(sql):
        statements.append(sql)
        return next(queries)

    monkeypatch.setattr(report_module, "_query", fake_query)
    monkeypatch.setattr(
        report_module, "_failed_run_between", lambda _baseline, _run: False
    )
    result = report_module._quarantine_counts(
        {"CompletedOn": datetime(2026, 10, 17, tzinfo=timezone.utc)}, {}
    )
    assert result["total_quarantined"] == 0
    assert result["new_rejections"] == 0
    assert result["reason_counts"] == {"total": {}, "new": {}}
    assert result["bronze_snapshot_id"] == "101"
    assert "EXCEPT" not in statements[1]


def test_counts_preserve_duplicate_rows_and_reasons_per_scope(
    report_module, monkeypatch
):
    queries = iter(
        [
            ([["bronze", "111"], ["rejected", "222"]], "snapshot-query"),
            (
                [
                    ["summary", "total_records", "3"],
                    ["summary", "new_records", "2"],
                    ["reason", "total:invalid_amount", "3"],
                    ["reason", "new:invalid_amount", "2"],
                    ["reason", "new:missing_status", "1"],
                ],
                "counts-query",
            ),
        ]
    )
    statements = []

    def fake_query(sql):
        statements.append(sql)
        return next(queries)

    monkeypatch.setattr(report_module, "_query", fake_query)
    monkeypatch.setattr(
        report_module, "_failed_run_between", lambda _baseline, _run: False
    )
    result = report_module._quarantine_counts(
        {"CompletedOn": datetime(2026, 10, 17, tzinfo=timezone.utc)},
        {"job_run_id": "previous", "bronze_snapshot_id": "100"},
    )
    assert result["total_quarantined"] == 3
    assert result["new_rejections"] == 2
    assert result["reason_counts"] == {
        "total": {"invalid_amount": 3},
        "new": {"invalid_amount": 2, "missing_status": 1},
    }
    assert "FOR VERSION AS OF 100" in statements[1]
    assert "EXCEPT" in statements[1]
    assert "UNNEST(r.rejection_reasons)" in statements[1]


def test_email_marks_initial_history_as_new_and_explains_multi_reason_counts(
    report_module,
):
    body = report_module._email_body(
        {
            "job_name": "payments-sdp",
            "job_run_id": "jr-1",
            "state": "SUCCEEDED",
            "started_on": "start",
            "completed_on": "end",
            "report_status": "complete",
            "total_quarantined": 2,
            "new_rejections": 2,
            "reason_counts": {
                "total": {"invalid_amount": 2},
                "new": {"invalid_amount": 2},
            },
            "previous_successful_job_run_id": None,
        }
    )
    assert "histórico completo contado como novo" in body
    assert "Um registro pode apresentar mais de um motivo" in body


def test_incomplete_email_does_not_render_zero_counts(report_module):
    body = report_module._email_body(
        {
            "job_name": "payments-sdp",
            "job_run_id": "jr-2",
            "state": "SUCCEEDED",
            "report_status": "incomplete",
            "counts_unavailable_reason": "snapshot indisponível",
        }
    )
    assert "Contagens: indisponíveis" in body
    assert "Total em quarentena" not in body


def test_report_path_splits_into_bucket_and_prefix(report_module):
    assert report_module._split_s3("s3://glue/reports/") == ("glue", "reports/")
