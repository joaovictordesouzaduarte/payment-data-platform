"""Reconcile Glue job runs and email quarantine counts through SNS."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

REGION = os.environ["AWS_REGION"]
JOB_NAME = os.environ["GLUE_JOB_NAME"]
DATABASE = os.environ["GLUE_DATABASE"]
WORKGROUP = os.environ["ATHENA_WORKGROUP"]
RESULTS = os.environ["ATHENA_RESULTS"]
REPORT_PREFIX = os.environ["REPORT_PREFIX"]
TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
TABLE_NAME = os.environ["STATE_TABLE"]
LOCK_TTL_SECONDS = 960
QUERY_TIMEOUT_SECONDS = 390
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

glue = boto3.client("glue", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
state = dynamodb.Table(TABLE_NAME)


def handler(event: dict, _context: object) -> dict:
    """Process the oldest unreported terminal job run in the seven-day window."""
    lock_id = f"{int(time.time())}-{os.getpid()}"
    try:
        state.put_item(
            Item={
                "pk": "CONTROL",
                "lock_id": lock_id,
                "expires_at": int(time.time()) + LOCK_TTL_SECONDS,
            },
            ConditionExpression="attribute_not_exists(pk) OR expires_at < :now",
            ExpressionAttributeValues={":now": int(time.time())},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return {"status": "locked"}
        raise

    try:
        retry_run_id = (
            event.get("job_run_id") if event.get("action") == "retry" else None
        )
        if retry_run_id:
            state.delete_item(Key={"pk": f"RUN#{retry_run_id}"})
        pending = _pending_runs(retry_run_id, event)
        if not pending:
            return {"status": "no_pending_runs"}
        run = pending[0]
        _process_run(run)
        return {"status": "processed", "job_run_id": run["Id"]}
    finally:
        state.delete_item(
            Key={"pk": "CONTROL"},
            ConditionExpression="lock_id = :lock_id",
            ExpressionAttributeValues={":lock_id": lock_id},
        )


def _pending_runs(retry_run_id: str | None, event: dict) -> list[dict]:
    now = datetime.now(timezone.utc)
    activation_item = state.get_item(Key={"pk": "ACTIVATION"}).get("Item")
    if activation_item:
        activated_on = datetime.fromisoformat(activation_item["activated_on"])
    else:
        activated_on = now
        event_run_id = event.get("detail", {}).get("jobRunId")
        if event_run_id:
            event_run = glue.get_job_run(JobName=JOB_NAME, RunId=event_run_id)["JobRun"]
            activated_on = min(activated_on, event_run.get("StartedOn", now))
        try:
            state.put_item(
                Item={"pk": "ACTIVATION", "activated_on": activated_on.isoformat()},
                ConditionExpression="attribute_not_exists(pk)",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            activated_on = datetime.fromisoformat(
                state.get_item(Key={"pk": "ACTIVATION"})["Item"]["activated_on"]
            )
    cutoff = max(now - timedelta(days=7), activated_on)
    runs: list[dict] = []
    next_token = None
    reached_cutoff = False
    while True:
        params = {"JobName": JOB_NAME, "MaxResults": 100}
        if next_token:
            params["NextToken"] = next_token
        response = glue.get_job_runs(**params)
        for run in response.get("JobRuns", []):
            started = run.get("StartedOn")
            if started and started < cutoff:
                reached_cutoff = True
                break
            if retry_run_id and run.get("Id") != retry_run_id:
                continue
            if started and run.get("JobRunState") in {
                "SUCCEEDED",
                "FAILED",
                "TIMEOUT",
                "STOPPED",
            }:
                runs.append(run)
        if reached_cutoff:
            break
        next_token = response.get("NextToken")
        if not next_token:
            break

    run_ids = {run["Id"] for run in runs}
    processed_ids: set[str] = set()
    run_keys = [{"pk": f"RUN#{run_id}"} for run_id in run_ids]
    for offset in range(0, len(run_keys), 100):
        keys = run_keys[offset : offset + 100]
        response = dynamodb.batch_get_item(
            RequestItems={
                TABLE_NAME: {
                    "Keys": keys,
                    "ConsistentRead": True,
                    "ProjectionExpression": "pk",
                }
            }
        )
        processed_ids.update(
            item["pk"].removeprefix("RUN#")
            for item in response.get("Responses", {}).get(TABLE_NAME, [])
        )
        while response.get("UnprocessedKeys", {}).get(TABLE_NAME, {}).get("Keys"):
            response = dynamodb.batch_get_item(
                RequestItems=response["UnprocessedKeys"]
            )
            processed_ids.update(
                item["pk"].removeprefix("RUN#")
                for item in response.get("Responses", {}).get(TABLE_NAME, [])
            )
    pending = [run for run in runs if run["Id"] not in processed_ids]
    return sorted(pending, key=lambda item: item["StartedOn"])


def _process_run(run: dict) -> None:
    run_id = run["Id"]
    run_state = run["JobRunState"]
    report: dict = {
        "job_name": JOB_NAME,
        "job_run_id": run_id,
        "state": run_state,
        "started_on": _iso(run.get("StartedOn")),
        "completed_on": _iso(run.get("CompletedOn")),
    }
    baseline = state.get_item(Key={"pk": "BASELINE"}).get("Item", {})
    if run_state == "SUCCEEDED":
        try:
            report.update(_quarantine_counts(run, baseline))
            report["report_status"] = "complete"
        except Exception as exc:  # Mark missing counts explicitly in the report.
            LOGGER.exception(
                "Quarantine counts unavailable for Glue JobRunId %s", run_id
            )
            report.update(
                {
                    "report_status": "incomplete",
                    "counts_unavailable_reason": str(exc)[:500],
                }
            )
    else:
        error = run.get("ErrorMessage") or run.get("ExecutionTime")
        report.update(
            {
                "report_status": "glue_failed",
                "counts_unavailable_reason": str(
                    error or "Glue job did not succeed"
                )[:500],
            }
        )

    report_key = f"{REPORT_PREFIX}{run_id}/report.json"
    s3 = boto3.client("s3", region_name=REGION)
    bucket, key = _split_s3(REPORT_PREFIX)
    s3.put_object(
        Bucket=bucket,
        Key=f"{key}{run_id}/report.json",
        Body=json.dumps(report, default=_json_default, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=f"Quarentena Glue {run_state}: {run_id}"[:100],
        Message=_email_body(report),
    )
    state.put_item(
        Item={
            "pk": f"RUN#{run_id}",
            "state": run_state,
            "report_key": report_key,
            "expires_at": int(time.time()) + 14 * 24 * 60 * 60,
        }
    )
    if run_state == "SUCCEEDED" and report.get("report_status") == "complete":
        state.put_item(
            Item={
                "pk": "BASELINE",
                "job_run_id": run_id,
                "bronze_snapshot_id": report["bronze_snapshot_id"],
                "rejected_snapshot_id": report["rejected_snapshot_id"],
                "completed_on": report["completed_on"],
            }
        )


def _quarantine_counts(run: dict, baseline: dict) -> dict:
    completed = run.get("CompletedOn")
    if not completed:
        raise ValueError("Glue run has no CompletedOn timestamp")
    timestamp = completed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    snapshot_sql = f"""
        SELECT
            'bronze' AS table_name,
            max_by(snapshot_id, committed_at) AS snapshot_id
        FROM \"{DATABASE}\".\"bronze_payments$snapshots\"
        WHERE committed_at <= TIMESTAMP '{timestamp}'

        UNION ALL

        SELECT
            'rejected' AS table_name,
            max_by(snapshot_id, committed_at) AS snapshot_id
        FROM \"{DATABASE}\".\"silver_payments_rejected$snapshots\"
        WHERE committed_at <= TIMESTAMP '{timestamp}'
    """
    snapshot_rows, snapshot_query_id = _query(snapshot_sql)
    snapshots = {row[0]: row[1] for row in snapshot_rows if len(row) > 1}
    bronze_snapshot = snapshots.get("bronze")
    rejected_snapshot = snapshots.get("rejected")
    if not bronze_snapshot or not rejected_snapshot:
        raise ValueError(
            "No Iceberg snapshot exists for bronze or quarantine at run completion"
        )

    rejected = (
        f'"{DATABASE}"."silver_payments_rejected" '
        f"FOR VERSION AS OF {int(rejected_snapshot)}"
    )
    bronze = f'"{DATABASE}"."bronze_payments" FOR VERSION AS OF {int(bronze_snapshot)}'
    previous_bronze = baseline.get("bronze_snapshot_id")
    if previous_bronze:
        prior = (
            f'"{DATABASE}"."bronze_payments" '
            f"FOR VERSION AS OF {int(previous_bronze)}"
        )
        new_files = f"""
            new_files AS (
                SELECT DISTINCT source_file
                FROM {bronze}
                WHERE source_file IS NOT NULL

                EXCEPT

                SELECT DISTINCT source_file
                FROM {prior}
                WHERE source_file IS NOT NULL
        )"""
    else:
        new_files = (
            "new_files AS (SELECT DISTINCT source_file "
            f"FROM {bronze} WHERE source_file IS NOT NULL)"
        )

    counts_sql = f"""
            WITH {new_files},
            rejected AS (
                SELECT source_file, rejection_reasons
                FROM {rejected}
            ),
            summary AS (
                SELECT
                    'total_records' AS metric,
                    count(*) AS metric_count
                FROM rejected

                UNION ALL

                SELECT
                    'new_records',
                    count(*)
                FROM rejected AS r
                JOIN new_files AS n ON r.source_file = n.source_file
            ),
            reasons AS (
                SELECT
                    'total' AS scope,
                    reason,
                    count(*) AS reason_count
                FROM rejected
                CROSS JOIN UNNEST(rejection_reasons) AS t(reason)
                GROUP BY reason

                UNION ALL

                SELECT
                    'new' AS scope,
                    reason,
                    count(*) AS reason_count
                FROM rejected AS r
                JOIN new_files AS n ON r.source_file = n.source_file
                CROSS JOIN UNNEST(r.rejection_reasons) AS t(reason)
                GROUP BY reason
            )
            SELECT
                'summary' AS row_type,
                metric AS name,
                metric_count AS value
            FROM summary

            UNION ALL

            SELECT
                'reason',
                concat(scope, ':', reason),
                reason_count
            FROM reasons
        """
    rows, count_query_id = _query(counts_sql)
    totals = {row[1]: int(row[2]) for row in rows if row[0] == "summary"}
    reasons = {"total": {}, "new": {}}
    for row in rows:
        if row[0] == "reason":
            scope, reason = row[1].split(":", 1)
            reasons[scope][reason] = int(row[2])
    return {
        "total_quarantined": totals.get("total_records", 0),
        "new_rejections": totals.get("new_records", 0),
        "reason_counts": reasons,
        "bronze_snapshot_id": str(bronze_snapshot),
        "rejected_snapshot_id": str(rejected_snapshot),
        "previous_successful_job_run_id": baseline.get("job_run_id"),
        "recovered_after_failed_run": _failed_run_between(baseline, run),
        "athena_query_ids": [snapshot_query_id, count_query_id],
    }


def _failed_run_between(baseline: dict, run: dict) -> bool:
    previous_completion = baseline.get("completed_on")
    current_start = run.get("StartedOn")
    if not previous_completion or not current_start:
        return False
    if isinstance(previous_completion, str):
        previous_completion = datetime.fromisoformat(previous_completion)
    next_token = None
    while True:
        params = {"JobName": JOB_NAME, "MaxResults": 100}
        if next_token:
            params["NextToken"] = next_token
        response = glue.get_job_runs(**params)
        for item in response.get("JobRuns", []):
            completed = item.get("CompletedOn")
            if (
                item.get("JobRunState") in {"FAILED", "TIMEOUT", "STOPPED"}
                and completed
                and previous_completion < completed < current_start
            ):
                return True
            if item.get("StartedOn") and item["StartedOn"] < previous_completion:
                return False
        next_token = response.get("NextToken")
        if not next_token:
            return False


def _query(sql: str) -> tuple[list[list[str | None]], str]:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": RESULTS},
        WorkGroup=WORKGROUP,
    )
    query_id = response["QueryExecutionId"]
    deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        query = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        query_state = query["Status"]["State"]
        if query_state == "SUCCEEDED":
            result = athena.get_query_results(QueryExecutionId=query_id)
            rows = [
                [cell.get("VarCharValue") for cell in row.get("Data", [])]
                for row in result.get("ResultSet", {}).get("Rows", [])[1:]
            ]
            return rows, query_id
        if query_state in {"FAILED", "CANCELLED"}:
            raise RuntimeError(
                query["Status"].get("StateChangeReason", "Athena query failed")
            )
        time.sleep(2)
    athena.stop_query_execution(QueryExecutionId=query_id)
    raise TimeoutError(f"Athena query timed out: {query_id}")


def _email_body(report: dict) -> str:
    lines = [
        f"Pipeline: {report['job_name']}",
        f"Execução: {report['job_run_id']}",
        f"Estado: {report['state']}",
        f"Início: {report.get('started_on') or 'indisponível'}",
        f"Fim: {report.get('completed_on') or 'indisponível'}",
    ]
    if report.get("report_status") != "complete":
        lines.extend(
            [
                f"Relatório: incompleto ({report.get('report_status')})",
                "Contagens: indisponíveis",
                f"Detalhe: {report.get('counts_unavailable_reason', 'não informado')}",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "",
            f"Total em quarentena: {report['total_quarantined']}",
            f"Novas rejeições: {report['new_rejections']}",
            "Baseline: "
            + str(
                report.get("previous_successful_job_run_id")
                or "primeiro relatório; histórico completo contado como novo"
            ),
        ]
    )
    if report.get("recovered_after_failed_run"):
        lines.append(
            "Novas rejeições incluem arquivos recebidos durante execução(ões) "
            "Glue sem sucesso."
        )
    lines.extend(["", "Motivo                         Total   Novas"])
    total = report["reason_counts"]["total"]
    new = report["reason_counts"]["new"]
    for reason in sorted(set(total) | set(new)):
        lines.append(f"{reason:<30} {total.get(reason, 0):>5} {new.get(reason, 0):>7}")
    lines.append(
        "\nUm registro pode apresentar mais de um motivo; "
        "a soma pode superar o total."
    )
    return "\n".join(lines)


def _split_s3(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError("Report prefix must be an s3:// URI")
    bucket, _, key = uri[5:].partition("/")
    return bucket, key


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def _json_default(value: object) -> str:
    if isinstance(value, (datetime, Decimal)):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")
