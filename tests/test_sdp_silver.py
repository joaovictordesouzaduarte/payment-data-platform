"""Execute the deployed silver queries with local Spark and ANSI mode enabled.

Only SDP registration is stubbed; expressions, filters and windows use real Spark.
Run with pytest and PySpark 3.5+ installed (Glue 6.0 uses Spark 4.1.1).
"""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql.types import (  # noqa: E402
    DoubleType,
    StringType,
    StructField,
    StructType,
)

FIELDS = (
    "event_id",
    "merchant_id",
    "user_id",
    "amount",
    "amount_usd",
    "currency",
    "status",
    "event_timestamp",
    "source_file",
    "country",
)
SCHEMA = StructType([StructField(name, StringType(), True) for name in FIELDS])


def _row(**overrides):
    row = dict(
        event_id="event-1",
        merchant_id="merchant-1",
        user_id="user-1",
        amount="22.70",
        amount_usd=None,
        currency="USD",
        status="captured",
        event_timestamp="2026-10-10T00:30:00Z",
        source_file="s3://bronze/payments/raw/test.json.gz",
        country="BR",
    )
    row.update(overrides)
    return row


def _load(spark, silver, rows):
    spark.createDataFrame(rows, SCHEMA).createOrReplaceTempView("bronze_payments")
    silver.silver_payments_temp().createOrReplaceTempView("silver_payments_temp")


def test_invalid_rows_are_quarantined_without_ansi_failures(spark, silver):
    cases = [
        ({field: value}, f"missing_{field}")
        for field in ("event_id", "merchant_id", "user_id")
        for value in (None, "", " \t\n ")
    ] + [
        ({"amount": None}, "missing_amount"),
        ({"amount": "banana"}, "invalid_amount"),
        ({"amount": "1e100"}, "invalid_amount"),
        ({"amount": "NaN"}, "invalid_amount"),
        ({"amount": "Infinity"}, "invalid_amount"),
        ({"amount": "-1"}, "amount_decimal_non_positive"),
        ({"amount": "0"}, "amount_decimal_non_positive"),
        ({"amount": "0.001"}, "amount_decimal_non_positive"),
        ({"status": None}, "missing_status"),
        ({"status": " \t "}, "missing_status"),
        ({"status": "banana"}, "invalid_status"),
        ({"currency": "XYZ"}, "invalid_currency"),
        ({"currency": ""}, "invalid_currency"),
        ({"currency": " \t "}, "invalid_currency"),
        ({"event_timestamp": None}, "missing_event_timestamp"),
        ({"event_timestamp": " "}, "missing_event_timestamp"),
        ({"event_timestamp": "banana"}, "invalid_event_timestamp"),
        ({"event_timestamp": "2026-02-30T00:00:00Z"}, "invalid_event_timestamp"),
    ]
    # Use source_file to identify cases even when the event ID itself is missing.
    rows = [
        _row(**changes, source_file=f"case-{i}") for i, (changes, _) in enumerate(cases)
    ]
    _load(spark, silver, rows)
    rejected = {
        row.source_file: row.rejection_reasons
        for row in silver.silver_payments_rejected().collect()
    }
    assert rejected == {f"case-{i}": [reason] for i, (_, reason) in enumerate(cases)}
    assert silver.silver_payments().count() == 0


def test_valid_domains_normalization_default_and_event_date(spark, silver):
    rows = [
        _row(
            event_id=f"event-{i}",
            currency=f" {currency.lower()} ",
            status=f" {status.upper()} ",
            event_timestamp="2026-10-09T22:30:00-03:00",
        )
        for i, (currency, status) in enumerate(
            (currency, status)
            for currency in silver.VALID_CURRENCIES
            for status in silver.VALID_STATUSES
        )
    ] + [_row(event_id="default-currency", currency=None)]
    _load(spark, silver, rows)
    accepted = silver.silver_payments().collect()
    assert len(accepted) == len(rows)
    assert silver.silver_payments_rejected().count() == 0
    for row in accepted:
        assert row.currency_normalized in silver.VALID_CURRENCIES
        assert row.status_normalized in silver.VALID_STATUSES
        assert str(row.amount_decimal) == "22.70"
        assert (row.year, row.month, row.day) == ("2026", "10", "10")
    default = next(row for row in accepted if row.event_id == "default-currency")
    assert default.currency_normalized == "USD"


def test_invalid_latest_duplicate_does_not_hide_valid_event(spark, silver):
    _load(
        spark,
        silver,
        [
            _row(),
            _row(status="banana", event_timestamp="2026-10-10T01:00:00Z"),
        ],
    )
    accepted = silver.silver_payments().collect()
    assert len(accepted) == 1
    assert accepted[0].status == "captured"
    rejected = silver.silver_payments_rejected().collect()
    assert len(rejected) == 1
    assert rejected[0].rejection_reasons == ["invalid_status"]


def test_latest_valid_event_and_merchant_key_survive_replay(spark, silver):
    rows = [
        _row(amount="10"),
        _row(amount="99", event_timestamp="2026-10-10T01:00:00Z"),
        _row(merchant_id="merchant-2", amount="50"),
    ]
    _load(spark, silver, rows + rows)
    accepted = silver.silver_payments().collect()
    assert {row.merchant_id: str(row.amount_decimal) for row in accepted} == {
        "merchant-1": "99.00",
        "merchant-2": "50.00",
    }
    assert len(accepted) == 2
    assert silver.silver_payments_rejected().count() == 0


def test_all_rejection_reasons_are_preserved(spark, silver):
    _load(spark, silver, [_row(user_id="", amount="0", status="banana")])
    rejected = silver.silver_payments_rejected().collect()
    assert len(rejected) == 1
    assert set(rejected[0].rejection_reasons) == {
        "missing_user_id",
        "amount_decimal_non_positive",
        "invalid_status",
    }


def test_registry_numeric_amounts_reject_non_finite_and_overflow(spark, silver):
    schema = StructType(
        [
            StructField(name, DoubleType() if name == "amount" else StringType())
            for name in FIELDS
        ]
    )
    amounts = [22.7, float("nan"), float("inf"), float("-inf"), 1e100]
    rows = [
        _row(event_id=f"event-{i}", amount=value) for i, value in enumerate(amounts)
    ]
    spark.createDataFrame(rows, schema).createOrReplaceTempView("bronze_payments")
    silver.silver_payments_temp().createOrReplaceTempView("silver_payments_temp")
    accepted = silver.silver_payments().collect()
    assert len(accepted) == 1
    assert str(accepted[0].amount_decimal) == "22.70"
    rejected = silver.silver_payments_rejected().collect()
    assert len(rejected) == 4
    assert all(row.rejection_reasons == ["invalid_amount"] for row in rejected)


def test_new_and_legacy_amount_precedence_and_quarantine(spark, silver):
    _load(
        spark,
        silver,
        [
            _row(event_id="legacy", amount=None, amount_usd="12.34"),
            _row(event_id="new", amount="23.45"),
            _row(event_id="both", amount="34.56", amount_usd="99.99"),
            _row(event_id="bad-new", amount="banana", amount_usd="99.99"),
            _row(event_id="missing", amount=None, amount_usd=None),
        ],
    )
    accepted = silver.silver_payments().collect()
    assert {row.event_id: str(row.amount) for row in accepted} == {
        "legacy": "12.34",
        "new": "23.45",
        "both": "34.56",
    }
    assert "amount_usd" not in accepted[0].asDict()
    rejected = {
        row.event_id: row for row in silver.silver_payments_rejected().collect()
    }
    assert rejected["bad-new"].rejection_reasons == ["invalid_amount"]
    assert rejected["bad-new"].amount_usd == "99.99"
    assert rejected["missing"].rejection_reasons == ["missing_amount"]


def test_equal_timestamp_conflict_is_deterministic(spark, silver):
    rows = [_row(user_id="user-a", amount="10"), _row(user_id="user-z", amount="99")]
    results = []
    for order in (rows, list(reversed(rows)), rows + rows):
        _load(spark, silver, order)
        results.append(silver.silver_payments().collect()[0].asDict())
    assert results[0] == results[1] == results[2]
    assert results[0]["user_id"] == "user-z"
