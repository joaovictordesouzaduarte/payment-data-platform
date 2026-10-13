"""Unit tests for PaymentEvent contract."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from payments_gateway.models import PaymentEvent, PaymentStatus


def test_payment_event_defaults_and_partition_key():
    event = PaymentEvent(
        merchant_id="mrc_0001",
        user_id="usr_000001",
        amount=42.5,
        status=PaymentStatus.CAPTURED,
    )

    assert event.event_id
    assert event.currency == "USD"
    assert event.schema_version == 3
    assert event.ingest_source == "payment_gateway"
    assert event.partition_key() == "mrc_0001"
    assert event.event_timestamp.tzinfo is not None


def test_currency_normalized_to_upper():
    event = PaymentEvent(
        merchant_id="mrc_0001",
        user_id="usr_000001",
        amount=10.0,
        currency="usd",
        status=PaymentStatus.AUTHORIZED,
    )
    assert event.currency == "USD"


def test_naive_timestamp_becomes_utc():
    naive = datetime(2026, 7, 14, 12, 0, 0)
    event = PaymentEvent(
        merchant_id="mrc_0001",
        user_id="usr_000001",
        amount=10.0,
        status=PaymentStatus.FAILED,
        event_timestamp=naive,
    )
    assert event.event_timestamp.tzinfo == timezone.utc


def test_amount_must_be_positive():
    with pytest.raises(ValidationError):
        PaymentEvent(
            merchant_id="mrc_0001",
            user_id="usr_000001",
            amount=0,
            status=PaymentStatus.CAPTURED,
        )


def test_to_kinesis_data_is_line_delimited_json():
    event = PaymentEvent(
        event_id="evt-1",
        merchant_id="mrc_0001",
        user_id="usr_000001",
        amount=9.99,
        status=PaymentStatus.CAPTURED,
    )
    raw = event.to_kinesis_data()
    assert raw.endswith(b"\n")
    assert b'"event_id":"evt-1"' in raw or b'"event_id": "evt-1"' in raw
    assert b"mrc_0001" in raw


def test_json_schema_export_contains_required_fields():
    schema = PaymentEvent.json_schema_export()
    props = schema.get("properties", {})
    for field in (
        "event_id",
        "merchant_id",
        "user_id",
        "amount",
        "currency",
        "status",
        "event_timestamp",
        "schema_version",
        "ingest_source",
    ):
        assert field in props


@pytest.mark.parametrize("amount", ["0", "-1", "NaN", "Infinity", "0.001", "1e100"])
def test_invalid_monetary_values_are_rejected(amount):
    with pytest.raises(ValidationError):
        PaymentEvent(merchant_id="m", user_id="u", amount=amount, status="captured")


def test_decimal_wire_precision_and_legacy_field_rejection():
    event = PaymentEvent(
        merchant_id="m",
        user_id="u",
        amount="9999999999999999.99",
        currency="BRL",
        status="captured",
    )
    assert event.amount == Decimal("9999999999999999.99")
    payload = json.loads(event.to_kinesis_data())
    assert payload["amount"] == "9999999999999999.99"
    assert payload["currency"] == "BRL"
    assert payload["schema_version"] == 3
    assert "amount_usd" not in payload
    with pytest.raises(ValidationError, match="not amount_usd"):
        PaymentEvent(**payload, amount_usd=1)
