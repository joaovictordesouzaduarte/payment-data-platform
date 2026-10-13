"""Simulator helpers."""

from payments_gateway.models import PaymentStatus
from payments_gateway.simulator import generate_event


def test_generate_event_has_contract_fields():
    event = generate_event()
    assert event.merchant_id.startswith("mrc_")
    assert event.user_id.startswith("usr_")
    assert event.amount > 0
    assert isinstance(event.status, PaymentStatus)
    assert event.ingest_source == "payment_gateway"
    assert event.schema_version == 3
