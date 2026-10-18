"""Fault injection wire behavior and finite publishing, without AWS access."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from payments_gateway.chaos_simulator import (
    SCENARIOS,
    build_parser,
    generate_raw_event,
    main,
    run_simulator,
)
from payments_gateway.config import Settings
from payments_gateway.models import PaymentEvent
from payments_gateway.producer import PaymentEventProducer


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_faults_bypass_validation_and_preserve_wire_routing(scenario):
    event = generate_raw_event(scenario=scenario, index=1)
    wire = event.to_kinesis_data()
    assert wire.endswith(b"\n")
    payload = json.loads(wire, parse_float=Decimal)
    assert event.partition_key().startswith("mrc_")
    if scenario == "invalid-currency":
        # The gateway checks length; silver enforces the supported currency domain.
        assert payload["currency"] == "ZZZ"
    elif scenario == "blank":
        # Blank identifiers are accepted by the model and rejected in silver.
        assert payload["merchant_id"].strip() == ""
    else:
        with pytest.raises(ValidationError):
            PaymentEvent.model_validate(payload)
    if scenario in {"missing", "null", "wrong-type", "blank", "multiple"}:
        assert payload.get("merchant_id") != event.partition_key()


def test_late_event_is_valid_and_uses_old_event_time():
    event = generate_raw_event(late_seconds=172800)
    parsed = PaymentEvent.model_validate(event.payload)
    age = (datetime.now(timezone.utc) - parsed.event_timestamp).total_seconds()
    assert 172800 <= age < 172805


def test_valid_chaos_event_serializes_amount_as_exact_json_number():
    event = generate_raw_event()
    payload = json.loads(event.to_kinesis_data(), parse_float=Decimal)
    assert isinstance(payload["amount"], Decimal)
    assert payload["amount"] == event.payload["amount"]


def test_dry_run_never_builds_aws_client(monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        raise AssertionError("Dry run must not create an AWS client")

    monkeypatch.setattr(
        "payments_gateway.chaos_simulator.PaymentEventProducer", forbidden
    )
    assert (
        main(["--dry-run", "--count", "11", "--bad-ratio", "1", "--late-ratio", "1"])
        == 0
    )
    output = capsys.readouterr()
    assert len([json.loads(line) for line in output.out.splitlines()]) == 11
    summary = json.loads(output.err)
    assert summary["corrupted"] == summary["backdated"] == 11
    assert all(summary[f"scenario:{name}"] == 1 for name in SCENARIOS)


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--count", "0"),
        ("--rate", "nan"),
        ("--bad-ratio", "1.1"),
        ("--late-ratio", "-1"),
        ("--late-seconds", "inf"),
        ("--late-seconds", "-1"),
    ],
)
def test_invalid_options(flag, value):
    with pytest.raises(SystemExit) as exc:
        main(["--dry-run", flag, value])
    assert exc.value.code == 2


def test_failed_records_stop_at_count_and_retry_only_pending(monkeypatch, capsys):
    class Client:
        def __init__(self):
            self.calls = []

        def put_records(self, **kwargs):
            self.calls.append(kwargs["Records"])
            return {"Records": [{"ErrorCode": "throttled"} for _ in kwargs["Records"]]}

    client = Client()
    producer = PaymentEventProducer(
        settings=Settings(
            aws_region="us-east-1",
            kinesis_stream_name="test",
            batch_size=2,
            max_retries=1,
        ),
        client=client,
    )
    monkeypatch.setattr(producer, "_sleep_backoff", lambda attempt: None)
    monkeypatch.setattr(
        "payments_gateway.chaos_simulator.time.sleep", lambda seconds: None
    )
    args = build_parser().parse_args(
        ["--count", "3", "--bad-ratio", "1", "--scenario", "missing"]
    )
    assert run_simulator(args, producer) == 1
    assert [len(records) for records in client.calls] == [2, 2, 1, 1]
    assert client.calls[0] == client.calls[1]
    summary = json.loads(capsys.readouterr().err)
    assert summary["generated"] == summary["failed"] == 3
