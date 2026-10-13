"""Read old and new JSON with the actual Registry-to-Spark schema adapter."""

import importlib.util
import json
from decimal import Decimal
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")


def test_registry_bronze_to_silver_reads_historical_and_new_json(
    spark, silver, monkeypatch, tmp_path
):
    import boto3
    import pyspark
    from pyspark.sql import functions as F

    from payments_gateway.models import PaymentEvent

    root = Path(__file__).resolve().parents[1]
    definition = json.loads(
        (root / "infra/terraform/schemas/payment_event.json").read_text()
    )
    requests = []

    class Registry:
        def get_schema_version(self, **kwargs):
            requests.append(kwargs)
            return {"SchemaDefinition": json.dumps(definition)}

    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: Registry())
    monkeypatch.setattr(
        pyspark,
        "pipelines",
        SimpleNamespace(
            create_streaming_table=lambda *args, **kwargs: None,
            append_flow=lambda **kwargs: lambda query: query,
        ),
    )
    path = root / "pipelines/sdp/payments/transformations/01_bronze.py.tftpl"
    spec = importlib.util.spec_from_file_location(
        "payments_sdp_bronze",
        path,
        loader=SourceFileLoader("payments_sdp_bronze", str(path)),
    )
    bronze = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bronze)
    assert "amount" not in definition.get("required", [])
    assert "amount_usd" not in definition.get("required", [])
    assert "type" not in definition["properties"]["amount"]

    core = dict(
        merchant_id="m",
        user_id="u",
        status="captured",
        currency="BRL",
        event_timestamp="2026-10-12T00:00:00Z",
    )
    payloads = [
        dict(core, event_id="legacy", amount_usd=12.34),
        json.loads(
            PaymentEvent(
                **core, event_id="new", amount="9999999999999999.99"
            ).to_kinesis_data()
        ),
        dict(core, event_id="numeric", amount=23.45),
    ]
    file = tmp_path / "events.json"
    file.write_text("\n".join(json.dumps(row) for row in payloads))
    monkeypatch.setattr(bronze, "BRONZE_PATH", str(tmp_path))
    registry_stream = bronze.ingest_bronze()
    assert "amount" in registry_stream.schema.fieldNames()
    assert "amount_usd" in registry_stream.schema.fieldNames()
    (
        spark.read.schema(registry_stream.schema)
        .json(str(file))
        .drop("source_file")
        .withColumn("source_file", F.input_file_name())
        .createOrReplaceTempView("bronze_payments")
    )
    silver.silver_payments_temp().createOrReplaceTempView("silver_payments_temp")
    rows = silver.silver_payments().collect()
    assert {row.event_id: row.amount for row in rows} == {
        "legacy": Decimal("12.34"),
        "new": Decimal("9999999999999999.99"),
        "numeric": Decimal("23.45"),
    }
    assert silver.silver_payments_rejected().count() == 0
