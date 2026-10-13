"""Current SDP gold queries: normalized groups, currency units and failed facts."""

import importlib.util
from decimal import Decimal
from pathlib import Path

from test_sdp_silver import _load, _row


def test_gold_normalizes_groups_and_preserves_currency(spark, silver):
    _load(
        spark,
        silver,
        [
            _row(
                event_id="old",
                amount=None,
                amount_usd="10.10",
                status=" Captured ",
                currency=" usd ",
            ),
            _row(event_id="new", amount="20.20", status="CAPTURED", currency="USD"),
            _row(event_id="brl", amount="100.00", status="captured", currency="brl"),
            _row(event_id="failed", amount="5.50", status=" FAILED ", currency=" usd "),
            _row(event_id="authorized", amount="2.00", status="authorized"),
            _row(event_id="refunded", amount="3.00", status="refunded"),
        ],
    )
    silver.silver_payments().createOrReplaceTempView("silver_payments")
    path = (
        Path(__file__).resolve().parents[1]
        / "pipelines/sdp/payments/transformations/03_gold.py"
    )
    spec = importlib.util.spec_from_file_location("payments_sdp_gold", path)
    gold = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gold)
    try:
        gold.fact_payments_temp().createOrReplaceTempView("fact_payments_temp")
        accepted = gold.fact_payments().collect()
        rejected = gold.fact_payments_rejected().collect()
        by_key = {(row.status, row.currency): row for row in accepted}
        usd = by_key["captured", "USD"]
        assert usd.amount == Decimal("30.30")
        assert usd.txn_count == 2
        assert usd.unique_users == 1
        assert by_key["captured", "BRL"].amount == Decimal("100.00")
        assert set(by_key) == {
            ("captured", "USD"),
            ("captured", "BRL"),
            ("authorized", "USD"),
            ("refunded", "USD"),
        }
        assert len(rejected) == 1
        assert rejected[0].status == "failed"
        assert rejected[0].currency == "USD"
        assert rejected[0].amount == Decimal("5.50")
        assert sum(row.txn_count for row in accepted + rejected) == 6
    finally:
        spark.catalog.dropTempView("fact_payments_temp")
        spark.catalog.dropTempView("silver_payments")
