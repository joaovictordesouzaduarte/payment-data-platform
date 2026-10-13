"""Shared real-Spark fixtures; only SDP registration is replaced for local tests."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("payments-silver-dq-tests")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def silver(spark, monkeypatch):
    import pyspark

    def register(**kwargs):
        return lambda query: query

    monkeypatch.setattr(
        pyspark,
        "pipelines",
        SimpleNamespace(temporary_view=register, materialized_view=register),
        raising=False,
    )
    path = (
        Path(__file__).resolve().parents[1]
        / "pipelines/sdp/payments/transformations/02_silver.py"
    )
    spec = importlib.util.spec_from_file_location("payments_sdp_silver", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module
    for view in ("bronze_payments", "silver_payments_temp"):
        spark.catalog.dropTempView(view)
