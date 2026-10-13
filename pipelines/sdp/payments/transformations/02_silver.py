"""SDP silver views: typed payment events and a data-quality quarantine."""

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.active()

VALID_STATUSES = ("authorized", "captured", "failed", "refunded")
# Supported transaction currencies for this portfolio's payment source.
VALID_CURRENCIES = ("USD", "EUR", "BRL", "GBP", "JPY", "CAD", "MXN")


def _missing_text(name: str) -> Column:
    return F.col(name).isNull() | F.col(name).rlike(r"^\s*$")


RULES = (
    ("missing_event_id", _missing_text("event_id")),
    ("missing_merchant_id", _missing_text("merchant_id")),
    ("missing_user_id", _missing_text("user_id")),
    ("missing_amount", F.col("_amount_input").isNull()),
    (
        "invalid_amount",
        F.col("_amount_input").isNotNull() & F.col("amount_decimal").isNull(),
    ),
    ("amount_decimal_non_positive", F.col("amount_decimal") <= 0),
    ("missing_status", _missing_text("status_normalized")),
    (
        "invalid_status",
        ~_missing_text("status_normalized")
        & ~F.col("status_normalized").isin(*VALID_STATUSES),
    ),
    ("invalid_currency", ~F.col("currency_normalized").isin(*VALID_CURRENCIES)),
    ("missing_event_timestamp", _missing_text("event_timestamp")),
    (
        "invalid_event_timestamp",
        ~_missing_text("event_timestamp") & F.col("event_timestamp_utc").isNull(),
    ),
)


@dp.temporary_view(comment="Validated payment events with rejection reasons")
def silver_payments_temp() -> DataFrame:
    compatible = spark.table("bronze_payments").select(
        "*",
        F.coalesce(
            F.col("amount").cast("string"), F.col("amount_usd").cast("string")
        ).alias("_amount_input"),
    )
    typed = compatible.select(
        "*",
        F.expr("try_cast(_amount_input AS decimal(18,2))").alias("amount_decimal"),
        F.lower(F.trim("status")).alias("status_normalized"),
        F.upper(F.trim(F.coalesce(F.col("currency"), F.lit("USD")))).alias(
            "currency_normalized"
        ),
        F.try_to_timestamp("event_timestamp").alias("event_timestamp_utc"),
    )
    return typed.select(
        "*",
        F.filter(
            F.array(*[F.when(predicate, F.lit(name)) for name, predicate in RULES]),
            lambda value: value.isNotNull(),
        ).alias("rejection_reasons"),
        F.date_format("event_timestamp_utc", "yyyy").alias("year"),
        F.date_format("event_timestamp_utc", "MM").alias("month"),
        F.date_format("event_timestamp_utc", "dd").alias("day"),
    )


@dp.materialized_view(
    comment="Rejected payment events",
    partition_cols=["year", "month", "day"],
)
def silver_payments_rejected() -> DataFrame:
    return spark.table("silver_payments_temp").filter(F.size("rejection_reasons") > 0)


@dp.materialized_view(
    comment="Validated and deduplicated payment events",
    partition_cols=["year", "month", "day"],
)
def silver_payments() -> DataFrame:
    return (
        spark.table("silver_payments_temp")
        .filter(F.size("rejection_reasons") == 0)
        .select(
            "*",
            F.when(F.col("amount_decimal") < 10, "micro")
            .when(F.col("amount_decimal") < 50, "small")
            .when(F.col("amount_decimal") < 100, "medium")
            .otherwise("large")
            .alias("amount_band"),
            F.row_number()
            .over(
                Window.partitionBy(["event_id", "merchant_id"]).orderBy(
                    F.col("event_timestamp_utc").desc(),
                    # Stable conflict policy: highest canonical payload, then path.
                    F.to_json(
                        F.struct(
                            "user_id",
                            "amount_decimal",
                            "currency_normalized",
                            "status_normalized",
                            "currency",
                            "status",
                            "country",
                            "event_timestamp",
                        )
                    ).desc(),
                    F.col("source_file").desc_nulls_last(),
                )
            )
            .alias("_rank"),
        )
        .filter(F.col("_rank") == 1)
        .select(
            "event_id",
            "merchant_id",
            "user_id",
            F.col("amount_decimal").alias("amount"),
            "currency",
            "status",
            "event_timestamp",
            "source_file",
            "amount_decimal",
            "status_normalized",
            "currency_normalized",
            "event_timestamp_utc",
            "rejection_reasons",
            "amount_band",
            "year",
            "month",
            "day",
            "country",
        )
    )
