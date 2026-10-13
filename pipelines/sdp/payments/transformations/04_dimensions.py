"""SDP shared date, merchant-country and payment-status dimensions."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

spark = SparkSession.active()

CALENDAR_START = "2020-01-01"
CALENDAR_END = "2030-12-31"

PAYMENT_STATUS_REFERENCE = (
    ("authorized", "Authorized", "Payment authorized; excluded from GMV.", False),
    ("captured", "Captured", "Payment captured; included in GMV.", True),
    ("failed", "Failed", "Payment failed; excluded from GMV.", False),
    ("refunded", "Refunded", "Payment refunded; excluded from GMV.", False),
)

# DDL strings trigger remote analysis, which SDP query functions prohibit.
PAYMENT_STATUS_SCHEMA = StructType(
    [
        StructField("status_code", StringType(), True),
        StructField("status_name", StringType(), True),
        StructField("status_description", StringType(), True),
        StructField("is_revenue", BooleanType(), True),
    ]
)


@dp.materialized_view(
    comment="Payment statuses; only captured payments contribute to GMV",
    table_properties={"format-version": "2"},
)
def dim_status() -> DataFrame:
    return spark.createDataFrame(
        PAYMENT_STATUS_REFERENCE,
        schema=PAYMENT_STATUS_SCHEMA,
    )


COUNTRY_REFERENCE = (
    ("BR", "BRA", "Brazil", "Americas", "Latin America and the Caribbean", False),
    ("CA", "CAN", "Canada", "Americas", "Northern America", False),
    ("DE", "DEU", "Germany", "Europe", "Western Europe", False),
    (
        "GB",
        "GBR",
        "United Kingdom of Great Britain and Northern Ireland",
        "Europe",
        "Northern Europe",
        False,
    ),
    ("JP", "JPN", "Japan", "Asia", "Eastern Asia", False),
    ("MX", "MEX", "Mexico", "Americas", "Latin America and the Caribbean", False),
    ("US", "USA", "United States of America", "Americas", "Northern America", False),
    ("unknown", None, "Unknown", "Unknown", "Unknown", True),
)

COUNTRY_SCHEMA = StructType(
    [
        StructField("country_code", StringType(), True),
        StructField("country_code_alpha3", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("subregion_name", StringType(), True),
        StructField("is_unknown", BooleanType(), True),
    ]
)


@dp.materialized_view(
    comment="Merchant-country reference for simulator countries and unknown",
    table_properties={"format-version": "2"},
)
def dim_country() -> DataFrame:
    return spark.createDataFrame(
        COUNTRY_REFERENCE,
        schema=COUNTRY_SCHEMA,
    )


@dp.materialized_view(
    comment="Calendar dimension from 2020-01-01 through 2030-12-31",
)
def dim_date() -> DataFrame:
    calendar_dates = spark.range(1).select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(CALENDAR_START)),
                F.to_date(F.lit(CALENDAR_END)),
            )
        ).alias("calendar_date")
    )

    day_of_week = ((F.dayofweek("calendar_date") + 5) % 7) + 1
    return calendar_dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.date_format("calendar_date", "yyyy-MM-dd").alias("full_date"),
        "calendar_date",
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.date_format("calendar_date", "yyyy-MM").alias("year_month"),
        F.weekofyear("calendar_date").alias("week_of_year"),
        F.dayofmonth("calendar_date").alias("day_of_month"),
        day_of_week.cast("int").alias("day_of_week"),
        F.date_format("calendar_date", "EEEE").alias("day_name"),
        F.dayofweek("calendar_date").isin(1, 7).alias("is_weekend"),
        (F.col("calendar_date") == F.last_day("calendar_date")).alias("is_month_end"),
        (
            F.month("calendar_date").isin(3, 6, 9, 12)
            & (F.col("calendar_date") == F.last_day("calendar_date"))
        ).alias("is_quarter_end"),
        (F.date_format("calendar_date", "MM-dd") == F.lit("12-31")).alias(
            "is_year_end"
        ),
    )
