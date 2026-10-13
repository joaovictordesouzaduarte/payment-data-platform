from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.active()


@dp.temporary_view(comment="Fact payments temporary view")
def fact_payments_temp() -> DataFrame:

    return spark.sql("""
    WITH payments_summary AS (
        select
            CONCAT(year, '-', month, '-', day) as full_date,
            year,
            month,
            day,
            merchant_id,
            status_normalized AS status,
            COALESCE(country, 'unknown') AS country,
            currency_normalized AS currency,
            sum(amount_decimal) as amount,
            count(*) as txn_count,
            count(distinct user_id) as unique_users
        from silver_payments
        GROUP BY year,
            month,
            day,
            merchant_id,
            status_normalized,
            COALESCE(country, 'unknown'),
            currency_normalized
        )
        SELECT p.*, case when p.amount < 100 then 'micro'
            when p.amount < 250 then 'small'
            when p.amount < 1000 then 'medium'
            else 'large' end as amount_band
        FROM payments_summary p
        order by 1 desc

    """)


@dp.materialized_view(
    comment="Fact payments for successful/refunded transactions",
    partition_cols=["full_date", "country"],
)
def fact_payments() -> DataFrame:
    return spark.sql("""
    SELECT * FROM fact_payments_temp
    where status <> 'failed'
    """)


@dp.materialized_view(
    comment="Fact payments for failed transactions",
    partition_cols=["full_date", "country"],
)
def fact_payments_rejected() -> DataFrame:
    return spark.sql("""
        SELECT * FROM fact_payments_temp
        WHERE status = 'failed'
    """)
