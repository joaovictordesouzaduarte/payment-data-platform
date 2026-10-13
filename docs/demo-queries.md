# Athena demo queries

Use workgroup `rtmsp-dev-payments` and the SDP Iceberg tables below. Replace the
date with the day represented by your events. The legacy star DDL in
`sql/athena/gold/` does not create the current SDP output.

`amount` is in the transaction currency; preserve currency in aggregations.
GMV means captured amount only. Gold `txn_count` is already aggregated: use
`SUM(txn_count)`, not `COUNT(*)`. Gold `unique_users` cannot be summed for an
exact cross-group distinct count; query silver for that metric.

## Top merchants by GMV within each currency

```sql
SELECT merchant_id, full_date, currency,
       SUM(amount) AS gmv,
       SUM(txn_count) AS captured_count,
       SUM(amount) / NULLIF(SUM(txn_count), 0) AS average_payment_amount
FROM rtmsp_dev_payments.fact_payments
WHERE full_date = '2026-10-12' AND status = 'captured'
GROUP BY merchant_id, full_date, currency
ORDER BY currency, gmv DESC;
```

## Highest failure rate with at least five events

Failed payments are business outcomes in `fact_payments_rejected`, separate
from invalid data in `silver_payments_rejected`. Read both facts for the total.

```sql
WITH all_payments AS (
    SELECT * FROM rtmsp_dev_payments.fact_payments
    UNION ALL
    SELECT * FROM rtmsp_dev_payments.fact_payments_rejected
)
SELECT merchant_id, full_date, currency,
       SUM(txn_count) AS event_count,
       SUM(CASE WHEN status = 'failed' THEN txn_count ELSE 0 END) AS failed_count,
       CAST(SUM(CASE WHEN status = 'failed' THEN txn_count ELSE 0 END) AS DOUBLE)
           / SUM(txn_count) AS failure_rate
FROM all_payments
WHERE full_date = '2026-10-12'
GROUP BY merchant_id, full_date, currency
HAVING SUM(txn_count) >= 5
ORDER BY failure_rate DESC;
```

## Daily captured amounts by merchant country

```sql
SELECT f.full_date, c.country_name, f.currency,
       SUM(f.amount) AS gmv, SUM(f.txn_count) AS captured_count
FROM rtmsp_dev_payments.fact_payments f
LEFT JOIN rtmsp_dev_payments.dim_country c ON f.country = c.country_code
LEFT JOIN rtmsp_dev_payments.dim_status s ON f.status = s.status_code
WHERE f.full_date = '2026-10-12' AND f.status = 'captured'
GROUP BY f.full_date, c.country_name, f.currency
ORDER BY f.currency, gmv DESC;
```

## Exact distinct captured users

```sql
SELECT year, month, day, currency_normalized AS currency,
       COUNT(DISTINCT user_id) AS unique_users
FROM rtmsp_dev_payments.silver_payments
WHERE year = '2026' AND month = '10' AND day = '12'
  AND status_normalized = 'captured'
GROUP BY year, month, day, currency_normalized;
```

## Data quality rejection counts

Invalid timestamps have null event-day partitions, so omit the date filter when
investigating parsing failures. Counts by reason can overlap for the same row.

```sql
SELECT reason, COUNT(*) AS rejected_rows
FROM rtmsp_dev_payments.silver_payments_rejected
CROSS JOIN UNNEST(rejection_reasons) AS t(reason)
GROUP BY reason
ORDER BY rejected_rows DESC;
```
