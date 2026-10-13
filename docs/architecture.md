# Payment Data Platform — Kinesis → Firehose → S3 → Glue batch

This is a **payments data platform**, not a real-time stream-processing system. Kinesis is one ingest source. Firehose writes bronze files so Glue can process them in batch.

## Architecture

```text
Payment Gateway Producer (simulator / FastAPI)
        │  PutRecords (partition key = merchant_id)
        ▼
Amazon Kinesis Data Streams  (payments source)
        │  Firehose reads the stream
        ▼
Amazon Kinesis Data Firehose
        │  GZIP JSON, buffered (64 MiB / 60s)
        ▼
S3 Bronze  (raw landing)
  payments/raw/year=YYYY/month=MM/day=DD/hour=HH/   ← delivery time
  payments/errors/...
        │  Glue crawler (on-demand, incremental)
        ▼
Glue Data Catalog  (bronze_raw)
        │
Glue Schema Registry  (JSON, BACKWARD_ALL)
        │  latest PaymentEvent schema → bronze Spark read
        ▼
Glue 6.0 SDP job  (pipelines/sdp/payments)
        │
        ├─ bronze_payments
        ├─ silver_payments / silver_payments_rejected
        ├─ fact_payments / fact_payments_rejected     ← gold
        └─ dim_date / dim_country / dim_status ← shared dimensions
        │
        ▼
Athena workgroup  (results on gold/athena-results/)
```

## Design choices

| Decision | Rationale |
|---|---|
| Kinesis Data Streams | Payments source (not a stream processor) |
| Firehose → S3 | Durable bronze files sized for Glue batch (avoids tiny objects) |
| Glue Schema Registry | Bronze reads the latest JSON contract instead of a hardcoded Spark schema |
| `PaymentEvent` allows extras | Simulator (and future producers) can land optional fields; unknown keys are not dropped |
| Glue 6.0 SDP + Athena | Spark Declarative Pipeline (bronze→silver→gold); Athena for SQL |
| Partition key = `merchant_id` | Routes a merchant to the same shard; PutRecords retries can reorder |
| Line-delimited JSON + `schema_version` | Glue / Athena friendly; `3` marks the current monetary contract |
| Glue crawler on `payments/raw/` only | Catalogs bronze Hive partitions; excludes `payments/errors/` |
| Incremental crawler (`CRAWL_NEW_FOLDERS_ONLY`) | First run is full; later runs add new hour prefixes only |
| Silver/gold as Apache Iceberg v2 | ACID commits Athena can query; Glue 6.0 can write v3, but Athena cannot read it yet |
| Silver partitioned by event time | Firehose hour is delivery time; analytics need `event_timestamp` |
| Quarantine rejected rows | Quality without a separate DQ product |
| Gold daily grain | `fact_payments` aggregates silver by day / merchant / status / country / currency |
| GMV = captured only | Authorized/failed/refunded are not revenue; gold still keeps authorized and refunded in `fact_payments` |
| Terraform IaC (interactive Docker workspace) | Review `terraform plan -out=tfplan` then `apply tfplan` |

AWS resource names still use the `rtmsp-*` prefix so existing stacks are not recreated.

## Payment event contract

Core fields on `PaymentEvent` (`src/payments_gateway/models.py`):

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | Dedup key |
| `merchant_id` | string | Partition + join key |
| `user_id` | string | Payer identifier |
| `amount` | Decimal `> 0`, up to 18 digits / 2 decimal places | Value in `currency`; JSON decimal string, no FX conversion |
| `currency` | string | ISO-4217, default `USD` |
| `status` | enum | `authorized` / `captured` / `failed` / `refunded` |
| `event_timestamp` | datetime (UTC) | Event time |
| `schema_version` | int | `3` = current amount contract; historical `1` / `2` remain readable |
| `ingest_source` | string | Always `payment_gateway` |

The model uses `extra="allow"`. The simulator may attach any of these extras (~40% of events): `country`, `payment_method`, `channel`, `card_brand`, `fx_rate`.

New events use schema version 3 and publish `amount` as a decimal string in the
transaction currency. The producer/API enforce positive Decimal values with up
to 18 digits and 2 fractional digits; inputs with greater fractional precision
are rejected. Numeric API input is accepted, but strings preserve exact input
precision. This two-decimal scale is a portfolio convention for all currencies.
The producer rejects the legacy `amount_usd` field. Historical numeric amounts
are interpreted in their recorded currency, without FX conversion or inference
of real-world historical units; Spark casts round them to two decimal places.

The Registry declares both monetary fields optional. Legacy `amount_usd` retains
its numeric type; `amount` is unconstrained because historical JSON schemas are
open to arbitrary additional properties. Constraining a newly declared property
can violate `BACKWARD_ALL`. The bronze adapter maps it to Spark `StringType`;
producer and silver apply strict monetary rules. This preserves old payloads
without disabling compatibility. AWS registration compatibility is not verified
by local tests.

Silver chooses non-null `amount` before `amount_usd`. A malformed non-null
`amount` is quarantined even when a valid legacy value exists. Accepted silver
exposes decimal `amount` and `amount_decimal`; only bronze and quarantine retain
the legacy field.

Glue Registry schema ([`infra/terraform/schemas/payment_event.json`](../infra/terraform/schemas/payment_event.json)) is the bronze read contract: core payload fields plus `country` and `source_file`. `schema_version` and `ingest_source` stay on the producer; they are not in the registry schema. Compatibility is `BACKWARD_ALL`.

`country` represents the merchant's country. Silver preserves it from extras; gold substitutes the internal value `unknown` when it is null. Transaction currency remains the explicit `currency` field. Other extras land in bronze when present but are not projected into silver.

## S3 medallion layout

| Layer | Location | Storage |
|---|---|---|
| Bronze | `s3://{bronze}/payments/raw/` (Firehose), `payments/errors/` | S3 Standard |
| SDP artifacts | `s3://{glue}/glue/pipelines/payments/` (code + state) | S3 Standard |
| Iceberg warehouse | `s3://{glue}/iceberg-warehouse/` | S3 Standard |
| Silver tables | Catalog: `bronze_payments`, `silver_payments`, `silver_payments_rejected` | Iceberg v2 |
| Gold tables | Catalog: `fact_payments`, `fact_payments_rejected`, `dim_date`, `dim_country`, `dim_status` | Iceberg v2 |
| Athena results | `s3://{gold}/athena-results/` | S3 Standard |

Bronze Athena DDL: [`sql/athena/bronze/payment_events_raw.sql`](../sql/athena/bronze/payment_events_raw.sql) (Hive/JSON + partition projection). Iceberg tables are registered by the SDP job. Substitute bucket names from Terraform output.

The bronze crawler targets `s3://{bronze}/payments/raw/` (table level 3). Schema policy is `LOG`. The crawler infers JSON types (`event_timestamp` is typically a string); the Registry schema plus `PaymentEvent` remain the contract. Do not crawl silver or gold: Iceberg metadata is the catalog. If Hive/Parquet tables already exist under the same names, drop them before the SDP job runs.

Terraform: registry + schema in [`infra/terraform/registry.tf`](../infra/terraform/registry.tf) and [`infra/terraform/schema.tf`](../infra/terraform/schema.tf). Outputs include `glue_schema_registry` and `glue_events_schema`.

## SDP transforms

Implemented in [`pipelines/sdp/payments/`](../pipelines/sdp/payments/).

**`bronze_payments`** — Firehose JSON read with the latest Registry schema, fetched and mapped to a Spark schema inside `ingest_bronze`, plus `source_file`. Streaming table; `write.spark.accept-any-schema` is on so later registry versions can land.

The reader explicitly projects `event_id`, `merchant_id`, `user_id`,
`amount_usd`, `currency`, `status`, `event_timestamp`, `source_file`, `country`,
`amount`, matching the evolved Iceberg table where `amount` was appended last.
This write order is independent of Registry property order; both monetary
properties must be present in the read schema. Deploy the existing Terraform
`pipeline_bronze` object and rerun SDP to apply the ordering fix. It changes no
types and requires no table deletion, checkpoint reset, or new permissions.
The repository correction has not been verified in an AWS job run.

**`silver_payments`**

- Read `COALESCE(amount, amount_usd)` as a decimal; normalize status/currency; parse timestamp
- Quarantine null, empty, or whitespace-only event/merchant/user IDs and statuses;
  reject statuses outside `authorized`, `captured`, `failed`, and `refunded`
- Convert amounts with `try_cast` to `decimal(18,2)`; reject missing amounts,
  failed conversions (including non-finite values and overflow), and values
  that are non-positive after rounding to two decimal places
- Parse timestamps with `try_to_timestamp`; quarantine missing/blank timestamps
  and invalid parsed values instead of failing under ANSI mode
- Accept normalized currencies `USD`, `EUR`, `BRL`, `GBP`, `JPY`, `CAD`, and `MXN`
  (the supported simulator domain, not all ISO-4217 currencies). Null currency
  still defaults to `USD`; blank or unsupported currency is rejected
- Derive per-event `amount_band` (`<10` micro, `<50` small, `<100` medium, else large)
- Filter valid rows before deduplicating on `(event_id, merchant_id)` by descending
  parsed timestamp; an invalid newer duplicate cannot hide an older valid row
- Keep `country` for gold
- Partitioned by event-time `year` / `month` / `day`

**`silver_payments_rejected`** — rows with one or more `rejection_reasons`.

Reasons are `missing_event_id`, `missing_merchant_id`, `missing_user_id`,
`missing_amount`, `invalid_amount`, `amount_decimal_non_positive`,
`missing_status`, `invalid_status`, `invalid_currency`, `missing_event_timestamp`,
and `invalid_event_timestamp`. A row can carry multiple reasons. The former
`amount_decimal_negative` reason is replaced by `amount_decimal_non_positive`.
Raw fields and `source_file` remain available in quarantine; timestamp parse
failures have null event-time partitions. No UUID validation or additional
business fields are introduced. Equal-timestamp conflicts choose the lexicographically greatest canonical payload, then greatest non-null source file path; this is a deterministic conflict policy, not a business update sequence.

Publish `02_silver.py` through the existing Terraform `pipeline_silver` object
and rerun SDP to refresh silver and dependent gold views. This is repository
configuration, not verified AWS deployment; no new IAM permissions or resources
are required. The SDP quality CI job runs the full current test suite against
local Spark 4.1.1 with ANSI mode enabled, using identity decorators for SDP
registration only. It does not validate the Glue execution graph or AWS state.
The suite also exercises real JSON parsing through the bronze schema adapter and
gold SQL, replacing the obsolete `payments_lake` tests. With project dev
dependencies, Python 3.11, uv and Java 17 available, run:

```bash
uv run --group dev --with pyspark==4.1.1 python -m pytest -q
```

**`fact_payments_temp`** — daily grain from `silver_payments`: `year`, `month`, `day`, `merchant_id`, `status`, `country` (`unknown` if null), `currency`. Metrics: `amount` (sum of `amount_decimal`), `txn_count`, `unique_users`. Aggregated `amount_band`: `<100` micro, `<250` small, `<1000` medium, else large. `full_date` is `year-month-day`.

Gold groups on normalized status/currency and exposes these as `status` and
`currency`. `amount` is in transaction-currency units; sums and comparisons
should retain currency. `unique_users` cannot be summed to obtain an exact
distinct count across groups. The YAML sets the Spark session timezone to UTC.
Terraform renders `01_bronze.py.tftpl` as `01_bronze.py`; its placeholders are
resolved before S3 upload.

**`fact_payments`** — that grain where `status <> 'failed'`. Partitioned by `full_date`, `country`.

**`fact_payments_rejected`** — the same grain where `status = 'failed'`. Partitioned by `full_date`, `country`.

**`dim_date`** — unpartitioned shared calendar dimension with one row for each date from `2020-01-01` through `2030-12-31`. It exposes `date_key`, `full_date`, `calendar_date`, calendar attributes, and month-, quarter-, and year-end flags. `day_of_week` uses Monday=1 through Sunday=7. Current facts join it through `full_date`.

**`dim_country`** — unpartitioned Iceberg v2 materialized view in `04_dimensions.py`. The versioned reference contains BR, CA, DE, GB, JP, MX, US and the internal `unknown` member. Columns: `country_code`, `country_code_alpha3`, `country_name`, `region_name`, `subregion_name`, `is_unknown`. Names and geography follow [UN M49](https://unstats.un.org/unsd/methodology/m49/), reviewed 2026-09-27; BR and MX use subregion `Latin America and the Caribbean`. The unknown member has a null alpha-3 code and `is_unknown = true`. Join both facts on `f.country = c.country_code`; use a left join to retain unmapped codes, and extend `COUNTRY_REFERENCE` for new countries. The reference is packaged in the existing Terraform `pipeline_dimensions` object, with no external lookup, extra dependencies or IAM changes. Apply the code upload and run SDP to create/refresh the dimension in the configured catalog and warehouse; this change is repository configuration, not verified AWS deployment.

Country describes the merchant, but the simulator currently samples it independently of merchant and currency. A stable merchant-country relationship and local-currency consistency are not enforced by the generator. `dim_country` does not derive transaction currency.

**`dim_status`** — unpartitioned Iceberg v2 materialized view in `04_dimensions.py`, with four rows matching the gateway's `PaymentStatus`: `authorized`, `captured`, `failed`, `refunded`. Columns are `status_code`, `status_name`, `status_description`, and `is_revenue`. Only `captured` has `is_revenue = true`; this flag expresses the project's GMV convention. Join either fact on `f.status = s.status_code`. The dimension does not normalize or validate raw statuses; use a left join when preserving unexpected values is necessary. The existing `pipeline_dimensions` Terraform upload packages this view without extra permissions or dependencies. Deploy the file and run SDP to materialize it; AWS deployment is not verified by the repository change.

Reference dimension schemas are declared as module-level `StructType` objects. Do not pass DDL schema strings to `createDataFrame` inside SDP query functions: Spark Connect parses those through remote analysis, which SDP blocks with `ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION`. This correction applies to both `dim_country` and `dim_status`; their columns and reference values are unchanged. Upload the corrected `04_dimensions.py` through Terraform and rerun the existing job.

The view is now named `dim_status` (formerly `dim_payment_status`), with the same columns. Publish `04_dimensions.py` and run SDP to materialize the new name. This repository change does not rename or delete existing AWS tables. A legacy `dim_status` table has a different schema and must be checked before deployment; do not use the old SQL DDL to create the SDP dimension.

Example GMV query using the current daily fact and SDP `dim_status`:

```sql
SELECT f.full_date, f.currency, SUM(f.amount) AS gmv,
       SUM(f.txn_count) AS captured_count
FROM rtmsp_dev_payments.fact_payments f
JOIN rtmsp_dev_payments.dim_status s ON f.status = s.status_code
WHERE s.is_revenue = true
GROUP BY f.full_date, f.currency;
```

GMV queries should still filter `status = 'captured'`. `sql/athena/gold/` star DDL (`fact_payment` + dims) is leftover from an earlier design; SDP gold contains the two `fact_payments*` tables and the shared dimensions above.

## Pipeline write logs

The catalog explicitly selects `org.apache.iceberg.metrics.LoggingMetricsReporter`
through `spark.sql.catalog.${catalog}.metrics-reporter-impl` in the pipeline
YAML. Terraform starts Spark with `spark.log.level=INFO` in the existing Glue
job's `--conf` argument. Iceberg's bundled reporter emits `CommitReport` entries
with table/snapshot/operation and added, removed, and total data-file/record
counters for every persisted layer, including quarantine and dimensions.
Temporary views have no writes. No Spark action is executed inside SDP query
functions for logging, and no library, service, or IAM permission is added.

Counts describe Iceberg file writes, not raw Firehose objects or new business
events. Silver/gold fully recompute, so replacement writes may have positive
added-file/record counts with unchanged business totals. Missing reports do not
establish a zero-write or successful run. The Glue driver log stream identifies
the job run; multiple commits per table are possible. Deploy the YAML and job
configuration before verifying AWS log delivery.

## Later

- Iceberg v3 when Athena can read format-version 3
- SCD Type 2 on `dim_merchant` (derived `failure_tier`) in dbt on Athena
- Optional extra payment-adjacent sources (settlements or chargebacks on S3)
- Remote Terraform state
