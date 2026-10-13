# Payment Data Platform

An AWS batch payments lake for a portfolio: a typed gateway publishes to
Kinesis, Firehose persists raw JSON on S3, and Glue 6.0 Spark Declarative
Pipelines build Iceberg tables queried through Athena.

```text
Simulator / FastAPI → Kinesis → Firehose → S3 raw
                                           ↓ Registry-backed schema
                                      Glue 6.0 SDP
                                           ↓
                       bronze → silver + quarantine → daily gold + dimensions
                                           ↓
                                         Athena
```

The system has one payments source. It demonstrates batch analytics rather
than a real-time processing SLA. Resource names retain the `rtmsp-*` prefix.
[Architecture](docs/architecture.md) and [analyst queries](docs/demo-queries.md)
describe the current implementation and operational boundaries. Repository
configuration does not prove successful AWS deployment.

## Layers and monetary contract

| Layer | Output |
|---|---|
| Raw | GZIP NDJSON in bronze `payments/raw/year=/month=/day=/hour=` by delivery time |
| SDP bronze | `bronze_payments`, streaming table with persisted checkpoint |
| Silver | `silver_payments`, validated and deduplicated per `(event_id, merchant_id)` |
| Quality quarantine | `silver_payments_rejected` with all applicable rejection reasons |
| Gold | `fact_payments` and `fact_payments_rejected`, daily merchant/status/country/currency aggregates |
| Dimensions | `dim_date`, `dim_country`, `dim_status` |

New producer/API/simulator events use version 3 and `amount`, a positive Decimal
in the transaction's `currency`, serialized as a JSON string to preserve
precision. The portfolio uses at most 18 digits and 2 decimal places for every
supported currency; this is not a currency-specific minor-unit system. Inputs
with more than two fractional digits are rejected by the producer. No FX
conversion occurs. GMV includes only captured amounts and must retain currency.

Historical version 1/2 raw files with numeric `amount_usd` remain readable.
The Registry includes both fields as optional; silver selects non-null `amount`
first, then the legacy field, and converts to `decimal(18,2)`. For the synthetic
history, legacy values are interpreted in the recorded currency without unit
inference or conversion. Invalid new amounts do not silently fall back.
Accepted silver exposes `amount` and `amount_decimal`; legacy values remain in
bronze/quarantine. Gold aggregates normalized status and currency. Distinct user
counts are not additive across gold groups.

## Prerequisites

- Python 3.10+ and uv for the gateway; Python 3.11 and Java 17 for Spark tests.
- Docker through WSL on Windows for Terraform and local AWS CLI operations.
- Gitignored root `.aws/config` and `.aws/credentials`, plus `.env` copied from
  `.env.example`. Never commit credentials or Terraform state.
- A deploy identity with [the deploy permissions](docs/iam-deploy-policy.json).

The Terraform workspace loads `.env`; its entrypoint writes shared AWS config
inside the container. All local Terraform/AWS CLI commands run there, not on
the host. Deploying requires AWS credentials; local tests do not contact AWS.

## Deploy

```bash
./infra/tf.sh up
./infra/tf.sh shell
```

Inside the Terraform workspace:

```bash
terraform init
terraform plan -out=tfplan
terraform show tfplan
terraform apply tfplan
terraform output
```

Review before applying the saved plan. Useful outputs are `kinesis_stream_name`,
`medallion_buckets`, `glue_payments_sdp_job`, `glue_schema_registry`,
`glue_events_schema`, and `athena_workgroup`. Producer permissions are exposed
through `producer_iam_policy_arn`. Disable alarms with `enable_alarms = false`
when the deploy identity lacks their required permissions.

The code/state and Iceberg warehouse use the **glue** bucket. Athena results use
`gold/athena-results/`. Logical silver/gold layers do not imply physical storage
in those buckets. Terraform renders `01_bronze.py.tftpl` and resolves its
placeholders before upload; the uploaded file is named `01_bronze.py`.
Terraform uploads all four transformations and pipeline YAML.

A normal job run does not replay files already checkpointed.

## Publish demo events

```bash
uv sync --group dev
# Create/edit .env with the stream and AWS settings before publishing
uv run payments-simulator --rate 10 --duration 30
uv run uvicorn payments_gateway.api:app --reload --port 8080
```

Example request:

```bash
curl -X POST http://127.0.0.1:8080/payments/events \
  -H 'Content-Type: application/json' \
  -d '{"merchant_id":"mrc_0001","user_id":"usr_000001","amount":"25.50","currency":"BRL","status":"captured"}'
```

The API no longer accepts an `amount_usd`-only request. New payloads use version
3 by default. Events may carry simulator extras, with unchanged business fields.

After Firehose lands the data (buffer defaults 64 MiB / 60 seconds), run inside
the Terraform workspace:

```bash
# Optional: catalog raw for direct Athena exploration
aws glue start-crawler --name rtmsp-dev-payments-bronze-raw
aws glue start-job-run --job-name rtmsp-dev-payments-sdp
```

The crawler targets only raw bronze; SDP reads S3 using the Registry contract
and manages its own Iceberg catalog entries. Silver and gold materialized views
fully recompute on each run; bronze reads new files using checkpoints. Query
with workgroup `rtmsp-dev-payments` using the current demo SQL, not legacy star
DDL in `sql/athena/gold/`.

## Tests

```bash
uv run pytest -q
uv run ruff check src tests pipelines
uv run black --check src tests pipelines
# Includes Spark tests; requires Java 17 and Python 3.11
uv run --with pyspark==4.1.1 python -m pytest -q
```

Without PySpark, Spark test modules skip. The full suite covers producer/API
serialization, old/new raw JSON, monetary precision, quality quarantine,
deduplication/replay and gold normalization. Local Spark tests replace SDP
registration only; Glue runtime and AWS behavior require deployed validation.
CI runs gateway tests, Ruff, full Spark tests, Black, Terraform fmt and validate.

## Cost and teardown

Kinesis, Firehose, Glue runs, storage and Athena queries incur AWS charges.
When finished with the Terraform workspace, exit and run `./infra/tf.sh down`.
For stack teardown, review a saved destroy plan inside the workspace. Nonempty
S3 buckets may prevent teardown; decide deliberately which raw/history to keep.
Athena DROP of an Iceberg table deletes its table data. Preserve raw S3 files
when rebuilding the analytics tables.

## Later work

Potential extensions include a second source with a reconciliation use case,
data observability, and measured performance tuning. SCD2, remote Terraform
state and Iceberg v3 are outside the current scope. No additional platform is
required to demonstrate the current portfolio pipeline.

## License

[MIT](LICENSE)
