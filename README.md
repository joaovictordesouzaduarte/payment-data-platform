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

Generated producer/API/simulator events use version 3 and `amount`, a positive
Decimal in the transaction's `currency`, serialized as an exact JSON number to
match the Registry contract and preserve its decimal digits on the wire. The
portfolio uses at most 18 digits and 2 decimal places for every supported
currency; this is not a currency-specific minor-unit system. Inputs
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
- A deploy identity with the [core policy and enabled-feature complements](docs/iam-deploy.md).
  Replace `ACCOUNT_ID` in the policy templates before attaching them.

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
through `producer_iam_policy_arn` for a separately credentialed producer. The
core deployment policy also allows batched `PutRecords` to the configured
payments stream used by the gateway's root `.env`. Attach the alarms complement
when `enable_alarms = true`, and the quarantine
complement when `enable_quarantine_notifications = true`. Keep complements
until Terraform has removed their resources when disabling a feature.

The code/state and Iceberg warehouse use the **glue** bucket. Athena results use
`gold/athena-results/`. Logical silver/gold layers do not imply physical storage
in those buckets. Terraform renders `01_bronze.py.tftpl` and resolves its
placeholders before upload; the uploaded file is named `01_bronze.py`.
Terraform uploads all four transformations and pipeline YAML.

Quarantine reports can be enabled separately from CloudWatch alarms in
`infra/terraform/terraform.tfvars`:

```hcl
enable_quarantine_notifications = true
quarantine_notification_emails  = ["data-ops@example.com"]
```

Each recipient must confirm the SNS subscription email. After each Glue
terminal run, the report includes total quarantine rows and new rejected rows
by `source_file`, with counts for each rejection reason. The initial report
counts existing history as new. See [docs/architecture.md](docs/architecture.md)
for reconciliation, snapshots, cost, and recovery details.

A normal job run does not replay files already checkpointed.

## Generate demo events

```bash
uv sync --group dev
# Create/edit .env with the stream and AWS settings before publishing
uv run payments-simulator --rate 10 --duration 30
PYTHONPATH=src uv run --no-sync uvicorn payments_gateway.api:app --reload --reload-dir src/payments_gateway --port 8080
```

The API generates payment events; it does not accept arbitrary payment payloads.
Send generation settings to `/payments/events/generate`. The defaults generate
25 valid events:

```bash
curl -X POST http://127.0.0.1:8080/payments/events/generate \
  -H 'Content-Type: application/json' \
  -d '{"count":100}'
```

To send a mix of invalid and late-arriving events, configure independent ratios.
They are probabilities, so actual counts can vary around the requested
proportions.
`scenario` selects the fault applied to generated invalid events; when omitted,
the generator cycles through its fault scenarios.

```bash
curl -X POST http://127.0.0.1:8080/payments/events/generate \
  -H 'Content-Type: application/json' \
  -d '{"count":100,"bad_ratio":0.4,"late_ratio":0.3,"late_seconds":172800,"scenario":"invalid-amount"}'
```

The request body contains generation controls, not a payment event. The response
includes attempted, succeeded, failed, corrupted and backdated counts, plus up
to five sample payloads. New valid events use version 3 and may carry simulator
extras. The opt-in `payments-chaos-simulator` CLI remains available for larger
or paced demos and for local NDJSON dry runs.

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

## Fault injection and late arrivals

`payments-chaos-simulator` publishes raw NDJSON to the existing Kinesis stream,
using the same `.env`, batching and retry settings as the normal simulator.
It deliberately bypasses `PaymentEvent` validation after creating a valid base
event. The API validates generation parameters, then mutates selected generated
events according to the requested chaos settings.

```bash
# Preview locally: no credentials or AWS calls; stdout is NDJSON, stderr is a summary.
uv run payments-chaos-simulator --dry-run --count 100 --bad-ratio 0.4 --late-ratio 0.3 --late-seconds 172800

# Publish 500 events at 10 events/s: approximately 40% bad, 30% two days late.
uv run payments-chaos-simulator --count 500 --rate 10 --bad-ratio 0.4 --late-ratio 0.3 --late-seconds 172800

# Only valid late arrivals, all dated seven days before sending.
uv run payments-chaos-simulator --count 100 --bad-ratio 0 --late-ratio 1 --late-seconds 604800

# Target one quality fault in every event.
uv run payments-chaos-simulator --count 100 --bad-ratio 1 --scenario missing
```

`--bad-ratio` (default 0.3) and `--late-ratio` (default 0.2) are independent
probabilities, not exact quotas. Both accept 0..1. `--late-seconds` defaults to
86400 and accepts positive delays up to 365 days. It backdates event time;
it does not sleep for that delay. `--rate` controls sending speed; dry-run skips
pacing. `--count` counts generated attempts even when delivery fails. Exhausted
record retries produce a nonzero exit code. Summary counters describe generated
faults and backdating, not observed lake rejection or delivery guarantees.

Without `--scenario`, faulty events cycle through `missing`, `null`, `blank`,
`invalid-status`, `invalid-currency`, `invalid-amount`, `non-positive-amount`,
`overflow-amount`, `invalid-timestamp`, `wrong-type`, and `multiple`.
Missing/null/blank/wrong-type cases rotate over event ID, merchant ID, user ID,
amount, status and timestamp. Wrong types use a nested JSON object; multiple
faults combine a missing user, null merchant, invalid amount/status/currency.
Transport routing retains the original merchant key even when the payload's
merchant is broken. All records remain syntactically valid JSON; no new business
fields or Registry versions are introduced.

Late and quality selections can overlap. A corrupted/missing timestamp cannot
retain a usable old event time. Use `--bad-ratio 0` to isolate late handling.
Firehose delivery partitions stay current; valid late events should appear in
historical silver event-day partitions and corresponding gold dates after SDP
runs. There is no additional watermark or lateness rejection in this script.
Malformed values may be coerced or nulled by the bronze reader before silver;
unknown extras alone are allowed by the open contract and are not necessarily
quarantined. Inspect raw S3 and `silver_payments_rejected.rejection_reasons` to
verify outcomes. The script does not deploy resources or run crawler/SDP jobs.
