# Agent notes

This is a **payments data platform** (batch lake), not a real-time stream processor. A typed gateway publishes events to **Kinesis**. **Firehose** lands durable bronze files on S3. **Glue 6.0 SDP** reads the latest JSON contract from **Glue Schema Registry** and builds bronze, silver, and gold tables for **Athena**.

Resource names use the `rtmsp-*` prefix so existing stacks are not recreated.

**Architecture (source of truth):** [docs/architecture.md](docs/architecture.md). Read that file before changing ingest, storage, transforms, or IaC. Analyst queries: [docs/demo-queries.md](docs/demo-queries.md). IAM for deploy: [docs/iam-deploy-policy.json](docs/iam-deploy-policy.json).

```text
Payment Gateway (simulator / FastAPI)
        │  PutRecords (partition key = merchant_id)
        ▼
Kinesis Data Streams
        │
        ▼
Firehose  →  S3 bronze (GZIP NDJSON, Hive hour partitions)
        │
        ▼
Glue Schema Registry (JSON, BACKWARD_ALL) → bronze read schema
        │
        ▼
Glue 6.0 SDP (bronze→silver→gold)
        ├─ bronze_payments
        ├─ silver_payments / silver_payments_rejected
        ├─ fact_payments / fact_payments_rejected
        └─ dim_date / dim_country / dim_status
        │
        ▼
Athena workgroup
```

Gold is implemented in SDP as daily aggregates (`fact_payments` and `fact_payments_rejected`) plus shared `dim_date`, `dim_country`, and `dim_status` dimensions. The star DDL (`fact_payment` + dims) in `sql/athena/gold/` is a legacy design, not the current SDP output. Do not add another source or migrate to Iceberg v3 unless requested.

## Mandatory infrastructure documentation

Every infrastructure modification must update this `AGENTS.md` in the same change, before the work is considered complete. This includes resource additions, changes, and removals; IAM; storage paths; schemas and catalogs; pipeline deployment; runtime versions; configuration and defaults; monitoring; and deployment or local-container workflows.

Update the affected sections with the resulting behavior, resource names, paths, permissions, commands, and operational requirements. Remove obsolete guidance rather than only appending a change note. Keep `docs/architecture.md` and other affected operational documentation consistent as well. Verify the documentation against the implementation; distinguish repository configuration from verified AWS deployment state, and never record credentials or secrets.

## Requirements

Agents and local work need the following before touching AWS or running the producer.

### Tooling

- Python 3.10+ and [uv](https://docs.astral.sh/uv/)
- Docker (Terraform and AWS CLI run in a container; no host Terraform or AWS CLI)
- WSL is the expected environment for `infra/tf.sh` on Windows

**Stack:** Python 3.10 · FastAPI · Pydantic · uv · Terraform 1.15.8 · Kinesis · Firehose · S3 · Glue 6.0 (Spark 4.1.1) · Apache Iceberg v2 · Athena · CloudWatch · pytest · Ruff · Black

### AWS shared config (required)

The repo expects a gitignored **`.aws/`** directory at the project root with both files:

```text
.aws/config
.aws/credentials
```

These are the standard AWS CLI shared-config files. Never commit them. `.aws/` is already in `.gitignore`.

`.aws/config`:

```ini
[default]
region = us-east-1
output = json
```

`.aws/credentials`:

```ini
[default]
aws_access_key_id = <key>
aws_secret_access_key = <secret>
```

Add `aws_session_token` under `[default]` when using temporary credentials.

Deployment permissions are split into [core](docs/iam-deploy-policy.json), [alarms](docs/iam-deploy-alarms-policy.json) and [quarantine reports](docs/iam-deploy-quarantine-policy.json) customer managed policy templates. Replace `ACCOUNT_ID` with the deployment account ID; templates target `rtmsp-dev` in `us-east-1`. Attach core always, alarms when `enable_alarms = true` (default), and quarantine when `enable_quarantine_notifications = true`; attach all three for the full stack. Keep optional permissions until Terraform has removed their resources. The core policy grants batched `kinesis:PutRecords` only to `rtmsp-dev-payments-events` because the root `.env` credentials are shared by the local gateway and Terraform workspace. The Terraform-created `producer_iam_policy_arn` remains available for a separately credentialed producer. Core does not grant interactive Glue sessions or ad hoc Athena queries.

See [docs/iam-deploy.md](docs/iam-deploy.md) for installation, resource scope and validation limits. S3 object permissions cover only pipeline code/YAML; bucket suffix `????????` matches Terraform's eight-character random suffix and can be replaced by actual bucket names after deployment. `PassRole` is restricted to exact role/service pairs. Only log-group discovery, Kinesis enhanced-monitoring changes and crawler creation require `Resource: "*"`; all are region-restricted, and crawler creation requires the existing Terraform project/environment/managed-by tags. Execution-role policies are unchanged, and deployment IAM administration is not a permissions boundary. Policies were statically reviewed against provider 6.56.0 and AWS authorization metadata, not installed or verified by AWS deployment. Quarantine recipients remain controlled by `quarantine_notification_emails`.

For `AccessDenied` on SNS topics, quarantine Lambda IAM role, or Lambda log group, verify that the alarms/quarantine complements are attached to the deploy identity and each `ACCOUNT_ID` placeholder matches that account before expanding core permissions.

### App environment

Copy `.env.example` to `.env` (also gitignored). The gateway and simulator read it via `payments_gateway.config.Settings`. Required values:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `KINESIS_STREAM_NAME` (default in the example: `rtmsp-dev-payments-events`)

Optional: `AWS_REGION` / `AWS_DEFAULT_REGION` (default `us-east-1`), `KINESIS_BATCH_SIZE`, `KINESIS_MAX_RETRIES`, `LOG_LEVEL`.

The Terraform Compose workspace (`infra/docker-compose.terraform.yml`) loads `.env` and the image entrypoint writes `/root/.aws/config` and `/root/.aws/credentials` inside the container.

### Terraform overrides

If `infra/terraform/terraform.tfvars` is missing, `./infra/tf.sh up` copies it from `terraform.tfvars.example`. Do not commit `*.tfvars` or Terraform state.

## Local AWS debugging

Any debug of AWS services from the **local** environment must use the running container image `rtmsp-terraform:1.15.8` (Compose service `terraform` in `infra/docker-compose.terraform.yml`). Do not call `aws` or `terraform` on the host.

```bash
./infra/tf.sh up
./infra/tf.sh shell
```

Use the AWS CLI and Terraform inside that shell. Credentials come from `.env` (materialized as `/root/.aws/config` and `/root/.aws/credentials` by `infra/docker-entrypoint.sh`). Host `.aws/` is the local shared-config requirement; do not rely on a host-installed AWS CLI.

Inside the shell:

```bash
terraform init
terraform plan -out=tfplan
terraform show tfplan
terraform apply tfplan
```

Apply never re-plans; apply the saved `tfplan`. Then `./infra/tf.sh down` when done.

Useful outputs: `kinesis_stream_name`, `medallion_buckets`, `glue_database_name`, `glue_crawler_name`, `glue_payments_sdp_job`, `glue_schema_registry`, `glue_events_schema`, `athena_workgroup`, `producer_iam_policy_arn`.

## Repository layout

```text
docs/                      Architecture, IAM policy, demo Athena queries
infra/terraform/           S3, Kinesis, Firehose, Glue crawler + Registry + SDP, Athena, IAM, alarms
infra/tf.sh                Docker Terraform workspace (only way to run terraform/aws locally)
pipelines/sdp/payments/    Glue 6.0 Spark Declarative Pipeline (bronze→silver→gold)
src/payments_gateway/      Event contract, Kinesis producer, simulator, FastAPI
sql/athena/                Reference DDL, including legacy gold star design
tests/                     Gateway + transform unit tests
data/                      Local bronze samples + Hadoop Iceberg warehouse (gitignored)
```

| Path | Role |
|---|---|
| `src/payments_gateway/models.py` | Core `PaymentEvent` contract; optional extra fields allowed |
| `src/payments_gateway/producer.py` | Batched Kinesis `PutRecords` |
| `src/payments_gateway/simulator.py` | CLI event generator (`payments-simulator`) |
| `src/payments_gateway/api.py` | Thin FastAPI injector — no helper functions that start with `_` |
| `src/payments_gateway/config.py` | Settings from `.env` |
| `pipelines/sdp/payments/transformations/` | Registry-backed bronze template, silver + quarantine, daily gold aggregates |
| `infra/terraform/schemas/payment_event.json` | JSON Registry contract used to build the bronze Spark schema |
| `pipelines/sdp/payments/spark-pipeline.yml.tftpl` | Catalog, pipeline state, and Iceberg warehouse configuration |
| `src/reports/quarantine_report_lambda/handler.py` | SNS quarantine report Lambda entrypoint; packaged by `infra/terraform/quarantine_notifications.tf` |
| `infra/terraform/` | All AWS resources; local state on the host bind-mount |

## Payment event contract

The producer's core contract is below. `PaymentEvent` uses `extra="allow"`, preserving optional fields in the raw payload.

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | Dedup key |
| `merchant_id` | string | Kinesis partition key + join key |
| `user_id` | string | Payer identifier |
| `amount` | Decimal `> 0`, up to 18 digits / 2 decimal places | Value in `currency`; JSON decimal string, no FX conversion |
| `currency` | string | ISO-4217, default `USD` |
| `status` | enum | `authorized` / `captured` / `failed` / `refunded` |
| `event_timestamp` | datetime (UTC) | Event time (silver partitions use this, not Firehose delivery hour) |
| `schema_version` | int | New producer/API/simulator events use `3`; historical raw events use `1` / `2` |
| `ingest_source` | string | Always `payment_gateway` |

The simulator adds 1–3 optional fields to approximately 40% of events: `country`, `payment_method`, `channel`, `card_brand`, and `fx_rate`. Do not introduce additional business fields without a task requirement.

`amount` is a positive Python `Decimal` constrained to `decimal(18,2)` precision and serialized as an exact JSON number. `simplejson` with `use_decimal=True` writes the Decimal token without converting it to `float`; the chaos simulator uses the same serializer for valid events. API requests accept decimal strings or numbers; use strings for exact monetary input. The new producer rejects `amount_usd` and schema versions below 3. Historical `amount_usd` is a read-only legacy alias, interpreted in the recorded `currency` for this synthetic portfolio; no historical FX conversion or real-world unit inference is performed. The two-decimal scale is a portfolio convention for all supported currencies, not a currency-specific minor-unit implementation. Legacy numbers are rounded by Spark's decimal cast; new producer inputs with more than two decimal places are rejected.

The Registry keeps `amount_usd` as optional numeric and declares optional `amount` as numeric in `infra/terraform/schemas/payment_event.json`. Bronze fetches the latest Registry schema inside `ingest_bronze` and maps its properties to Spark types there (`number` maps to `DoubleType`). The producer must emit `amount` as a JSON number to match this read schema; quoted numeric strings are read as null. Producer and silver enforce monetary validation. Silver chooses non-null `amount` first, otherwise legacy `amount_usd`; invalid non-null new amounts are quarantined rather than falling back. Missing/invalid monetary reason names are now `missing_amount` / `invalid_amount`. The accepted silver exposes canonical decimal `amount` and keeps `amount_decimal` for existing aggregations; `amount_usd` is retained only in bronze and quarantine.

The Registry JSON schema contains the core payload fields plus `country` and `source_file`, but excludes `schema_version` and `ingest_source`. Bronze loads its latest version with `glue:GetSchemaVersion`; schema compatibility is `BACKWARD_ALL`. `source_file` is populated from the input file path. Extras outside the Registry contract remain in raw S3 JSON but are not projected into silver. `country` represents the merchant's country. Silver keeps it; gold substitutes the internal value `unknown` when it is null. Transaction currency remains the explicit `currency` field. This repository behavior takes effect in AWS after deploying the transformation and refreshing the SDP gold views.

GMV is **captured** amount only. Authorized, failed, and refunded are not revenue. `fact_payments` includes authorized and refunded payments, so GMV queries must explicitly filter `status = 'captured'`.

## Lake layers

Bronze explicitly projects columns in the evolved Iceberg table order:
`event_id`, `merchant_id`, `user_id`, `amount_usd`, `currency`, `status`,
`event_timestamp`, `source_file`, `country`, `amount`. Registry property order
does not determine the write order. Both monetary properties must exist in the
Registry read schema. This avoids the observed `amount is out of order, before
country` write failure. Publish the rendered reader using the existing Terraform
`pipeline_bronze` resource and rerun SDP; this ordering fix requires no table
deletion, checkpoint reset, IAM change, or type conversion. Deployment of this
fix has not been verified in AWS.

| Layer | Grain | What you get |
|---|---|---|
| Bronze | raw Firehose files | GZIP NDJSON under `payments/raw/year=/month=/day=/hour=` (delivery time) |
| SDP `bronze_payments` | ingested events | Registry-backed streaming table; `write.spark.accept-any-schema` enabled |
| Silver `silver_payments` | one row per event / merchant | Iceberg v2; typed; deduped on `(event_id, merchant_id)` after validation |
| Silver `silver_payments_rejected` | bad bronze rows | Iceberg v2 quarantine (`rejection_reasons`) |
| Gold `fact_payments` | day / merchant / status / country / currency | Daily aggregates with `status <> 'failed'` |
| Gold `fact_payments_rejected` | same daily grain | Aggregates with `status = 'failed'`; distinct from silver quality quarantine |
| Gold `dim_date` | one row per calendar day | Calendar from 2020-01-01 through 2030-12-31; joins current facts on `full_date` |
| Gold `dim_country` | one row per merchant-country code | BR, CA, DE, GB, JP, MX, US and `unknown`; joins facts on `country = country_code` |
| Gold `dim_status` | one row per payment status | `authorized`, `captured`, `failed`, `refunded`; joins facts on `status = status_code` |

Silver derives `amount_decimal`, normalized status/currency, parsed timestamp, event-time partitions, and `amount_band`. It rejects null/empty/whitespace-only event, merchant and user IDs; missing/blank or unsupported statuses; missing amounts, failed decimal conversions (including non-finite values and overflow), and amounts non-positive after rounding to `decimal(18,2)`; missing/blank or unparseable timestamps; and unsupported currencies. Status domain is `authorized`, `captured`, `failed`, `refunded`. Supported normalized currencies are `USD`, `EUR`, `BRL`, `GBP`, `JPY`, `CAD`, `MXN` (the simulator domain, not the full ISO-4217 list). Null currency retains the `USD` default; blank currency is rejected. `try_cast` and `try_to_timestamp` prevent malformed values from failing the query under ANSI mode.

`rejection_reasons` preserves all applicable reasons: `missing_event_id`, `missing_merchant_id`, `missing_user_id`, `missing_amount`, `invalid_amount`, `amount_decimal_non_positive`, `missing_status`, `invalid_status`, `invalid_currency`, `missing_event_timestamp`, `invalid_event_timestamp`. `amount_decimal_non_positive` replaces `amount_decimal_negative`. Raw fields and `source_file` remain in quarantine; invalid timestamps yield null event-time partitions. UUID format is not validated. Valid rows are filtered before ranking each `(event_id, merchant_id)` by descending `event_timestamp_utc`, so invalid newer duplicates cannot suppress valid older events. Equal-timestamp conflicts choose the lexicographically greatest canonical payload (including raw and normalized fields), then greatest non-null source file path. This is a deterministic conflict policy, not a business update sequence.

Deploy the corrected `02_silver.py` via the existing Terraform `pipeline_silver` upload and run SDP to refresh silver and downstream gold. No new IAM permissions or resources are needed. These changes describe repository configuration, not verified AWS deployment. `tests/test_sdp_silver.py` executes the real Spark queries with ANSI mode enabled and only stubs SDP registration. The CI `sdp-quality` job uses Python 3.11, Java 17 and Spark 4.1.1; these tests do not prove Glue 6.0 execution-graph compatibility. Without PySpark installed the new test module is skipped; use the explicit command below to execute it.

Gold metrics are `amount` (sum of `amount_decimal`), `txn_count`, and `unique_users`. Both gold tables partition by `full_date` and `country`. Gold amount bands use thresholds 100 / 250 / 1000; silver per-event bands use 10 / 50 / 100.

Gold groups by `status_normalized` and `currency_normalized`, exposing them as `status` and `currency`. Amounts and amount bands use transaction-currency units: do not sum across currencies or label non-USD amounts as USD. `unique_users` is not additive across groups. Pipeline YAML sets `spark.sql.session.timeZone` to `UTC` for consistent timestamp parsing and event-day partitions. Terraform renders `transformations/01_bronze.py.tftpl` and uploads the resulting Python file as `transformations/01_bronze.py`.

Deploy the producer before generating new events. Existing raw files whose `amount` values are quoted strings remain incompatible with the numeric Registry schema and will continue to read as null if replayed unchanged; changing the producer affects only future events. To include those historical events, republish corrected numeric payloads or apply an explicit compatibility transform before bronze parsing. Preserve `payments/raw/`; no AWS schema registration, deploy, replay, or job run has been executed by these repository edits.

`dim_date` provides `date_key`, `full_date`, `calendar_date`, year / quarter / month / week / day attributes, and month-, quarter-, and year-end flags. `day_of_week` is ISO-style Monday=1 through Sunday=7. It is intentionally unpartitioned because it contains a small fixed calendar.

`dim_status` is an unpartitioned Iceberg v2 materialized view in `04_dimensions.py`. Columns are `status_code`, `status_name`, `status_description`, and `is_revenue`; only `captured` has `is_revenue = true`, reflecting the project's GMV rule, not accounting revenue recognition. Its four reference rows match `PaymentStatus` in the gateway contract. No extra unknown status is introduced. Use a left join to preserve unexpected raw status values, since the dimension does not validate or normalize fact data. The existing Terraform `pipeline_dimensions` upload includes this view; deploy it and run SDP to create/refresh the table in the configured catalog and warehouse. No new permissions or dependencies are needed. This is repository configuration, not verified AWS deployment.

`dim_country` is an unpartitioned Iceberg v2 materialized view in `04_dimensions.py`, with `country_code`, `country_code_alpha3`, `country_name`, `region_name`, `subregion_name`, and `is_unknown`. Its versioned reference covers the seven simulator countries plus `unknown` (null alpha-3 code). Names and regions follow UN M49, reviewed 2026-09-27; BR/MX use subregion `Latin America and the Caribbean`. There are no runtime network lookups or new IAM permissions. Extend `COUNTRY_REFERENCE` before supporting more countries; use left joins to preserve facts with unmapped codes. Currency remains in the facts. The simulator currently chooses country independently of merchant and currency, so it does not enforce a stable merchant country or local-currency rule. Deploy the existing `pipeline_dimensions` S3 object through Terraform and run SDP to materialize this view; AWS deployment has not been verified for this change.

Terraform creates four S3 buckets: `bronze`, `silver`, `gold`, and `glue`, with AES256 encryption and public access blocked. Current `glue_job.tf` and the pipeline YAML place code at `s3://{glue}/glue/pipelines/payments/`, state below its `state/` prefix, and the Iceberg warehouse at `s3://{glue}/iceberg-warehouse`. Athena results use `s3://{gold}/athena-results/`. Resolve bucket names from `medallion_buckets`; logical silver/gold layers do not imply their tables use the corresponding buckets. The architecture document uses these same paths.

Terraform uploads all four transformation files and the rendered pipeline YAML. The configured Glue job uses version 6.0, two `G.1X` workers, a 20-minute timeout, no retries, one concurrent run, and the shared crawler/SDP IAM role.

Pipeline write logging uses Iceberg's bundled `org.apache.iceberg.metrics.LoggingMetricsReporter`, explicitly selected by `spark.sql.catalog.${catalog}.metrics-reporter-impl` in the YAML. The Glue job's existing `--conf` argument also sets `spark.log.level=INFO` at startup. Each Iceberg `CommitReport` records table name, snapshot ID, operation, and added/removed/total data-file and record counts for all persisted bronze/silver/gold tables, quarantine, and dimensions. This is a catalog setting; do not add Spark actions inside SDP query functions to log counts. Temporary views have no commits. These metrics count output files and rewritten rows, not new business events or Firehose source files. Silver/gold full replacement can add files without increasing GMV. A missing report is not proof of zero writes or job success; inspect run state and snapshots. Query the Glue job-run driver stream in the default Spark/system log group (`/aws-glue/jobs/error`). No new resources, dependencies, storage paths, or IAM permissions are introduced. Deploy the existing `pipeline_yaml` object and updated Glue job to apply the logging configuration; AWS log delivery has not been verified.

The optional raw Athena DDL in `sql/athena/bronze/payment_events_raw.sql` now includes string `amount` alongside numeric `amount_usd`, for direct old/new raw exploration. It is separate from SDP's Registry-backed Iceberg table. Its `CREATE IF NOT EXISTS` does not update an existing external table; publish a compatible catalog update or recreate that optional raw table deliberately. Bucket placeholders must be resolved from `medallion_buckets`. This reference DDL change has not been applied to AWS.

Quarantine email reporting is opt-in: set `enable_quarantine_notifications = true` and a non-empty `quarantine_notification_emails` list in the ignored Terraform tfvars file. The `rtmsp-{environment}-payments-quarantine-report` Lambda source is `src/reports/quarantine_report_lambda/handler.py`; Terraform packages it to `infra/terraform/build/quarantine-report-lambda.zip` using `archive_file` in `infra/terraform/quarantine_notifications.tf`. The Lambda is invoked on Glue terminal state events and every five minutes to reconcile missed events from activation through the previous seven days. It serializes work with DynamoDB and reports one oldest unprocessed run per invocation. SNS topic and subscriptions are separate from the alarms topic; each email recipient must confirm the SNS subscription. Glue failures send run state and diagnostic text with counts unavailable.

Successful reports query `silver_payments_rejected` and `bronze_payments` at Iceberg snapshot IDs selected at the Glue completion time. Total counts include all rejected rows; new counts include rows whose `source_file` first appeared since the prior successful Glue run. The first report treats all existing files as new. Failed-run bronze files are included in the next successful report. A change to validation on previously processed files can change the total without changing new counts. Every rejection reason is counted; a row may have multiple reasons. Snapshots must remain available for at least 30 days. Reports and Athena query IDs are stored at `s3://{glue}/glue/pipelines/payments/quarantine-reports/{JobRunId}/`; Athena results stay under `s3://{gold}/athena-results/`. The Lambda execution role can read and write Athena query results only under that results prefix, and can write report manifests only under the quarantine reports prefix. Each report uses two Athena queries and incurs scan charges. Incomplete snapshots/queries are reported with counts unavailable, never as zero. Inspect the report manifest, Lambda log group `/aws/lambda/rtmsp-{environment}-payments-quarantine-report`, and quarantine event DLQ before recovery. Incomplete reports are checkpointed to prevent repeat emails; after fixing the cause, invoke the Lambda manually with `{"action":"retry","job_run_id":"<JobRunId>"}`. EventBridge and Lambda SNS delivery plus DynamoDB checkpointing are not atomic, so a retry at the publish boundary can duplicate an email with the same JobRunId. AWS deployment and email delivery are not verified by repository changes.

Bronze crawler is on-demand by default (optional `glue_crawler_schedule`) and incremental (`CRAWL_NEW_FOLDERS_ONLY`) on `payments/raw/` only. Do not crawl silver or gold — Iceberg metadata is the catalog. If Hive/Parquet tables already exist under the SDP table names, drop them before the job runs. Athena workgroup: `rtmsp-dev-payments`.

## Common commands

`payments-chaos-simulator` is an opt-in raw fault injector using the existing
Kinesis stream and producer batching/retries. It bypasses model validation only
for its own mutated payloads; the API and normal simulator retain validation.
Use `uv run payments-chaos-simulator --dry-run --count 100` for local NDJSON
without credentials or AWS calls. For publishing, the existing `.env` and AWS
prerequisites apply. `--bad-ratio` (default 0.3) and `--late-ratio` (default 0.2)
are independent probabilities; `--late-seconds` (default 86400, maximum 365 days)
backdates event time rather than delaying execution. Use `--bad-ratio 0
--late-ratio 1 --late-seconds 172800` to isolate valid two-day-late events.
`--count` bounds attempts including exhausted failures; `--rate` controls sending
speed. Missing/null/blank fields, invalid domains/amounts/timestamps, nested wrong
types and multiple faults are available through `--scenario`; see README.
Transport partition keys retain the original valid merchant even if its payload
field is removed or corrupted. Bronze parsing can coerce/null malformed values;
generated fault counts do not guarantee quarantine counts. Firehose partitions
remain delivery-time based; valid late records use historical silver/gold days
after SDP runs. No resource, IAM, schema or pipeline changes are required, and
AWS behavior has not been verified by this local implementation.

The status view is now named `dim_status` (formerly `dim_payment_status`). Its natural key remains `status_code`. Publish `04_dimensions.py` and rerun SDP to materialize the new name; this code change does not rename or delete an existing AWS table. Check for a legacy `dim_status` table before deployment, since its old SQL schema differs from the SDP schema.

SDP reference dimensions use module-level `StructType` schemas, not DDL strings passed to `createDataFrame` inside query functions. String schemas trigger Spark Connect `AnalyzePlan` and fail with `ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION`. Keep reference payloads small enough for inline local relations. The schema correction must be uploaded through the existing `pipeline_dimensions` Terraform resource before rerunning Glue; local validation is not confirmation of a successful AWS job.

```bash
# App
uv sync --group dev
uv run payments-simulator --rate 10 --duration 30
PYTHONPATH=src uv run --no-sync uvicorn payments_gateway.api:app --reload --reload-dir src/payments_gateway --port 8080

# Quality
uv run pytest -q
uv run --group dev --with pyspark==4.1.1 python -m pytest -q
uv run ruff check src tests pipelines
uv run black src tests pipelines

# After Firehose lands (buffer defaults: 64 MiB / 60s), catalog bronze then run SDP
# (inside the Terraform workspace shell)
aws glue start-crawler --name rtmsp-dev-payments-bronze-raw
aws glue start-job-run --job-name rtmsp-dev-payments-sdp
```

CI (`.github/workflows/ci.yml`) runs gateway pytest, Ruff on `src tests pipelines`, and `terraform fmt` / `validate` on every push. The `sdp-quality` job runs Black checks and the full current test suite with Spark 4.1.1, Python 3.11 and Java 17. Shared fixtures only replace SDP registration, not Spark transformations. Obsolete tests for removed `payments_lake` helpers have been replaced by current bronze/silver and gold tests; the removed catalog-helper test has no remaining implementation. Spark tests skip when PySpark is not installed. Run the explicit Spark command above to include them. No new runtime dependency or IAM permission is needed.

Local transformation tests use PySpark with mocked AWS clients and SDP registration. The bronze test loads `01_bronze.py.tftpl` with an explicit Python source loader and reads temporary local JSON files. No AWS credentials are needed for these tests.

## Agent conventions

- Read [docs/architecture.md](docs/architecture.md) before changing pipeline or AWS design.
- Update `AGENTS.md` for every infrastructure change in the same delivery; follow the mandatory infrastructure documentation section above.
- Match existing naming, layout, and style. Prefer the smallest change that solves the request.
- Reuse existing helpers; put every function that starts with `_` in a business/lib module, never in an API module (`src/payments_gateway/api.py`).
- Format Python with Black (line length 88, Python 3.10) before finishing.
- Respond and comment in English unless asked otherwise.
- Conventional commits (`feat`, `fix`, `chore`, `docs`, `refactor`, `test`). Commit only when asked.
- Do not commit `.env`, `.aws/`, `*.tfstate`, `terraform.tfvars`, or other secrets.
- Do not call `aws` or `terraform` on the host; use `./infra/tf.sh shell`.
- Do not write exploits, attack procedures, or expand IAM beyond what the task needs.
- Iceberg v3, SCD Type 2, extra sources, and remote Terraform state are later work — see architecture “Later” — unless the user asks for them. Daily SDP gold and Glue Schema Registry already exist.
