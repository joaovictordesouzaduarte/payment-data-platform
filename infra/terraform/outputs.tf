output "aws_region" {
  description = "AWS region where payment data platform resources were created."
  value       = var.aws_region
}

output "payments_bronze_bucket" {
  description = "S3 bronze bucket — raw Firehose landing for payments."
  value       = local.payments_bronze_bucket.bucket
}

output "payments_silver_bucket" {
  description = "S3 silver bucket — Iceberg payment_events and quarantine."
  value       = local.payments_silver_bucket.bucket
}

output "payments_gold_bucket" {
  description = "S3 gold bucket — Iceberg star (fact_payment + dimensions)."
  value       = local.payments_gold_bucket.bucket
}

output "medallion_buckets" {
  description = "Map of medallion layer → S3 bucket name."
  value = {
    for layer, bucket in aws_s3_bucket.medallion : layer => bucket.bucket
  }
}

output "payments_bronze_prefix" {
  description = "Hive-style prefix used by Firehose for successful deliveries."
  value       = "payments/raw/"
}

output "payments_errors_prefix" {
  description = "Prefix used by Firehose for failed deliveries."
  value       = "payments/errors/"
}

output "kinesis_stream_name" {
  description = "Kinesis Data Stream name for payment events."
  value       = aws_kinesis_stream.payments_events.name
}

output "kinesis_stream_arn" {
  description = "Kinesis Data Stream ARN."
  value       = aws_kinesis_stream.payments_events.arn
}

output "firehose_delivery_stream_name" {
  description = "Firehose delivery stream name."
  value       = aws_kinesis_firehose_delivery_stream.payments_to_s3.name
}

output "producer_iam_policy_arn" {
  description = "Attach this policy to the IAM principal running the payments producer."
  value       = aws_iam_policy.producer.arn
}

output "alarms_sns_topic_arn" {
  description = "SNS topic for payments pipeline CloudWatch alarms (null when alarms are disabled)."
  value       = one(aws_sns_topic.payments_alarms[*].arn)
}

output "quarantine_reports_sns_topic_arn" {
  description = "SNS topic for post-Glue quarantine reports (null when disabled)."
  value       = one(aws_sns_topic.quarantine_reports[*].arn)
}

output "glue_database_name" {
  description = "Glue Data Catalog database for payments (bronze crawler target)."
  value       = aws_glue_catalog_database.payments.name
}

output "glue_crawler_name" {
  description = "Glue crawler that catalogs Firehose bronze NDJSON under payments/raw/."
  value       = aws_glue_crawler.payments_bronze_raw.name
}

output "glue_crawler_role_arn" {
  description = "IAM role assumed by the bronze Glue crawler and the SDP job."
  value       = aws_iam_role.glue_crawler.arn
}

output "glue_payments_sdp_job" {
  description = "Glue 6.0 Spark Declarative Pipeline job (bronze → silver Iceberg)."
  value       = aws_glue_job.payments_sdp.name
}

output "glue_schema_registry" {
  description = "Glue Schema Registry name used by bronze to load the payment event contract."
  value       = aws_glue_registry.registry.registry_name
}

output "glue_events_schema" {
  description = "Glue schema name (JSON) for payment events."
  value       = aws_glue_schema.events.schema_name
}

output "athena_workgroup" {
  description = "Athena workgroup for payments lake queries (results land on gold)."
  value       = aws_athena_workgroup.payments.name
}

