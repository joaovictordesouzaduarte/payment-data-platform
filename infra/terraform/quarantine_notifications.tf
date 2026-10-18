locals {
  quarantine_enabled = var.enable_quarantine_notifications
  quarantine_name    = "${var.project_name}-${var.environment}-payments-quarantine-report"
  quarantine_reports = "s3://${local.payments_glue_bucket.bucket}/glue/pipelines/payments/quarantine-reports/"
  quarantine_athena  = "s3://${local.payments_gold_bucket.bucket}/athena-results/"
}

resource "aws_sns_topic" "quarantine_reports" {
  count = local.quarantine_enabled ? 1 : 0
  name  = "${local.quarantine_name}-email"

  lifecycle {
    precondition {
      condition     = length(var.quarantine_notification_emails) > 0
      error_message = "Set at least one quarantine_notification_emails address when quarantine notifications are enabled."
    }
  }
}

resource "aws_sns_topic_subscription" "quarantine_reports_email" {
  for_each = local.quarantine_enabled ? toset(var.quarantine_notification_emails) : toset([])

  topic_arn = aws_sns_topic.quarantine_reports[0].arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_dynamodb_table" "quarantine_report_state" {
  count        = local.quarantine_enabled ? 1 : 0
  name         = "${local.quarantine_name}-state"
  billing_mode = "PAY_PER_REQUEST" 
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

resource "aws_sqs_queue" "quarantine_event_dlq" {
  count                     = local.quarantine_enabled ? 1 : 0
  name                      = "${local.quarantine_name}-events-dlq"
  message_retention_seconds = 86400
}

resource "aws_iam_role" "quarantine_report_lambda" {
  count = local.quarantine_enabled ? 1 : 0
  name  = "${local.quarantine_name}-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "quarantine_report_lambda" {
  count = local.quarantine_enabled ? 1 : 0
  name  = "${local.quarantine_name}-runtime"
  role  = aws_iam_role.quarantine_report_lambda[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.quarantine_report_lambda[0].arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetJobRuns", "glue:GetJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = [local.payments_gold_bucket.arn, local.payments_glue_bucket.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [
          "${local.payments_glue_bucket.arn}/*",
          "${local.payments_gold_bucket.arn}/athena-results/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${local.payments_gold_bucket.arn}/athena-results/*", "${local.payments_glue_bucket.arn}/glue/pipelines/payments/quarantine-reports/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:BatchGetItem"]
        Resource = aws_dynamodb_table.quarantine_report_state[0].arn
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.quarantine_reports[0].arn
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.quarantine_event_dlq[0].arn
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "quarantine_report_lambda" {
  count             = local.quarantine_enabled ? 1 : 0
  name              = "/aws/lambda/${local.quarantine_name}"
  retention_in_days = 30
}

data "archive_file" "quarantine_report_lambda" {
  count       = local.quarantine_enabled ? 1 : 0
  type        = "zip"
  source_file = "${path.module}/../../src/reports/quarantine_report_lambda/handler.py"
  output_path = "${path.module}/build/quarantine-report-lambda.zip"
}

resource "aws_lambda_function" "quarantine_report" {
  count            = local.quarantine_enabled ? 1 : 0
  function_name    = local.quarantine_name
  role             = aws_iam_role.quarantine_report_lambda[0].arn
  handler          = "handler.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.quarantine_report_lambda[0].output_path
  source_code_hash = data.archive_file.quarantine_report_lambda[0].output_base64sha256
  timeout          = 900
  memory_size      = 512

  environment {
    variables = {
      GLUE_JOB_NAME     = aws_glue_job.payments_sdp.name
      GLUE_DATABASE     = aws_glue_catalog_database.payments.name
      ATHENA_WORKGROUP  = aws_athena_workgroup.payments.name
      ATHENA_RESULTS    = local.quarantine_athena
      REPORT_PREFIX     = local.quarantine_reports
      SNS_TOPIC_ARN     = aws_sns_topic.quarantine_reports[0].arn
      STATE_TABLE       = aws_dynamodb_table.quarantine_report_state[0].name
    }
  }

  depends_on = [aws_iam_role_policy.quarantine_report_lambda, aws_cloudwatch_log_group.quarantine_report_lambda]
}

resource "aws_lambda_function_event_invoke_config" "quarantine_report" {
  count                        = local.quarantine_enabled ? 1 : 0
  function_name                = aws_lambda_function.quarantine_report[0].function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 21600

  destination_config {
    on_failure {
      destination = aws_sqs_queue.quarantine_event_dlq[0].arn
    }
  }
}

resource "aws_sqs_queue_policy" "quarantine_event_dlq" {
  count     = local.quarantine_enabled ? 1 : 0
  queue_url = aws_sqs_queue.quarantine_event_dlq[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.quarantine_event_dlq[0].arn
      Condition = { ArnEquals = { "aws:SourceArn" = [aws_cloudwatch_event_rule.quarantine_job_state[0].arn, aws_cloudwatch_event_rule.quarantine_reconcile[0].arn] } }
    }]
  })
}

resource "aws_cloudwatch_event_rule" "quarantine_job_state" {
  count       = local.quarantine_enabled ? 1 : 0
  name        = "${local.quarantine_name}-job-state"
  description = "Reconcile quarantine report after Glue job state changes."
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.payments_sdp.name]
      state   = ["SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED"]
    }
  })
}

resource "aws_cloudwatch_event_rule" "quarantine_reconcile" {
  count               = local.quarantine_enabled ? 1 : 0
  name                = "${local.quarantine_name}-reconcile"
  description         = "Find Glue terminal runs whose EventBridge state event was missed."
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "quarantine_job_state" {
  count          = local.quarantine_enabled ? 1 : 0
  rule           = aws_cloudwatch_event_rule.quarantine_job_state[0].name
  arn            = aws_lambda_function.quarantine_report[0].arn
  target_id      = "quarantine-report-lambda"
  dead_letter_config { arn = aws_sqs_queue.quarantine_event_dlq[0].arn }
  retry_policy {
    maximum_event_age_in_seconds = 86400
    maximum_retry_attempts       = 12
  }
  depends_on = [aws_sqs_queue_policy.quarantine_event_dlq]
}

resource "aws_cloudwatch_event_target" "quarantine_reconcile" {
  count     = local.quarantine_enabled ? 1 : 0
  rule      = aws_cloudwatch_event_rule.quarantine_reconcile[0].name
  arn       = aws_lambda_function.quarantine_report[0].arn
  target_id = "quarantine-report-reconciler"
  dead_letter_config { arn = aws_sqs_queue.quarantine_event_dlq[0].arn }
  depends_on = [aws_sqs_queue_policy.quarantine_event_dlq]
}

resource "aws_lambda_permission" "quarantine_job_state" {
  count         = local.quarantine_enabled ? 1 : 0
  statement_id  = "AllowEventBridgeGlueState"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.quarantine_report[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.quarantine_job_state[0].arn
}

resource "aws_lambda_permission" "quarantine_reconcile" {
  count         = local.quarantine_enabled ? 1 : 0
  statement_id  = "AllowEventBridgeReconcile"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.quarantine_report[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.quarantine_reconcile[0].arn
}
