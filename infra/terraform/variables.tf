variable "aws_region" {
  description = "AWS region for payment data platform resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short project identifier used in resource names."
  type        = string
  default     = "rtmsp"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "kinesis_retention_hours" {
  description = "Kinesis data retention in hours (24 is enough for learning / replay)."
  type        = number
  default     = 24
}

variable "firehose_buffer_size_mb" {
  description = "Firehose S3 buffer size in MiB (balances SLA vs small-file problem)."
  type        = number
  default     = 64
}

variable "firehose_buffer_interval_seconds" {
  description = "Firehose S3 buffer interval in seconds."
  type        = number
  default     = 60
}

variable "alarm_email" {
  description = "Optional email for SNS alarm notifications. Leave empty to skip subscription."
  type        = string
  default     = ""
}

variable "enable_alarms" {
  description = "Create the SNS topic and CloudWatch alarms. Set false when the deploy identity lacks SNS/CloudWatch permissions."
  type        = bool
  default     = true
}

variable "glue_crawler_schedule" {
  description = "Optional Glue crawler cron (for example cron(0 * * * ? *)). Empty means on-demand only."
  type        = string
  default     = ""
}

variable "enable_quarantine_notifications" {
  description = "Create post-Glue quarantine reports through SNS email."
  type        = bool
  default     = false
}

variable "quarantine_notification_emails" {
  description = "Email recipients for Glue quarantine reports. Each SNS subscription must be confirmed."
  type        = list(string)
  default     = ["jduarte.studies@gmail.com"]

  validation {
    condition     = length(var.quarantine_notification_emails) == length(distinct(var.quarantine_notification_emails))
    error_message = "quarantine_notification_emails must not contain duplicates."
  }
}
