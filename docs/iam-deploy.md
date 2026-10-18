# Deployment permissions

The JSON files are templates for the repository's default `rtmsp` / `dev` /
`us-east-1` stack. Replace **`ACCOUNT_ID`** with the deployment account's 12-digit
ID before creating customer managed IAM policies. For another environment or
region, also replace `rtmsp-dev`, `rtmsp_dev`, `us-east-1`, and the crawler's
`Project` / `Environment` request-tag conditions. Do not replace them with `*`.

| Attach to the deployment identity | When needed |
|---|---|
| [iam-deploy-policy.json](iam-deploy-policy.json) | Always: Terraform resources, pipeline uploads, crawler/job start, status/stop, and batched event publishing to the payments stream |
| [iam-deploy-alarms-policy.json](iam-deploy-alarms-policy.json) | `enable_alarms = true` (default) |
| [iam-deploy-quarantine-policy.json](iam-deploy-quarantine-policy.json) | `enable_quarantine_notifications = true` |

For the complete application with both options enabled, attach all three.
Keep a complement attached while Terraform removes its resources; detach it
after the disabling apply succeeds. These replace the former broad deployment
policy; leaving that policy or AdministratorAccess attached defeats the reduction.
Creating/attaching the deployment policies is an administrator bootstrap step,
not a permission granted to the deployment identity itself.

If Terraform returns `AccessDenied` while refreshing optional resources, first
check that each relevant complement is attached to the same deploy identity and
that its ARNs contain the correct account ID. For example,
`086997587178` must replace `ACCOUNT_ID` in every policy attached to user `iac`.
`sns:GetTopicAttributes` is needed by both SNS topics; `iam:GetRole` and
`logs:ListTagsForResource` for the quarantine Lambda role and log group are in
the quarantine complement. Re-run `terraform plan` after attaching or correcting
the policies; do not expand the core policy to `Resource: "*"` to clear refresh
errors.

Each file fits the [6,144-character customer managed policy limit](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html).
Do not combine them into one managed policy or paste them as an IAM user's inline
policy (which has a smaller aggregate limit). Keep the files separate when
installing them. `ACCOUNT_ID` is a literal placeholder: IAM policy variables
[cannot replace the account portion of an ARN](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html).

## Scope and exceptions

- Permissions cover creation, refresh, updates to the current configuration,
  replacement and deletion. Delete actions support Terraform lifecycle operations;
  no destroy or deployment was executed during this review.
- ARNs name only the stack's resources. S3 `????????` matches the eight-character
  random suffix from `random_id.bucket_suffix`. After the first deployment, replace
  these patterns with the exact four bucket names from `medallion_buckets` for
  tighter scope. Object access covers the pipeline YAML and transformation Python
  files only, excluding raw payments, the Iceberg warehouse, state and reports.
- `iam:PassRole` pairs each exact role with its service: Firehose, Glue or Lambda.
  There is no permission to attach policies to arbitrary users or roles.
- Only four actions retain `Resource: "*"`, because the AWS authorization reference
  does not support resource ARNs for them: `logs:DescribeLogGroups`,
  `kinesis:EnableEnhancedMonitoring`, `kinesis:DisableEnhancedMonitoring`, and
  `glue:CreateCrawler`. All require `us-east-1`; crawler creation additionally
  requires the existing Terraform `Project=rtmsp`, `Environment=dev` and
  `ManagedBy=terraform` tags. Those tags do not enforce a crawler name, and the
  Kinesis monitoring actions remain account-wide within that region.
- Glue database deletion includes its catalog, tables and user-defined functions
  for hierarchical authorization. This does not grant standalone table or
  partition mutation actions. SDP and the crawler use their execution role.
- S3 configuration reads are retained because `aws_s3_bucket` refreshes them even
  when the corresponding settings are absent from HCL. Lambda reads include
  `GetPolicy`, `GetFunctionCodeSigningConfig` and `ListVersionsByFunction`;
  DynamoDB includes `DescribeContinuousBackups`; SQS includes `ListQueueTags`.
  These avoid refresh failures without granting backup, signing or queue-message
  administration.

## Running the application

The deployment identity can start and inspect the existing crawler and SDP job.
The core policy also grants `kinesis:PutRecords` to the single payments stream,
because the local gateway and Terraform workspace both read credentials from
the root `.env`. It does not grant writes to another stream or the single-record
`PutRecord` operation. Interactive Glue sessions, ad hoc Athena queries, manual
report invocation and direct lake-data access remain separate operator or analyst
responsibilities; automatic reports use the Lambda execution role.

For a separately credentialed gateway, use the Terraform-created
`producer_iam_policy_arn`, which is limited to publishing and stream discovery.
Terraform creates that managed policy, not the producer identity or its
attachment; an administrator must attach it to the producer identity.

Use deployment credentials in the Terraform workspace's `.env`. Run the gateway
with producer credentials in its environment; restart the workspace with the
deployment credentials before the next Terraform operation.

This review narrows the **deployment identity**, not the inline execution policies
in `infra/terraform/glue.tf`, `iam.tf` and `quarantine_notifications.tf`. A deployer
that can write those role policies and update executable code remains a trusted
administrator of this application. These templates are not an escalation-proof
permissions boundary.

## Verification

Reviewed against the current HCL and AWS provider **6.56.0** in the local lock
file, including its [resource implementation](https://github.com/hashicorp/terraform-provider-aws/tree/v6.56.0/internal/service).
Action names and global-only exceptions were checked against AWS's service
authorization data for [Glue](https://servicereference.us-east-1.amazonaws.com/v1/glue/glue.json),
[Kinesis](https://servicereference.us-east-1.amazonaws.com/v1/kinesis/kinesis.json)
and the other services used here. Provider upgrades or new resource settings
require another permission review.

Local checks validate JSON, size, resource scope and sensitive permission guards.
They do not prove an AWS deployment succeeds. No IAM policy was installed, no AWS
plan/apply was run, and effective permissions (SCPs, boundaries, other attached
policies and Lake Formation settings) were not inspected. After an administrator
installs the policies, validate with the documented container workflow:
`terraform plan -out=tfplan`, review with `terraform show tfplan`, then
`terraform apply tfplan`. Nonempty S3 buckets still require deliberate data
cleanup before deletion; this policy does not grant bulk lake cleanup.
