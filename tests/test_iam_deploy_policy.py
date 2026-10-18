"""Offline security guardrails for the deployment policy templates."""

import json
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[1] / "docs"
POLICIES = sorted(DOCS.glob("iam-deploy*-policy.json"))
GLOBAL_ACTIONS = {
    "logs:DescribeLogGroups",
    "kinesis:EnableEnhancedMonitoring",
    "kinesis:DisableEnhancedMonitoring",
    "glue:CreateCrawler",
}
PASS_ROLES = {
    "rtmsp-dev-payments-firehose": "firehose.amazonaws.com",
    "rtmsp-dev-payments-glue-crawler": "glue.amazonaws.com",
    "rtmsp-dev-payments-quarantine-report-lambda": "lambda.amazonaws.com",
}


def statements(path):
    return json.loads(path.read_text(encoding="utf-8"))["Statement"]


def resources(statement):
    value = statement["Resource"]
    return [value] if isinstance(value, str) else value


@pytest.mark.parametrize("path", POLICIES, ids=lambda p: p.name)
def test_templates_fit_managed_policy_quota_after_account_substitution(path):
    text = path.read_text(encoding="utf-8").replace("ACCOUNT_ID", "123456789012")
    policy = json.loads(text)
    assert policy["Version"] == "2012-10-17"
    assert len(json.dumps(policy, separators=(",", ":"))) <= 6144


@pytest.mark.parametrize("path", POLICIES, ids=lambda p: p.name)
def test_global_access_is_limited_to_documented_aws_exceptions(path):
    for statement in statements(path):
        assert statement["Effect"] == "Allow"
        assert "NotAction" not in statement and "NotResource" not in statement
        for action in statement["Action"]:
            assert "*" not in action and "?" not in action
        if "*" in resources(statement):
            assert set(statement["Action"]) <= GLOBAL_ACTIONS
            conditions = statement["Condition"]["StringEquals"]
            assert conditions["aws:RequestedRegion"] == "us-east-1"
            if "glue:CreateCrawler" in statement["Action"]:
                assert conditions["aws:RequestTag/Project"] == "rtmsp"
                assert conditions["aws:RequestTag/Environment"] == "dev"
                assert conditions["aws:RequestTag/ManagedBy"] == "terraform"


@pytest.mark.parametrize("path", POLICIES, ids=lambda p: p.name)
def test_resource_arns_do_not_span_accounts_regions_or_iam_identities(path):
    for statement in statements(path):
        for resource in resources(statement):
            if resource == "*":
                continue
            prefix, partition, service, region, account, name = resource.split(":", 5)
            assert (prefix, partition) == ("arn", "aws")
            if service == "s3":
                assert region == account == ""
                assert name.startswith("rtmsp-dev-payments-")
            else:
                assert account == "ACCOUNT_ID"
                assert region == ("" if service == "iam" else "us-east-1")
            if service == "iam":
                assert name in {
                    *(f"role/{role}" for role in PASS_ROLES),
                    "policy/rtmsp-dev-payments-producer",
                }


def test_pass_role_pairs_each_role_with_only_its_execution_service():
    found = {}
    for path in POLICIES:
        for statement in statements(path):
            if "iam:PassRole" not in statement["Action"]:
                continue
            assert statement["Action"] == ["iam:PassRole"]
            assert len(resources(statement)) == 1
            role = resources(statement)[0].rsplit("/", 1)[1]
            service = statement["Condition"]["StringEquals"]["iam:PassedToService"]
            assert service == PASS_ROLES[role]
            found[role] = service
    assert found == PASS_ROLES


def test_deployer_cannot_publish_events_query_data_or_modify_other_identities():
    prohibited = {
        "kinesis:PutRecord",
        "kinesis:PutRecords",
        "athena:StartQueryExecution",
        "glue:CreateSession",
        "glue:RunStatement",
        "iam:AttachRolePolicy",
        "iam:AttachUserPolicy",
        "iam:PutUserPolicy",
        "iam:CreateAccessKey",
        "iam:CreateUser",
        "sts:AssumeRole",
    }
    for path in POLICIES:
        for statement in statements(path):
            assert not prohibited.intersection(statement["Action"])
            if {
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:GetObjectTagging",
                "s3:PutObjectTagging",
                "s3:DeleteObjectTagging",
            }.intersection(statement["Action"]):
                for resource in resources(statement):
                    assert resource.startswith("arn:aws:s3:::rtmsp-dev-payments-glue-")
                    assert resource.endswith(
                        (
                            "/glue/pipelines/payments/spark-pipeline.yml",
                            "/glue/pipelines/payments/transformations/*.py",
                        )
                    )


def test_optional_services_are_absent_from_core_policy():
    optional = {"lambda", "events", "dynamodb", "sqs", "sns", "cloudwatch"}
    for statement in statements(DOCS / "iam-deploy-policy.json"):
        assert not optional.intersection(a.split(":")[0] for a in statement["Action"])
