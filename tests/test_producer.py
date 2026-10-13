"""Unit tests for batched Kinesis producer (mocked client)."""

from __future__ import annotations

from payments_gateway.config import Settings
from payments_gateway.models import PaymentEvent, PaymentStatus
from payments_gateway.producer import PaymentEventProducer


def _event(event_id: str, merchant_id: str = "mrc_0001") -> PaymentEvent:
    return PaymentEvent(
        event_id=event_id,
        merchant_id=merchant_id,
        user_id="usr_000001",
        amount=12.34,
        status=PaymentStatus.CAPTURED,
    )


class FakeKinesisClient:
    def __init__(self, fail_first_n: int = 0) -> None:
        self.fail_first_n = fail_first_n
        self.calls: list[dict] = []
        self._call_count = 0

    def put_records(self, StreamName: str, Records: list[dict]) -> dict:
        self._call_count += 1
        self.calls.append({"StreamName": StreamName, "Records": Records})

        results = []
        for idx, _ in enumerate(Records):
            if self._call_count == 1 and idx < self.fail_first_n:
                results.append(
                    {
                        "ErrorCode": "ProvisionedThroughputExceededException",
                        "ErrorMessage": "throttled",
                    }
                )
            else:
                results.append({"SequenceNumber": f"seq-{idx}", "ShardId": "shard-000"})
        return {
            "Records": results,
            "FailedRecordCount": sum(1 for r in results if "ErrorCode" in r),
        }


def test_publish_empty_returns_zero():
    producer = PaymentEventProducer(
        settings=Settings(
            aws_region="us-east-1",
            kinesis_stream_name="test-stream",
            batch_size=100,
            max_retries=3,
            log_level="INFO",
        ),
        client=FakeKinesisClient(),
    )
    result = producer.publish([])
    assert result.attempted == 0
    assert result.succeeded == 0


def test_publish_batches_and_uses_merchant_partition_key():
    client = FakeKinesisClient()
    producer = PaymentEventProducer(
        settings=Settings(
            aws_region="us-east-1",
            kinesis_stream_name="test-stream",
            batch_size=2,
            max_retries=3,
            log_level="INFO",
        ),
        client=client,
    )
    events = [_event("e1", "mrc_a"), _event("e2", "mrc_b"), _event("e3", "mrc_c")]
    result = producer.publish(events)

    assert result.succeeded == 3
    assert result.failed == 0
    assert len(client.calls) == 2  # batch_size=2 → 2+1
    assert client.calls[0]["Records"][0]["PartitionKey"] == "mrc_a"
    assert client.calls[0]["Records"][1]["PartitionKey"] == "mrc_b"


def test_partial_failure_retries_and_succeeds():
    client = FakeKinesisClient(fail_first_n=1)
    producer = PaymentEventProducer(
        settings=Settings(
            aws_region="us-east-1",
            kinesis_stream_name="test-stream",
            batch_size=100,
            max_retries=3,
            log_level="INFO",
        ),
        client=client,
    )
    # Avoid real sleep during backoff
    producer._sleep_backoff = staticmethod(lambda _attempt: None)  # type: ignore[method-assign]

    result = producer.publish([_event("e1"), _event("e2")])
    assert result.succeeded == 2
    assert result.failed == 0
    assert len(client.calls) == 2
    assert len(client.calls[1]["Records"]) == 1
