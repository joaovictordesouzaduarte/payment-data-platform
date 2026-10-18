"""Batched Kinesis PutRecords producer for payment events."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Iterable, Protocol, Sequence

import boto3
from botocore.exceptions import ClientError

from payments_gateway.config import Settings

logger = logging.getLogger(__name__)


class KinesisEvent(Protocol):
    """Serialized event interface shared by validated and fault-injection events."""

    @property
    def event_id(self) -> str: ...

    def partition_key(self) -> str: ...

    def to_kinesis_data(self) -> bytes: ...


# Kinesis PutRecords limits
MAX_RECORDS_PER_PUT = 500
MAX_PAYLOAD_BYTES = 5 * 1024 * 1024  # 5 MiB
MAX_RECORD_BYTES = 1 * 1024 * 1024  # 1 MiB per record


@dataclass
class PublishResult:
    """Outcome of a publish attempt across one or more PutRecords calls."""

    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    failed_event_ids: list[str] = field(default_factory=list)


class PaymentEventProducer:
    """Publishes PaymentEvent batches to Amazon Kinesis Data Streams."""

    def __init__(
        self,
        settings: Settings | None = None,
        client=None,
    ) -> None:
        self.settings = settings or Settings.from_env()
        if client is not None:
            self._client = client
        else:
            self._client = boto3.client(
                "kinesis",
                region_name=self.settings.aws_region,
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
            )
        self._batch_size = min(self.settings.batch_size, MAX_RECORDS_PER_PUT)

    def publish(self, events: Sequence[KinesisEvent]) -> PublishResult:
        if not events:
            return PublishResult()

        result = PublishResult(attempted=len(events))
        for chunk in self._chunk_events(events):
            chunk_result = self._put_records_with_retry(chunk)
            result.succeeded += chunk_result.succeeded
            result.failed += chunk_result.failed
            result.failed_event_ids.extend(chunk_result.failed_event_ids)
        return result

    def publish_one(self, event: KinesisEvent) -> PublishResult:
        return self.publish([event])

    def _chunk_events(
        self, events: Sequence[KinesisEvent]
    ) -> Iterable[list[KinesisEvent]]:
        chunk: list[KinesisEvent] = []
        chunk_bytes = 0

        for event in events:
            data = event.to_kinesis_data()
            if len(data) > MAX_RECORD_BYTES:
                raise ValueError(
                    f"Event {event.event_id} exceeds Kinesis 1 MiB record limit "
                    f"({len(data)} bytes)"
                )

            # + partition key overhead is small; track data size primarily.
            next_size = chunk_bytes + len(data)
            if chunk and (
                len(chunk) >= self._batch_size or next_size > MAX_PAYLOAD_BYTES
            ):
                yield chunk
                chunk = []
                chunk_bytes = 0

            chunk.append(event)
            chunk_bytes += len(data)

        if chunk:
            yield chunk

    def _put_records_with_retry(self, events: list[KinesisEvent]) -> PublishResult:
        records = [
            {
                "Data": event.to_kinesis_data(),
                "PartitionKey": event.partition_key(),
            }
            for event in events
        ]
        pending_events = list(events)
        pending_records = list(records)
        result = PublishResult(attempted=len(events))

        for attempt in range(self.settings.max_retries + 1):
            try:
                response = self._client.put_records(
                    StreamName=self.settings.kinesis_stream_name,
                    Records=pending_records,
                )
            except ClientError:
                logger.exception(
                    "PutRecords failed",
                    extra={
                        "stream": self.settings.kinesis_stream_name,
                        "attempt": attempt,
                        "record_count": len(pending_records),
                    },
                )
                if attempt >= self.settings.max_retries:
                    result.failed += len(pending_events)
                    result.failed_event_ids.extend(e.event_id for e in pending_events)
                    return result
                self._sleep_backoff(attempt)
                continue

            failed_records = []
            failed_events = []
            for idx, record_result in enumerate(response.get("Records", [])):
                if "ErrorCode" in record_result:
                    failed_records.append(pending_records[idx])
                    failed_events.append(pending_events[idx])
                else:
                    result.succeeded += 1

            if not failed_records:
                return result

            logger.warning(
                "Partial PutRecords failure; retrying",
                extra={
                    "failed_count": len(failed_records),
                    "attempt": attempt,
                    "stream": self.settings.kinesis_stream_name,
                },
            )
            pending_records = failed_records
            pending_events = failed_events

            if attempt >= self.settings.max_retries:
                result.failed += len(failed_events)
                result.failed_event_ids.extend(e.event_id for e in failed_events)
                return result

            self._sleep_backoff(attempt)

        return result

    @staticmethod
    def _sleep_backoff(attempt: int) -> None:
        # Exponential backoff with full jitter
        base = min(2**attempt, 8)
        time.sleep(random.uniform(0, base))
