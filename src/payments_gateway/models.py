"""Payment event data contract for bronze landing."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Money = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]


class PaymentStatus(str, Enum):
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    FAILED = "failed"
    REFUNDED = "refunded"


class PaymentEvent(BaseModel):
    """Canonical payment gateway event published to Kinesis."""

    model_config = ConfigDict(extra="allow")

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    merchant_id: str
    user_id: str
    amount: Money
    currency: str = Field(default="USD", min_length=3, max_length=3)
    status: PaymentStatus
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    schema_version: int = Field(default=3, ge=3)
    ingest_source: str = Field(default="payment_gateway")

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_amount(cls, value: Any) -> Any:
        """Legacy amounts are read from the lake, never published by this producer."""
        if isinstance(value, dict) and "amount_usd" in value:
            raise ValueError("Use amount in the transaction currency, not amount_usd")
        return value

    @field_validator("currency")
    @classmethod
    def currency_upper(cls, value: str) -> str:
        return value.upper()

    @field_validator("event_timestamp")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def partition_key(self) -> str:
        """Route a merchant's records to the same shard; PutRecords may reorder."""
        return self.merchant_id

    def to_kinesis_data(self) -> bytes:
        """Serialize as newline-delimited JSON bytes for Firehose aggregation."""
        return (self.model_dump_json() + "\n").encode("utf-8")

    @classmethod
    def json_schema_export(cls) -> dict[str, Any]:
        return cls.model_json_schema()
