"""Thin FastAPI surface for injecting payment events into Kinesis."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from payments_gateway.config import Settings
from payments_gateway.models import Money, PaymentEvent, PaymentStatus
from payments_gateway.producer import PaymentEventProducer, PublishResult
from payments_gateway.simulator import generate_event

logger = logging.getLogger(__name__)


class CreatePaymentRequest(BaseModel):
    """Inbound payment event payload (event_id optional — generated if omitted)."""

    merchant_id: str
    user_id: str
    amount: Money
    currency: str = Field(default="USD", min_length=3, max_length=3)
    status: PaymentStatus
    event_id: str | None = None
    schema_version: int = Field(default=3, ge=3)


class GeneratePaymentsRequest(BaseModel):
    """Auto-generate demo payment events and publish them to Kinesis."""

    count: int = Field(default=25, ge=1, le=500)


class PublishResponse(BaseModel):
    attempted: int
    succeeded: int
    failed: int
    failed_event_ids: list[str]
    event: PaymentEvent


class GeneratePaymentsResponse(BaseModel):
    attempted: int
    succeeded: int
    failed: int
    failed_event_ids: list[str]
    sample_events: list[PaymentEvent]


def get_settings() -> Settings:
    return Settings.from_env()


def get_producer(
    settings: Annotated[Settings, Depends(get_settings)],
) -> PaymentEventProducer:
    return PaymentEventProducer(settings=settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info(
        "Payments gateway API starting",
        extra={"stream": settings.kinesis_stream_name, "region": settings.aws_region},
    )
    yield


app = FastAPI(
    title="Payment Data Platform",
    description="Payment event injector → Kinesis (source) → Firehose → S3 bronze",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/payments/events",
    response_model=PublishResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_payment_event(
    body: CreatePaymentRequest,
    producer: Annotated[PaymentEventProducer, Depends(get_producer)],
) -> PublishResponse:
    payload = body.model_dump(exclude_none=True)
    event = PaymentEvent(**payload)
    result: PublishResult = producer.publish_one(event)

    if result.failed:
        logger.error(
            "Failed to publish payment event",
            extra={"event_id": event.event_id, "failed": result.failed},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "Failed to publish event to Kinesis",
                "event_id": event.event_id,
                "failed_event_ids": result.failed_event_ids,
            },
        )

    return PublishResponse(
        attempted=result.attempted,
        succeeded=result.succeeded,
        failed=result.failed,
        failed_event_ids=result.failed_event_ids,
        event=event,
    )


@app.post(
    "/payments/events/generate",
    response_model=GeneratePaymentsResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def generate_payment_events(
    producer: Annotated[PaymentEventProducer, Depends(get_producer)],
    body: GeneratePaymentsRequest | None = None,
) -> GeneratePaymentsResponse:
    """Generate fake payment events and push them to Kinesis (portfolio demo)."""
    request = body or GeneratePaymentsRequest()
    events = [generate_event() for _ in range(request.count)]
    result = producer.publish(events)

    if result.succeeded == 0 and result.failed > 0:
        logger.error(
            "Failed to publish generated payment events",
            extra={"attempted": result.attempted, "failed": result.failed},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "Failed to publish generated events to Kinesis",
                "failed_event_ids": result.failed_event_ids,
            },
        )

    return GeneratePaymentsResponse(
        attempted=result.attempted,
        succeeded=result.succeeded,
        failed=result.failed,
        failed_event_ids=result.failed_event_ids,
        sample_events=events[:5],
    )
