"""FastAPI surface for generating payment events for Kinesis."""

from __future__ import annotations

import logging
import random
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from payments_gateway.chaos_simulator import RawEvent, SCENARIOS, generate_raw_event
from payments_gateway.config import Settings
from payments_gateway.producer import PaymentEventProducer, PublishResult

logger = logging.getLogger(__name__)


class GeneratePaymentsRequest(BaseModel):
    """Generate demo events, optionally adding data quality faults or lateness."""

    count: int = Field(default=25, ge=1, le=500)
    bad_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    late_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    late_seconds: float = Field(default=86400, gt=0, le=31536000)
    scenario: Literal[
        "missing",
        "null",
        "blank",
        "invalid-status",
        "invalid-currency",
        "invalid-amount",
        "non-positive-amount",
        "overflow-amount",
        "invalid-timestamp",
        "wrong-type",
        "multiple",
    ] | None = None


class GeneratePaymentsResponse(BaseModel):
    attempted: int
    succeeded: int
    failed: int
    failed_event_ids: list[str]
    corrupted: int
    backdated: int
    sample_events: list[dict[str, Any]]


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
    "/payments/events/generate",
    response_model=GeneratePaymentsResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def generate_payment_events(
    producer: Annotated[PaymentEventProducer, Depends(get_producer)],
    body: GeneratePaymentsRequest | None = None,
) -> GeneratePaymentsResponse:
    """Gera eventos de pagamento e os publica no Kinesis.

    Campos do body:
        count: Quantidade de eventos a gerar (de 1 a 500; padrão: 25).
        bad_ratio: Proporção de eventos que receberá dados inválidos, de 0 a 1.
            Por exemplo, 0.2 significa cerca de 20% dos eventos; como a escolha
            é aleatória, a quantidade exata pode variar. Padrão: 0 (nenhum).
        late_ratio: Proporção de eventos que terá o event_timestamp atrasado,
            também de 0 a 1. É independente de bad_ratio, então um evento pode
            ser inválido e atrasado ao mesmo tempo. Padrão: 0 (nenhum).
        late_seconds: Quanto atrasar o event_timestamp dos eventos selecionados
            por late_ratio, em segundos (maior que 0 e até 365 dias; padrão:
            86400, ou 1 dia). Só tem efeito quando late_ratio é maior que 0.
        scenario: Tipo de dado inválido a gerar nos eventos selecionados por
            bad_ratio. Opções: missing, null, blank, invalid-status,
            invalid-currency, invalid-amount, non-positive-amount,
            overflow-amount, invalid-timestamp, wrong-type e multiple. Se
            omitido, a API alterna entre os cenários. Só tem efeito quando
            bad_ratio é maior que 0.

    Exemplo: ``{"count": 100, "bad_ratio": 0.1, "late_ratio": 0.2,
    "late_seconds": 172800, "scenario": "invalid-amount"}`` gera 100 eventos;
    cerca de 10% terão valor inválido e cerca de 20% terão timestamp atrasado
    em dois dias. Os grupos podem se sobrepor. A resposta informa quantos foram
    publicados, quantos falharam, quantos foram inválidos ou atrasados e inclui
    até cinco eventos de exemplo.
    """
    request = body or GeneratePaymentsRequest()
    events: list[RawEvent] = []
    corrupted = 0
    backdated = 0
    for _ in range(request.count):
        is_corrupted = random.random() < request.bad_ratio
        is_backdated = random.random() < request.late_ratio
        scenario = None
        if is_corrupted:
            scenario = request.scenario or SCENARIOS[corrupted % len(SCENARIOS)]
            corrupted += 1
        backdated += int(is_backdated)
        events.append(
            generate_raw_event(
                late_seconds=request.late_seconds if is_backdated else 0,
                scenario=scenario,
                index=corrupted - 1,
            )
        )
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
        corrupted=corrupted,
        backdated=backdated,
        sample_events=[event.payload for event in events[:5]],
    )
