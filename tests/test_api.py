"""API tests for POST /payments/events."""

from __future__ import annotations

from fastapi.testclient import TestClient

from payments_gateway.api import app, get_producer
from payments_gateway.models import PaymentEvent, PaymentStatus
from payments_gateway.producer import PublishResult


class StubProducer:
    def __init__(self, result: PublishResult | None = None) -> None:
        self.result = result or PublishResult(attempted=1, succeeded=1, failed=0)
        self.published: list[PaymentEvent] = []

    def publish_one(self, event: PaymentEvent) -> PublishResult:
        self.published.append(event)
        if self.result.failed:
            self.result.failed_event_ids = [event.event_id]
        return self.result

    def publish(self, events: list[PaymentEvent]) -> PublishResult:
        self.published.extend(events)
        if self.result.failed and not self.result.failed_event_ids:
            self.result.failed_event_ids = [e.event_id for e in events]
        if self.result.attempted == 1 and len(events) > 1:
            # Default stub success path for batch generate
            return PublishResult(
                attempted=len(events),
                succeeded=len(events),
                failed=0,
                failed_event_ids=[],
            )
        return PublishResult(
            attempted=len(events),
            succeeded=0 if self.result.failed else len(events),
            failed=len(events) if self.result.failed else 0,
            failed_event_ids=(
                [e.event_id for e in events] if self.result.failed else []
            ),
        )


def test_health():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_create_payment_event_accepted():
    stub = StubProducer()
    app.dependency_overrides[get_producer] = lambda: stub
    client = TestClient(app)

    try:
        response = client.post(
            "/payments/events",
            json={
                "merchant_id": "mrc_0001",
                "user_id": "usr_000001",
                "amount": 25.5,
                "status": "captured",
            },
        )
        assert response.status_code == 202
        body = response.json()
        assert body["succeeded"] == 1
        assert body["event"]["merchant_id"] == "mrc_0001"
        assert body["event"]["status"] == PaymentStatus.CAPTURED.value
        assert body["event"]["amount"] == "25.5"
        assert body["event"]["schema_version"] == 3
        assert "amount_usd" not in body["event"]
        assert len(stub.published) == 1
    finally:
        app.dependency_overrides.clear()


def test_create_payment_event_kinesis_failure():
    stub = StubProducer(PublishResult(attempted=1, succeeded=0, failed=1))
    app.dependency_overrides[get_producer] = lambda: stub
    client = TestClient(app)

    try:
        response = client.post(
            "/payments/events",
            json={
                "merchant_id": "mrc_0001",
                "user_id": "usr_000001",
                "amount": 25.5,
                "status": "failed",
            },
        )
        assert response.status_code == 502
    finally:
        app.dependency_overrides.clear()


def test_create_payment_event_validation_error():
    stub = StubProducer()
    app.dependency_overrides[get_producer] = lambda: stub
    client = TestClient(app)

    try:
        response = client.post(
            "/payments/events",
            json={
                "merchant_id": "mrc_0001",
                "user_id": "usr_000001",
                "amount": -1,
                "status": "captured",
            },
        )
        assert response.status_code == 422
    finally:
        app.dependency_overrides.clear()


def test_generate_payment_events():
    stub = StubProducer()
    app.dependency_overrides[get_producer] = lambda: stub
    client = TestClient(app)

    try:
        response = client.post(
            "/payments/events/generate",
            json={"count": 10},
        )
        assert response.status_code == 202
        body = response.json()
        assert body["attempted"] == 10
        assert body["succeeded"] == 10
        assert body["failed"] == 0
        assert len(body["sample_events"]) == 5
        assert len(stub.published) == 10
        assert stub.published[0].ingest_source == "payment_gateway"
    finally:
        app.dependency_overrides.clear()


def test_generate_payment_events_default_count():
    stub = StubProducer()
    app.dependency_overrides[get_producer] = lambda: stub
    client = TestClient(app)

    try:
        response = client.post("/payments/events/generate")
        assert response.status_code == 202
        body = response.json()
        assert body["attempted"] == 25
        assert body["succeeded"] == 25
    finally:
        app.dependency_overrides.clear()


def test_legacy_only_amount_request_is_rejected():
    stub = StubProducer()
    app.dependency_overrides[get_producer] = lambda: stub
    try:
        response = TestClient(app).post(
            "/payments/events",
            json={
                "merchant_id": "m",
                "user_id": "u",
                "amount_usd": 10,
                "status": "captured",
            },
        )
        assert response.status_code == 422
        assert stub.published == []
    finally:
        app.dependency_overrides.clear()
