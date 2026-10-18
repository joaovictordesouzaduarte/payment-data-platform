"""API tests for generated payment events."""

from __future__ import annotations

from fastapi.testclient import TestClient

from payments_gateway.api import app, get_producer
from payments_gateway.producer import PublishResult


class StubProducer:
    def __init__(self, result: PublishResult | None = None) -> None:
        self.result = result or PublishResult(attempted=1, succeeded=1, failed=0)
        self.published = []

    def publish(self, events) -> PublishResult:
        self.published.extend(events)
        if self.result.failed:
            failed_ids = [event.event_id for event in events]
            self.result.failed_event_ids = failed_ids
            return PublishResult(
                attempted=len(events),
                succeeded=0,
                failed=len(events),
                failed_event_ids=failed_ids,
            )
        return PublishResult(
            attempted=len(events),
            succeeded=len(events),
            failed=0,
            failed_event_ids=[],
        )


def test_health():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_generate_payment_events_kinesis_failure():
    stub = StubProducer(PublishResult(attempted=1, succeeded=0, failed=1))
    app.dependency_overrides[get_producer] = lambda: stub
    client = TestClient(app)

    try:
        response = client.post(
            "/payments/events/generate",
            json={"count": 1},
        )
        assert response.status_code == 502
    finally:
        app.dependency_overrides.clear()

def test_generate_payment_events_validation_error():
    stub = StubProducer()
    app.dependency_overrides[get_producer] = lambda: stub
    client = TestClient(app)

    try:
        response = client.post(
            "/payments/events/generate",
            json={"count": 1, "bad_ratio": 1.1},
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
            json={
                "count": 10,
                "bad_ratio": 1,
                "late_ratio": 1,
                "late_seconds": 172800,
                "scenario": "invalid-amount",
            },
        )
        assert response.status_code == 202
        body = response.json()
        assert body["attempted"] == 10
        assert body["succeeded"] == 10
        assert body["failed"] == 0
        assert body["corrupted"] == 10
        assert body["backdated"] == 10
        assert len(body["sample_events"]) == 5
        assert len(stub.published) == 10
        assert stub.published[0].payload["ingest_source"] == "payment_gateway"
        assert stub.published[0].payload["amount"] == "not-a-number"
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
