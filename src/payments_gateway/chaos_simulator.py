"""Inject raw payment quality faults and old event times into the existing stream."""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import simplejson

from payments_gateway.config import Settings
from payments_gateway.producer import PaymentEventProducer
from payments_gateway.simulator import generate_event

REQUIRED_FIELDS = (
    "event_id",
    "merchant_id",
    "user_id",
    "amount",
    "status",
    "event_timestamp",
)
SCENARIOS = (
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
)


@dataclass(frozen=True)
class RawEvent:
    """Keep wire routing independent of intentionally broken payload identifiers."""

    payload: dict[str, Any]
    event_id: str
    routing_key: str

    def partition_key(self) -> str:
        return self.routing_key

    def to_kinesis_data(self) -> bytes:
        return (
            simplejson.dumps(
                self.payload,
                use_decimal=True,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")


def corrupt_payload(payload: dict[str, Any], scenario: str, index: int) -> None:
    """Apply a reproducible fault; cycle through fields/types across bad events."""
    field = REQUIRED_FIELDS[index % len(REQUIRED_FIELDS)]
    if scenario == "missing":
        payload.pop(field, None)
    elif scenario == "null":
        payload[field] = None
    elif scenario == "blank":
        payload[field] = "   "
    elif scenario == "invalid-status":
        payload["status"] = "pending_unknown"
    elif scenario == "invalid-currency":
        payload["currency"] = "ZZZ"
    elif scenario == "invalid-amount":
        payload["amount"] = "not-a-number"
    elif scenario == "non-positive-amount":
        payload["amount"] = "-12.50"
    elif scenario == "overflow-amount":
        payload["amount"] = "10000000000000000.00"
    elif scenario == "invalid-timestamp":
        payload["event_timestamp"] = "not-a-timestamp"
    elif scenario == "wrong-type":
        payload[field] = {"unexpected": [1, None, "bad"]}
    elif scenario == "multiple":
        payload.pop("user_id", None)
        payload.update(merchant_id=None, amount="bad", status="unknown", currency="ZZZ")
    else:
        raise ValueError(f"Unknown scenario: {scenario}")


def generate_raw_event(
    *, late_seconds: float = 0, scenario: str | None = None, index: int = 0
) -> RawEvent:
    """Start with a valid event, backdate it, then optionally corrupt its payload."""
    event = generate_event()
    payload = event.model_dump(mode="json")
    payload["amount"] = event.amount
    timestamp = datetime.now(timezone.utc) - timedelta(seconds=late_seconds)
    payload["event_timestamp"] = timestamp.isoformat()
    if scenario is not None:
        corrupt_payload(payload, scenario, index)
    return RawEvent(payload, event.event_id, event.merchant_id)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count", type=int, default=100, help="Events attempted (default: 100)"
    )
    parser.add_argument(
        "--rate", type=float, default=10, help="Events/second (default: 10)"
    )
    parser.add_argument(
        "--bad-ratio", type=float, default=0.3, help="Fault probability, 0..1"
    )
    parser.add_argument(
        "--late-ratio", type=float, default=0.2, help="Late probability, 0..1"
    )
    parser.add_argument(
        "--late-seconds",
        type=float,
        default=86400,
        help="Event-time delay (default: 86400 = 1 day)",
    )
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        help="Use one fault; default cycles through all faults",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write NDJSON to stdout without AWS or credentials",
    )
    return parser


def run_simulator(
    args: argparse.Namespace, producer: PaymentEventProducer | None = None
) -> int:
    """Send a finite number of attempts, reusing the producer's partial retries."""
    if not args.dry_run and producer is None:
        settings = Settings.from_env()
        if settings.batch_size <= 0 or settings.max_retries < 0:
            raise ValueError("Batch size must be positive and retries non-negative")
        producer = PaymentEventProducer(settings=settings)
    batch_size = min(producer.settings.batch_size, 100) if producer else 100
    batch: list[RawEvent] = []
    counts: Counter[str] = Counter()
    bad_index = 0

    def flush() -> None:
        if producer and batch:
            result = producer.publish(batch)
            counts["published"] += result.succeeded
            counts["failed"] += result.failed
        batch.clear()

    started = time.monotonic()
    try:
        for index in range(args.count):
            if not args.dry_run:
                time.sleep(max(0, started + index / args.rate - time.monotonic()))
            late = random.random() < args.late_ratio
            bad = random.random() < args.bad_ratio
            scenario = None
            if bad:
                scenario = args.scenario or SCENARIOS[bad_index % len(SCENARIOS)]
            event = generate_raw_event(
                late_seconds=args.late_seconds if late else 0,
                scenario=scenario,
                index=bad_index,
            )
            bad_index += int(bad)
            counts["generated"] += 1
            counts["backdated"] += int(late)
            counts["corrupted"] += int(bad)
            if scenario:
                counts[f"scenario:{scenario}"] += 1
            if args.dry_run:
                sys.stdout.write(event.to_kinesis_data().decode("utf-8"))
            else:
                batch.append(event)
                if len(batch) >= batch_size:
                    flush()
    except KeyboardInterrupt:
        counts["interrupted"] = 1
    finally:
        flush()
    print(json.dumps(dict(counts), sort_keys=True), file=sys.stderr)
    return 130 if counts["interrupted"] else int(counts["failed"] > 0)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.count <= 0:
        parser.error("--count must be > 0")
    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("--rate must be finite and > 0")
    for name in ("bad_ratio", "late_ratio"):
        if not 0 <= getattr(args, name) <= 1:
            parser.error(f"--{name.replace('_', '-')} must be between 0 and 1")
    if not math.isfinite(args.late_seconds) or not 0 < args.late_seconds <= 31536000:
        parser.error("--late-seconds must be > 0 and <= 31536000 (365 days)")
    return run_simulator(args)


if __name__ == "__main__":
    raise SystemExit(main())
