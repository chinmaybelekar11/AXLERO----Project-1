"""
telemetry_producer.py
----------------------
Kafka producer that generates mock truck telemetry events and publishes them
to the `truck-telemetry` topic using confluent-kafka.

Design decisions (see docs/kafka_architecture.md for full rationale):
  * Message key = truck_id  -> guarantees all events for a given truck land
    on the same partition, which downstream per-truck workers rely on.
  * Value = JSON  -> simple, human-readable, easy for every team member's
    language/library of choice to parse.

Usage:
    python producer/telemetry_producer.py --events 1000 --trucks 20
    python producer/telemetry_producer.py --events 100000 --trucks 50 --rate 0   # max speed
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producer.producer_config import get_producer_config, get_default_topic  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("telemetry_producer")


class TelemetryProducer:
    """Reusable wrapper around confluent_kafka.Producer for truck telemetry."""

    def __init__(self, topic: str | None = None, config: dict | None = None):
        self.topic = topic or get_default_topic()
        self.config = config or get_producer_config()
        self.producer = Producer(self.config)

        self._delivered = 0
        self._failed = 0

    # ------------------------------------------------------------------
    # Delivery handling
    # ------------------------------------------------------------------
    def _delivery_callback(self, err, msg):
        if err is not None:
            self._failed += 1
            logger.error("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            self._delivered += 1

    # ------------------------------------------------------------------
    # Event generation
    # ------------------------------------------------------------------
    @staticmethod
    def generate_event(truck_id: str) -> dict:
        """Generate one mock telemetry event for a given truck."""
        return {
            "truck_id": truck_id,
            "temperature": round(random.uniform(-5.0, 45.0), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

    @staticmethod
    def truck_ids(count: int) -> list[str]:
        return [f"TRUCK-{i:03d}" for i in range(1, count + 1)]

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    def send(self, event: dict) -> None:
        """Publish a single event, keyed by truck_id, value serialized as JSON."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=str(event["truck_id"]).encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=self._delivery_callback,
            )
            # Serve delivery-report callbacks from previous produce() calls
            # without blocking. This keeps the internal librdkafka queue
            # from filling up during bursts.
            self.producer.poll(0)
        except BufferError:
            logger.warning("Local producer queue is full — waiting for space.")
            self.producer.poll(1)
            self.send(event)

    def run(self, num_events: int, num_trucks: int, rate_per_sec: float = 0.0) -> None:
        """
        Publish `num_events` mock telemetry events spread across `num_trucks`
        distinct trucks.

        rate_per_sec: if > 0, throttle to roughly this many events/sec.
                      if 0, publish as fast as possible (used by the benchmark).
        """
        trucks = self.truck_ids(num_trucks)
        interval = (1.0 / rate_per_sec) if rate_per_sec > 0 else 0.0

        logger.info(
            "Publishing %d events across %d trucks to topic '%s' (rate=%s)",
            num_events, num_trucks, self.topic,
            "unlimited" if rate_per_sec <= 0 else f"{rate_per_sec}/sec",
        )

        start = time.perf_counter()
        for i in range(num_events):
            truck_id = trucks[i % num_trucks]
            event = self.generate_event(truck_id)
            self.send(event)
            if interval:
                time.sleep(interval)

        self.flush()
        elapsed = time.perf_counter() - start
        logger.info(
            "Done. delivered=%d failed=%d elapsed=%.2fs (%.0f events/sec)",
            self._delivered, self._failed, elapsed,
            num_events / elapsed if elapsed > 0 else 0,
        )

    def flush(self, timeout: float = 30.0) -> None:
        """Block until all outstanding messages are delivered or timeout."""
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning("%d messages were not delivered before flush timeout.", remaining)

    @property
    def stats(self) -> dict:
        return {"delivered": self._delivered, "failed": self._failed}


def parse_args():
    parser = argparse.ArgumentParser(description="StreamForge telemetry producer")
    parser.add_argument("--events", type=int, default=1000, help="Total number of events to publish")
    parser.add_argument("--trucks", type=int, default=20, help="Number of distinct truck IDs to simulate")
    parser.add_argument("--rate", type=float, default=0.0, help="Target events/sec (0 = max speed)")
    parser.add_argument("--topic", type=str, default=None, help="Override target topic")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = TelemetryProducer(topic=args.topic)
    try:
        producer.run(num_events=args.events, num_trucks=args.trucks, rate_per_sec=args.rate)
    except KeyboardInterrupt:
        logger.info("Interrupted — flushing remaining messages...")
        producer.flush()


if __name__ == "__main__":
    main()
