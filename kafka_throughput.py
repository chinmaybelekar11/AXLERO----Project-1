"""
kafka_throughput.py
--------------------
Producer throughput benchmark for the truck-telemetry topic.

Measures ACTUAL delivered throughput — never hardcodes a result. If the
local machine can't hit the project's mid-review target (~100,000
events/sec) the real, measured number is what gets reported.

Usage:
    python benchmark/kafka_throughput.py --events 500000 --trucks 100
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producer.producer_config import get_producer_config, get_default_topic  # noqa: E402
from kafka.kafka_admin import get_admin_client, describe_topic  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka_throughput")


class ThroughputBenchmark:
    def __init__(self, topic: str | None = None):
        self.topic = topic or get_default_topic()
        self.producer = Producer(get_producer_config())
        self.delivered = 0
        self.failed = 0

    def _callback(self, err, msg):
        if err is not None:
            self.failed += 1
        else:
            self.delivered += 1

    def run(self, num_events: int, num_trucks: int) -> dict:
        trucks = [f"TRUCK-{i:03d}" for i in range(1, num_trucks + 1)]

        logger.info(
            "Starting benchmark: %d events, %d trucks, topic='%s'",
            num_events, num_trucks, self.topic,
        )

        start = time.perf_counter()
        for i in range(num_events):
            truck_id = trucks[i % num_trucks]
            event = {
                "truck_id": truck_id,
                "temperature": round(20 + (i % 25) * 0.7, 2),
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=truck_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=self._callback,
                )
            except BufferError:
                self.producer.poll(0.1)
                self.producer.produce(
                    topic=self.topic,
                    key=truck_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=self._callback,
                )

            # Service the delivery-report queue periodically without
            # blocking the main send loop.
            if i % 1000 == 0:
                self.producer.poll(0)

        remaining = self.producer.flush(60)
        elapsed = time.perf_counter() - start

        events_per_sec = self.delivered / elapsed if elapsed > 0 else 0

        partition_count = self._get_partition_count()

        result = {
            "total_events_requested": num_events,
            "total_events_delivered": self.delivered,
            "total_events_failed": self.failed,
            "undelivered_after_flush_timeout": remaining,
            "elapsed_seconds": round(elapsed, 3),
            "events_per_sec": round(events_per_sec, 2),
            "partition_count": partition_count,
        }
        return result

    def _get_partition_count(self) -> int | None:
        try:
            admin = get_admin_client()
            metadata = admin.list_topics(topic=self.topic, timeout=10)
            if self.topic in metadata.topics:
                return len(metadata.topics[self.topic].partitions)
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not fetch partition count: %s", e)
        return None


def print_report(result: dict):
    print("\n" + "=" * 50)
    print(" KAFKA PRODUCER THROUGHPUT BENCHMARK — RESULT")
    print("=" * 50)
    for k, v in result.items():
        print(f"  {k:32s}: {v}")
    print("=" * 50)
    target = 100_000
    pct = (result["events_per_sec"] / target) * 100 if target else 0
    print(f"  Mid-review target reference    : {target} events/sec")
    print(f"  Achieved                       : {pct:.1f}% of target")
    print("  NOTE: this is the real measured number on this machine.")
    print("=" * 50 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka producer throughput benchmark")
    parser.add_argument("--events", type=int, default=200000, help="Number of events to send")
    parser.add_argument("--trucks", type=int, default=100, help="Number of distinct truck IDs")
    parser.add_argument("--topic", type=str, default=None, help="Override target topic")
    return parser.parse_args()


def main():
    args = parse_args()
    bench = ThroughputBenchmark(topic=args.topic)
    result = bench.run(num_events=args.events, num_trucks=args.trucks)
    print_report(result)


if __name__ == "__main__":
    main()
