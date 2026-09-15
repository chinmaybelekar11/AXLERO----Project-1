"""
worker.py
---------
A Kafka architecture / demo worker that proves the consumer-group
architecture this module owns: subscription, partition assignment,
consumption, manual offset commits, graceful shutdown and rebalance
callbacks.

This worker does NOT perform stream processing, windowing, aggregation or
any of the 5-minute-average logic — that belongs to the Stream Processing
team. It only proves the Kafka plumbing works end to end, and prints
diagnostic output so it's obvious what Kafka is doing.

Run several copies at once (same consumer group) to see partitions get
distributed, then Ctrl+C one of them to see a live rebalance:

    # terminal 1
    python consumer/worker.py --id worker-1
    # terminal 2
    python consumer/worker.py --id worker-2
    # terminal 3
    python consumer/worker.py --id worker-3
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from consumer.consumer_config import get_consumer_config, get_default_topic  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)


class TelemetryWorker:
    def __init__(self, worker_id: str, topic: str | None = None, group_id: str | None = None):
        self.worker_id = worker_id
        self.topic = topic or get_default_topic()
        self.logger = logging.getLogger(f"worker.{worker_id}")

        config = get_consumer_config(group_id=group_id, client_id=worker_id)
        self.group_id = config["group.id"]
        self.consumer = Consumer(config)

        self._running = False
        self._messages_processed = 0
        self._commit_every = int(os.getenv("WORKER_COMMIT_EVERY_N", "50"))

    # ------------------------------------------------------------------
    # Rebalance callbacks — this is the core proof that Kafka's real
    # consumer-group protocol is being used, not a simulation.
    # ------------------------------------------------------------------
    def _on_assign(self, consumer, partitions):
        self.logger.info(
            "Partitions assigned: %s",
            [f"{p.topic}[{p.partition}]" for p in partitions],
        )

    def _on_revoke(self, consumer, partitions):
        self.logger.info(
            "Partitions revoked: %s",
            [f"{p.topic}[{p.partition}]" for p in partitions],
        )
        # Commit whatever we've processed before giving up the partitions.
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            self.logger.warning("Commit during revoke failed (non-fatal for demo): %s", e)

    def _on_lost(self, consumer, partitions):
        self.logger.warning(
            "Partitions lost (no time to commit): %s",
            [f"{p.topic}[{p.partition}]" for p in partitions],
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self):
        self.consumer.subscribe(
            [self.topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        self._running = True
        self.logger.info(
            "Worker started. id=%s group=%s topic=%s",
            self.worker_id, self.group_id, self.topic,
        )

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        try:
            self._poll_loop()
        finally:
            self._shutdown()

    def _handle_shutdown(self, signum, frame):
        self.logger.info("Shutdown signal received — finishing current batch...")
        self._running = False

    def _poll_loop(self):
        since_last_commit = 0
        while self._running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                self.logger.error("Consumer error: %s", msg.error())
                continue

            self._process_message(msg)
            self._messages_processed += 1
            since_last_commit += 1

            # Manual, periodic sync commit -> at-least-once semantics.
            if since_last_commit >= self._commit_every:
                self.consumer.commit(asynchronous=False)
                since_last_commit = 0

    def _process_message(self, msg):
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.logger.warning("Skipping malformed message at offset %d", msg.offset())
            return

        self.logger.info(
            "Received event | partition=%d offset=%d truck_id=%s temperature=%s",
            msg.partition(), msg.offset(),
            event.get("truck_id"), event.get("temperature"),
        )
        # NOTE: This worker deliberately does NOT compute the 5-minute
        # rolling average. That logic belongs to the Stream Processing
        # module and consumes this same topic independently.

    def _shutdown(self):
        self.logger.info(
            "Worker '%s' shutting down. Total messages processed: %d",
            self.worker_id, self._messages_processed,
        )
        try:
            self.consumer.commit(asynchronous=False)
        except KafkaException:
            pass
        self.consumer.close()


def parse_args():
    parser = argparse.ArgumentParser(description="StreamForge Kafka demo worker")
    parser.add_argument("--id", type=str, default=f"worker-{int(time.time())}", help="Worker/client id")
    parser.add_argument("--topic", type=str, default=None, help="Topic to subscribe to")
    parser.add_argument("--group", type=str, default=None, help="Consumer group id")
    return parser.parse_args()


def main():
    args = parse_args()
    worker = TelemetryWorker(worker_id=args.id, topic=args.topic, group_id=args.group)
    worker.start()


if __name__ == "__main__":
    main()
