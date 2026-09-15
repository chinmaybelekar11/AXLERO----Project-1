"""
producer_config.py
-------------------
All producer configuration is environment-driven so the same code runs
locally, in CI, or on a teammate's machine without code changes.
"""

import os


def get_producer_config() -> dict:
    """
    Returns the confluent-kafka producer configuration dict.

    Tuned for a local college-demo throughput benchmark (~100k events/sec
    target) without adding unnecessary production hardening (no SASL/SSL,
    no idempotence tuning beyond sane defaults).
    """
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": os.getenv("PRODUCER_CLIENT_ID", "streamforge-telemetry-producer"),

        # Batch aggressively for throughput — safe for a local demo.
        "linger.ms": int(os.getenv("PRODUCER_LINGER_MS", "5")),
        "batch.size": int(os.getenv("PRODUCER_BATCH_SIZE", "131072")),  # 128 KB
        "compression.type": os.getenv("PRODUCER_COMPRESSION", "lz4"),

        # acks=1: leader-ack is enough for a college demo; avoids paying the
        # latency cost of full ISR acknowledgement, which is not needed here.
        "acks": os.getenv("PRODUCER_ACKS", "1"),

        "queue.buffering.max.messages": int(os.getenv("PRODUCER_QUEUE_MAX_MESSAGES", "500000")),
        "queue.buffering.max.kbytes": int(os.getenv("PRODUCER_QUEUE_MAX_KBYTES", "1048576")),

        "retries": int(os.getenv("PRODUCER_RETRIES", "3")),
        "retry.backoff.ms": int(os.getenv("PRODUCER_RETRY_BACKOFF_MS", "100")),
    }


def get_default_topic() -> str:
    return os.getenv("TOPIC_TELEMETRY", "truck-telemetry")
