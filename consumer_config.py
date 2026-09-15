"""
consumer_config.py
-------------------
All consumer/worker configuration is environment-driven.
"""

import os


def get_consumer_config(group_id: str | None = None, client_id: str | None = None) -> dict:
    """
    Returns the confluent-kafka consumer configuration dict.

    Commit strategy: manual, synchronous commit AFTER processing each batch
    (enable.auto.commit=False). This gives "at-least-once" semantics, which
    is the right, honest tradeoff for a college demo — we do not claim
    exactly-once here (see docs/kafka_architecture.md, section "Offset
    Management").
    """
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": group_id or os.getenv("CONSUMER_GROUP_ID", "streamforge-workers"),
        "client.id": client_id or os.getenv("WORKER_CLIENT_ID", "streamforge-worker"),

        "auto.offset.reset": os.getenv("CONSUMER_AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,

        # Reasonable session/heartbeat values for a local demo — small enough
        # that rebalances during the "stop a worker" demo happen quickly.
        "session.timeout.ms": int(os.getenv("CONSUMER_SESSION_TIMEOUT_MS", "10000")),
        "heartbeat.interval.ms": int(os.getenv("CONSUMER_HEARTBEAT_INTERVAL_MS", "3000")),
        "max.poll.interval.ms": int(os.getenv("CONSUMER_MAX_POLL_INTERVAL_MS", "300000")),

        # Cooperative-sticky avoids the classic "stop the world" rebalance
        # and is the modern default recommendation.
        "partition.assignment.strategy": os.getenv(
            "CONSUMER_ASSIGNMENT_STRATEGY", "cooperative-sticky"
        ),
    }


def get_default_topic() -> str:
    return os.getenv("TOPIC_TELEMETRY", "truck-telemetry")


def get_default_group_id() -> str:
    return os.getenv("CONSUMER_GROUP_ID", "streamforge-workers")
