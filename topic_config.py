"""
topic_config.py
----------------
Central definition of every Kafka topic owned by the Kafka + Architecture
module. Other modules (Stream Processing, RocksDB, FastAPI dashboard) should
treat this file as the single source of truth for topic names, partition
counts and replication factors when they need to know what to connect to.

Do NOT hardcode topic names anywhere else in the codebase — import them
from here instead.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    key: str
    value_format: str
    purpose: str


# ---------------------------------------------------------------------------
# truck-telemetry
#
# Primary ingestion topic. Every raw telemetry event produced by the IoT /
# Python producer lands here. This is the topic the Stream Processing team
# (Faust/Bytewax) consumes from directly.
# ---------------------------------------------------------------------------
TRUCK_TELEMETRY = TopicSpec(
    name=os.getenv("TOPIC_TELEMETRY", "truck-telemetry"),
    partitions=int(os.getenv("TELEMETRY_PARTITIONS", "6")),
    replication_factor=int(os.getenv("REPLICATION_FACTOR", "1")),
    key="truck_id (string)",
    value_format="JSON — {truck_id, temperature, timestamp}",
    purpose=(
        "Primary ingestion topic. Raw truck telemetry events produced by the "
        "Python producer. Consumed by the Stream Processing module."
    ),
)

# ---------------------------------------------------------------------------
# truck-state-changelog
#
# Kafka-side of the RocksDB team's changelog pattern. This module only
# defines/creates the topic and its message contract — RocksDB itself and
# the recovery logic belong to the State Store team.
# ---------------------------------------------------------------------------
TRUCK_STATE_CHANGELOG = TopicSpec(
    name=os.getenv("TOPIC_CHANGELOG", "truck-state-changelog"),
    partitions=int(os.getenv("CHANGELOG_PARTITIONS", "6")),
    replication_factor=int(os.getenv("REPLICATION_FACTOR", "1")),
    key="truck_id (string)",
    value_format=(
        "JSON — {truck_id, window, state: {sum, count}, timestamp}. "
        "Exact shape of 'state' is owned by the RocksDB team."
    ),
    purpose=(
        "Changelog topic used by the State Store (RocksDB) module to persist "
        "and recover per-truck windowed aggregation state. Compaction is "
        "recommended (log.cleanup.policy=compact) once the RocksDB team "
        "finalizes their key strategy."
    ),
)

ALL_TOPICS = [TRUCK_TELEMETRY, TRUCK_STATE_CHANGELOG]


def describe_all() -> str:
    """Return a human-readable summary of every topic this module owns."""
    lines = []
    for t in ALL_TOPICS:
        lines.append(
            f"- {t.name}\n"
            f"    partitions           : {t.partitions}\n"
            f"    replication_factor   : {t.replication_factor}\n"
            f"    key                  : {t.key}\n"
            f"    value_format         : {t.value_format}\n"
            f"    purpose              : {t.purpose}\n"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    print(describe_all())
