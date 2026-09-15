"""
kafka_admin.py
---------------
Thin wrapper around confluent_kafka.admin.AdminClient used by every other
script in this module (create_topics.py, tests, scripts) so that admin
operations (create / list / describe / delete topics) live in one place.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Iterable

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from confluent_kafka import KafkaException

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("kafka_admin")


def get_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_admin_client(bootstrap_servers: str | None = None) -> AdminClient:
    bootstrap_servers = bootstrap_servers or get_bootstrap_servers()
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def create_topic(admin: AdminClient, name: str, partitions: int, replication_factor: int) -> None:
    """Create a single topic. Safe to call if the topic already exists."""
    new_topic = NewTopic(
        topic=name,
        num_partitions=partitions,
        replication_factor=replication_factor,
    )
    futures = admin.create_topics([new_topic], request_timeout=15)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Created topic '%s' (partitions=%d, rf=%d)", topic, partitions, replication_factor)
        except KafkaException as e:
            # TOPIC_ALREADY_EXISTS is not an error for our purposes
            if "already exists" in str(e).lower():
                logger.info("Topic '%s' already exists — skipping.", topic)
            else:
                logger.error("Failed to create topic '%s': %s", topic, e)
                raise


def create_topics(admin: AdminClient, specs: Iterable) -> None:
    for spec in specs:
        create_topic(admin, spec.name, spec.partitions, spec.replication_factor)


def list_topics(admin: AdminClient, timeout: float = 10.0) -> list[str]:
    metadata = admin.list_topics(timeout=timeout)
    return sorted(metadata.topics.keys())


def describe_topic(admin: AdminClient, name: str, timeout: float = 10.0) -> None:
    metadata = admin.list_topics(topic=name, timeout=timeout)
    if name not in metadata.topics:
        logger.warning("Topic '%s' not found.", name)
        return

    topic_meta = metadata.topics[name]
    if topic_meta.error is not None:
        logger.error("Error describing topic '%s': %s", name, topic_meta.error)
        return

    print(f"\nTopic: {name}")
    print(f"  Partition count: {len(topic_meta.partitions)}")
    for pid, pmeta in sorted(topic_meta.partitions.items()):
        print(
            f"  Partition {pid}: leader={pmeta.leader} "
            f"replicas={pmeta.replicas} isrs={pmeta.isrs}"
        )


def delete_topic(admin: AdminClient, name: str) -> None:
    futures = admin.delete_topics([name], operation_timeout=30)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Deleted topic '%s'", topic)
        except KafkaException as e:
            logger.error("Failed to delete topic '%s': %s", topic, e)
            raise


def check_broker_connectivity(admin: AdminClient, timeout: float = 10.0) -> bool:
    """Return True if we can reach the cluster and fetch metadata."""
    try:
        metadata = admin.list_topics(timeout=timeout)
        logger.info(
            "Connected to Kafka cluster. Brokers: %s",
            [f"{b.id}@{b.host}:{b.port}" for b in metadata.brokers.values()],
        )
        return True
    except KafkaException as e:
        logger.error("Could not connect to Kafka cluster: %s", e)
        return False


if __name__ == "__main__":
    # Simple CLI: python kafka_admin.py [list|describe <topic>|delete <topic>|health]
    admin = get_admin_client()
    args = sys.argv[1:]

    if not args or args[0] == "health":
        ok = check_broker_connectivity(admin)
        sys.exit(0 if ok else 1)
    elif args[0] == "list":
        for t in list_topics(admin):
            print(t)
    elif args[0] == "describe" and len(args) > 1:
        describe_topic(admin, args[1])
    elif args[0] == "delete" and len(args) > 1:
        delete_topic(admin, args[1])
    else:
        print("Usage: python kafka_admin.py [health|list|describe <topic>|delete <topic>]")
