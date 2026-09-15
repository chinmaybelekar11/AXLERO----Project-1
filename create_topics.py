"""
create_topics.py
-----------------
Creates every topic this module owns (truck-telemetry, truck-state-changelog)
with the partition counts / replication factors defined in topic_config.py.

Usage:
    python kafka/create_topics.py
    python kafka/create_topics.py --recreate     # delete + recreate (dev only)
"""

import argparse
import sys
import time

from dotenv import load_dotenv

load_dotenv()

from kafka_admin import (  # noqa: E402
    get_admin_client,
    create_topics,
    delete_topic,
    list_topics,
    check_broker_connectivity,
    logger,
)
from topic_config import ALL_TOPICS  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Create StreamForge Kafka topics")
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Delete existing topics before recreating them (local dev only).",
    )
    args = parser.parse_args()

    admin = get_admin_client()

    if not check_broker_connectivity(admin):
        logger.error("Cannot reach Kafka broker. Is it running? See docs/kafka_architecture.md")
        return 1

    existing = list_topics(admin)

    if args.recreate:
        for spec in ALL_TOPICS:
            if spec.name in existing:
                logger.info("Recreate requested — deleting '%s'", spec.name)
                delete_topic(admin, spec.name)
        # Kafka needs a moment to fully propagate deletion before recreation
        time.sleep(3)

    create_topics(admin, ALL_TOPICS)

    logger.info("Topic setup complete. Current topics on cluster:")
    for t in list_topics(admin):
        print(f"  - {t}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
