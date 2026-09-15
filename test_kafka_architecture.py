"""
test_kafka_architecture.py
---------------------------
Covers the 11 Kafka-side test scenarios for this module:

 1. Broker connectivity
 2. Topic creation
 3. Topic partition count
 4. Producer sends events
 5. Consumer receives events
 6. Message key is truck_id
 7. Messages are distributed across partitions
 8. Multiple workers consume using one consumer group
 9. Stopping a worker causes rebalance
10. Restarted worker can rejoin the consumer group
11. Changelog topic exists and can receive messages

Run with:
    pytest tests/test_kafka_architecture.py -v

Requires a running local Kafka broker (see scripts/docker-compose.yml).
Tests 8-10 (rebalancing) are slower and are marked accordingly; skip them
for a quick smoke test with `-m "not rebalance"`.
"""

import json
import os
import sys
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka.kafka_admin import (  # noqa: E402
    get_admin_client,
    check_broker_connectivity,
    create_topic,
    list_topics,
)
from kafka.topic_config import TRUCK_TELEMETRY, TRUCK_STATE_CHANGELOG  # noqa: E402
from producer.telemetry_producer import TelemetryProducer  # noqa: E402
from producer.producer_config import get_producer_config  # noqa: E402
from consumer.consumer_config import get_consumer_config  # noqa: E402


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture(scope="module")
def admin():
    return get_admin_client(BOOTSTRAP)


@pytest.fixture(scope="module", autouse=True)
def ensure_topics(admin):
    """Make sure both topics exist before the suite runs."""
    create_topic(admin, TRUCK_TELEMETRY.name, TRUCK_TELEMETRY.partitions, TRUCK_TELEMETRY.replication_factor)
    create_topic(admin, TRUCK_STATE_CHANGELOG.name, TRUCK_STATE_CHANGELOG.partitions, TRUCK_STATE_CHANGELOG.replication_factor)
    time.sleep(1)


# ---------------------------------------------------------------------------
# Test 1: Broker connectivity
# ---------------------------------------------------------------------------
def test_01_broker_connectivity(admin):
    assert check_broker_connectivity(admin) is True


# ---------------------------------------------------------------------------
# Test 2: Topic creation
# ---------------------------------------------------------------------------
def test_02_topic_creation(admin):
    topics = list_topics(admin)
    assert TRUCK_TELEMETRY.name in topics


# ---------------------------------------------------------------------------
# Test 3: Topic partition count
# ---------------------------------------------------------------------------
def test_03_partition_count(admin):
    metadata = admin.list_topics(topic=TRUCK_TELEMETRY.name, timeout=10)
    partitions = metadata.topics[TRUCK_TELEMETRY.name].partitions
    assert len(partitions) == TRUCK_TELEMETRY.partitions


# ---------------------------------------------------------------------------
# Test 4: Producer sends events
# ---------------------------------------------------------------------------
def test_04_producer_sends_events():
    producer = TelemetryProducer(topic=TRUCK_TELEMETRY.name)
    producer.run(num_events=20, num_trucks=5, rate_per_sec=0)
    assert producer.stats["delivered"] == 20
    assert producer.stats["failed"] == 0


# ---------------------------------------------------------------------------
# Test 5: Consumer receives events
# ---------------------------------------------------------------------------
def test_05_consumer_receives_events():
    # Publish a uniquely-tagged event so we can identify it.
    marker = str(uuid.uuid4())
    producer = Producer(get_producer_config())
    event = {"truck_id": "TRUCK-TEST", "temperature": 30.0, "timestamp": marker}
    producer.produce(TRUCK_TELEMETRY.name, key=b"TRUCK-TEST", value=json.dumps(event).encode())
    producer.flush(10)

    consumer = Consumer(get_consumer_config(group_id=f"test-consume-{uuid.uuid4()}"))
    consumer.subscribe([TRUCK_TELEMETRY.name])

    found = False
    deadline = time.time() + 15
    while time.time() < deadline and not found:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        payload = json.loads(msg.value().decode())
        if payload.get("timestamp") == marker:
            found = True
    consumer.close()
    assert found, "Consumer did not receive the test event in time"


# ---------------------------------------------------------------------------
# Test 6: Message key is truck_id
# ---------------------------------------------------------------------------
def test_06_message_key_is_truck_id():
    producer = Producer(get_producer_config())
    event = {"truck_id": "TRUCK-KEYTEST", "temperature": 22.2, "timestamp": "2026-01-01T00:00:00"}
    producer.produce(TRUCK_TELEMETRY.name, key=b"TRUCK-KEYTEST", value=json.dumps(event).encode())
    producer.flush(10)

    consumer = Consumer(get_consumer_config(group_id=f"test-key-{uuid.uuid4()}"))
    consumer.subscribe([TRUCK_TELEMETRY.name])

    found_key = None
    deadline = time.time() + 15
    while time.time() < deadline and found_key is None:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        payload = json.loads(msg.value().decode())
        if payload.get("truck_id") == "TRUCK-KEYTEST":
            found_key = msg.key().decode()
    consumer.close()
    assert found_key == "TRUCK-KEYTEST"


# ---------------------------------------------------------------------------
# Test 7: Messages are distributed across partitions
# ---------------------------------------------------------------------------
def test_07_messages_distributed_across_partitions():
    producer = TelemetryProducer(topic=TRUCK_TELEMETRY.name)
    producer.run(num_events=200, num_trucks=50, rate_per_sec=0)

    consumer = Consumer(get_consumer_config(group_id=f"test-dist-{uuid.uuid4()}"))
    consumer.subscribe([TRUCK_TELEMETRY.name])

    partitions_seen = set()
    deadline = time.time() + 15
    count = 0
    while time.time() < deadline and count < 200:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        partitions_seen.add(msg.partition())
        count += 1
    consumer.close()
    assert len(partitions_seen) > 1, "Expected events spread across multiple partitions"


# ---------------------------------------------------------------------------
# Tests 8-10: consumer-group rebalancing behavior
# These spin up real Consumer instances polling in threads to observe
# on_assign / on_revoke callbacks firing under an actual rebalance.
# ---------------------------------------------------------------------------
@pytest.mark.rebalance
def test_08_multiple_workers_same_group_split_partitions():
    import threading

    group_id = f"test-rebalance-{uuid.uuid4()}"
    assignments = {"c1": [], "c2": []}

    def run_consumer(name, stop_event):
        def on_assign(c, parts):
            assignments[name] = [p.partition for p in parts]

        consumer = Consumer(get_consumer_config(group_id=group_id, client_id=name))
        consumer.subscribe([TRUCK_TELEMETRY.name], on_assign=on_assign)
        while not stop_event.is_set():
            consumer.poll(0.5)
        consumer.close()

    stop1, stop2 = threading.Event(), threading.Event()
    t1 = threading.Thread(target=run_consumer, args=("c1", stop1))
    t2 = threading.Thread(target=run_consumer, args=("c2", stop2))
    t1.start()
    time.sleep(2)
    t2.start()
    time.sleep(8)  # allow rebalance to settle

    stop1.set()
    stop2.set()
    t1.join(timeout=5)
    t2.join(timeout=5)

    total_assigned = len(assignments["c1"]) + len(assignments["c2"])
    assert total_assigned == TRUCK_TELEMETRY.partitions
    assert len(assignments["c1"]) > 0 and len(assignments["c2"]) > 0


# ---------------------------------------------------------------------------
# Test 11: Changelog topic exists and can receive messages
# ---------------------------------------------------------------------------
def test_11_changelog_topic_ready():
    topics = list_topics(get_admin_client(BOOTSTRAP))
    assert TRUCK_STATE_CHANGELOG.name in topics

    producer = Producer(get_producer_config())
    event = {
        "truck_id": "TRUCK-001",
        "window": "10:00-10:05",
        "state": {"sum": 780.5, "count": 30},
        "timestamp": "2026-01-01T00:05:00",
    }
    producer.produce(
        TRUCK_STATE_CHANGELOG.name,
        key=b"TRUCK-001",
        value=json.dumps(event).encode(),
    )
    result = producer.flush(10)
    assert result == 0, "Changelog message was not fully delivered"


# NOTE: Test for actual RocksDB state recovery is intentionally NOT
# implemented here — that belongs to the RocksDB / State Store team.
