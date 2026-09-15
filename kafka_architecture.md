# Kafka + Architecture Module — Design Document

**Owner:** Kafka + Architecture (this module)
**Project:** Stream Forge – Distributed Python Event Processor

This document explains the design of the Kafka foundation for the project
and clearly marks which parts belong to this module vs. other team members'
modules.

---

## 1. Scope

This module owns:

- The Kafka broker environment (local, KRaft mode)
- Topic design (`truck-telemetry`, `truck-state-changelog`)
- The telemetry producer
- The consumer-group / worker architecture (demo workers only — no
  processing logic)
- Partitioning strategy and key design
- Offset management strategy
- The integration contract other modules build against
- Kafka-side testing and throughput benchmarking

This module does **not** own (and does not implement):

- Stream processing / windowing / 5-minute aggregation (Faust/Bytewax) —
  **Stream Processing team**
- RocksDB state storage and crash recovery — **RocksDB team**
- FastAPI backend and React Flow dashboard — **Monitoring team**

Those teams plug into the topics, keys, and schemas defined here.

---

## 2. End-to-end architecture

```
                ┌──────────────────┐
                │  Python Producer │   <- this module
                └────────┬─────────┘
                         │
                         ▼
                ┌──────────────────┐
                │  Apache Kafka    │   <- this module
                │     Broker       │
                └────────┬─────────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          Partition 0 Partition 1 ... Partition N   <- this module
              │          │          │
              └──────────┼──────────┘
                         ▼
                ┌──────────────────┐
                │ Consumer Group   │   <- this module (architecture only)
                │streamforge-workers│
                └────────┬─────────┘
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
          Worker 1    Worker 2    Worker 3          <- this module (demo only)
             │           │           │
             └───────────┼───────────┘
                         ▼
              Other Team Modules                     <- NOT this module
        (Stream Processing / Windowing / Aggregation)

                RocksDB State                         <- NOT this module
                      ↓
              Kafka Changelog Topic                   <- this module (topic only)
              (truck-state-changelog)
```

---

## 3. Kafka environment

Local development uses **Apache Kafka in KRaft mode** (no Zookeeper),
via `scripts/docker-compose.yml`.

| Setting | Value | Why |
|---|---|---|
| Process roles | `broker,controller` | Single-node KRaft combines both roles — no Zookeeper needed for local dev |
| Listeners | `PLAINTEXT` (internal, 29092), `CONTROLLER` (9093), `PLAINTEXT_HOST` (external, 9092) | Separates intra-cluster traffic, controller quorum traffic, and host-machine client traffic |
| Advertised listener (host) | `localhost:9092` | What our Python scripts (running on the host, not in Docker) connect to |
| Log dirs | `/var/lib/kafka/data` (docker volume) | Persist topic data across container restarts |
| Auto-create topics | `false` | Topics are created explicitly via `kafka/create_topics.py` so partition counts/RF are always what we intend, not Kafka defaults |
| Default partitions | 6 | Matches `truck-telemetry`'s partition count |

We deliberately **do not** configure SASL/SSL, multi-broker replication,
rack awareness, or other production hardening — the spec calls for a local
college demonstration, not a production deployment.

---

## 4. Topics

See `kafka/topic_config.py` for the single source of truth. Summary:

| Topic | Partitions | Replication Factor | Key | Value | Purpose |
|---|---|---|---|---|---|
| `truck-telemetry` | 6 | 1 | `truck_id` | JSON telemetry | Primary ingestion topic |
| `truck-state-changelog` | 6 | 1 | `truck_id` | JSON state snapshot | Kafka-side of RocksDB's changelog pattern |

Both use 6 partitions — enough to demonstrate real parallelism and
rebalancing with 2-3 workers, without being excessive for a local
single-broker demo.

---

## 5. Message key design

The producer uses **`truck_id` as the Kafka message key**.

Why this matters: Kafka guarantees that all messages with the same key are
routed to the same partition (via the default hash partitioner), as long as
the partition count doesn't change. Since downstream workers do **per-truck**
processing (e.g. a rolling 5-minute average for `TRUCK-001`), we need every
event for `TRUCK-001` to be processed in order by the same
partition/consumer — otherwise the Stream Processing team would need to do
their own re-shuffling. Keying by `truck_id` gives them that guarantee for
free.

---

## 6. Partition architecture

- `truck-telemetry` has 6 partitions.
- Partitioning is left to Kafka's default key-based partitioner — no
  manual partition assignment (avoided per spec, since it would break the
  guarantee above under scaling).
- Use `scripts/kafka_cli.sh describe truck-telemetry` or
  `kafka/kafka_admin.py describe truck-telemetry` to inspect leaders,
  replicas, and ISRs per partition.

---

## 7. Consumer group architecture

Consumer group: **`streamforge-workers`**

```
Kafka
   ↓
Partitions (6)
   ↓
Consumer Group (streamforge-workers)
   ↓
Worker 1, Worker 2, Worker 3, ...
```

`consumer/worker.py` is a **demo/architecture worker** — it proves
subscription, partition assignment, consumption, and graceful
shutdown/rebalancing work correctly. It intentionally does **not**
implement the 5-minute rolling average; that belongs to Stream Processing,
which will run its own consumers (Faust/Bytewax) against the same group
pattern, or a separate group depending on their design.

We use `cooperative-sticky` partition assignment (the modern
recommendation over `range`/`roundrobin`) so rebalances only move the
partitions that actually need to move, rather than revoking everything
from everyone.

---

## 8. Offset management

Strategy: **manual, synchronous commits**, every N messages (default 50)
and on partition revoke/shutdown (`enable.auto.commit=False`).

This gives **at-least-once** delivery semantics: if a worker crashes after
processing a message but before committing, that message will be
re-delivered to whichever consumer picks up the partition next. We do
**not** claim exactly-once — implementing true exactly-once would require
transactional producers/consumers and coordination with the state-store
layer (RocksDB team), which is out of scope for this module.

This is a reasonable, honest tradeoff for a college demo: it's simple to
reason about, survives worker crashes without losing data, and makes the
at-least-once vs exactly-once distinction explicit for the viva.

---

## 9. Changelog architecture (Kafka side only)

```
RocksDB State
      ↓
Kafka Changelog Topic (truck-state-changelog)
```

This module creates and documents the `truck-state-changelog` topic and its
expected message shape (see `schemas/changelog_schema.json`), but does
**not** implement RocksDB, state recovery, or the actual aggregation logic
that produces changelog entries. The exact contents of the `state` object
are finalized with the RocksDB team.

---

## 10. Downstream integration

See `docs/integration_contract.md` for the full contract other team
members build against.
