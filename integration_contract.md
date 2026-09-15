# Downstream Integration Contract

This is the contract the **Kafka + Architecture** module provides to the
rest of the Stream Forge team. Connect your module to these topics,
this key, and this schema — you should not need to redesign anything on
the Kafka side.

---

## Connection

```
Bootstrap server (local dev): localhost:9092
```

Set `KAFKA_BOOTSTRAP_SERVERS` in your own `.env` to override.

---

## Topic: `truck-telemetry`  (→ Stream Processing team)

| Field | Value |
|---|---|
| Partitions | 6 |
| Replication factor | 1 |
| Key | `truck_id` (UTF-8 string) |
| Value | JSON (UTF-8 bytes) |

**Value schema** (also in `schemas/telemetry_schema.json`):

```json
{
  "truck_id": "TRUCK-001",
  "temperature": 27.5,
  "timestamp": "2026-09-15T10:30:00"
}
```

**Guarantee:** all events for a given `truck_id` land on the same
partition (Kafka's default key hashing), so a single consumer instance in
your consumer group will see all of that truck's events, in order.

**How to consume it (any language/library):** subscribe to
`truck-telemetry` with your own consumer group id (e.g.
`streamforge-stream-processor`) — do **not** reuse `streamforge-workers`,
which is this module's demo group, unless you intend to split the same
partitions between us (not recommended; use your own group so both can
run independently).

---

## Topic: `truck-state-changelog`  (→ RocksDB / State Store team)

| Field | Value |
|---|---|
| Partitions | 6 |
| Replication factor | 1 |
| Key | `truck_id` (UTF-8 string) |
| Value | JSON (UTF-8 bytes) |

**Minimum expected value shape** (also in `schemas/changelog_schema.json`):

```json
{
  "truck_id": "TRUCK-001",
  "window": "10:00-10:05",
  "state": {
    "sum": 780.5,
    "count": 30
  },
  "timestamp": "2026-09-15T10:05:00"
}
```

The `state` object's exact contents are owned by you (RocksDB team) — add
whatever fields your recovery logic needs. Keep `truck_id` as the message
key so per-truck state changelogs stay on one partition, matching the
telemetry topic's partitioning.

This module only creates the topic and documents this contract — the
producer that writes to it, and the RocksDB recovery logic that reads from
it, are your responsibility.

---

## Consumer group naming convention

To avoid accidentally splitting partitions with this module's demo
workers, use a distinct consumer group per module, e.g.:

| Module | Suggested group id |
|---|---|
| Kafka + Architecture (this module, demo only) | `streamforge-workers` |
| Stream Processing | `streamforge-stream-processor` |
| Monitoring / metrics consumer (if any) | `streamforge-monitor` |

---

## What NOT to redesign

- Do not change `truck-telemetry`'s partition count without telling this
  module — it changes which partition a given `truck_id` hashes to for
  **existing** consumers with in-flight offsets, and affects the "same
  truck → same partition" guarantee during a rebalance transition.
- Do not manually assign partitions to producers/consumers — rely on
  Kafka's key-based partitioning and consumer-group rebalancing, which is
  what this architecture is built around.

---

## Questions / changes

If your module's needs require a change to topic config, partitioning, or
schema, raise it with the Kafka + Architecture owner rather than working
around it locally — that keeps this contract accurate for everyone.
