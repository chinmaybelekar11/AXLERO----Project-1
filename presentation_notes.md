# Presentation Notes — Kafka + Architecture

Short talking points for explaining this module in the final review.

### What I built
A Kafka-based distributed event ingestion architecture: a Python producer
publishing mock truck telemetry, a partitioned `truck-telemetry` topic, and
a consumer-group of workers that can scale horizontally and rebalance
automatically. Also the `truck-state-changelog` topic and integration
contract the rest of the team builds on.

### Why Kafka?
Because the project needs scalable, ordered, distributed event streaming
that decouples producers from consumers — trucks can send data
continuously while multiple independent workers (and other team modules)
consume it at their own pace, with replay and fault tolerance built in.

### Why partitions?
Partitions are the unit of parallelism in Kafka. More partitions = more
consumers can read in parallel = higher throughput. Six partitions let us
demonstrate real distributed processing with 2-3 workers.

### Why consumer groups?
A consumer group lets Kafka automatically divide a topic's partitions
among multiple consumer instances, and automatically reassign them if a
consumer joins or leaves. That's what makes horizontal scaling and fault
tolerance possible without custom coordination code.

### Why truck ID as key?
So all events for the same truck are always routed to the same partition,
guaranteeing order and locality for per-truck processing (like the
5-minute rolling average the Stream Processing team computes).

### What happens if a worker fails?
Kafka's consumer-group protocol detects the failure (missed heartbeats /
session timeout) and triggers a rebalance: the failed worker's partitions
are reassigned to the remaining live workers, which pick up from the last
committed offset. No data is lost, though a message being processed at the
moment of failure may be redelivered (at-least-once).

### What is a changelog?
A Kafka topic used to persist state changes so they can be replayed to
rebuild state after a crash — here, it's the mechanism the RocksDB team
uses to recover per-truck rolling-average state without starting from
zero.

### How does my module connect to other modules?
Through clearly defined Kafka topics, a consistent message key
(`truck_id`), and documented JSON schemas — see
`docs/integration_contract.md`. Any language or library that speaks Kafka
can plug in.
