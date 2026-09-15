# Viva Questions — Kafka + Architecture

Scoped strictly to this module. Answers are intentionally short — expand
verbally in the viva using `docs/kafka_architecture.md` for detail.

**Q: What is Apache Kafka?**
A distributed event streaming platform — a durable, ordered, replicated
log that decouples producers from consumers.

**Q: What is a Kafka broker?**
A single Kafka server that stores data and serves client requests. A
cluster is made of one or more brokers.

**Q: What is a topic?**
A named stream of messages — the logical channel producers write to and
consumers read from (`truck-telemetry` in this project).

**Q: What is a partition?**
An ordered, append-only log that a topic is split into. Partitions are the
unit of parallelism and scalability in Kafka.

**Q: What is replication factor?**
How many copies of each partition are kept across brokers, for fault
tolerance. Set to 1 here since this is a single-broker local demo.

**Q: What is a producer?**
A client that publishes (writes) messages to a topic.

**Q: What is a consumer?**
A client that subscribes to and reads messages from a topic.

**Q: What is a consumer group?**
A set of consumers that cooperate to consume a topic, with Kafka
automatically dividing the topic's partitions among the group's members.

**Q: What is an offset?**
A per-partition, monotonically increasing ID identifying a message's
position in the log. Consumers track offsets to know what they've already
read.

**Q: What is the message key, and why does it matter?**
`truck_id` in this project. Kafka's default partitioner hashes the key to
pick a partition, so same key → same partition, every time (partition
count unchanged) — giving ordering and locality per truck.

**Q: What is partitioning?**
The process of splitting a topic's data across partitions, done here via
key-based hashing rather than manual/round-robin assignment.

**Q: What is rebalancing?**
When consumer group membership changes (a consumer joins, leaves, or
crashes), Kafka reassigns partitions among the remaining group members.

**Q: What is consumer lag?**
The difference between the latest offset produced and the latest offset a
consumer has committed — indicates how far behind a consumer is.

**Q: What is a bootstrap server?**
The initial broker address(es) a client connects to, to discover the rest
of the cluster's metadata (`localhost:9092` here).

**Q: What is KRaft?**
Kafka's built-in consensus protocol (Kafka Raft) that replaces Zookeeper
for cluster metadata and controller election — used here for a
Zookeeper-free local setup.

**Q: What do Kafka acknowledgements (acks) mean?**
How many broker replicas must acknowledge a write before the producer
considers it successful. `acks=1` here means the partition leader
acknowledged it — a deliberate speed/durability tradeoff appropriate for a
local demo.

**Q: What is a delivery callback?**
A function the producer calls (asynchronously) once a message has either
been delivered or failed, used here to track delivered/failed counts.

**Q: What is a changelog (in this context)?**
A Kafka topic (`truck-state-changelog`) used by the state-management
architecture to persist and later replay/recover per-truck aggregation
state.

**Q: What does fault tolerance mean here?**
If a worker crashes, Kafka rebalances its partitions to other live
workers, which resume from the last committed offset — no manual
intervention needed.

**Q: What throughput did you measure?**
Answered live from `benchmark/kafka_throughput.py` output — report the
actual measured events/sec on the demo machine, not a target number.

**Q: Why Kafka instead of direct API calls between services?**
Direct calls couple producers and consumers tightly (both must be up at
the same time, and speed-matched). Kafka decouples them, buffers bursts,
allows replay, and lets multiple independent consumers (this module's
workers, the Stream Processing module, etc.) read the same stream
independently.

**Q: Why multiple workers?**
To demonstrate horizontal scalability — more workers in the same group can
consume more partitions in parallel, up to the partition count.

**Q: Why is truck ID the key and not, say, a random UUID per event?**
Because downstream processing is per-truck (rolling averages per truck).
A random key would scatter one truck's events across partitions,
breaking per-truck ordering guarantees.

**Q: What exactly did you implement?**
The Kafka broker setup (KRaft, local docker-compose), topic design and
creation scripts, the telemetry producer, a demo consumer/worker proving
the consumer-group architecture (subscription, assignment, offsets,
rebalancing, graceful shutdown), the changelog topic and its contract, the
throughput benchmark, and the integration documentation other team members
build against. I did not implement stream processing, windowing,
aggregation, RocksDB, FastAPI, or React.
