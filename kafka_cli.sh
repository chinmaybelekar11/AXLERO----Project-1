#!/usr/bin/env bash
# kafka_cli.sh
#
# Convenience wrappers around the Kafka CLI tools bundled with the
# apache/kafka docker image, run against the local broker started via
# scripts/docker-compose.yml.
#
# Usage:
#   ./scripts/kafka_cli.sh list
#   ./scripts/kafka_cli.sh describe truck-telemetry
#   ./scripts/kafka_cli.sh create truck-telemetry 6 1
#   ./scripts/kafka_cli.sh delete truck-telemetry
#   ./scripts/kafka_cli.sh consumer-groups

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
CONTAINER="streamforge-kafka"

exec_in_container() {
  docker exec -i "$CONTAINER" "$@"
}

case "${1:-}" in
  list)
    exec_in_container /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
    ;;
  describe)
    topic="${2:?Usage: kafka_cli.sh describe <topic>}"
    exec_in_container /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$topic"
    ;;
  create)
    topic="${2:?Usage: kafka_cli.sh create <topic> <partitions> <replication_factor>}"
    partitions="${3:-6}"
    rf="${4:-1}"
    exec_in_container /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
      --create --topic "$topic" --partitions "$partitions" --replication-factor "$rf"
    ;;
  delete)
    topic="${2:?Usage: kafka_cli.sh delete <topic>}"
    exec_in_container /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --delete --topic "$topic"
    ;;
  consumer-groups)
    exec_in_container /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --list
    ;;
  describe-group)
    group="${2:?Usage: kafka_cli.sh describe-group <group>}"
    exec_in_container /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" \
      --describe --group "$group"
    ;;
  *)
    echo "Usage: $0 {list|describe <topic>|create <topic> <partitions> <rf>|delete <topic>|consumer-groups|describe-group <group>}"
    exit 1
    ;;
esac
