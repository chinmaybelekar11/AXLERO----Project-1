#!/usr/bin/env bash
# run_workers.sh
#
# Launches N demo workers (same consumer group) as background processes so
# you can watch Kafka distribute partitions across them, and watch a live
# rebalance when you kill one.
#
# Usage:
#   ./scripts/run_workers.sh 3        # start 3 workers
#   # ... watch logs in worker_logs/ ...
#   kill <pid-of-one-worker>          # trigger a rebalance
#   ./scripts/stop_workers.sh         # stop everything

set -euo pipefail

NUM_WORKERS="${1:-3}"
LOG_DIR="$(dirname "$0")/../worker_logs"
mkdir -p "$LOG_DIR"

cd "$(dirname "$0")/.."

for i in $(seq 1 "$NUM_WORKERS"); do
  worker_id="worker-$i"
  echo "Starting $worker_id (log: $LOG_DIR/$worker_id.log)"
  python -m consumer.worker --id "$worker_id" > "$LOG_DIR/$worker_id.log" 2>&1 &
  echo $! >> "$LOG_DIR/pids.txt"
done

echo ""
echo "$NUM_WORKERS workers started. Tail logs with:"
echo "  tail -f worker_logs/worker-*.log"
echo "Stop all with:"
echo "  ./scripts/stop_workers.sh"
