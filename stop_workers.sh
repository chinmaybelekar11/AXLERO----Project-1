#!/usr/bin/env bash
# stop_workers.sh
#
# Stops every worker started by run_workers.sh.

set -euo pipefail

LOG_DIR="$(dirname "$0")/../worker_logs"
PID_FILE="$LOG_DIR/pids.txt"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No running workers found (no $PID_FILE)."
  exit 0
fi

while read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping worker pid $pid"
    kill -TERM "$pid"
  fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "All tracked workers stopped."
