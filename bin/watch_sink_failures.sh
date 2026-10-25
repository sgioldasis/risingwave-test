#!/bin/bash

# Lightweight runner for sink failure watchdog.
# Starts scripts/watch_sink_failures.py in background by default.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

PID_FILE="${SINK_WATCHDOG_PID_FILE:-/tmp/rw_sink_watchdog.pid}"
LOG_DIR="${SINK_WATCHDOG_LOG_DIR:-$PROJECT_ROOT/logs}"
LOG_FILE="${SINK_WATCHDOG_LOG_FILE:-$LOG_DIR/sink_watchdog.log}"

POLL_INTERVAL="${SINK_WATCHDOG_POLL_INTERVAL_SEC:-10}"
LOOKBACK_MINUTES="${SINK_WATCHDOG_LOOKBACK_MINUTES:-10}"
CONTAINER_NAME="${SINK_WATCHDOG_CONTAINER_NAME:-dagster-webserver}"
STATE_FILE="${SINK_WATCHDOG_STATE_FILE:-/tmp/rw_sink_watchdog_state.json}"

if [ "${SINK_WATCHDOG_ENABLED:-1}" = "0" ]; then
    echo "Sink watchdog disabled (SINK_WATCHDOG_ENABLED=0)."
    exit 0
fi

is_running() {
    local pid="$1"
    kill -0 "$pid" 2>/dev/null
}

if [ -f "$PID_FILE" ]; then
    EXISTING_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "$EXISTING_PID" ] && is_running "$EXISTING_PID"; then
        echo "Sink watchdog already running (PID: $EXISTING_PID)."
        exit 0
    fi
    rm -f "$PID_FILE"
fi

# Fallback: kill any orphaned instances not tracked by the PID file
ORPHAN_PIDS="$(pgrep -f "watch_sink_failures.py" 2>/dev/null || true)"
if [ -n "$ORPHAN_PIDS" ]; then
    echo "Found orphaned watchdog processes ($ORPHAN_PIDS), killing them..."
    echo "$ORPHAN_PIDS" | xargs kill 2>/dev/null || true
    sleep 1
    echo "$ORPHAN_PIDS" | xargs kill -9 2>/dev/null || true
fi

mkdir -p "$LOG_DIR"

CMD=(
    python3
    scripts/watch_sink_failures.py
    --poll-interval-sec "$POLL_INTERVAL"
    --lookback-minutes "$LOOKBACK_MINUTES"
    --container-name "$CONTAINER_NAME"
    --state-file "$STATE_FILE"
)

if [ "${1:-}" = "--foreground" ]; then
    echo "Starting sink watchdog in foreground..."
    exec "${CMD[@]}"
fi

echo "Starting sink watchdog in background..."
nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
WATCHDOG_PID=$!
echo "$WATCHDOG_PID" > "$PID_FILE"

# Give process a moment to fail fast on startup issues.
sleep 1
if is_running "$WATCHDOG_PID"; then
    echo "Sink watchdog started (PID: $WATCHDOG_PID)"
    echo "Log: $LOG_FILE"
    exit 0
fi

echo "Failed to start sink watchdog. Check log: $LOG_FILE"
rm -f "$PID_FILE"
exit 1
