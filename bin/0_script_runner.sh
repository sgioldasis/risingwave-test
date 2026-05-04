#!/bin/bash

# Script to manage the script_runner web process
# This script will kill any existing script_runner processes and start a new one

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
LOG_FILE="${LOG_DIR}/script_runner.log"
STARTUP_TIMEOUT_SECONDS="${SCRIPT_RUNNER_STARTUP_TIMEOUT_SECONDS:-20}"

echo "=== Managing Script Runner ==="
echo ""

# Find and kill any existing script_runner processes (Python only, not this shell script)
echo "Checking for existing script_runner processes..."
# Use a pattern that matches the Python script but not this shell script
PIDS=$(pgrep -f "scripts/script_runner.py" || true)

if [ -n "$PIDS" ]; then
    echo "Found existing script_runner processes: $PIDS"
    echo "Killing existing processes..."
    echo "$PIDS" | xargs kill -9 2>/dev/null || true
    echo "✅ Existing processes killed"
else
    echo "No existing script_runner processes found"
fi

echo ""
echo "Starting script_runner..."
echo ""

# Start the script runner web application in the background and verify readiness.
cd "$PROJECT_ROOT"
mkdir -p "$LOG_DIR"

RUNNER_CMD=""
if [ -x ".venv/bin/python" ]; then
    RUNNER_CMD=".venv/bin/python scripts/script_runner.py"
elif command -v python >/dev/null 2>&1; then
    RUNNER_CMD="python scripts/script_runner.py"
elif command -v uv >/dev/null 2>&1; then
    RUNNER_CMD="uv run python scripts/script_runner.py"
else
    echo "❌ Could not find python or uv to launch script_runner"
    exit 1
fi

echo "Launching with: $RUNNER_CMD"
nohup bash -lc "$RUNNER_CMD" >"$LOG_FILE" 2>&1 &
NEW_PID=$!

is_port_open() {
    if command -v nc >/dev/null 2>&1; then
        nc -z localhost 4001 >/dev/null 2>&1
        return $?
    fi

    (echo >/dev/tcp/127.0.0.1/4001) >/dev/null 2>&1
    return $?
}

elapsed=0
while [ "$elapsed" -lt "$STARTUP_TIMEOUT_SECONDS" ]; do
    if is_port_open; then
        echo "✅ script_runner is running"
        echo "PID: $NEW_PID"
        echo "URL: http://localhost:4001"
        echo "Logs: $LOG_FILE"
        exit 0
    fi

    if ! kill -0 "$NEW_PID" >/dev/null 2>&1; then
        echo "❌ script_runner exited during startup"
        echo "Last log lines:"
        tail -n 40 "$LOG_FILE" || true
        exit 1
    fi

    sleep 1
    elapsed=$((elapsed + 1))
done

echo "❌ script_runner did not become ready within ${STARTUP_TIMEOUT_SECONDS}s"
echo "Last log lines:"
tail -n 40 "$LOG_FILE" || true
exit 1
