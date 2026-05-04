#!/bin/bash

# Script to stop the script_runner web process started by bin/0_script_runner.sh

set -e

echo "=== Stopping Script Runner ==="
echo ""

PIDS=$(pgrep -f "scripts/script_runner.py" || true)

if [ -n "$PIDS" ]; then
    echo "Found script_runner processes: $PIDS"
    echo "Killing processes..."
    echo "$PIDS" | xargs kill -9 2>/dev/null || true
    echo "✅ script_runner processes killed"
else
    echo "No script_runner processes found"
fi

# Fallback: kill anything still listening on port 4001
if command -v lsof >/dev/null 2>&1; then
    PORT_PIDS=$(lsof -ti tcp:4001 || true)
    if [ -n "$PORT_PIDS" ]; then
        echo "Killing leftover processes on port 4001: $PORT_PIDS"
        echo "$PORT_PIDS" | xargs kill -9 2>/dev/null || true
        echo "✅ Port 4001 freed"
    fi
fi

echo ""
echo "Done."
