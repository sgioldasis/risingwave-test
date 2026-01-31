#!/bin/bash

# Script to stop Docker Compose services with volumes and kill dashboard
# This script should be run from the dbt folder

set -e

echo "=== Stopping Dashboard ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Kill dashboard process if running
if [ -f "$SCRIPT_DIR/.dashboard.pid" ]; then
    DASHBOARD_PID=$(cat "$SCRIPT_DIR/.dashboard.pid")
    if ps -p "$DASHBOARD_PID" > /dev/null 2>&1; then
        echo "Stopping dashboard (PID: $DASHBOARD_PID)..."
        kill "$DASHBOARD_PID" 2>/dev/null || true
        sleep 1
        # Force kill if still running
        if ps -p "$DASHBOARD_PID" > /dev/null 2>&1; then
            kill -9 "$DASHBOARD_PID" 2>/dev/null || true
        fi
        echo "✅ Dashboard stopped"
    else
        echo "Dashboard process not found (already stopped)"
    fi
    rm -f "$SCRIPT_DIR/.dashboard.pid"
else
    # Try to find and kill dashboard process by name
    if pgrep -f "python dashboard.py" > /dev/null 2>&1; then
        echo "Stopping dashboard process..."
        pkill -f "python dashboard.py" 2>/dev/null || true
        sleep 1
        echo "✅ Dashboard stopped"
    else
        echo "No dashboard process found"
    fi
fi

echo ""
echo "=== Stopping Docker Compose Services ==="
echo "Changing to parent directory to run docker compose down --volumes"
echo ""

# Save current directory
ORIGINAL_DIR=$(pwd)

# Change to parent directory
cd ..

echo "Running: docker compose down --volumes"
docker compose down --volumes

# Change back to original directory
cd "$ORIGINAL_DIR"

echo ""
echo "=== Cleaning up local directories ==="

# Remove target and logs directories if they exist
if [ -d "target" ]; then
    echo "Removing target/ directory..."
    rm -rf target
    echo "✅ target/ directory removed"
else
    echo "target/ directory does not exist, skipping..."
fi

if [ -d "logs" ]; then
    echo "Removing logs/ directory..."
    rm -rf logs
    echo "✅ logs/ directory removed"
else
    echo "logs/ directory does not exist, skipping..."
fi

echo ""
echo "✅ Dashboard stopped"
echo "✅ Docker Compose services stopped and volumes cleaned up"
echo "✅ Local directories (target/, logs/) cleaned up"
echo "All services have been terminated and persistent data removed."
