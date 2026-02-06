#!/bin/bash

# Script to manage the script_runner web process
# This script will kill any existing script_runner processes and start a new one

set -e

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

# Start the script runner web application
uv run python scripts/script_runner.py
