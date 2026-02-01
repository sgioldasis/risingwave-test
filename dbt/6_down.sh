#!/bin/bash

# Script to stop Docker Compose services with volumes and kill dashboard
# This script should be run from the dbt folder

set -e

echo "=== Stopping Modern Dashboard ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Kill backend process if running
if [ -f "$SCRIPT_DIR/.backend.pid" ]; then
    BACKEND_PID=$(cat "$SCRIPT_DIR/.backend.pid")
    if ps -p "$BACKEND_PID" > /dev/null 2>&1; then
        echo "Stopping backend (PID: $BACKEND_PID)..."
        kill "$BACKEND_PID" 2>/dev/null || true
        sleep 1
        # Force kill if still running
        if ps -p "$BACKEND_PID" > /dev/null 2>&1; then
            kill -9 "$BACKEND_PID" 2>/dev/null || true
        fi
        echo "✅ Backend stopped"
    else
        echo "Backend process not found (already stopped)"
    fi
    rm -f "$SCRIPT_DIR/.backend.pid"
else
    # Try to find and kill backend process by name
    if pgrep -f "modern-dashboard/backend/api.py" > /dev/null 2>&1; then
        echo "Stopping backend process..."
        pkill -f "modern-dashboard/backend/api.py" 2>/dev/null || true
        sleep 1
        echo "✅ Backend stopped"
    else
        echo "No backend process found"
    fi
fi

# Kill frontend process if running
if [ -f "$SCRIPT_DIR/.frontend.pid" ]; then
    FRONTEND_PID=$(cat "$SCRIPT_DIR/.frontend.pid")
    if ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
        echo "Stopping frontend (PID: $FRONTEND_PID)..."
        kill "$FRONTEND_PID" 2>/dev/null || true
        sleep 1
        # Force kill if still running
        if ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
            kill -9 "$FRONTEND_PID" 2>/dev/null || true
        fi
        echo "✅ Frontend stopped"
    else
        echo "Frontend process not found (already stopped)"
    fi
    rm -f "$SCRIPT_DIR/.frontend.pid"
else
    # Try to find and kill frontend process by name
    if pgrep -f "vite" > /dev/null 2>&1; then
        echo "Stopping frontend process..."
        pkill -f "vite" 2>/dev/null || true
        sleep 1
        echo "✅ Frontend stopped"
    else
        echo "No frontend process found"
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

# Remove marimo cache directory if it exists
if [ -d "__marimo__" ]; then
    echo "Removing __marimo__/ directory..."
    rm -rf __marimo__
    echo "✅ __marimo__/ directory removed"
else
    echo "__marimo__/ directory does not exist, skipping..."
fi

echo ""
echo "=== Cleaning up log files ==="

# Remove log files if they exist
if [ -f "$SCRIPT_DIR/backend.log" ]; then
    echo "Removing backend.log..."
    rm -f "$SCRIPT_DIR/backend.log"
    echo "✅ backend.log removed"
fi

if [ -f "$SCRIPT_DIR/frontend.log" ]; then
    echo "Removing frontend.log..."
    rm -f "$SCRIPT_DIR/frontend.log"
    echo "✅ frontend.log removed"
fi

echo ""
echo "✅ Dashboard stopped"
echo "✅ Docker Compose services stopped and volumes cleaned up"
echo "✅ Local directories (target/, logs/, __marimo__/) cleaned up"
echo "All services have been terminated and persistent data removed."
