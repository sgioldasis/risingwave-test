#!/bin/bash

# Script to stop Docker Compose services with volumes and kill dashboard
# This script should be run from the project root

set -e

echo "=== Stopping Modern Dashboard ==="
echo ""

# Kill backend process if running
if [ -f ".backend.pid" ]; then
    BACKEND_PID=$(cat ".backend.pid")
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
    rm -f ".backend.pid"
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
if [ -f ".frontend.pid" ]; then
    FRONTEND_PID=$(cat ".frontend.pid")
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
    rm -f ".frontend.pid"
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

# Kill dashboard process if running
if [ -f ".dashboard.pid" ]; then
    DASHBOARD_PID=$(cat ".dashboard.pid")
    if ps -p "$DASHBOARD_PID" > /dev/null 2>&1; then
        echo "Stopping dashboard (PID: $DASHBOARD_PID)..."
        kill "$DASHBOARD_PID" 2>/dev/null || true
        sleep 1
        echo "✅ Dashboard stopped"
    fi
    rm -f ".dashboard.pid"
fi

echo ""
echo "=== Stopping Docker Compose Services ==="
echo "Running docker compose down --volumes from project root"
echo ""

docker compose down --volumes

echo ""
echo "=== Cleaning up local directories ==="

# Remove target and logs directories if they exist
if [ -d "dbt/target" ]; then
    echo "Removing dbt/target/ directory..."
    rm -rf dbt/target
    echo "✅ dbt/target/ directory removed"
else
    echo "dbt/target/ directory does not exist, skipping..."
fi

if [ -d "dbt/logs" ]; then
    echo "Removing dbt/logs/ directory..."
    rm -rf dbt/logs
    echo "✅ dbt/logs/ directory removed"
else
    echo "dbt/logs/ directory does not exist, skipping..."
fi

# Remove marimo cache directory if it exists
if [ -d "scripts/__marimo__" ]; then
    echo "Removing scripts/__marimo__/ directory..."
    rm -rf scripts/__marimo__
    echo "✅ scripts/__marimo__/ directory removed"
else
    echo "scripts/__marimo__/ directory does not exist, skipping..."
fi

echo ""
echo "=== Cleaning up log files ==="

# Remove log files if they exist
if [ -f "backend.log" ]; then
    echo "Removing backend.log..."
    rm -f "backend.log"
    echo "✅ backend.log removed"
fi

if [ -f "frontend.log" ]; then
    echo "Removing frontend.log..."
    rm -f "frontend.log"
    echo "✅ frontend.log removed"
fi

echo ""
echo "✅ Dashboard stopped"
echo "✅ Docker Compose services stopped and volumes cleaned up"
echo "✅ Local directories (dbt/target/, dbt/logs/, scripts/__marimo__/) cleaned up"
echo "All services have been terminated and persistent data removed."
