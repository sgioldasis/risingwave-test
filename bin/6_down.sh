#!/bin/bash

# Script to stop Docker Compose services with volumes and kill dashboard
# This script should be run from the project root

set -e

echo "=== Stopping Producer ==="
if pgrep -f "scripts/producer.py" > /dev/null 2>&1; then
    echo "Stopping producer process..."
    pkill -f "scripts/producer.py" 2>/dev/null || true
    sleep 1
    # Force kill if still running
    if pgrep -f "scripts/producer.py" > /dev/null 2>&1; then
        pkill -9 -f "scripts/producer.py" 2>/dev/null || true
    fi
    echo "✅ Producer stopped"
else
    echo "No producer process found"
fi

# Also ensure producer port is closed if it uses one (though default is usually logic-based)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS - use lsof instead
    kill $(lsof -t -i:5000) 2>/dev/null || true
else
    # Linux syntax
    fuser -k 5000/tcp 2>/dev/null || true
fi

echo ""

echo "=== Stopping Modern Dashboard ==="
echo ""

# Kill the script runner monitoring processes for dashboards first
# (this will gracefully shut down the child processes via the trap)
if pgrep -f "bash bin/4_run_modern.sh" > /dev/null 2>&1; then
    echo "Stopping modern dashboard monitoring script..."
    pkill -f "bash bin/4_run_modern.sh" 2>/dev/null || true
    sleep 1
    # Force kill if still running
    if pgrep -f "bash bin/4_run_modern.sh" > /dev/null 2>&1; then
        pkill -9 -f "bash bin/4_run_modern.sh" 2>/dev/null || true
    fi
    echo "✅ Modern dashboard monitoring stopped"
fi

if pgrep -f "bash bin/4_run_dashboard.sh" > /dev/null 2>&1; then
    echo "Stopping legacy dashboard monitoring script..."
    pkill -f "bash bin/4_run_dashboard.sh" 2>/dev/null || true
    sleep 1
    # Force kill if still running
    if pgrep -f "bash bin/4_run_dashboard.sh" > /dev/null 2>&1; then
        pkill -9 -f "bash bin/4_run_dashboard.sh" 2>/dev/null || true
    fi
    echo "✅ Legacy dashboard monitoring stopped"
fi

# Force kill ports used by dashboards
echo "Cleaning up dashboard ports..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS - use lsof instead
    kill $(lsof -t -i:4000) 2>/dev/null || true
    kill $(lsof -t -i:8000) 2>/dev/null || true
    kill $(lsof -t -i:8050) 2>/dev/null || true
else
    # Linux syntax
    fuser -k 4000/tcp 2>/dev/null || true
    fuser -k 8000/tcp 2>/dev/null || true
    fuser -k 8050/tcp 2>/dev/null || true
fi

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
    echo "Stopping backend processes..."
    
    # Kill backend API processes
    pkill -f "modern-dashboard/backend/api.py" 2>/dev/null || true
    pkill -f "uvicorn.*api:app" 2>/dev/null || true
    
    sleep 1
    
    # Force kill if still running
    pkill -9 -f "modern-dashboard/backend/api.py" 2>/dev/null || true
    pkill -9 -f "uvicorn.*api:app" 2>/dev/null || true
    
    echo "✅ Backend processes stopped"
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
    echo "Stopping frontend processes (npm/node/vite)..."
    
    # Kill npm run dev processes
    pkill -f "npm run dev" 2>/dev/null || true
    
    # Kill vite processes
    pkill -f "node_modules/.bin/vite" 2>/dev/null || true
    pkill -f "node.*vite" 2>/dev/null || true
    
    # Kill any node process in the frontend directory
    pkill -f "modern-dashboard/frontend" 2>/dev/null || true
    
    # Kill esbuild processes (spawned by vite)
    pkill -f "esbuild.*--service" 2>/dev/null || true
    
    sleep 2
    
    # Force kill if still running
    pkill -9 -f "npm run dev" 2>/dev/null || true
    pkill -9 -f "node_modules/.bin/vite" 2>/dev/null || true
    pkill -9 -f "node.*vite" 2>/dev/null || true
    pkill -9 -f "modern-dashboard/frontend" 2>/dev/null || true
    pkill -9 -f "esbuild.*--service" 2>/dev/null || true
    
    echo "✅ Frontend processes stopped"
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

# Final cleanup of any potential stale PID files in project root
rm -f .backend.pid .frontend.pid .dashboard.pid

echo ""
echo "=== Final Port Cleanup ===" 
echo "Waiting for processes to fully terminate..."
sleep 2

# Force kill any remaining processes on dashboard ports
echo "Ensuring all dashboard ports are freed..."
lsof -ti :4000 | xargs kill -9 2>/dev/null || true
lsof -ti :8000 | xargs kill -9 2>/dev/null || true
lsof -ti :8050 | xargs kill -9 2>/dev/null || true

sleep 1

# Verify ports are free
if lsof -i :4000 > /dev/null 2>&1; then
    echo "⚠️  Warning: Port 4000 still in use"
    lsof -i :4000
else
    echo "✅ Port 4000 is free"
fi

if lsof -i :8000 > /dev/null 2>&1; then
    echo "⚠️  Warning: Port 8000 still in use"
    lsof -i :8000
else
    echo "✅ Port 8000 is free"
fi

if lsof -i :8050 > /dev/null 2>&1; then
    echo "⚠️  Warning: Port 8050 still in use"
    lsof -i :8050
else
    echo "✅ Port 8050 is free"
fi

echo ""
echo "=== Cleaning dbt directories in Dagster container ==="
if docker ps | grep -q "dagster-webserver"; then
    echo "Removing dbt/target and dbt/logs inside dagster-webserver container..."
    docker exec dagster-webserver bash -c "rm -rf /workspace/dbt/target /workspace/dbt/logs" || echo "⚠️  Directory removal failed, continuing anyway"
    echo "✅ dbt directories cleaned"
else
    echo "dagster-webserver container not running, skipping dbt directory cleanup"
fi

echo ""
echo "=== Stopping Docker Compose Services ==="
echo "Running docker compose down --volumes from project root"
echo ""

docker compose down --volumes

echo ""
echo "=== Cleaning up dbt directories on host ==="
echo "Using temporary Docker container to remove root-owned files..."
docker run --rm -v "$(pwd)/dbt:/dbt" alpine:latest rm -rf /dbt/target /dbt/logs 2>/dev/null || true
if [ -d "dbt/target" ] || [ -d "dbt/logs" ]; then
    echo "⚠️  Warning: dbt/target or dbt/logs may still exist (check permissions)"
else
    echo "✅ dbt/target and dbt/logs removed successfully"
fi

echo ""
echo "=== Cleaning up other local directories ==="

# NOTE: The following directory cleanup sections are commented out because
# these directories are created by Docker containers running as root, and
# therefore are owned by root on the host filesystem. The script would fail
# with permission denied errors when trying to remove them.
#
# docker-compose.yml has been updated to mount only specific directories
# (orchestration/, dbt/) instead of the entire project folder to minimize
# root-owned files. However, dbt/target/ and dbt/logs/ are still created
# inside the container when dbt runs, so they remain owned by root.
#
# To clean these directories, either:
# 1. Run this script with sudo: sudo ./bin/6_down.sh
# 2. Or manually remove them: sudo rm -rf dbt/target dbt/logs

# Remove target and logs directories if they exist
# NOTE: Commented out - dbt/target is owned by root (created by Dagster container)
# if [ -d "dbt/target" ]; then
#     echo "Removing dbt/target/ directory..."
#     rm -rf dbt/target
#     echo "✅ dbt/target/ directory removed"
# else
#     echo "dbt/target/ directory does not exist, skipping..."
# fi

# NOTE: Commented out - dbt/logs is owned by root (created by Dagster container)
# if [ -d "dbt/logs" ]; then
#     echo "Removing dbt/logs/ directory..."
#     rm -rf dbt/logs
#     echo "✅ dbt/logs/ directory removed"
# else
#     echo "dbt/logs/ directory does not exist, skipping..."
# fi

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
echo "✅ Log files (backend.log, frontend.log) cleaned up"

# Only show warning if directories still exist
if [ -d "dbt/target" ] || [ -d "dbt/logs" ]; then
    echo ""
    echo "⚠️  Note: The following directories were NOT cleaned up because they are owned by root:"
    if [ -d "dbt/target" ]; then
        echo "   - dbt/target/"
    fi
    if [ -d "dbt/logs" ]; then
        echo "   - dbt/logs/"
    fi
    echo ""
    echo "   To clean them, run: sudo rm -rf dbt/target dbt/logs"
fi

echo ""
echo "All services have been terminated and persistent data removed."
