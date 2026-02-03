#!/bin/bash

# Script to run the legacy dashboard
# This script should be run from the project root

echo "Starting Real-time E-commerce Funnel Dashboard..."
echo "Dashboard will be available at: http://localhost:8050"
echo ""

# Check if dashboard is already running
if pgrep -f "python scripts/dashboard.py" > /dev/null 2>&1; then
    echo "⚠️  Dashboard is already running!"
    echo "   Visit: http://localhost:8050"
    exit 0
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Use uv to run the dashboard in background
if command -v uv &> /dev/null; then
    echo "Using uv to run dashboard in background..."
    uv run python scripts/dashboard.py > logs/dashboard.log 2>&1 &
else
    echo "uv not found, falling back to system python..."
    # Activate virtual environment if it exists
    if [ -d ".venv" ]; then
        source ".venv/bin/activate"
    fi
    python scripts/dashboard.py > logs/dashboard.log 2>&1 &
fi

DASHBOARD_PID=$!
echo $DASHBOARD_PID > .dashboard.pid

echo "✅ Dashboard started in background (PID: $DASHBOARD_PID)"
echo "   Visit: http://localhost:8050"
echo "   Logs:  logs/dashboard.log"
echo ""
echo "To stop the dashboard, run: ./bin/6_down.sh"
