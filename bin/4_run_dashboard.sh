#!/bin/bash

# Script to run the legacy dashboard
# This script should be run from the project root

echo "Starting Real-time E-commerce Funnel Dashboard..."
echo "Dashboard will be available at: http://localhost:8050"
echo ""

# Kill any existing dashboard processes
pkill -f "python scripts/dashboard.py" 2>/dev/null
sleep 0.5

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
echo ""
echo "Monitoring process (Ctrl+C to stop)..."
echo "==========================================="

# Keep script running and monitor the dashboard process
trap 'echo ""; echo "Stopping dashboard..."; kill $DASHBOARD_PID 2>/dev/null; exit 0' INT TERM

# Wait for process to exit
wait $DASHBOARD_PID
DASHBOARD_EXIT_CODE=$?

echo ""
echo "⚠️ Dashboard process exited (code: $DASHBOARD_EXIT_CODE)"
exit $DASHBOARD_EXIT_CODE
