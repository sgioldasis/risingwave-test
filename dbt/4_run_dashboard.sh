#!/bin/bash

echo "Starting Real-time E-commerce Funnel Dashboard..."
echo "Dashboard will be available at: http://localhost:8050"
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if dashboard is already running
if pgrep -f "python dashboard.py" > /dev/null 2>&1; then
    echo "⚠️  Dashboard is already running!"
    echo "   Visit: http://localhost:8050"
    exit 0
fi

# Use uv to run the dashboard in background
if command -v uv &> /dev/null; then
    echo "Using uv to run dashboard in background..."
    cd "$SCRIPT_DIR"
    uv run python dashboard.py > logs/dashboard.log 2>&1 &
else
    echo "uv not found, falling back to system python..."
    # Activate virtual environment if it exists
    if [ -d "$SCRIPT_DIR/.venv" ]; then
        source "$SCRIPT_DIR/.venv/bin/activate"
    fi
    python "$SCRIPT_DIR/dashboard.py" > "$SCRIPT_DIR/logs/dashboard.log" 2>&1 &
fi

DASHBOARD_PID=$!
echo $DASHBOARD_PID > "$SCRIPT_DIR/.dashboard.pid"

echo "✅ Dashboard started in background (PID: $DASHBOARD_PID)"
echo "   Visit: http://localhost:8050"
echo "   Logs:  dbt/logs/dashboard.log"
echo ""
echo "To stop the dashboard, run: ./6_down.sh"
