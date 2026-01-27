#!/bin/bash

echo "Starting Real-time E-commerce Funnel Dashboard..."
echo "Dashboard will be available at: http://localhost:8050"
echo "Press Ctrl+C to stop the dashboard"
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Use uv to run the dashboard
if command -v uv &> /dev/null; then
    echo "Using uv to run dashboard..."
    cd "$SCRIPT_DIR"
    uv run python dashboard.py
else
    echo "uv not found, falling back to system python..."
    # Activate virtual environment if it exists
    if [ -d "$SCRIPT_DIR/.venv" ]; then
        source "$SCRIPT_DIR/.venv/bin/activate"
        echo "Virtual environment activated"
    fi
    python "$SCRIPT_DIR/dashboard.py"
fi