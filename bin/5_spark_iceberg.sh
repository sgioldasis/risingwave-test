#!/bin/bash
set -e

# Script to run the Spark Notebook for Iceberg User Activity Flow
# This script should be run from the project root

echo "🚀 Starting Spark Notebook for Iceberg User Activity Flow"
echo "============================================================"

# Fixed port for marimo
MARIMO_PORT=2718

# Function to kill existing marimo processes
kill_existing_marimo() {
    echo "🧹 Cleaning up existing marimo processes..."
    
    # Kill process from PID file if it exists
    if [ -f /tmp/marimo.pid ]; then
        OLD_PID=$(cat /tmp/marimo.pid)
        if kill -0 $OLD_PID 2>/dev/null; then
            echo "   Stopping marimo process (PID: $OLD_PID)..."
            kill $OLD_PID 2>/dev/null || true
            sleep 1
            # Force kill if still running
            if kill -0 $OLD_PID 2>/dev/null; then
                kill -9 $OLD_PID 2>/dev/null || true
            fi
        fi
        rm -f /tmp/marimo.pid
    fi
    
    # Kill any process using the marimo port
    local PORT_PID=$(lsof -Pi :$MARIMO_PORT -sTCP:LISTEN -t 2>/dev/null)
    if [ -n "$PORT_PID" ]; then
        echo "   Killing process using port $MARIMO_PORT (PID: $PORT_PID)..."
        kill $PORT_PID 2>/dev/null || true
        sleep 1
        # Force kill if still running
        if kill -0 $PORT_PID 2>/dev/null; then
            kill -9 $PORT_PID 2>/dev/null || true
        fi
    fi
    
    # Also kill any marimo processes by name
    local MARIMO_PIDS=$(pgrep -f "marimo edit" 2>/dev/null || true)
    if [ -n "$MARIMO_PIDS" ]; then
        echo "   Killing any remaining marimo processes..."
        echo "$MARIMO_PIDS" | xargs kill -9 2>/dev/null || true
    fi
    
    # Wait a moment for cleanup
    sleep 1
    echo "✅ Cleanup complete"
    echo ""
}

# Check if marimo is already running and kill it to restart fresh
if lsof -Pi :$MARIMO_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "ℹ️  Marimo is already running at http://localhost:$MARIMO_PORT"
    echo "🔄 Restarting to load latest version..."
    kill_existing_marimo
else
    # Still clean up any stale processes
    kill_existing_marimo 2>/dev/null || true
fi

# Run the marimo notebook in edit mode
# Use --no-browser flag since we'll open it ourselves
# This ensures the URL is printed and browser opens correctly
echo "📝 Starting marimo server..."
echo "🌐 The notebook will be available at: http://localhost:$MARIMO_PORT"
echo ""

# Start marimo detached (nohup) so it survives when this script is stopped
# The --no-token flag disables the authentication prompt
# Use --headless to not auto-open browser (we do it ourselves)
# Use a fixed port so the browser knows where to connect
nohup uv run marimo edit --headless --no-token -p $MARIMO_PORT scripts/user_activity_flow.py > /tmp/marimo.log 2>&1 &
MARIMO_PID=$!

# Store PID for later cleanup
echo $MARIMO_PID > /tmp/marimo.pid

echo "🔄 Marimo started with PID: $MARIMO_PID"

# Wait for server to be ready and open browser
echo "⏳ Waiting for server to start..."
for i in {1..30}; do
    if curl -s http://localhost:$MARIMO_PORT >/dev/null 2>&1; then
        echo "✅ Server ready at http://localhost:$MARIMO_PORT"
        echo "🌐 Opening browser..."
        python3 -c "import webbrowser; webbrowser.open('http://localhost:$MARIMO_PORT')" 2>/dev/null || true
        break
    fi
    sleep 0.5
done

echo ""
echo "✨ Marimo notebook is running in the background"
echo "📍 URL: http://localhost:$MARIMO_PORT"
echo "📝 Logs: tail -f /tmp/marimo.log"
echo "🔄 To restart with latest changes, just run this script again"
echo "⛔ To stop manually: kill \$(cat /tmp/marimo.pid)"
echo ""
echo "This script has completed. The notebook server remains running."
