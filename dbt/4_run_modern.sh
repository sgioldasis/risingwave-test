#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Kill any existing processes on these ports
fuser -k 8000/tcp 2>/dev/null
fuser -k 3000/tcp 2>/dev/null

# Start the Backend
echo "Starting FastAPI Backend..."
nohup uv run python3 modern-dashboard/backend/api.py > "$SCRIPT_DIR/backend.log" 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID > "$SCRIPT_DIR/.backend.pid"

# Start the Frontend
echo "Starting Vite Frontend..."
cd modern-dashboard/frontend && nohup npm run dev > "$SCRIPT_DIR/frontend.log" 2>&1 &
FRONTEND_PID=$!
echo $FRONTEND_PID > "$SCRIPT_DIR/.frontend.pid"

echo ""
echo "✅ Dashboard running at http://localhost:3000"
echo "✅ API running at http://localhost:8000"
echo ""
echo "Backend PID: $BACKEND_PID (saved to .backend.pid)"
echo "Frontend PID: $FRONTEND_PID (saved to .frontend.pid)"
echo ""
echo "Backend logs: $SCRIPT_DIR/backend.log"
echo "Frontend logs: $SCRIPT_DIR/frontend.log"
echo ""
echo "To stop the application, run: ./6_down.sh"
