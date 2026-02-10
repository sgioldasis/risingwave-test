#!/bin/bash

# Script to run the modern React dashboard
# This script should be run from the project root

echo "Starting Modern Dashboard..."
echo "Dashboard will be available at: http://localhost:4000"
echo ""

# Kill any existing processes on these ports
fuser -k 8000/tcp 2>/dev/null
fuser -k 4000/tcp 2>/dev/null

# Also kill by process pattern
pkill -f "python3 backend/api.py" 2>/dev/null
pkill -f "npm run dev" 2>/dev/null
sleep 0.5

# Create logs directory if it doesn't exist
mkdir -p logs

# Start the Backend
echo "Starting FastAPI Backend..."
(cd modern-dashboard && nohup uv run python3 backend/api.py > ../backend.log 2>&1) &
BACKEND_PID=$!
echo $BACKEND_PID > .backend.pid

# Start the Frontend
echo "Starting Vite Frontend..."
(cd modern-dashboard/frontend && nohup npm run dev > ../../frontend.log 2>&1) &
FRONTEND_PID=$!
echo $FRONTEND_PID > .frontend.pid

echo ""
echo "✅ Dashboard running at http://localhost:4000"
echo "✅ API running at http://localhost:8000"
echo ""
echo "Backend PID: $BACKEND_PID (saved to .backend.pid)"
echo "Frontend PID: $FRONTEND_PID (saved to .frontend.pid)"
echo ""
echo "Backend logs: backend.log"
echo "Frontend logs: frontend.log"
echo ""
echo "To stop the application, run: ./bin/6_down.sh"
echo ""
echo "Monitoring processes (Ctrl+C to stop)..."
echo "==========================================="

# Keep script running and monitor child processes
trap 'echo ""; echo "Stopping dashboard..."; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit 0' INT TERM

# Wait for either process to exit
while kill -0 $BACKEND_PID 2>/dev/null && kill -0 $FRONTEND_PID 2>/dev/null; do
    sleep 2
done

# If we get here, one of the processes died
echo ""
echo "⚠️ One of the dashboard processes exited unexpectedly"
kill $BACKEND_PID 2>/dev/null
kill $FRONTEND_PID 2>/dev/null
exit 1
