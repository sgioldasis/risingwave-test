#!/bin/bash

# Script to run the modern React dashboard
# This script should be run from the project root

# Kill any existing processes on these ports
fuser -k 8000/tcp 2>/dev/null
fuser -k 4000/tcp 2>/dev/null

# Create logs directory if it doesn't exist
mkdir -p logs

# Start the Backend
echo "Starting FastAPI Backend..."
cd modern-dashboard
nohup uv run python3 backend/api.py > ../backend.log 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID > ../.backend.pid
cd ..

# Start the Frontend
echo "Starting Vite Frontend..."
cd modern-dashboard/frontend && nohup npm run dev > ../../frontend.log 2>&1 &
FRONTEND_PID=$!
cd ../..
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
