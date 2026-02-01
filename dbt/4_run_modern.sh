#!/bin/bash

# Kill any existing processes on these ports
fuser -k 8000/tcp 2>/dev/null
fuser -k 3000/tcp 2>/dev/null

# Start the Backend
echo "Starting FastAPI Backend..."
uv run python3 modern-dashboard/backend/api.py &
BACKEND_PID=$!

# Start the Frontend
echo "Starting Vite Frontend..."
cd modern-dashboard/frontend && npm run dev &
FRONTEND_PID=$!

echo "Dashboard running at http://localhost:3000"
echo "API running at http://localhost:8000"

# Keep the script running to monitor
wait $BACKEND_PID $FRONTEND_PID
