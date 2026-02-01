#!/bin/bash

# Script to start Docker Compose services
# This script should be run from the dbt folder

set -e

echo "=== Starting Docker Compose Services ==="
echo "Changing to parent directory to run docker compose up -d"
echo ""

# Save current directory
ORIGINAL_DIR=$(pwd)

# Change to parent directory
cd ..

echo "Running: docker compose up -d"
docker compose up -d

# Change back to original directory
cd "$ORIGINAL_DIR"

echo ""
echo "Installing Python dependencies with uv..."
uv sync

echo ""
echo "Checking modern dashboard dependencies..."
if [ ! -d "modern-dashboard/frontend/node_modules" ]; then
    echo "node_modules not found. Installing frontend dependencies..."
    cd modern-dashboard/frontend && npm install && cd ../..
    echo "✅ Frontend dependencies installed"
else
    echo "✅ Frontend dependencies already installed (node_modules exists)"
fi

echo ""
echo "✅ Docker Compose services started successfully!"
echo "✅ Python dependencies installed with uv sync"
echo "You can now run your dbt models and applications."