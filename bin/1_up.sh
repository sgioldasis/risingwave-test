#!/bin/bash

# Script to start Docker Compose services
# This script should be run from the project root

set -e

echo "=== Starting Docker Compose Services ==="
echo "Running docker compose up -d from project root"
echo ""

# Run from project root (where docker-compose.yml is located)
docker compose up -d

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
