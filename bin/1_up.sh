#!/bin/bash

# Script to start Docker Compose services
# This script should be run from the project root

set -e

echo "=== Pre-flight: Cleaning up any existing Dagster state ==="
# Stop and remove Dagster containers first to release the volume
# This is necessary because Docker won't remove a volume that's in use
docker compose rm -fsv dagster-daemon dagster-webserver 2>/dev/null || true

# Remove the dagster-storage volume to ensure a clean state
# This prevents "Version table 'alembic_version' has more than one head" errors
echo "Removing dagster-storage volume..."
docker volume rm -f dagster-storage 2>/dev/null || true
echo "✅ Cleaned up Dagster storage"

echo ""
echo "=== Starting Docker Compose Services ==="
echo "Running docker compose up -d from project root"
echo ""

# Run from project root (where docker-compose.yml is located)
# Use --build to ensure images are rebuilt when Dockerfiles change
docker compose up --build -d

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
echo "✅ Dagster started with fresh storage (no Alembic conflicts)"
echo "You can now run your dbt models and applications."
