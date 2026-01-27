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
echo "✅ Docker Compose services started successfully!"
echo "You can now run your dbt models and applications."