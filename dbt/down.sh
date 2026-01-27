#!/bin/bash

# Script to stop Docker Compose services with volumes
# This script should be run from the dbt folder

set -e

echo "=== Stopping Docker Compose Services ==="
echo "Changing to parent directory to run docker compose down --volumes"
echo ""

# Save current directory
ORIGINAL_DIR=$(pwd)

# Change to parent directory
cd ..

echo "Running: docker compose down --volumes"
docker compose down --volumes

# Change back to original directory
cd "$ORIGINAL_DIR"

echo ""
echo "✅ Docker Compose services stopped and volumes cleaned up successfully!"
echo "All services have been terminated and persistent data removed."