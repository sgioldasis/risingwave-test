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
echo "=== Cleaning up local directories ==="

# Remove target and logs directories if they exist
if [ -d "target" ]; then
    echo "Removing target/ directory..."
    rm -rf target
    echo "✅ target/ directory removed"
else
    echo "target/ directory does not exist, skipping..."
fi

if [ -d "logs" ]; then
    echo "Removing logs/ directory..."
    rm -rf logs
    echo "✅ logs/ directory removed"
else
    echo "logs/ directory does not exist, skipping..."
fi

echo ""
echo "✅ Docker Compose services stopped and volumes cleaned up successfully!"
echo "✅ Local directories (target/, logs/) cleaned up!"
echo "All services have been terminated and persistent data removed."