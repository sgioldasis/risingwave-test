#!/bin/bash
# Fix Alembic 'more than one head' error by completely resetting Dagster storage
# This script is called by 6_down.sh and can also be run standalone

set -e

echo "=== Fixing Alembic Migration Error ==="
echo ""

# Stop all Dagster-related containers (if running)
echo "Ensuring Dagster containers are stopped..."
docker compose stop dagster-webserver dagster-daemon 2>/dev/null || true

# Remove the dagster-storage volume completely
echo "Removing dagster-storage volume..."
docker volume rm -f dagster-storage 2>/dev/null || true

# Also remove any dangling volumes
echo "Cleaning up dangling volumes..."
docker volume prune -f 2>/dev/null || true

# Clean up any local dagster_storage directory (if running outside Docker)
if [ -d "./dagster_storage" ]; then
    echo "Removing local dagster_storage directory..."
    rm -rf ./dagster_storage
fi

echo ""
echo "✅ Alembic issue fixed. Dagster storage has been reset."
