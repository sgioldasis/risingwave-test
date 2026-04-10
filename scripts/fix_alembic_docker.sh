#!/bin/bash
# Fix Dagster Alembic migration issue inside Docker container

set -e

echo "Fixing Dagster Alembic migration issue..."
echo "=========================================="

# Check if running inside Docker container
if [ -f /.dockerenv ]; then
    echo "Running inside Docker container"
    # Run the Python fix script
    /opt/dagster-venv/bin/python /workspace/scripts/fix_dagster_alembic.py
else
    echo "Running from host - executing in dagster-webserver container"
    
    # Check if the container is running
    if docker compose ps dagster-webserver | grep -q "running"; then
        docker compose exec dagster-webserver /opt/dagster-venv/bin/python /workspace/scripts/fix_dagster_alembic.py
    else
        echo "ERROR: dagster-webserver container is not running"
        echo "Please start the containers first with: docker compose up -d dagster-webserver"
        exit 1
    fi
fi

echo ""
echo "Done! You can now restart Dagster if needed."
