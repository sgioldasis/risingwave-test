#!/bin/bash

# Script to run dbt commands inside the dagster-webserver container
# This script should be run from the project root

set -e

echo "=== Running dbt inside dagster-webserver container ==="
echo "Command: docker exec dagster-webserver dbt run --profiles-dir ./dbt"
echo ""

# Wait for Lakekeeper warehouse to be ready
echo "Checking Lakekeeper warehouse availability..."
for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if curl -s http://localhost:8181/management/v1/warehouse | grep -q "risingwave-warehouse"; then
        echo "✓ Lakekeeper warehouse is ready"
        break
    fi
    echo "  Waiting for Lakekeeper warehouse... (attempt $attempt/10)"
    sleep 2
done

echo ""

# Run dbt inside the dagster container (cd into /workspace/dbt and run with profiles-dir .)
docker exec dagster-webserver bash -c "cd /workspace/dbt && dbt run --profiles-dir ."
