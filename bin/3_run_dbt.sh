#!/bin/bash

# Script to run dbt commands inside the dagster-webserver container
# This script should be run from the project root

set -e

echo "=== Running dbt inside dagster-webserver container ==="
echo "Command: docker exec dagster-webserver dbt run --profiles-dir ./dbt"
echo ""

# Run dbt inside the dagster container (cd into /workspace/dbt and run with profiles-dir .)
docker exec dagster-webserver bash -c "cd /workspace/dbt && dbt run --profiles-dir ."
