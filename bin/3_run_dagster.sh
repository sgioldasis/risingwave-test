#!/bin/bash

# Script to run Dagster for the realtime funnel project
# This script should be run from the project root

echo "Starting Dagster orchestration for realtime funnel project..."
echo "Dagster UI will be available at: http://localhost:3000"
echo ""

# Get the project root (where this script is located from bin/)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Check if orchestration directory exists
if [ ! -d "orchestration" ]; then
    echo "Error: orchestration directory not found at $PROJECT_ROOT/orchestration"
    exit 1
fi

# Create dagster storage directory
mkdir -p dagster_storage/logs

# Check if dbt manifest exists, if not run dbt parse
if [ ! -f "dbt/target/manifest.json" ]; then
    echo "dbt manifest not found. Running dbt parse to generate manifest..."
    cd dbt && dbt parse && cd "$PROJECT_ROOT"
fi

# Set environment variable for dagster home to use our config
export DAGSTER_HOME="$PROJECT_ROOT"
# Add project root to PYTHONPATH so orchestration module can be found
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Check if uv is available
if command -v uv &> /dev/null; then
    echo "Using uv to run Dagster..."
    uv run dagster-webserver -m orchestration.definitions --host 0.0.0.0 --port 3000
else
    echo "Using dagster-webserver directly..."
    dagster-webserver -m orchestration.definitions --host 0.0.0.0 --port 3000
fi
