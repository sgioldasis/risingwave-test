#!/bin/bash

# Query funnel analytics from Iceberg tables via DuckDB
#
# This script runs duckdb_query_iceberg.py to fetch and display
# funnel analytics (viewers, carters, purchasers) from Iceberg.
#
# Usage:
#   ./bin/5_query_iceberg.sh              # Show funnel analytics
#   ./bin/5_query_iceberg.sh --debug      # Show debug info with raw counts
#   ./bin/5_query_iceberg.sh --live       # Live monitoring mode

set -e

echo "================================================================================"
echo "🧊 Querying Iceberg Tables via DuckDB"
echo "================================================================================"
echo ""

# Prefer the project's virtual environment so duckdb and other deps resolve.
if [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
elif command -v uv &> /dev/null; then
    PY="uv run python"
elif command -v python3 &> /dev/null; then
    PY="python3"
else
    echo "❌ Error: no Python interpreter found (.venv, uv, or python3)"
    exit 1
fi

# Run the query script with any provided arguments
exec $PY scripts/duckdb_query_iceberg.py "$@"
