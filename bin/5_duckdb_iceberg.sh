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

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed"
    exit 1
fi

# Run the query script with any provided arguments
python3 scripts/duckdb_query_iceberg.py "$@"
