#!/bin/bash

# Query funnel analytics from Iceberg tables via DuckDB
#
# This script runs query_raw_iceberg.py to fetch and display
# funnel analytics (viewers, carters, purchasers) from Iceberg.
#
# Usage:
#   ./5_query_iceberg.sh              # Show funnel analytics
#   ./5_query_iceberg.sh --debug      # Show debug info with raw counts
#   ./5_query_iceberg.sh --live       # Live monitoring mode

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "================================================================================"
echo "🧊 Querying Iceberg Tables via DuckDB"
echo "================================================================================"
echo ""

cd "$SCRIPT_DIR"

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed"
    exit 1
fi

# Run the query script with any provided arguments
python3 query_raw_iceberg.py "$@"
