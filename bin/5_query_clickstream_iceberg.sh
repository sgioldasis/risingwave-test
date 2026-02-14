#!/usr/bin/env bash
# Query latest rows from clickstream Iceberg tables

cd "$(dirname "$0")/.."

echo "📊 Querying Clickstream Iceberg Tables..."
echo ""

uv run python scripts/query_clickstream_iceberg.py "$@"
