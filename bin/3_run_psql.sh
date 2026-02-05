#!/bin/bash

# Script to run the combined SQL file via psql
# This script should be run from the project root

set -e

echo "=== Running RisingWave models via psql ==="
echo ""

# Configuration
PSQL_HOST="${PSQL_HOST:-localhost}"
PSQL_PORT="${PSQL_PORT:-4566}"
PSQL_DB="${PSQL_DB:-dev}"
PSQL_USER="${PSQL_USER:-root}"
SQL_FILE="${1:-sql/all_models.sql}"

# Check if the SQL file exists
if [ ! -f "${SQL_FILE}" ]; then
    echo "Error: SQL file not found: ${SQL_FILE}"
    exit 1
fi

echo "Running SQL file: ${SQL_FILE}"
echo "Host: ${PSQL_HOST}:${PSQL_PORT}"
echo "Database: ${PSQL_DB}"
echo "User: ${PSQL_USER}"
echo ""

psql -h "${PSQL_HOST}" -p "${PSQL_PORT}" -d "${PSQL_DB}" -U "${PSQL_USER}" -f "${SQL_FILE}"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ All SQL statements executed successfully!"
    echo ""
    echo "Created objects:"
    echo "  - Connection: lakekeeper_catalog_conn"
    echo "  - Sources: src_page, src_cart, src_purchase"
    echo "  - Materialized View: funnel"
    echo "  - Iceberg Tables: iceberg_cart_events, iceberg_page_views, iceberg_purchases"
    echo "  - Sinks: iceberg_cart_events_sink, iceberg_page_views_sink, iceberg_purchases_sink"
else
    echo ""
    echo "✗ Error: Some SQL statements failed"
    exit 1
fi
