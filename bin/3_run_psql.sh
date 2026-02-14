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
echo "Running SQL file: ${SQL_FILE}"
echo "Host: ${PSQL_HOST}:${PSQL_PORT}"
echo "Database: ${PSQL_DB}"
echo "User: ${PSQL_USER}"
echo ""

# Use stdbuf to force line buffering and -a to echo all commands
# This ensures output appears immediately in the web runner
stdbuf -oL psql -h "${PSQL_HOST}" -p "${PSQL_PORT}" -d "${PSQL_DB}" -U "${PSQL_USER}" -f "${SQL_FILE}" -a

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ All SQL statements executed successfully!"
    echo ""
    echo "Created objects:"
    echo "  - Connection: lakekeeper_catalog_conn"
    echo ""
    echo "  Kafka Sources:"
    echo "    - src_page, src_cart, src_purchase"
    echo ""
    echo "  Rich Tables (Direct Producer):"
    echo "    - users, sessions, devices, campaigns, page_catalog, clickstream_events"
    echo ""
    echo "  Materialized View:"
    echo "    - funnel"
    echo ""
    echo "  Original Iceberg Tables (Funnel):"
    echo "    - iceberg_cart_events, iceberg_page_views, iceberg_purchases"
    echo ""
    echo "  New Clickstream Iceberg Tables:"
    echo "    - iceberg_users, iceberg_sessions, iceberg_devices"
    echo "    - iceberg_campaigns, iceberg_page_catalog, iceberg_clickstream_events"
    echo ""
    echo "  Original Sinks (Funnel):"
    echo "    - iceberg_cart_events_sink, iceberg_page_views_sink, iceberg_purchases_sink"
    echo ""
    echo "  New Clickstream Sinks:"
    echo "    - iceberg_users_sink, iceberg_sessions_sink, iceberg_devices_sink"
    echo "    - iceberg_campaigns_sink, iceberg_page_catalog_sink, iceberg_clickstream_events_sink"
else
    echo ""
    echo "✗ Error: Some SQL statements failed"
    exit 1
fi
