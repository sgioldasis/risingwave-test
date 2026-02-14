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

# Run dbt clean first to clear cached compiled files
echo "=== Running dbt clean to clear target directory ==="
docker exec dagster-webserver bash -c "cd /workspace/dbt && dbt clean --profiles-dir ."
echo "✓ dbt clean completed"
echo ""

# Check if we need full cleanup (only if sources exist with wrong schema)
echo "=== Checking source schemas ==="
NEED_CLEANUP=false

# Check if sources exist by listing them
SOURCE_COUNT=$(psql -h localhost -p 4566 -d dev -U root -Atc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('src_cart', 'src_purchase', 'src_page')" 2>/dev/null | head -1)

if [ "$SOURCE_COUNT" -gt 0 ]; then
    # Sources exist, check if amount is numeric (old schema) vs DOUBLE (new schema)
    AMOUNT_TYPE=$(psql -h localhost -p 4566 -d dev -U root -Atc "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'src_purchase' AND column_name = 'amount'" 2>/dev/null | head -1)
    if [ "$AMOUNT_TYPE" = "numeric" ] || [ "$AMOUNT_TYPE" = "decimal" ]; then
        echo "⚠️  src_purchase has old schema (amount: $AMOUNT_TYPE), need cleanup"
        NEED_CLEANUP=true
    elif [ "$AMOUNT_TYPE" = "double" ] || [ "$AMOUNT_TYPE" = "DOUBLE" ] || [ "$AMOUNT_TYPE" = "double precision" ]; then
        echo "✓ src_purchase schema is correct (amount: $AMOUNT_TYPE)"
    else
        echo "⚠️  src_purchase schema unknown (amount: '$AMOUNT_TYPE'), need cleanup to be safe"
        NEED_CLEANUP=true
    fi
else
    echo "✓ No sources exist yet"
fi

if [ "$NEED_CLEANUP" = "true" ]; then
    echo ""
    echo "=== Performing full cleanup due to schema changes ==="
    
    # Drop sources
    psql -h localhost -p 4566 -d dev -U root -c "DROP SOURCE IF EXISTS src_cart CASCADE;" 2>/dev/null || true
    psql -h localhost -p 4566 -d dev -U root -c "DROP SOURCE IF EXISTS src_purchase CASCADE;" 2>/dev/null || true
    psql -h localhost -p 4566 -d dev -U root -c "DROP SOURCE IF EXISTS src_page CASCADE;" 2>/dev/null || true
    
    # Drop Iceberg tables
    psql -h localhost -p 4566 -d dev -U root -c "DROP TABLE IF EXISTS iceberg_cart_events CASCADE;" 2>/dev/null || true
    psql -h localhost -p 4566 -d dev -U root -c "DROP TABLE IF EXISTS iceberg_purchases CASCADE;" 2>/dev/null || true
    psql -h localhost -p 4566 -d dev -U root -c "DROP TABLE IF EXISTS iceberg_page_views CASCADE;" 2>/dev/null || true
    
    echo "✓ Objects dropped"
    echo ""
    
    # Wait a moment for changes to propagate
    sleep 2
    
    # Purge Lakekeeper catalog tables to clear cached schemas
    echo "=== Purging Lakekeeper catalog tables ==="
    curl -s -X DELETE "http://localhost:8181/catalog/v1/namespaces/public/tables/iceberg_cart_events" 2>/dev/null || true
    curl -s -X DELETE "http://localhost:8181/catalog/v1/namespaces/public/tables/iceberg_purchases" 2>/dev/null || true
    curl -s -X DELETE "http://localhost:8181/catalog/v1/namespaces/public/tables/iceberg_page_views" 2>/dev/null || true
    echo "✓ Lakekeeper tables purged (if they existed)"
    echo ""
    
    # Clean MinIO Iceberg data
    echo "=== Cleaning MinIO Iceberg data ==="
    docker exec minio-0 mc rm --recursive --force minio/risingwave-warehouse/public/iceberg_cart_events 2>/dev/null || true
    docker exec minio-0 mc rm --recursive --force minio/risingwave-warehouse/public/iceberg_purchases 2>/dev/null || true
    docker exec minio-0 mc rm --recursive --force minio/risingwave-warehouse/public/iceberg_page_views 2>/dev/null || true
    echo "✓ MinIO data cleaned (if it existed)"
    echo ""
    
    # Explicitly recreate sources with correct schema BEFORE dbt run
    echo "=== Recreating sources with correct schema ==="
    psql -h localhost -p 4566 -d dev -U root -c "
    CREATE SOURCE IF NOT EXISTS src_cart (
        user_id int,
        item_id varchar,
        event_time timestamp
    ) WITH (
        connector = 'kafka',
        topic = 'cart_events',
        properties.bootstrap.server = 'redpanda:9092',
        scan.startup.mode = 'earliest'
    ) FORMAT PLAIN ENCODE JSON;"
    
    psql -h localhost -p 4566 -d dev -U root -c "
    CREATE SOURCE IF NOT EXISTS src_purchase (
        user_id int,
        amount DOUBLE,
        event_time timestamp
    ) WITH (
        connector = 'kafka',
        topic = 'purchases',
        properties.bootstrap.server = 'redpanda:9092',
        scan.startup.mode = 'earliest'
    ) FORMAT PLAIN ENCODE JSON;"
    
    psql -h localhost -p 4566 -d dev -U root -c "
    CREATE SOURCE IF NOT EXISTS src_page (
        user_id int,
        page_id varchar,
        event_time timestamp
    ) WITH (
        connector = 'kafka',
        topic = 'page_views',
        properties.bootstrap.server = 'redpanda:9092',
        scan.startup.mode = 'earliest'
    ) FORMAT PLAIN ENCODE JSON;"
    
    echo "✓ Sources recreated"
    echo ""
else
    echo "✓ No schema changes detected, skipping cleanup"
    echo ""
fi

# Run dbt inside the dagster container
echo "=== Running dbt ==="
docker exec dagster-webserver bash -c "cd /workspace/dbt && dbt run --profiles-dir . --full-refresh"

echo ""
echo "=== dbt run completed ==="
