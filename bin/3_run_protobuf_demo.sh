#!/usr/bin/env bash
# End-to-end runner for the RisingWave protobuf + nested data demo.
#
#   1. Ensure topic `orders` exists in Redpanda.
#   2. Produce N protobuf-encoded Order messages (also registers the schema
#      under subject `orders-value` in the Schema Registry).
#   3. Apply sql/protobuf_demo.sql against RisingWave — creates source, the
#      revenue rollup MV, two `ENGINE = iceberg` tables in Lakekeeper, and
#      upsert sinks into them.
#   4. Wait for the first iceberg commit (~5s) and print row counts.
#
# Re-run any time: it is idempotent (DROP SOURCE/MV/TABLE/SINK IF EXISTS).

set -euo pipefail

COUNT="${COUNT:-200}"
TPS="${TPS:-0}"
TOPIC="${TOPIC:-orders}"

PSQL_HOST="${PSQL_HOST:-localhost}"
PSQL_PORT="${PSQL_PORT:-4566}"
PSQL_DB="${PSQL_DB:-dev}"
PSQL_USER="${PSQL_USER:-root}"

# Time to wait for the first iceberg checkpoint after sinks are created.
# Sinks use commit_checkpoint_interval=5; 10s is comfortably above that.
# Set ICEBERG_WAIT=0 to skip the wait/verification step.
ICEBERG_WAIT="${ICEBERG_WAIT:-10}"

psql_run() {
    PGPASSWORD="" psql \
        -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
        -v ON_ERROR_STOP=1 -t -A "$@"
}

echo "=== [1/4] ensuring Kafka topic '$TOPIC' exists ==="
docker exec redpanda rpk topic create "$TOPIC" --partitions 3 2>/dev/null || \
    echo "    topic already exists, continuing."

echo ""
echo "=== [2/4] producing $COUNT protobuf orders at ${TPS} TPS ==="
uv run python scripts/produce_protobuf_orders.py --count "$COUNT" --tps "$TPS"

echo ""
echo "=== [3/4] applying sql/protobuf_demo.sql against RisingWave ==="
PGPASSWORD="" psql \
    -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
    -v ON_ERROR_STOP=1 \
    -f sql/protobuf_demo.sql

if [ "$ICEBERG_WAIT" -gt 0 ]; then
    echo ""
    echo "=== [4/4] waiting ${ICEBERG_WAIT}s for first iceberg commit ==="
    echo "    (commit_checkpoint_interval = 5s on both managed iceberg sinks)"
    sleep "$ICEBERG_WAIT"

    orders_n=$(psql_run -c "SELECT count(*) FROM rw_managed_proto_orders;"  | tr -d '[:space:]')
    rev_n=$(psql_run    -c "SELECT count(*) FROM rw_managed_proto_revenue;" | tr -d '[:space:]')
    echo "    rw_managed_proto_orders   rows: $orders_n"
    echo "    rw_managed_proto_revenue  rows: $rev_n"

    if [ "$orders_n" = "0" ] || [ "$rev_n" = "0" ]; then
        echo ""
        echo "Note: still zero — give it another ~10s and re-run the SELECTs."
    fi
else
    echo ""
    echo "=== [4/4] skipped (ICEBERG_WAIT=0) ==="
    echo "Iceberg tables will populate ~5s after sinks were created."
fi

echo ""
echo "=== done ==="
echo "Tip: open Redpanda Console at http://localhost:9090 to inspect the"
echo "     registered schema (subject 'orders-value') and the orders topic."
echo "Same iceberg tables are now visible to:"
echo "  🦆 DuckDB Iceberg   — ./bin/5_duckdb_iceberg.sh"
echo "  🔥 Spark Iceberg    — ./bin/5_spark_iceberg.sh"
echo "  🧊 Trino Iceberg    — ./bin/5_marimo_risingwave.sh"
echo "Look for tables 'public.rw_managed_proto_orders' and 'public.rw_managed_proto_revenue'."
