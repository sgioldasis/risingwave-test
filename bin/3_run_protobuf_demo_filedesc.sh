#!/usr/bin/env bash
# End-to-end runner for the RisingWave protobuf demo using a FileDescriptorSet
# (.pb) — NO Schema Registry. Companion to bin/3_run_protobuf_demo.sh.
#
#   1. Verify the RisingWave containers have /proto bind-mounted (added to
#      docker-compose.yml). If the stack predates that change, prompt the user
#      to recreate the affected services.
#   2. Ensure topic `orders_filedesc` exists in Redpanda.
#   3. Compile proto/events.proto -> proto/events.pb (FileDescriptorSet with
#      --include_imports) and produce N raw-protobuf Order messages.
#   4. Apply sql/protobuf_demo_filedesc.sql against RisingWave — creates source,
#      revenue rollup MV, two `ENGINE = iceberg` tables in Lakekeeper, and
#      upsert sinks into them.
#   5. Wait for the first iceberg commit (~5s) and print row counts.
#
# Re-run any time: idempotent (DROP SOURCE/MV/TABLE/SINK IF EXISTS in the SQL).

set -euo pipefail

COUNT="${COUNT:-200}"
TPS="${TPS:-0}"
TOPIC="${TOPIC:-orders_filedesc}"

PSQL_HOST="${PSQL_HOST:-localhost}"
PSQL_PORT="${PSQL_PORT:-4566}"
PSQL_DB="${PSQL_DB:-dev}"
PSQL_USER="${PSQL_USER:-root}"

ICEBERG_WAIT="${ICEBERG_WAIT:-10}"

psql_run() {
    PGPASSWORD="" psql \
        -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
        -v ON_ERROR_STOP=1 -t -A "$@"
}

echo "=== [1/5] checking /proto bind-mount in RisingWave containers ==="
for svc in frontend-node-0 compute-node-0 meta-node-0; do
    if ! docker exec "$svc" test -d /proto 2>/dev/null; then
        echo "ERROR: container '$svc' is missing /proto bind-mount." >&2
        echo "       Run: docker compose up -d --force-recreate $svc" >&2
        exit 1
    fi
done
echo "    ok: /proto present in frontend/compute/meta."

echo ""
echo "=== [2/5] ensuring Kafka topic '$TOPIC' exists ==="
docker exec redpanda rpk topic create "$TOPIC" --partitions 3 2>/dev/null || \
    echo "    topic already exists, continuing."

echo ""
echo "=== [3/5] producing $COUNT raw-protobuf orders at ${TPS} TPS ==="
echo "         (also (re)builds proto/events.pb for schema.location)"
uv run python scripts/produce_protobuf_orders_filedesc.py --count "$COUNT" --tps "$TPS"

# After producing, confirm the descriptor is visible from inside the container.
if ! docker exec frontend-node-0 test -s /proto/events.pb; then
    echo "ERROR: /proto/events.pb not visible inside frontend-node-0." >&2
    exit 1
fi

echo ""
echo "=== [4/5] applying sql/protobuf_demo_filedesc.sql against RisingWave ==="
PGPASSWORD="" psql \
    -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
    -v ON_ERROR_STOP=1 \
    -f sql/protobuf_demo_filedesc.sql

if [ "$ICEBERG_WAIT" -gt 0 ]; then
    echo ""
    echo "=== [5/5] waiting ${ICEBERG_WAIT}s for first iceberg commit ==="
    echo "    (commit_checkpoint_interval = 5s on both managed iceberg sinks)"
    sleep "$ICEBERG_WAIT"

    orders_n=$(psql_run -c "SELECT count(*) FROM rw_managed_proto_fd_orders;"  | tr -d '[:space:]')
    rev_n=$(psql_run    -c "SELECT count(*) FROM rw_managed_proto_fd_revenue;" | tr -d '[:space:]')
    echo "    rw_managed_proto_fd_orders   rows: $orders_n"
    echo "    rw_managed_proto_fd_revenue  rows: $rev_n"

    if [ "$orders_n" = "0" ] || [ "$rev_n" = "0" ]; then
        echo ""
        echo "Note: still zero — give it another ~10s and re-run the SELECTs."
    fi
else
    echo ""
    echo "=== [5/5] skipped (ICEBERG_WAIT=0) ==="
    echo "Iceberg tables will populate ~5s after sinks were created."
fi

echo ""
echo "=== done ==="
echo "Tip: inspect raw payloads with:"
echo "     docker exec redpanda rpk topic consume $TOPIC -n 1 -o oldest"
echo "Same iceberg tables are now visible to:"
echo "  🦆 DuckDB Iceberg   — ./bin/5_duckdb_iceberg.sh"
echo "  🔥 Spark Iceberg    — ./bin/5_spark_iceberg.sh"
echo "  🧊 Trino Iceberg    — ./bin/5_marimo_risingwave.sh"
echo "Look for tables 'public.rw_managed_proto_fd_orders' and 'public.rw_managed_proto_fd_revenue'."
