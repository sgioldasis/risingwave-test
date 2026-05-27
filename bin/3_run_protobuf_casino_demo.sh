#!/usr/bin/env bash
# End-to-end runner for the RisingWave protobuf + Cronus CasinoRoundInfoDto demo.
#
#   1. Ensure topic `casino_rounds` exists in Redpanda.
#   2. Produce N protobuf-encoded CasinoRoundInfoDto messages (also registers
#      the schema under subject `casino_rounds-value` in the Schema Registry).
#   3. Apply sql/protobuf_casino_demo.sql against RisingWave — creates source,
#      flattened MV + volume rollup MV, two `ENGINE = iceberg` tables in
#      Lakekeeper, and upsert sinks into them.
#   4. Wait for the first iceberg commit (~5s) and print row counts.
#
# Idempotent: re-run any time (DROP ... IF EXISTS in the SQL).

set -euo pipefail

COUNT="${COUNT:-200}"
TPS="${TPS:-0}"
TOPIC="${TOPIC:-casino_rounds}"

PSQL_HOST="${PSQL_HOST:-localhost}"
PSQL_PORT="${PSQL_PORT:-4566}"
PSQL_DB="${PSQL_DB:-dev}"
PSQL_USER="${PSQL_USER:-root}"

# Sinks use commit_checkpoint_interval=5; 10s is comfortably above that.
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
echo "=== [2/4] producing $COUNT protobuf casino rounds at ${TPS} TPS ==="
TOPIC="$TOPIC" uv run python scripts/produce_protobuf_casino_rounds.py \
    --count "$COUNT" --tps "$TPS"

echo ""
echo "=== [3/4] applying sql/protobuf_casino_demo.sql against RisingWave ==="
PGPASSWORD="" psql \
    -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
    -v ON_ERROR_STOP=1 \
    -f sql/protobuf_casino_demo.sql

ICEBERG_POLL_ATTEMPTS="${ICEBERG_POLL_ATTEMPTS:-3}"

if [ "$ICEBERG_WAIT" -gt 0 ]; then
    echo ""
    echo "=== [4/4] waiting for first iceberg commit (poll up to ${ICEBERG_POLL_ATTEMPTS}x ${ICEBERG_WAIT}s) ==="
    echo "    (commit_checkpoint_interval = 5s on both managed iceberg sinks)"

    rounds_n=0
    vol_n=0
    for attempt in $(seq 1 "$ICEBERG_POLL_ATTEMPTS"); do
        sleep "$ICEBERG_WAIT"
        rounds_n=$(psql_run -c "SELECT count(*) FROM rw_managed_casino_rounds;"  | tr -d '[:space:]')
        vol_n=$(psql_run    -c "SELECT count(*) FROM rw_managed_casino_volume;" | tr -d '[:space:]')
        echo "    attempt ${attempt}/${ICEBERG_POLL_ATTEMPTS}: rounds=${rounds_n}  volume=${vol_n}"
        if [ "$rounds_n" != "0" ] && [ "$vol_n" != "0" ]; then
            break
        fi
    done

    if [ "$rounds_n" = "0" ] || [ "$vol_n" = "0" ]; then
        echo ""
        echo "Note: still zero after ${ICEBERG_POLL_ATTEMPTS} polls — give it another ~10s and re-run the SELECTs."
    fi
else
    echo ""
    echo "=== [4/4] skipped (ICEBERG_WAIT=0) ==="
fi

echo ""
echo "=== done ==="
echo "Tip: open Redpanda Console at http://localhost:9090 to inspect the"
echo "     registered schema (subject '${TOPIC}-value') and the topic."
echo "Same iceberg tables are now visible to:"
echo "  🦆 DuckDB Iceberg   — ./bin/5_duckdb_iceberg.sh"
echo "  🔥 Spark Iceberg    — ./bin/5_spark_iceberg.sh"
echo "  🧊 Marimo/Trino     — ./bin/5_marimo_risingwave.sh"
echo "Look for tables 'public.rw_managed_casino_rounds' and 'public.rw_managed_casino_volume'."
