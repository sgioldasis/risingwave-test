#!/usr/bin/env bash
# End-to-end runner for the RisingWave protobuf + nested data demo.
#
#   1. Ensure topic `orders` exists in Redpanda.
#   2. Produce N protobuf-encoded Order messages (also registers the schema
#      under subject `orders-value` in the Schema Registry).
#   3. Apply sql/protobuf_demo.sql against RisingWave.
#
# Re-run any time: it is idempotent (DROP SOURCE/MV IF EXISTS in the SQL).

set -euo pipefail

COUNT="${COUNT:-200}"
TPS="${TPS:-20}"
TOPIC="${TOPIC:-orders}"

PSQL_HOST="${PSQL_HOST:-localhost}"
PSQL_PORT="${PSQL_PORT:-4566}"
PSQL_DB="${PSQL_DB:-dev}"
PSQL_USER="${PSQL_USER:-root}"

echo "=== [1/3] ensuring Kafka topic '$TOPIC' exists ==="
docker exec redpanda rpk topic create "$TOPIC" --partitions 3 2>/dev/null || \
    echo "    topic already exists, continuing."

echo ""
echo "=== [2/3] producing $COUNT protobuf orders at ${TPS} TPS ==="
uv run python scripts/produce_protobuf_orders.py --count "$COUNT" --tps "$TPS"

echo ""
echo "=== [3/3] applying sql/protobuf_demo.sql against RisingWave ==="
PGPASSWORD="" psql \
    -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
    -v ON_ERROR_STOP=1 \
    -f sql/protobuf_demo.sql

echo ""
echo "=== done ==="
echo "Tip: open Redpanda Console at http://localhost:9090 to inspect the"
echo "     registered schema (subject 'orders-value') and the orders topic."
