#!/usr/bin/env bash
# End-to-end runner for the RisingWave Avro + nested data demo.
#
#   1. Ensure topic `orders_avro` exists in Redpanda.
#   2. Produce N Avro-encoded Order messages (also registers the schema
#      under subject `orders_avro-value` in the Schema Registry).
#   3. Apply sql/avro_demo.sql against RisingWave.
#
# Re-run any time: it is idempotent (DROP SOURCE/MV IF EXISTS in the SQL).

set -euo pipefail

COUNT="${COUNT:-200}"
TPS="${TPS:-20}"
TOPIC="${TOPIC:-orders_avro}"

PSQL_HOST="${PSQL_HOST:-localhost}"
PSQL_PORT="${PSQL_PORT:-4566}"
PSQL_DB="${PSQL_DB:-dev}"
PSQL_USER="${PSQL_USER:-root}"

SR_URL="${SR_URL:-http://localhost:8081}"
SUBJECT="${SUBJECT:-${TOPIC}-value}"

# Reset both the Schema Registry subject AND the Kafka topic so the demo
# starts from a clean slate. Without this, leftover messages from a prior
# (possibly broken) schema version reference schema ids that no longer
# resolve, and the RisingWave source stalls when reading from earliest.
echo "=== [1/3] resetting Kafka topic '$TOPIC' and SR subject '$SUBJECT' ==="
docker exec redpanda rpk topic delete "$TOPIC" 2>/dev/null || true
docker exec redpanda rpk topic create "$TOPIC" --partitions 3 >/dev/null

curl -fsS -X DELETE "$SR_URL/subjects/$SUBJECT"               -o /dev/null || true
curl -fsS -X DELETE "$SR_URL/subjects/$SUBJECT?permanent=true" -o /dev/null || true
echo "    topic recreated, SR subject cleared."

echo ""
echo "=== [2/3] producing $COUNT Avro orders at ${TPS} TPS ==="
TOPIC="$TOPIC" uv run python scripts/produce_avro_orders.py --count "$COUNT" --tps "$TPS"

echo ""
echo "=== [3/3] applying sql/avro_demo.sql against RisingWave ==="
PGPASSWORD="" psql \
    -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
    -v ON_ERROR_STOP=1 \
    -f sql/avro_demo.sql

echo ""
echo "=== done ==="
echo "Tip: open Redpanda Console at http://localhost:9090 to inspect the"
echo "     registered schema (subject 'orders_avro-value') and the orders_avro topic."
