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
#   4. Apply sql/protobuf_demo_filedesc.sql against RisingWave.
#
# Re-run any time: idempotent (DROP SOURCE/MV IF EXISTS in the SQL).

set -euo pipefail

COUNT="${COUNT:-200}"
TPS="${TPS:-20}"
TOPIC="${TOPIC:-orders_filedesc}"

PSQL_HOST="${PSQL_HOST:-localhost}"
PSQL_PORT="${PSQL_PORT:-4566}"
PSQL_DB="${PSQL_DB:-dev}"
PSQL_USER="${PSQL_USER:-root}"

echo "=== [1/4] checking /proto bind-mount in RisingWave containers ==="
for svc in frontend-node-0 compute-node-0 meta-node-0; do
    if ! docker exec "$svc" test -d /proto 2>/dev/null; then
        echo "ERROR: container '$svc' is missing /proto bind-mount." >&2
        echo "       Run: docker compose up -d --force-recreate $svc" >&2
        exit 1
    fi
done
echo "    ok: /proto present in frontend/compute/meta."

echo ""
echo "=== [2/4] ensuring Kafka topic '$TOPIC' exists ==="
docker exec redpanda rpk topic create "$TOPIC" --partitions 3 2>/dev/null || \
    echo "    topic already exists, continuing."

echo ""
echo "=== [3/4] producing $COUNT raw-protobuf orders at ${TPS} TPS ==="
echo "         (also (re)builds proto/events.pb for schema.location)"
uv run python scripts/produce_protobuf_orders_filedesc.py --count "$COUNT" --tps "$TPS"

# After producing, confirm the descriptor is visible from inside the container.
if ! docker exec frontend-node-0 test -s /proto/events.pb; then
    echo "ERROR: /proto/events.pb not visible inside frontend-node-0." >&2
    exit 1
fi

echo ""
echo "=== [4/4] applying sql/protobuf_demo_filedesc.sql against RisingWave ==="
PGPASSWORD="" psql \
    -h "$PSQL_HOST" -p "$PSQL_PORT" -d "$PSQL_DB" -U "$PSQL_USER" \
    -v ON_ERROR_STOP=1 \
    -f sql/protobuf_demo_filedesc.sql

echo ""
echo "=== done ==="
echo "Tip: inspect raw payloads with:"
echo "     docker exec redpanda rpk topic consume $TOPIC -n 1 -o oldest"
