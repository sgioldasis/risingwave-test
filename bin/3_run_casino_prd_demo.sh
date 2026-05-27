#!/bin/bash
# =============================================================================
# Casino Production Demo
#
# End-to-end setup of the prod casino streaming demo on RisingWave:
#   1. (Re)compile proto FileDescriptorSet if protoc is available
#   2. Bring up Lakekeeper + MinIO (idempotent)
#   3. Create the prod Kafka source `src_casino_prd`
#   4. Create funnel/aggregation MVs and Iceberg sinks
#   5. Print row counts for verification
#
# Prereqs: docker compose stack already running (./bin/1_up.sh) and your
# host has network access to prd2-kafka-bootstrap.kaizengaming.net:443.
#
# Idempotent: safe to re-run. Each SQL file uses DROP ... IF EXISTS CASCADE.
# =============================================================================

set -euo pipefail

cd "$(dirname "$0")/.."

PROTO_DIR="proto"
PROTO_FILE="${PROTO_DIR}/casinoroundinfodto.proto"
PROTO_PB="${PROTO_DIR}/casinoroundinfodto.pb"

SQL_SOURCE="sql/casino_prd_source.sql"
SQL_PIPELINE="sql/casino_prd_funnel_iceberg.sql"

PSQL_URL="postgresql://root@localhost:4566/dev"

echo "=== [1/5] Proto descriptor ==="
if command -v protoc >/dev/null 2>&1; then
    if [ ! -f "$PROTO_FILE" ]; then
        echo "ERROR: $PROTO_FILE not found" >&2
        exit 1
    fi
    echo "Recompiling $PROTO_PB from $PROTO_FILE ..."
    protoc --include_imports --descriptor_set_out="$PROTO_PB" "$PROTO_FILE"
else
    echo "protoc not on PATH — using existing $PROTO_PB"
    if [ ! -f "$PROTO_PB" ]; then
        echo "ERROR: $PROTO_PB missing and protoc unavailable" >&2
        exit 1
    fi
fi
ls -la "$PROTO_PB"

echo ""
echo "=== [2/5] Lakekeeper + MinIO ==="
docker compose up -d lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap

echo ""
echo "=== [3/5] Create source src_casino_prd ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_SOURCE"

echo ""
echo "=== [4/5] Create MVs + Iceberg sinks ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_PIPELINE"

echo ""
echo "=== [5/5] Verifying row counts (waiting ~20s for ingest + first iceberg commit) ==="
sleep 20
psql "$PSQL_URL" <<'SQL'
\echo '--- Materialized views ---'
SELECT 'mv_casino_prd_rounds_flat'            AS view, COUNT(*) FROM mv_casino_prd_rounds_flat
UNION ALL SELECT 'mv_casino_prd_volume_by_company_game', COUNT(*) FROM mv_casino_prd_volume_by_company_game
UNION ALL SELECT 'mv_casino_prd_funnel',                  COUNT(*) FROM mv_casino_prd_funnel;

\echo '--- Iceberg sink tables (managed) ---'
SELECT 'rw_managed_casino_prd_rounds' AS tbl, COUNT(*) FROM rw_managed_casino_prd_rounds
UNION ALL SELECT 'rw_managed_casino_prd_volume', COUNT(*) FROM rw_managed_casino_prd_volume
UNION ALL SELECT 'rw_managed_casino_prd_funnel', COUNT(*) FROM rw_managed_casino_prd_funnel;
SQL

echo ""
echo "Done. See docs/PRODUCTION_CASINO_DEMO.md for the full walkthrough."
