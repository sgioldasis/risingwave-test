#!/bin/bash
# =============================================================================
# Casino Production Demo
#
# End-to-end setup of the prod casino streaming demo on RisingWave:
#   1. (Re)compile proto FileDescriptorSet if protoc is available
#   2. Ensure the RisingWave core nodes are up and accepting SQL on :4566
#   3. Bring up Lakekeeper + MinIO + Trino + Grafana (idempotent)
#   4. Create the prod Kafka table `src_casino_prd`
#   5. Create funnel/aggregation MVs and Iceberg sinks
#   6. Create the faithful nested raw Iceberg archive
#   7. Print row counts for verification
#
# Prereqs: docker compose stack files in place, host has network access to
# prd2-kafka-bootstrap.kaizengaming.net:443.
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
SQL_RAW_SINK="sql/casino_prd_raw_iceberg.sql"

PSQL_URL="postgresql://root@localhost:4566/dev"

echo "=== [1/7] Proto descriptor ==="
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
echo "=== [2/7] RisingWave core nodes ==="
# Idempotent: if the frontend is already accepting SQL on :4566 we skip the
# compose call entirely. Otherwise start meta, compute, compactor and frontend
# and wait for the SQL port to come up.
if psql "$PSQL_URL" -tAc 'SELECT 1' >/dev/null 2>&1; then
    echo "RisingWave already accepting SQL on localhost:4566 — skipping start."
else
    echo "Starting RisingWave core (meta, compute, compactor, frontend, minio)..."
    docker compose up -d minio-0 meta-node-0 compute-node-0 compactor-0 frontend-node-0
    echo -n "Waiting for frontend on :4566 "
    for i in $(seq 1 60); do
        if psql "$PSQL_URL" -tAc 'SELECT 1' >/dev/null 2>&1; then
            echo " ready."
            break
        fi
        echo -n "."
        sleep 2
        if [ "$i" -eq 60 ]; then
            echo ""
            echo "ERROR: RisingWave frontend did not come up within 120s" >&2
            exit 1
        fi
    done
fi

echo ""
echo "=== [3/7] Lakekeeper + MinIO + Trino + Grafana ==="
docker compose up -d lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap trino prometheus-0 grafana-0

echo ""
echo "=== [4/7] Create table src_casino_prd ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_SOURCE"

echo ""
echo "=== [5/7] Create flat MVs + Iceberg sinks ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_PIPELINE"

echo ""
echo "=== [6/7] Create raw nested Iceberg sink (mv_casino_raw -> rw_managed_casino_raw) ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_RAW_SINK"

echo ""
echo "=== [7/7] Verifying row counts (waiting ~20s for ingest + first iceberg commit) ==="
sleep 20
psql "$PSQL_URL" <<'SQL'
\echo '--- Materialized views ---'
SELECT 'mv_casino_raw'                          AS view, COUNT(*) FROM mv_casino_raw
UNION ALL SELECT 'mv_casino_transactions',            COUNT(*) FROM mv_casino_transactions
UNION ALL SELECT 'mv_casino_real_bet_events',         COUNT(*) FROM mv_casino_real_bet_events
UNION ALL SELECT 'mv_casino_real_bet_hourly_per_customer',
                                                      COUNT(*) FROM mv_casino_real_bet_hourly_per_customer;

\echo '--- Iceberg sink tables (managed) ---'
SELECT 'rw_managed_casino_raw'                AS tbl, COUNT(*) FROM rw_managed_casino_raw
UNION ALL SELECT 'rw_managed_casino_transactions',      COUNT(*) FROM rw_managed_casino_transactions
UNION ALL SELECT 'rw_managed_casino_real_bet_events',   COUNT(*) FROM rw_managed_casino_real_bet_events
UNION ALL SELECT 'rw_managed_casino_real_bet_hourly',   COUNT(*) FROM rw_managed_casino_real_bet_hourly;
SQL

echo ""
echo "Done. See docs/PRODUCTION_CASINO_DEMO.md for the full walkthrough."
