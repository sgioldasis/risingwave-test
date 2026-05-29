#!/bin/bash
# =============================================================================
# Casino Production Demo
#
# End-to-end setup of the prod casino + sportsbook streaming demo on RisingWave:
#   1. (Re)compile proto FileDescriptorSets if protoc is available
#   2. Ensure RisingWave core nodes are up and accepting SQL on :4566
#   3. Bring up Lakekeeper + MinIO + Trino + Grafana (idempotent)
#   4. Create prod Kafka source `src_casino_prd` (prd2, cronus.casino.out.gh)
#   5. Create prod Kafka source `src_bets_gh`    (prd4, bets-out-gh)
#   6. Create UC1 + UC2 MVs and Iceberg sinks
#   7. Create faithful nested raw Iceberg archive (mv_casino_raw)
#   8. Print row counts for verification
#
# Prereqs: docker compose stack files in place, host has network access to
#   prd2-kafka-bootstrap.kaizengaming.net:443  (casino, SSL)
#   prd4-kafka-bootstrap.kaizengaming.net:443  (sportsbook bets, SSL)
#
# Idempotent: safe to re-run. Each SQL file uses DROP ... IF EXISTS CASCADE.
# =============================================================================

set -euo pipefail

cd "$(dirname "$0")/.."

PROTO_DIR="proto"
CASINO_PROTO_FILE="${PROTO_DIR}/casinoroundinfodto.proto"
CASINO_PROTO_PB="${PROTO_DIR}/casinoroundinfodto.pb"
BETS_PROTO_FILE="${PROTO_DIR}/betinfo.proto"
BETS_PROTO_DESC="${PROTO_DIR}/betinfo.desc"

SQL_CASINO_SOURCE="sql/casino_prd_source.sql"
SQL_BETS_SOURCE="sql/casino_prd_bets_source.sql"
SQL_PIPELINE="sql/casino_prd_funnel_iceberg.sql"
SQL_RAW_SINK="sql/casino_prd_raw_iceberg.sql"

PSQL_URL="postgresql://root@localhost:4566/dev"

echo "=== [1/8] Proto descriptors ==="
if command -v protoc >/dev/null 2>&1; then
    # Casino proto
    if [ ! -f "$CASINO_PROTO_FILE" ]; then
        echo "ERROR: $CASINO_PROTO_FILE not found" >&2
        exit 1
    fi
    echo "Recompiling $CASINO_PROTO_PB ..."
    protoc --include_imports --descriptor_set_out="$CASINO_PROTO_PB" "$CASINO_PROTO_FILE"

    # Bets proto
    if [ ! -f "$BETS_PROTO_FILE" ]; then
        echo "ERROR: $BETS_PROTO_FILE not found" >&2
        echo "Fetch with: curl -fsSL -H 'Accept: text/plain' \\"
        echo "  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/betinfo \\"
        echo "  > proto/betinfo.proto"
        exit 1
    fi
    echo "Recompiling $BETS_PROTO_DESC ..."
    protoc --include_imports \
        --descriptor_set_out="$BETS_PROTO_DESC" \
        --proto_path=/opt/homebrew/include \
        --proto_path=proto \
        "$BETS_PROTO_FILE"
else
    echo "protoc not on PATH — using existing descriptors"
    if [ ! -f "$CASINO_PROTO_PB" ]; then
        echo "ERROR: $CASINO_PROTO_PB missing and protoc unavailable" >&2
        exit 1
    fi
    if [ ! -f "$BETS_PROTO_DESC" ]; then
        echo "ERROR: $BETS_PROTO_DESC missing and protoc unavailable" >&2
        exit 1
    fi
fi
ls -lh "$CASINO_PROTO_PB" "$BETS_PROTO_DESC"

echo ""
echo "=== [2/8] RisingWave core nodes ==="
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
echo "=== [3/8] Lakekeeper + MinIO + Trino + Grafana ==="
docker compose up -d lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap trino prometheus-0 grafana-0

echo ""
echo "=== [4/8] Create source src_casino_prd (prd2 — cronus.casino.out.gh) ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_CASINO_SOURCE"

echo ""
echo "=== [5/8] Create source src_bets_gh (prd4 — bets-out-gh) ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_BETS_SOURCE"

echo ""
echo "=== [6/8] Create UC1 + UC2 MVs and Iceberg sinks ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_PIPELINE"

echo ""
echo "=== [7/8] Create raw nested Iceberg archive (mv_casino_raw) ==="
psql "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_RAW_SINK"

echo ""
echo "=== [8/8] Verifying row counts (waiting ~20s for ingest + first Iceberg commit) ==="
sleep 20
psql "$PSQL_URL" <<'SQL'
\echo '--- UC1 materialized views ---'
SELECT 'mv_casino_transactions'    AS view, COUNT(*) FROM mv_casino_transactions
UNION ALL SELECT 'mv_casino_real_bet_events', COUNT(*) FROM mv_casino_real_bet_events
UNION ALL SELECT 'mv_casino_real_bet',        COUNT(*) FROM mv_casino_real_bet;

\echo ''
\echo '--- UC2 materialized views ---'
SELECT 'mv_casino_turnover_events',     COUNT(*) FROM mv_casino_turnover_events
UNION ALL SELECT 'mv_sportsbook_turnover_events', COUNT(*) FROM mv_sportsbook_turnover_events
UNION ALL SELECT 'mv_turnover_percentage',        COUNT(*) FROM mv_turnover_percentage;

\echo ''
\echo '--- Raw archive ---'
SELECT 'mv_casino_raw', COUNT(*) FROM mv_casino_raw;

\echo ''
\echo '--- Iceberg sink tables ---'
SELECT 'rw_managed_casino_real_bet',     COUNT(*) FROM rw_managed_casino_real_bet
UNION ALL SELECT 'rw_managed_turnover_percentage', COUNT(*) FROM rw_managed_turnover_percentage
UNION ALL SELECT 'rw_managed_casino_raw',          COUNT(*) FROM rw_managed_casino_raw;
SQL

echo ""
echo "Done. See docs/PRODUCTION_CASINO_DEMO.md for the full walkthrough."
