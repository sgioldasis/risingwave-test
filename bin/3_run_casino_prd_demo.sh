#!/bin/bash
# =============================================================================
# Casino Production Demo
#
# End-to-end setup of the prod casino + sportsbook streaming demo on RisingWave:
#   1. (Re)compile proto FileDescriptorSets if protoc is available
#   2. Ensure RisingWave core nodes are up and accepting SQL on :4566
#   3. Bring up Lakekeeper + MinIO + Trino + Grafana (idempotent)
#   4. Upload proto FileDescriptorSets to MinIO (the sources read the schema
#      from s3://hummock001/proto/ — see docs/PRODUCTION_CASINO_DEMO.md §3.2)
#   5. Create prod Kafka source `src_casino_prd` (prd2, cronus.casino.out.gh)
#   6. Create prod Kafka source `src_bets_gh`    (prd4, bets-out-gh)
#   7. Create UC1 + UC2 MVs and Iceberg sinks
#   8. Create faithful nested raw Iceberg archive (mv_casino_raw)
#   9. Print row counts for verification
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

# MinIO connection (matches docker-compose minio-0 + the s3.* options in the
# source SQL). Schema descriptors live under s3://${MINIO_BUCKET}/proto/.
MINIO_HOST_ENDPOINT="http://localhost:9301"
MINIO_ACCESS_KEY="hummockadmin"
MINIO_SECRET_KEY="hummockadmin"
MINIO_BUCKET="hummock001"

# Upload a local descriptor to MinIO at proto/<basename>. Prefers the host
# aws CLI (matches the documented flow); falls back to the mc client baked
# into the minio-0 container so the script works with no host S3 tooling.
upload_proto() {
    local localfile="$1"
    local key="proto/$(basename "$localfile")"

    if command -v aws >/dev/null 2>&1; then
        AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
            aws --endpoint-url "$MINIO_HOST_ENDPOINT" s3 cp "$localfile" "s3://${MINIO_BUCKET}/${key}"
    else
        echo "host aws not found — uploading via mc inside minio-0"
        docker exec minio-0 mc alias set local "http://localhost:9301" \
            "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null 2>&1 || true
        docker exec -i minio-0 mc pipe "local/${MINIO_BUCKET}/${key}" < "$localfile"
    fi
}

# psql wrapper that suppresses NOTICE-level messages on stderr while keeping
# ERRORs visible and preserving the exit code. RisingWave accepts
# SET client_min_messages = WARNING but does not honour it for internally-
# generated NOTICEs, so filtering must happen on the client side.
psql_quiet() {
    local tmpfile
    tmpfile=$(mktemp)
    psql "$@" 2>"$tmpfile"
    local rc=$?
    grep -v ": NOTICE:" "$tmpfile" >&2 || true
    rm -f "$tmpfile"
    return $rc
}

# Run an idempotent SQL file, retrying on a transient "database 1 reset".
# This cluster runs more CPU cores than the license allows, so RisingWave
# disables DatabaseFailureIsolation: any single streaming-job failure (e.g.
# the mv_turnover_percentage join backfill racing with live ingestion, or the
# iceberg JVM catalog cold start) resets the whole database. The DDL files all
# use DROP ... IF EXISTS / CREATE, so re-running after recovery is safe and
# usually succeeds once upstreams have settled. See docs/PRODUCTION_CASINO_DEMO.md §6.
run_sql_with_retry() {
    local file="$1"
    local attempts="${2:-3}"
    local i
    for i in $(seq 1 "$attempts"); do
        if psql_quiet "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$file"; then
            return 0
        fi
        if [ "$i" -lt "$attempts" ]; then
            echo "⚠ '$file' failed (attempt $i/$attempts) — likely a transient 'database 1 reset'."
            echo "  Waiting 15s for recovery to settle, then retrying (DDL is idempotent)..."
            sleep 15
        fi
    done
    echo "ERROR: '$file' still failing after $attempts attempts" >&2
    return 1
}

echo "=== [1/9] Proto descriptors ==="
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
echo "=== [2/9] RisingWave core nodes ==="
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
echo "=== [3/9] Lakekeeper + MinIO + Trino + Grafana + Redpanda ==="
docker compose up -d lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap trino prometheus-0 grafana-0 redpanda

echo ""
echo "=== [4/9] Upload proto descriptors to MinIO (s3://${MINIO_BUCKET}/proto/) ==="
# minio-0 is brought up in step 2; wait for it to answer before uploading.
echo -n "Waiting for MinIO on ${MINIO_HOST_ENDPOINT} "
for i in $(seq 1 30); do
    if curl -fsS "${MINIO_HOST_ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        echo " ready."
        break
    fi
    echo -n "."
    sleep 2
    if [ "$i" -eq 30 ]; then
        echo ""
        echo "ERROR: MinIO did not become healthy within 60s" >&2
        exit 1
    fi
done
upload_proto "$CASINO_PROTO_PB"
upload_proto "$BETS_PROTO_DESC"

echo ""
echo "=== [5/9] Create source src_casino_prd (prd2 — cronus.casino.out.gh) ==="
psql_quiet "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_CASINO_SOURCE"

echo ""
echo "=== [6/9] Create source src_bets_gh (prd4 — bets-out-gh) ==="
psql_quiet "$PSQL_URL" -v ON_ERROR_STOP=1 -f "$SQL_BETS_SOURCE"

echo ""
echo "=== [7/9] Create UC1 + UC2 MVs and Iceberg sinks ==="
run_sql_with_retry "$SQL_PIPELINE"

echo ""
echo "=== [8/9] Create raw nested Iceberg archive (mv_casino_raw) ==="
run_sql_with_retry "$SQL_RAW_SINK"

echo ""
echo "=== [9/9] Waiting for first Iceberg checkpoint, then creating Trino metadata views ==="
echo -n "Waiting for rw_managed_casino_real_bet in Lakekeeper "
for i in $(seq 1 40); do
    if curl -fsS "http://localhost:8181/catalog/v1/namespaces/public/tables/rw_managed_casino_real_bet" >/dev/null 2>&1; then
        echo " ready."
        break
    fi
    echo -n "."
    sleep 3
    if [ "$i" -eq 40 ]; then
        echo ""
        echo "WARNING: Iceberg table not found in Lakekeeper after 120s — view creation may fail" >&2
    fi
done

# Dollar-free Trino views over the Iceberg $snapshots metadata tables.
# Grafana interprets `$` as a variable, so the casino dashboard queries these
# views (not the raw "table$snapshots" names). Recreated here so they survive
# a stack restart. The Dagster casino_trino_views asset does the same.
echo "Creating Trino metadata views (snapshots) ..."
docker exec trino trino --execute "
CREATE OR REPLACE VIEW datalake.public.casino_real_bet_snapshots AS
  SELECT snapshot_id, operation, CAST(committed_at AS timestamp(6)) AS committed_at
  FROM datalake.public.\"rw_managed_casino_real_bet\$snapshots\";
CREATE OR REPLACE VIEW datalake.public.turnover_pct_snapshots AS
  SELECT snapshot_id, operation, CAST(committed_at AS timestamp(6)) AS committed_at
  FROM datalake.public.\"rw_managed_turnover_percentage\$snapshots\";
" 2>&1 | grep -v "WARNING\|jline\|terminal" || echo "⚠ Trino view creation had issues — check trino is up"

echo ""
echo "=== Verifying row counts ==="
psql "$PSQL_URL" <<'SQL'
\echo '--- UC1 materialized views ---'
SELECT 'mv_casino_transactions' AS view, COUNT(*) FROM mv_casino_transactions
UNION ALL SELECT 'mv_casino_real_bet',   COUNT(*) FROM mv_casino_real_bet;

\echo ''
\echo '--- UC2 materialized views ---'
SELECT 'mv_casino_turnover_90d'       AS view, COUNT(*) FROM mv_casino_turnover_90d
UNION ALL SELECT 'mv_sportsbook_turnover_90d',  COUNT(*) FROM mv_sportsbook_turnover_90d
UNION ALL SELECT 'mv_turnover_percentage',      COUNT(*) FROM mv_turnover_percentage;

\echo ''
\echo '--- Raw archive ---'
SELECT 'mv_casino_raw' AS view, COUNT(*) FROM mv_casino_raw;

\echo ''
\echo '--- Active sinks ---'
SELECT name, connector, status FROM rw_catalog.rw_sinks
WHERE name IN ('sink_casino_real_bet','sink_turnover_percentage',
               'sink_casino_real_bet_kafka','sink_turnover_percentage_kafka')
ORDER BY name;

\echo ''
\echo '--- Kafka output topics (check Redpanda has messages) ---'
\echo 'Run: docker exec redpanda rpk topic list | grep casino'
SQL

echo ""
echo "Done. See docs/poc/PRODUCTION_CASINO_DEMO.md for the full walkthrough."
