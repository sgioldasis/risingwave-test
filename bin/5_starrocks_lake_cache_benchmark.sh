#!/bin/bash

# Lake-scan data cache benchmark — quantifies what StarRocks' block/metadata
# caches buy on the Rewrite-OFF Iceberg query (the raw
# databricks_uc.sr_poc_external.funnel_summary_historical scan that the Query
# Rewrite dashboard's OFF chart runs). Method: REFRESH EXTERNAL TABLE drops the
# catalog's metadata cache, then the identical query runs twice with rewrite
# disabled and EXPLAIN ANALYZE reports each wall time — cold (metadata + data
# cache cold) vs warm. Run it ~5 minutes after the last lake query to see the
# full cold cost (the UC metadata cache TTL is 300s).
# Companion doc: docs/SR_POC_SERVING_FEATURES_DEMO.md

set -e

Q="SELECT DATE(window_start) AS day, country, SUM(viewers) AS viewers FROM databricks_uc.sr_poc_external.funnel_summary_historical GROUP BY day, country"

echo "=== Dropping catalog metadata cache (REFRESH EXTERNAL TABLE) ==="
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "REFRESH EXTERNAL TABLE databricks_uc.sr_poc_external.funnel_summary_historical;" 2>&1 | head -1

echo "=== Run 1 (cold: metadata cache + block cache cold) ==="
COLD=$(docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SET SESSION enable_materialized_view_rewrite=0; EXPLAIN ANALYZE $Q;" 2>/dev/null \
  | grep -oE "TotalTime: [0-9]+(s[0-9]+)?ms" | head -1)
echo "  $COLD"

echo "=== Run 2 (warm: caches populated) ==="
WARM=$(docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SET SESSION enable_materialized_view_rewrite=0; EXPLAIN ANALYZE $Q;" 2>/dev/null \
  | grep -oE "TotalTime: [0-9]+(s[0-9]+)?ms" | head -1)
echo "  $WARM"

echo ""
echo "Cold vs warm gap is the metadata/block-cache saving; the rewrite-ON MV path"
echo "compares at ~0.02s warm (see the Query Rewrite dashboard's ON/OFF pair)."
