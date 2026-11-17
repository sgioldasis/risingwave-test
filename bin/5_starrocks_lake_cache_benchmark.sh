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

cat <<'INTRO'

What the block/metadata caches actually buy on an Iceberg lake scan.

The query below reads funnel_summary_historical -- a table physically stored
in ADLS by Databricks (Unity Catalog), read by StarRocks through the
databricks_uc Iceberg REST catalog. This is the exact scan the Query Rewrite
dashboard's "OFF" chart runs (MV rewrite disabled). It runs twice:

  Run 1 (COLD)  -- REFRESH EXTERNAL TABLE first drops the catalog's metadata
                   cache; the data (block) cache is cold too. Cost includes
                   UC metadata fetch + MinIO/lake round-trips.
  Run 2 (WARM)  -- same query, caches now populated. The gap is what the
                   caches save.

Timing: run it ~5 minutes after the last lake query to see the FULL cold
cost (the UC metadata cache TTL is 300s). Timings come from EXPLAIN ANALYZE
(StarRocks actually executes the query and reports per-operator wall time).

INTRO

echo "The query both runs execute:"
echo "  $Q"
echo ""

echo "=== Step 1: drop the metadata cache (input: REFRESH EXTERNAL TABLE) ==="
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "REFRESH EXTERNAL TABLE databricks_uc.sr_poc_external.funnel_summary_historical;" 2>&1 | head -1

echo ""
echo "=== Step 2: Run 1 (cold: metadata cache + block cache cold) ==="
echo "  input: SET SESSION enable_materialized_view_rewrite=0; EXPLAIN ANALYZE $Q;"
COLD=$(docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SET SESSION enable_materialized_view_rewrite=0; EXPLAIN ANALYZE $Q;" 2>/dev/null \
  | grep -oE "TotalTime: [0-9]+(s[0-9]+)?ms" | head -1)
echo "  cold total: $COLD"

echo ""
echo "=== Step 3: Run 2 (warm: caches populated) -- identical input ==="
WARM=$(docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SET SESSION enable_materialized_view_rewrite=0; EXPLAIN ANALYZE $Q;" 2>/dev/null \
  | grep -oE "TotalTime: [0-9]+(s[0-9]+)?ms" | head -1)
echo "  warm total: $WARM"

echo ""
echo "=== Read the result ==="
echo "Cold-vs-warm gap = the metadata + block-cache saving on the lake path."
echo "The rewrite-ON MV path compares at ~0.02s warm (see the Query Rewrite"
echo "dashboard's ON/OFF pair) -- the local MV avoids the lake round-trips"
echo "entirely, which is the difference between StarRocks SERVING and"
echo "StarRocks FEDERATING."
