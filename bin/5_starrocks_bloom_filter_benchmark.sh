#!/bin/bash

# Bloom-filter index benchmark — point-lookup cost with vs without a bloom
# filter on a non-key column, on twin DUPLICATE KEY tables built from the
# same data. Negative-result demo (verified 2026-11-16): at 10M narrow-ish
# rows the warm scan is ~13ms with or without the bloom filter — the
# columnar scan is already fast enough that per-page bloom skipping buys
# nothing measurable on this data shape (narrow rows, ~5 distinct values per
# column page). StarRocks's bloom filter pays off with wider rows, larger
# segments and much higher cardinality; the PoC's real point-lookup case
# (wallet_transactions) is covered by the PRIMARY KEY index instead, so no
# bloom filter is applied to any live table. Kept as a re-runnable probe:
# bump SERIES below for bigger data.
# Companion doc: docs/SR_POC_SERVING_FEATURES_DEMO.md

set -e

DB="sr_local_db_sr_local_db"
MYSQL="docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root"
SERIES=10   # dim_users (1M rows) x SERIES => rows per twin table

echo "=== 1. Twin tables (identical data, one with bloom_filter_columns) ==="
$MYSQL -e "
DROP TABLE IF EXISTS ${DB}.bf_test_no;
DROP TABLE IF EXISTS ${DB}.bf_test_yes;
CREATE TABLE ${DB}.bf_test_no (
    country VARCHAR(32) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    pad VARCHAR(64)
) DUPLICATE KEY (country) DISTRIBUTED BY HASH(country);
CREATE TABLE ${DB}.bf_test_yes (
    country VARCHAR(32) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    pad VARCHAR(64)
) DUPLICATE KEY (country) DISTRIBUTED BY HASH(country)
PROPERTIES ('bloom_filter_columns' = 'user_id');"

echo "=== 2. Load ${SERIES}M rows into each (same data both sides) ==="
$MYSQL -e "
INSERT INTO ${DB}.bf_test_no SELECT country, user_id, md5(user_id) FROM ${DB}.dim_users CROSS JOIN TABLE(generate_series(1, ${SERIES}));
INSERT INTO ${DB}.bf_test_yes SELECT country, user_id, md5(user_id) FROM ${DB}.dim_users CROSS JOIN TABLE(generate_series(1, ${SERIES}));
SELECT (SELECT COUNT(*) FROM ${DB}.bf_test_no) AS no_bf_rows, (SELECT COUNT(*) FROM ${DB}.bf_test_yes) AS bf_rows;"

echo "=== 3. Point lookup (user_id = '123456'), 3 warm rounds each ==="
for tbl in bf_test_no bf_test_yes; do
  echo "--- ${tbl} ---"
  for i in 1 2 3; do
    $MYSQL -N -e "SET SESSION enable_materialized_view_rewrite=0; EXPLAIN ANALYZE SELECT COUNT(*) FROM ${DB}.${tbl} WHERE user_id = '123456';" 2>/dev/null \
      | grep -oE "TotalTime: [0-9]+(s[0-9]+)?ms" | head -1
  done
done

echo ""
echo "Expected result: no meaningful warm-scan difference at this scale —"
echo "documented as a negative result (see companion doc, 'Bloom filter')."
