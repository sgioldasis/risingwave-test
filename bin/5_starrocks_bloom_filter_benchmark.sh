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

DB="sr_local_db"
MYSQL="docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root"
SERIES=10   # dim_users (1M rows) x SERIES => rows per twin table

with_progress() { # $1 = label; rest = command; runs it with an hourglass
  # heartbeat (one line per 10s -- the runner renders output line-by-line,
  # so an in-place spinner isn't possible) and prints its output when done
  local label="$1"; shift
  local tmp; tmp=$(mktemp)
  "$@" >"$tmp" 2>&1 &
  local pid=$! start=$SECONDS
  printf "  ⏳ %s (0s)\n" "$label"
  while kill -0 "$pid" 2>/dev/null; do
    sleep 10
    printf "  ⏳ still working (%ds elapsed)\n" "$((SECONDS - start))"
  done
  wait "$pid"; local rc=$?
  cat "$tmp"; rm -f "$tmp"
  return $rc
}

cat <<'INTRO'

Does StarRocks's bloom-filter index pay off on this workload? A bloom filter
on a non-key column lets each column page SKIP rows that can't match a
point-lookup predicate -- the classic tool when queries filter on a column
that isn't part of any sort key.

This is an HONEST NEGATIVE-RESULT demo (verified 2026-11-16): at 10M narrow
rows the warm scan is ~13ms with OR without the filter -- the columnar scan
is already fast enough that page-level bloom skipping buys nothing on this
data shape (narrow rows, ~5 distinct values per column page). Where it pays
off: wider rows, bigger segments, higher cardinality. The PoC's real
point-lookup case (wallet_transactions) is covered by the PRIMARY KEY index
instead -- no bloom filter is applied to any live table.

The twin tables below make the comparison exact:
  bf_test_yes -- HAS the bloom filter (PROPERTIES 'bloom_filter_columns' = 'user_id')
  bf_test_no  -- the control: identical DDL and data, NO bloom filter
IDENTICAL data both sides; the only difference is that one property.

INTRO
echo ""
echo "=== 1. Twin tables (identical data, one with bloom_filter_columns) ==="
echo "    both tables: country VARCHAR, user_id VARCHAR, pad VARCHAR"
echo "    DUPLICATE KEY (country) DISTRIBUTED BY HASH(country)"
echo "    bf_test_yes adds: PROPERTIES ('bloom_filter_columns' = 'user_id')"
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

echo ""
echo "=== 2. Load ${SERIES}M rows into each (same data both sides) ==="
echo "    input: INSERT INTO <t> SELECT country, user_id, md5(user_id) FROM dim_users"
echo "           CROSS JOIN TABLE(generate_series(1, ${SERIES}))"
echo "    (this is the slow step -- about 2 minutes; watch the hourglass)"
with_progress "loading ${SERIES}M rows into each twin table..." $MYSQL -e "
INSERT INTO ${DB}.bf_test_no SELECT country, user_id, md5(user_id) FROM ${DB}.dim_users CROSS JOIN TABLE(generate_series(1, ${SERIES}));
INSERT INTO ${DB}.bf_test_yes SELECT country, user_id, md5(user_id) FROM ${DB}.dim_users CROSS JOIN TABLE(generate_series(1, ${SERIES}));
SELECT (SELECT COUNT(*) FROM ${DB}.bf_test_no) AS no_bf_rows, (SELECT COUNT(*) FROM ${DB}.bf_test_yes) AS bf_rows;"

echo ""
echo "=== 3. Point lookup (user_id = '123456'), 3 warm rounds each ==="
echo "    input per round: SET SESSION enable_materialized_view_rewrite=0;"
echo "      EXPLAIN ANALYZE SELECT COUNT(*) FROM <t> WHERE user_id = '123456';"
for tbl in bf_test_no bf_test_yes; do
  echo "--- ${tbl} ---"
  for i in 1 2 3; do
    $MYSQL -N -e "SET SESSION enable_materialized_view_rewrite=0; EXPLAIN ANALYZE SELECT COUNT(*) FROM ${DB}.${tbl} WHERE user_id = '123456';" 2>/dev/null \
      | grep -oE "TotalTime: [0-9]+(s[0-9]+)?ms" | head -1
  done
done

echo ""
echo "=== Read the result ==="
echo "No meaningful warm-scan difference at this scale -- the documented negative"
echo "result (see companion doc, 'Bloom filter'). The demo stays re-runnable:"
echo "bump SERIES= in this script for bigger data, where the filter starts to pay."
