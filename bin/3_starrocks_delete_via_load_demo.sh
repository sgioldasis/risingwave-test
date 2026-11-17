#!/bin/bash

# Delete-via-load demo — how corrections and tombstones actually flow into a
# StarRocks PRIMARY KEY table from a Kafka pipeline, no SQL shell required:
#
#   1. plain upsert (stream load, 3 rows)
#   2. upsert-with-correction (__op=0 — same key, new values; whole-row replace)
#   3. delete-via-load (__op=1 — the loaded row is a TOMBSTONE for its key)
#   4. plain DELETE ... WHERE (the SQL-side equivalent)
#
# __op values are the reverse of what you might guess (verified live
# 2026-11-16: __op=0 upserts, __op=1 deletes). The __op field rides in each
# JSON row and must be listed in the columns header. Stream load runs inside
# the superset container — the FE's "Expect: 100-continue" handshake hangs
# through Docker's host port-forward (see 3_starrocks_stream_load_demo.sh).
# Companion doc: docs/SR_POC_SERVING_FEATURES_DEMO.md

set -e

DB="sr_local_db"
TABLE="delete_via_load_demo"
MYSQL="docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root"

load() { # $1 = label suffix, $2 = JSON array payload
  echo "  POST ${FE_HTTP}/api/${DB}/${TABLE}/_stream_load   (columns: tx_id, note, amount, __op)"
  echo "  payload:"
  echo "  ${2}"
  docker exec -i superset sh -c "curl -sS --location-trusted -m 30 -u root: \
    -H 'Expect: 100-continue' \
    -H 'label: del_demo_${1}' \
    -H 'format: json' \
    -H 'strip_outer_array: true' \
    -H 'columns: tx_id, note, amount, __op' \
    -T - \
    '${FE_HTTP}/api/${DB}/${TABLE}/_stream_load'" <<JSON | python3 -c "import json,sys; r=json.load(sys.stdin); print('  Status:', r.get('Status'), '| rows:', r.get('NumberLoadedRows'))"
${2}
JSON
}

show() {
  $MYSQL -e "SELECT tx_id, note, amount FROM ${DB}.${TABLE} ORDER BY tx_id;"
}

FE_HTTP="http://starrocks:8030"
LABEL=$(date +%s)

cat <<'INTRO'

How corrections and deletes flow into a StarRocks PRIMARY KEY table as DATA
(no SQL needed) -- the CDC/change-data-capture pattern. Same stream-load
channel as the Stream Load demo; the only new ingredient is the __op field.

  Step 2  plain upsert        -> a key that doesn't exist = INSERT
  Step 3  __op=0 (upsert)     -> re-sending a key REPLACES the whole row
                                 (a CDC "update" event: pipelines never
                                 UPDATE, they re-emit the full row)
  Step 4  __op=1 (tombstone)  -> the loaded row is a DELETE marker for its
                                 key; the row vanishes (a CDC "delete" event)
  Step 5  plain DELETE        -> the SQL-side equivalent, for pipelines that
                                 CAN open a session (interactive fixes)

Gotcha (verified live): __op is reversed from intuition -- 0 means upsert,
1 means delete. It must ride in every JSON row AND in the columns header.

INTRO
echo "=== 1. Ensure target table (PRIMARY KEY on tx_id) ==="
$MYSQL -e "CREATE TABLE IF NOT EXISTS ${DB}.${TABLE} (
    tx_id VARCHAR(64) NOT NULL,
    note VARCHAR(64),
    amount DECIMAL(38,9)
) PRIMARY KEY (tx_id)
DISTRIBUTED BY HASH(tx_id);
TRUNCATE TABLE ${DB}.${TABLE};"

echo "=== 2. Plain upsert: stream load 3 rows (no __op) -- new keys = INSERT ==="
load "a_${LABEL}" '[{"tx_id":"TX-1","note":"charge","amount":10.0},{"tx_id":"TX-2","note":"charge","amount":20.0},{"tx_id":"TX-3","note":"charge","amount":30.0}]'
show

echo ""
echo "=== 3. Upsert-with-correction: TX-2 reloaded with __op=0 (whole-row replace) ==="
echo "    (watch TX-2: new note + amount. CDC update = re-emit the row, never UPDATE)"
load "b_${LABEL}" '[{"tx_id":"TX-2","note":"charge (corrected)","amount":25.0,"__op":0}]'
show

echo ""
echo "=== 4. Delete-via-load: TX-2 tombstoned with __op=1 ==="
echo "    (TX-2 vanishes -- no DELETE was run. The tombstone is just another row)"
load "c_${LABEL}" '[{"tx_id":"TX-2","note":"ignored","amount":0,"__op":1}]'
show

echo ""
echo "=== 5. SQL-side equivalent: DELETE ... WHERE tx_id = 'TX-3' ==="
echo "    (same result as step 4, but this needs a SQL session -- a pure Kafka"
echo "     pipeline cannot do this; the tombstone path is what it can do)"
$MYSQL -e "DELETE FROM ${DB}.${TABLE} WHERE tx_id = 'TX-3';"
show

echo ""
echo "TX-2 deleted by a loaded tombstone (__op=1), TX-3 by plain SQL DELETE —"
echo "the two paths a real CDC/correction pipeline uses."
