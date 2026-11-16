#!/bin/bash

# Stream Load demo — the third StarRocks ingestion path (next to Routine Load
# and external-engine writes). POSTs a small JSON batch straight to the FE's
# HTTP endpoint with an idempotent label (re-running with the same label is a
# no-op — "Label already exists"), then shows the landed rows.
#
# Runs curl inside the superset container: the FE REQUIRES the
# "Expect: 100-continue" handshake, and that handshake hangs when proxied
# through Docker's host port-forward (verified 2026-11-16: file/stdin POSTs
# from the host time out; from inside the docker network they succeed
# instantly). Companion doc: docs/SR_POC_SERVING_FEATURES_DEMO.md

set -e

DB="sr_local_db_sr_local_db"
TABLE="stream_load_demo"
MYSQL="docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root"
FE_HTTP="http://starrocks:8030"

echo "=== 1. Ensure target table (PRIMARY KEY on label, item_id) ==="
$MYSQL -e "CREATE TABLE IF NOT EXISTS ${DB}.${TABLE} (
    label VARCHAR(64) NOT NULL,
    item_id VARCHAR(32) NOT NULL,
    qty INT,
    price DECIMAL(38, 9),
    loaded_at DATETIME
) PRIMARY KEY (label, item_id)
DISTRIBUTED BY HASH(item_id);"

echo "=== 2. POST JSON batch via curl (Expect: 100-continue) ==="
LABEL="stream_load_demo_$(date +%s)"

# The batch label rides in the payload itself (every row carries it) —
# the PK table's label column then needs no expression trickery in the
# columns header; only loaded_at is computed in-flight (now()).
BATCH="[
  {\"label\": \"${LABEL}\", \"item_id\": \"SKU-001\", \"qty\": 3,  \"price\": 19.99},
  {\"label\": \"${LABEL}\", \"item_id\": \"SKU-002\", \"qty\": 1,  \"price\": 129.50},
  {\"label\": \"${LABEL}\", \"item_id\": \"SKU-003\", \"qty\": 7,  \"price\": 4.25},
  {\"label\": \"${LABEL}\", \"item_id\": \"SKU-004\", \"qty\": 12, \"price\": 0.99}
]"

docker exec -i superset sh -c "curl -sS --location-trusted -m 30 -u root: \
  -H 'Expect: 100-continue' \
  -H 'label: ${LABEL}' \
  -H 'columns: label, item_id, qty, price, loaded_at=now()' \
  -H 'format: json' \
  -H 'strip_outer_array: true' \
  -T - \
  '${FE_HTTP}/api/${DB}/${TABLE}/_stream_load'" \
  <<JSON | python3 -c "import json,sys; r=json.load(sys.stdin); print('Status:', r.get('Status'), '| OK rows:', r.get('NumberLoadedRows'), '| label:', r.get('Label'))"
${BATCH}
JSON

echo ""
echo "=== 3. Rows landed (loaded_at = now() computed in-flight) ==="
$MYSQL -e "SELECT * FROM ${DB}.${TABLE} WHERE label = '${LABEL}' ORDER BY item_id;"

echo ""
echo "=== 4. Idempotency: re-run with the SAME label (server rejects) ==="
docker exec -i superset sh -c "curl -sS --location-trusted -m 30 -u root: \
  -H 'Expect: 100-continue' \
  -H 'label: ${LABEL}' \
  -H 'columns: label, item_id, qty, price, loaded_at=now()' \
  -H 'format: json' \
  -H 'strip_outer_array: true' \
  -T - \
  '${FE_HTTP}/api/${DB}/${TABLE}/_stream_load'" \
  <<JSON | python3 -c "import json,sys; r=json.load(sys.stdin); print('Status:', r.get('Status'), '| message:', (r.get('Message') or '')[:80])"
${BATCH}
JSON
