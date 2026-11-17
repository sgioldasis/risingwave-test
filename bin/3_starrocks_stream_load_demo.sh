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

DB="sr_local_db"
TABLE="stream_load_demo"
MYSQL="docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root"
FE_HTTP="http://starrocks:8030"

cat <<'INTRO'

How data lands in StarRocks with NO database connection at all: a plain HTTP
PUT/POST of files to the FE's REST endpoint. This is the ingestion path a
microservice, a Spark/Flink job, or a nightly file drop uses -- no MySQL
session, no Kafka topic required, just HTTP.

  Step 1  ensure the target    -> stream load writes INTO an existing table
                                  (it never creates one); PRIMARY KEY on
                                  (label, item_id) = same key re-sent replaces
  Step 2  POST the batch       -> curl the JSON to /_stream_load with an
                                  idempotent LABEL; the FE returns a JSON
                                  receipt (Status: Success | OK rows)
  Step 3  show landed rows     -> loaded_at proves the 'loaded_at=now()'
                                  header computed the column server-side,
                                  in-flight, for every row
  Step 4  same label again     -> the server REJECTS the identical load with
                                  'Label Already Exists' -- that's the
                                  exactly-once guarantee: a client that lost
                                  the response can retry without double-loading

Gotcha (verified live): the FE requires the 'Expect: 100-continue' handshake,
and that handshake hangs when proxied through Docker's host port-forward --
so the curl runs inside the superset container, on the docker network.

INTRO
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
echo "    (the input below is everything the client sends; the output line is"
echo "     the FE's JSON receipt -- the same response a real producer parses)"
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

echo "POST ${FE_HTTP}/api/${DB}/${TABLE}/_stream_load"
echo "  -u root:                        # empty password"
echo "  -H 'Expect: 100-continue'       # required by the FE (hangs via host port-forward, hence curl inside the container)"
echo "  -H 'label: ${LABEL}'            # idempotency: same label = 'Label Already Exists'"
echo "  -H 'columns: label, item_id, qty, price, loaded_at=now()'  # loaded_at computed server-side"
echo "  -H 'format: json' -H 'strip_outer_array: true' -T -"
echo "Payload:"
echo "${BATCH}"
echo ""

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
echo ""
echo "=== 3. Rows landed (loaded_at = now() computed in-flight) ==="
echo "    (input to this step: SELECT * FROM ${DB}.${TABLE} WHERE label = '${LABEL}' --"
echo "     none of the loaded_at values came from the client)"
$MYSQL -e "SELECT * FROM ${DB}.${TABLE} WHERE label = '${LABEL}' ORDER BY item_id;"

echo ""
echo ""
echo "=== 4. Idempotency: re-run with the SAME label (server rejects) ==="
echo "    (identical POST, identical label -- watch Status flip from Success to"
echo "     'Label Already Exists': the exactly-once receipt a client retries against)"
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
