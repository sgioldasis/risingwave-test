#!/bin/bash

# Recover every piece of demo state that a fresh start wipes, so the reverse-ETL
# demo (bin/3_run_reverse_etl_demo.sh, the UI on port 4002) works right away
# after a Stop Everything (6_down.sh) + start. Idempotent: every phase probes
# first and skips what is already in place, so re-running it mid-session is a
# no-op beyond the probes.
#
# What gets wiped, and the phase that restores it:
#   A. StarRocks CN registration    -- FE metadata is container-local (no volume)
#   B. delta_federation catalog     -- base catalogs come from starrocks-init, this one
#                                      only from 3_setup_delta_federation.sh
#   C. the five demo syncs          -- RisingWave/Postgres/state volumes wiped by
#                                      6_down.sh (the staging Kafka topics survive: external)
#   D. reverse_etl db + PK table + Routine Load -- FE metadata again. A recreated load
#                                      starts at the topic's END, so an empty table is
#                                      replayed from offsets 0 (idempotent: PK upserts),
#                                      then the load is normalized back to no offsets --
#                                      left-in offsets would make the next schema-change
#                                      rebuild replay the whole topic again.
#
# Needs: containers up with Dagster healthy (3_run_reverse_etl_demo.sh sequences
# this itself) and the VPN on for the sync rebuild. Safe to run standalone.
#
# Usage: 2_recover_demo_state.sh [read|full]   (default full)
#   read  -- phases A+B only: CN registration + delta_federation catalog. The
#           minimum for querying the Databricks tables from StarRocks and over
#           Arrow Flight (seconds; no VPN needed). Flight itself needs nothing:
#           its port and JVM flag are set by the entrypoints on every start.
#   full  -- also the five syncs (C) and the serving table + Routine Load (D),
#           i.e. the whole demo UI including the sync animation.

set -e
cd "$(dirname "$0")/.."

MODE="${1:-full}"

MYSQL="docker exec starrocks mysql -h starrocks -P 9030 -u root"
LOAD="reverse_etl_delta_rl"

# stop + drop + create the Routine Load; $1 = "replay" (offsets 0) or "normalize" (no offsets).
# The COLUMNS/jsonpaths mirror the asset's builder (reverse_etl_starrocks_target.py
# _routine_load_ddl) — keep in sync if the asset's mapping changes.
recreate_load() {
    docker exec -i dagster-webserver python - "$1" "$LOAD" <<'PY'
import json
import sys

import pymysql

mode, load = sys.argv[1], sys.argv[2]
env = {}
for line in open("/workspace/.env"):
    line = line.strip()
    if line.startswith("KAFKA_OUTPUT_") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v.strip().strip('"')
conn = pymysql.connect(host="starrocks", port=9030, user="root", database="reverse_etl", connect_timeout=5)
with conn.cursor() as cur:
    # Partition count from the live load (never assume a fixed topic shape).
    parts = []
    try:
        cur.execute(f"SHOW ROUTINE LOAD FOR `reverse_etl`.`{load}`")
        props = cur.fetchone()[cur.description and [d[0] for d in cur.description].index("DataSourceProperties")]
        parts = json.loads(props).get("currentKafkaPartitions", "").split(",")
    except Exception:
        parts = []
    if not parts:
        raise SystemExit("cannot determine the topic's partitions — is the load present?")
def ddl(with_offsets: bool) -> str:
    off = ""
    if with_offsets:
        off = ("  \"kafka_partitions\" = '" + ",".join(parts) + "',\n"
               "  \"kafka_offsets\" = '" + ",".join("0" for _ in parts) + "',\n")
    return (
        f"CREATE ROUTINE LOAD `reverse_etl`.`{load}` ON `reverse_etl_delta_target`\n"
        "COLUMNS (`rid_a`,`id`,`description`,`updated_at_raw`,`op`,`rid_b`,"
        "`rid`=if(`rid_a` IS NULL, `rid_b`, `rid_a`),"
        "`updated_at`=str_to_date(`updated_at_raw`, '%Y-%m-%dT%H:%i:%s.%fZ'),"
        "`__op`=if(`op` = 'd', 1, 0))\n"
        "PROPERTIES (\"format\" = \"avro\", \"jsonpaths\" = '[\"$.after.rid\", \"$.after.id\", "
        "\"$.after.description\", \"$.after.updated_at\", \"$.op\", \"$.before.rid\"]')\n"
        "FROM KAFKA (\n"
        f"  \"kafka_broker_list\" = '{env['KAFKA_OUTPUT_BOOTSTRAP']}',\n"
        "  \"kafka_topic\" = 'reverse_etl_delta_topic',\n"
        "  \"confluent.schema.registry.url\" = 'http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7',\n"
        + off +
        "  \"property.security.protocol\" = 'SASL_SSL',\n"
        "  \"property.sasl.mechanism\" = 'SCRAM-SHA-512',\n"
        f"  \"property.sasl.username\" = '{env['KAFKA_OUTPUT_SASL_USERNAME']}',\n"
        f"  \"property.sasl.password\" = '{env['KAFKA_OUTPUT_SASL_PASSWORD']}'\n"
        ")"
    )
with conn.cursor() as cur:
    for stmt in (f"STOP ROUTINE LOAD FOR `reverse_etl`.`{load}`",
                 f"DROP ROUTINE LOAD FOR `reverse_etl`.`{load}`"):
        try:
            cur.execute(stmt)
        except pymysql.err.ProgrammingError:
            pass  # absent job on a fresh create
    cur.execute(ddl(with_offsets=(mode == "replay")))
conn.close()
print(f"{mode}: load recreated ({'offsets 0' if mode == 'replay' else 'no offsets'})")
PY
}

echo "=== Probing demo state ==="
PROBES=$(docker exec -i dagster-webserver python - <<'PY'
import json

import psycopg2
import pymysql

def sr(query, db=None):
    conn = pymysql.connect(host="starrocks", port=9030, user="root", database=db, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.description is None:
                return []
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        conn.close()

out = {}
try:
    out["cn_alive"] = sum(1 for row in sr("SHOW COMPUTE NODES") if str(row.get("Alive", "")).lower() == "true")
except Exception:
    out["cn_alive"] = 0
try:
    out["delta_federation"] = any(row.get("Catalog") == "delta_federation" for row in sr("SHOW CATALOGS"))
except Exception:
    out["delta_federation"] = False
try:
    conn = psycopg2.connect(host="frontend-node-0", port=4566, user="root", dbname="dev", connect_timeout=5)
    with conn.cursor() as cur:
        # Existence, not rows: an existing-but-empty target (e.g. right after a
        # setup, before the first sync) must not trigger a full rebuild.
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'reverse_etl_avro_target'")
        out["risingwave"] = cur.fetchone() is not None
    conn.close()
except Exception:
    out["risingwave"] = False
out["serving_db"] = any(row.get("Database") == "reverse_etl" for row in sr("SHOW DATABASES"))
load, count = None, None
if out["serving_db"]:
    loads = sr("SHOW ROUTINE LOAD", db="reverse_etl")
    load = loads[0].get("State") if loads else None
    try:
        count = sr("SELECT COUNT(*) AS n FROM reverse_etl_delta_target", db="reverse_etl")[0]["n"]
    except Exception:
        count = None
out["routine_load_state"] = load
out["serving_rows"] = count
print(json.dumps(out))
PY
)
echo "$PROBES"

get() { echo "$PROBES" | python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',''))"; }

# --- A. CN registration -----------------------------------------------------------
if [ "$(get cn_alive)" = "0" ]; then
    echo "=== A. Registering the CN with the fresh FE ==="
    $MYSQL --execute 'ALTER SYSTEM ADD COMPUTE NODE "starrocks-cn:9050"'
    for i in $(seq 1 30); do
        [ "$($MYSQL --execute "SHOW COMPUTE NODES\G" 2>/dev/null | grep -c "Alive: true")" -gt 0 ] && break
        if [ "$i" -eq 30 ]; then
            echo "  CN not alive after registration — restarting it (stale FE address)"
            docker compose restart starrocks-cn
        fi
        sleep 2
    done
    for i in $(seq 1 30); do
        [ "$($MYSQL --execute "SHOW COMPUTE NODES\G" 2>/dev/null | grep -c "Alive: true")" -gt 0 ] && {
            echo "✅ CN registered and alive"
            break
        }
        if [ "$i" -eq 30 ]; then echo "❌ CN still not alive" >&2; exit 1; fi
        sleep 2
    done
else
    echo "✅ A. CN already registered ($(get cn_alive) alive)"
fi

# --- B. delta_federation catalog ----------------------------------------------------
# In read mode a presence probe suffices. In full mode this runs AFTER the sync
# rebuild (C) and unconditionally: the reset drops + recreates the Databricks
# sources and UC mints new storage paths, so the HMS registrations must be
# re-pointed at the live tables — a stale registration keeps serving the dropped
# tables' files as GHOST DATA (550k rows that no longer exist in UC).
if [ "$MODE" != "full" ] && [ "$(get delta_federation)" = "True" ]; then
    echo "✅ B. delta_federation catalog already present"
else
    echo "=== B. Registering the delta_federation catalog ==="
    ./bin/3_setup_delta_federation.sh
fi

# --- C. the five demo syncs (full mode only) ----------------------------------------
if [ "$MODE" != "full" ]; then
    echo "ℹ️  read mode: skipping the syncs and serving-table phases (run with 'full' for those)"
elif [ "$(get risingwave)" != "True" ]; then
    echo "=== C. Rebuilding the demo syncs (reset + setup, in parallel) ==="
    ./bin/3_rebuild_reverse_etl.sh avro cdf orders delta company
    # C moved every demo-owned source to a new storage path — B must re-run after it.
    echo "=== B2. Re-registering the federation against the new table locations ==="
    ./bin/3_setup_delta_federation.sh
else
    echo "✅ C. Demo syncs already in place (RisingWave target table present)"
fi

# --- D. StarRocks serving table + Routine Load (full mode only) ---------------------
if [ "$MODE" != "full" ]; then
    echo ""
    echo "✅ Read-path recovery complete. Summary:"
    echo "   Demo UI: http://localhost:4002/?sync=delta"
    exit 0
fi
if [ "$(get serving_db)" != "True" ] || [ -z "$(get routine_load_state)" ] || [ "$(get routine_load_state)" = "None" ]; then
    echo "=== D. Recreating the StarRocks serving table + Routine Load (Dagster asset) ==="
    RUN_ID=$(python3 - <<'PY'
import json, urllib.request

URL = "http://localhost:3000/graphql"
def gql(query, variables=None):
    req = urllib.request.Request(URL, json.dumps({"query": query, "variables": variables or {}}).encode(),
                                 {"content-type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=120))["data"]

repo = gql("{repositoriesOrError{... on RepositoryConnection{nodes{name location{name}}}}}")["repositoriesOrError"]["nodes"][0]
selector = {"repositoryLocationName": repo["location"]["name"], "repositoryName": repo["name"],
            "jobName": "__ASSET_JOB", "assetSelection": [{"path": ["reverse_etl_delta_starrocks_target"]}]}
out = gql("mutation($s:JobOrPipelineSelector!){launchRun(executionParams:{selector:$s})"
          "{__typename ... on LaunchRunSuccess{run{runId}} ... on PythonError{message}}}", {"s": selector})["launchRun"]
if out["__typename"] != "LaunchRunSuccess":
    raise SystemExit(f"launch failed: {out}")
print(out["run"]["runId"])
PY
)
    python3 - "$RUN_ID" <<'PY'
import json, sys, time, urllib.request

URL = "http://localhost:3000/graphql"
q = 'query($id:ID!){pipelineRunOrError(runId:$id){... on PipelineRun{status stepStats{stepKey status}}}}'
for _ in range(60):
    req = urllib.request.Request(URL, json.dumps({"query": q, "variables": {"id": sys.argv[1]}}).encode(),
                                 {"content-type": "application/json"})
    d = json.load(urllib.request.urlopen(req, timeout=30))["data"]["pipelineRunOrError"]
    st = d.get("status")
    if isinstance(st, dict):
        st = st.get("value")
    if st in ("SUCCESS", "FAILURE", "CANCELED"):
        if st != "SUCCESS":
            raise SystemExit(f"asset run {st} — see the Dagster UI for the logs")
        print("✅ serving asset run succeeded")
        break
    time.sleep(5)
else:
    raise SystemExit("asset run did not finish in 5 minutes")
PY
else
    echo "✅ D. Serving db + Routine Load already in place ($(get routine_load_state))"
fi

# --- D2. Replay when the table is empty but the topic is not ------------------------
ROWS=$($MYSQL -D reverse_etl --execute "SELECT COUNT(*) AS n FROM reverse_etl_delta_target" 2>/dev/null | tail -1 | tr -d ' ')
if [ "$ROWS" = "0" ]; then
    # A full rebuild (phase C) leaves the topics empty by design: the reset drops
    # the Databricks sources, the setup recreates them seeded, and the demo grows
    # from make-change clicks. Only replay when the topic actually holds history
    # (the mid-session FE-wipe case: syncs intact, serving table wiped).
    EMPTY=$($MYSQL -D reverse_etl --execute "SHOW ROUTINE LOAD\G" 2>/dev/null | python3 -c "
import sys
latest = None
for line in sys.stdin:
    if line.strip().startswith('LatestSourcePosition:'):
        latest = line.split(':', 1)[1].strip()
import json
print(int(all(v == 0 for v in json.loads(latest).values())) if latest else 1)
")
    if [ "$EMPTY" = "1" ]; then
        echo "✅ D2. Topic is empty (fresh rebuild: sources recreated + seeded) — the load will" \
             "consume from offsets 0 as the demo produces. Nothing to replay."
    else
        echo "=== D2. Table empty, topic has history — replaying the Routine Load from offsets 0 ==="
        recreate_load replay
    echo "  Waiting for the replay to catch up (up to 8 minutes)..."
    CONVERGED=0
    for i in $(seq 1 96); do
        # Converged once every partition's Progress equals LatestSourcePosition
        # (and is past OFFSET_ZERO, i.e. at least one consume task committed).
        MATCH=$($MYSQL -D reverse_etl --execute "SHOW ROUTINE LOAD\G" 2>/dev/null | python3 -c "
import sys
progress = latest = None
for line in sys.stdin:
    if line.strip().startswith('Progress:'):
        progress = line.split(':', 1)[1].strip()
    if line.strip().startswith('LatestSourcePosition:'):
        latest = line.split(':', 1)[1].strip()
print(int(progress == latest and progress is not None and 'OFFSET_ZERO' not in progress))
")
        if [ "$MATCH" = "1" ]; then CONVERGED=1; break; fi
        sleep 5
    done
    ROWS=$($MYSQL -D reverse_etl --execute "SELECT COUNT(*) AS n FROM reverse_etl_delta_target" 2>/dev/null | tail -1 | tr -d ' ')
    echo "  Replay rows now: $ROWS"
    if [ "$CONVERGED" = "1" ]; then
        echo "  Caught up — normalizing the load back to no offsets (asset-canonical form)"
        recreate_load normalize
    else
        echo "⚠️  Replay not caught up within 8 minutes — leaving the offsets-0 load running;" \
             "it keeps consuming in the background. Re-run this script once it drains to normalize."
    fi
    fi
else
    echo "✅ D2. Serving table already holds data ($ROWS rows) — no replay needed"
fi

echo ""
echo "✅ Recovery complete."
$MYSQL -D reverse_etl --execute "SELECT COUNT(*) AS serving_rows FROM reverse_etl_delta_target" 2>/dev/null || true
echo "   Demo UI: http://localhost:4002/?sync=delta"
