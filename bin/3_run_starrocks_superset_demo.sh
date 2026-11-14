#!/bin/bash

# One-click bootstrap for the three Superset dashboards (port 3002):
#   - Funnel Dashboard (/superset/dashboard/funnel-dashboard)
#   - Wallet Upsert Demo (/superset/dashboard/wallet-upsert-demo)
#   - StarRocks Query Rewrite Demo (/superset/dashboard/query-rewrite-demo)
#
# Starts every container those dashboards need (RisingWave cluster, Redpanda,
# MinIO + Lakekeeper, Postgres, Dagster, the StarRocks FE/CN/init trio, and
# Superset + its asset-import init), waits for readiness, launches the
# starrocks_demo_setup_job Dagster run (the union job that builds all
# RisingWave/StarRocks objects for all three dashboards -- see
# orchestration/definitions.py), then starts the funnel and wallet producers
# in the background if they aren't already running.
#
# Re-click safety: if either producer is already running, the setup job is
# SKIPPED (rebuilding wallet_transactions while a producer writes is the
# known bad order -- see the caveat on starrocks_demo_setup_job itself), and
# the script finishes in seconds. Cold stack -> expect ~10 minutes for the
# setup job. Stop Everything (6_down.sh) tears the set down like any other.
#
# Needs the VPN on (Databricks UC access for the databricks_uc catalog).

set -e

START_SECONDS=$SECONDS

echo "=== Starting the containers the Superset dashboards need ==="
SERVICES="redpanda redpanda-init minio-0 lakekeeper lakekeeper-bootstrap lakekeeper-migrate lakekeeper-db meta-node-0 compute-node-0 frontend-node-0 compactor-0 compactor-1 postgres-0 dagster-webserver dagster-daemon starrocks starrocks-cn starrocks-init superset superset-init"
if ! docker compose up -d $SERVICES; then
    echo "❌ Container start failed — are the images built? Run ./bin/1_up.sh once" \
         "on a working network (VPN off) to build/pull them, then retry." >&2
    exit 1
fi

wait_dagster() {
    for i in $(seq 1 60); do
        state=$(docker inspect -f '{{.State.Health.Status}}' dagster-webserver 2>/dev/null || echo missing)
        if [ "$state" = "healthy" ]; then
            echo "✅ Dagster is ready."
            return 0
        fi
        if [ "$i" -eq 60 ]; then
            echo "❌ dagster-webserver did not become healthy after 2 minutes:" >&2
            docker compose ps dagster-webserver dagster-daemon >&2
            return 1
        fi
        sleep 2
    done
}

wait_one_shot() {
    # $1 = container name, $2 = what it proves, $3 = timeout seconds
    local name="$1" what="$2" timeout="${3:-300}"
    for i in $(seq 1 $((timeout / 2))); do
        state=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$name" 2>/dev/null || echo missing)
        case "$state" in
            "exited 0")
                echo "✅ $what"
                return 0
                ;;
            exited*)
                echo "❌ $name failed: $state" >&2
                docker logs --tail 80 "$name" >&2 || true
                return 1
                ;;
        esac
        if [ "$i" -eq $((timeout / 2)) ]; then
            echo "❌ $name did not complete after ${timeout}s:" >&2
            docker compose ps "$name" >&2
            return 1
        fi
        sleep 2
    done
}

wait_dagster
wait_one_shot starrocks-init "StarRocks is ready (FE + CN healthy, catalogs initialized)." 300
wait_one_shot superset-init "Superset is ready (dashboards imported)." 300

PRODUCER_RUNNING=$(pgrep -f "scripts/producer.py" || true)
WALLET_PRODUCER_RUNNING=$(pgrep -f "scripts/wallet_producer.py" || true)

if [ -n "$PRODUCER_RUNNING" ] || [ -n "$WALLET_PRODUCER_RUNNING" ]; then
    echo "=== Producer already running — SKIPPING starrocks_demo_setup_job ==="
    echo "   (rebuilding wallet_transactions while a producer writes is the known"
    echo "    bad order; stop the producer(s) or run 6_down.sh if you want a full rebuild)"
else
    echo "=== Launching starrocks_demo_setup_job (all three dashboards' objects) ==="
    echo "   Cold stack: expect ~5-10 minutes. Streaming the run status below."
    RUN_ID=$(python3 - <<'PYEOF'
import json, urllib.request

body = json.dumps({
    "query": """mutation Launch($ep: ExecutionParams!) { launchRun(executionParams: $ep) {
        ... on LaunchRunSuccess { run { runId } }
        ... on PythonError { message }
        ... on InvalidSubsetError { message }
    } }""",
    "variables": {"ep": {
        "runConfigData": "{}",
        "selector": {
            "jobName": "starrocks_demo_setup_job",
            "repositoryLocationName": "orchestration.definitions",
            "repositoryName": "__repository__",
        },
    }},
}).encode()
req = urllib.request.Request("http://localhost:3000/graphql", body, {"Content-Type": "application/json"})
resp = json.loads(urllib.request.urlopen(req, timeout=60).read())
data = resp["data"]["launchRun"]
if "run" not in data:
    print("LAUNCH_FAILED:" + json.dumps(data), flush=True)
else:
    print(data["run"]["runId"])
PYEOF
    )
    if ! [[ "$RUN_ID" =~ ^[0-9a-f-]{36}$ ]]; then
        echo "❌ Failed to launch starrocks_demo_setup_job: $RUN_ID" >&2
        exit 1
    fi
    echo "   Run id: $RUN_ID"

    python3 - "$RUN_ID" <<'PYEOF'
import json, sys, time, urllib.request

RUN = sys.argv[1]

def gql(q):
    req = urllib.request.Request(
        "http://localhost:3000/graphql",
        json.dumps({"query": q}).encode(),
        {"Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(req, timeout=60).read())

Q = ('query { runOrError(runId: "%s") { __typename '
     '... on Run { status stepStats { stepKey status } } '
     '... on PythonError { message } } }' % RUN)

last_steps = None
deadline = time.time() + 40 * 60
while True:
    if time.time() > deadline:
        print("❌ starrocks_demo_setup_job did not reach a terminal state in 40 minutes.", flush=True)
        raise SystemExit(1)
    try:
        r = gql(Q)["data"]["runOrError"]
        if r.get("message") and r.get("__typename") == "PythonError":
            print("❌ GraphQL error fetching run:", r["message"], flush=True)
            raise SystemExit(1)
        status = r.get("status")
        steps = sorted((s["stepKey"], s["status"]) for s in r.get("stepStats", []))
        if steps != last_steps:
            for key, st in steps:
                print(f"   [{st}] {key}", flush=True)
            last_steps = steps
        if status in ("SUCCESS", "FAILURE", "CANCELED"):
            if status == "SUCCESS":
                print("✅ starrocks_demo_setup_job finished successfully.", flush=True)
            else:
                print(f"❌ starrocks_demo_setup_job ended with {status}.", flush=True)
                for key, st in steps:
                    if st in ("FAILURE", "CANCELED"):
                        print(f"   failed step: {key}", flush=True)
                raise SystemExit(1)
            break
    except SystemExit:
        raise
    except Exception as e:
        print("   poll error:", e, flush=True)
    time.sleep(15)
PYEOF
fi

echo "=== Starting producers (background, logs in /tmp) ==="
if [ -n "$PRODUCER_RUNNING" ]; then
    echo "✅ Funnel producer already running (pid $(echo "$PRODUCER_RUNNING" | head -1))."
else
    # start_new_session=True detaches the producer into its own process
    # group, so the runner cleaning up this script's process tree can't
    # take the producers down with it (plain nohup+disown does not).
    python3 -c "import subprocess; subprocess.Popen(['./bin/3_run_producer.sh', '1'], stdout=open('/tmp/funnel_producer.log','w'), stderr=subprocess.STDOUT, start_new_session=True)"
    echo "✅ Funnel producer started at 1 TPS (log: /tmp/funnel_producer.log)."
fi
if [ -n "$WALLET_PRODUCER_RUNNING" ]; then
    echo "✅ Wallet producer already running (pid $(echo "$WALLET_PRODUCER_RUNNING" | head -1))."
else
    python3 -c "import subprocess; subprocess.Popen(['./bin/3_run_wallet_producer.sh', '1'], stdout=open('/tmp/wallet_producer.log','w'), stderr=subprocess.STDOUT, start_new_session=True)"
    echo "✅ Wallet producer started at 1 TPS (log: /tmp/wallet_producer.log)."
fi

echo ""
echo "=== StarRocks Superset demo is up (total time: $((SECONDS - START_SECONDS))s) ==="
echo "  📊 Superset:              http://localhost:3002 (admin / sr_poc_admin_2026)"
echo "  - Funnel Dashboard:       http://localhost:3002/superset/dashboard/funnel-dashboard/"
echo "  - Wallet Upsert Demo:     http://localhost:3002/superset/dashboard/wallet-upsert-demo/"
echo "  - Query Rewrite Demo:     http://localhost:3002/superset/dashboard/query-rewrite-demo/"
echo "    (the rewrite race reads best ~5 min after the dashboard was last loaded --"
echo "     cold UC metadata cache; see docs/SR_POC_QUERY_REWRITE_DEMO.md)"
echo "  🔍 Redpanda console:      http://localhost:9090"
echo "  ⚙️  Dagster:               http://localhost:3000"
