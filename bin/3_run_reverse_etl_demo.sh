#!/bin/bash

# Serve the reverse-ETL animation and the Jobotron panel with working buttons
# (launch each step's Dagster run, check the targets).
# Open http://localhost:4002/?sync=delta or http://localhost:4002/jobotron.
# First starts the containers the demo needs — Dagster, RisingWave and
# Redpanda+kafka-connect — plus the StarRocks serving stack by default (FE +
# CN, one-shot catalog init, and hms for the Delta federation, doc §13);
# compose's depends_on pulls MinIO + Lakekeeper in automatically. The rest of
# the stack (Trino, Superset, Grafana, Prometheus) stays down. It then runs
# bin/2_recover_demo_state.sh full, which restores whatever a fresh start
# wiped (CN registration, delta_federation catalog, the five syncs, the
# StarRocks serving table) so the demo works right away, even straight after
# a Stop Everything — no surprises. Needs the VPN
# on, and a devbox shell for the Postgres check. .env is sourced for the
# environment the checks read (the Jobotron topic check needs KAFKA_OUTPUT_*).
# Stop Everything (6_down.sh) tears this set down like any other start: its
# `docker compose down` covers every container of the project.

PORT=${PORT:-4002}

echo "=== Starting the demo's containers + the StarRocks serving stack ==="
DEMO_SERVICES="redpanda kafka-connect meta-node-0 compute-node-0 frontend-node-0 compactor-0 compactor-1 dagster-webserver dagster-daemon starrocks starrocks-cn starrocks-init hms"
if ! docker compose up -d $DEMO_SERVICES; then
    echo "❌ Container start failed — are the images built? Run ./bin/1_up.sh once" \
         "on a working network (VPN off) to build/pull them, then retry." >&2
    exit 1
fi

echo "=== Waiting for Dagster webserver to become healthy ==="
for i in $(seq 1 60); do
    state=$(docker inspect -f '{{.State.Health.Status}}' dagster-webserver 2>/dev/null || echo missing)
    if [ "$state" = "healthy" ]; then
        echo "✅ Dagster is ready."
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "❌ dagster-webserver did not become healthy after 2 minutes:" >&2
        docker compose ps $DEMO_SERVICES >&2
        exit 1
    fi
    sleep 2
done

echo "=== Waiting for StarRocks to become ready (FE + CN healthy, catalog init done) ==="
# starrocks-init gates on both FE and CN healthchecks, so its exit code is the
# single "fully ready" signal. On a re-start the container is already exited 0
# (compose does not re-run a completed one-shot) and this loop returns at once.
for i in $(seq 1 150); do
    state=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' starrocks-init 2>/dev/null || echo missing)
    case "$state" in
        "exited 0")
            echo "✅ StarRocks is ready."
            break
            ;;
        exited*)
            echo "❌ starrocks-init failed: $state" >&2
            docker logs --tail 80 starrocks-init >&2 || true
            exit 1
            ;;
    esac
    if [ "$i" -eq 150 ]; then
        echo "❌ starrocks-init did not complete after 5 minutes:" >&2
        docker compose ps starrocks starrocks-cn starrocks-init >&2
        exit 1
    fi
    sleep 2
done

echo "=== Recovering demo state (everything the demo needs) ==="
# 'full' mode: probes first, rebuilds only what a fresh start wiped (CN
# registration, federation catalog, the five syncs, the StarRocks serving
# table). After a Stop Everything this is the no-surprises path — the demo
# works when it finishes; mid-session re-clicks are seconds (probes only).
# Pass 'read' instead for the StarRocks/Flight read path only (fast, but the
# serving-path checks stay broken until a full run). See the phase table at
# the top of 2_recover_demo_state.sh.
./bin/2_recover_demo_state.sh full

echo "=== Demos on http://localhost:$PORT/?sync=delta and http://localhost:$PORT/jobotron ==="
set -a
. "$(dirname "$0")/../.env"
set +a
export PYTHONUNBUFFERED=1 PORT
exec uv run python scripts/reverse_etl_demo_server.py
