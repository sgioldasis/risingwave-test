#!/bin/bash

# Serve the reverse-ETL animation and the Jobotron panel with working buttons
# (launch each step's Dagster run, check the targets).
# Open http://localhost:4002/?sync=delta or http://localhost:4002/jobotron.
# First starts ONLY the containers the demo needs — Dagster, RisingWave and
# Redpanda+kafka-connect — not the whole stack (no StarRocks, Trino, Superset,
# MinIO/Lakekeeper, Grafana, Prometheus). Needs the VPN on, and a devbox shell
# for the Postgres check. .env is sourced for the environment the checks read
# (the Jobotron topic check needs KAFKA_OUTPUT_*).
# `--starrocks` (script-runner checkbox) adds the StarRocks pair (FE + CN) and
# its one-shot catalog init to the set.
# Stop Everything (6_down.sh) tears the minimal set down like any other start:
# its `docker compose down` covers every container of the project.

PORT=${PORT:-4002}

echo "=== Starting the demo's containers (minimal set) ==="
DEMO_SERVICES="redpanda kafka-connect meta-node-0 compute-node-0 frontend-node-0 compactor-0 compactor-1 dagster-webserver dagster-daemon"
if [ "${1:-}" = "--starrocks" ]; then
    echo "=== --starrocks: adding StarRocks FE + CN + catalog init ==="
    DEMO_SERVICES="$DEMO_SERVICES starrocks starrocks-cn starrocks-init"
fi
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

echo "=== Demos on http://localhost:$PORT/?sync=delta and http://localhost:$PORT/jobotron ==="
set -a
. "$(dirname "$0")/../.env"
set +a
export PYTHONUNBUFFERED=1 PORT
exec uv run python scripts/reverse_etl_demo_server.py
