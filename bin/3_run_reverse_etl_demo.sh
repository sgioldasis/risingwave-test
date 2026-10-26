#!/bin/bash

# Serve the reverse-ETL animation and the Jobotron panel with working buttons
# (launch each step's Dagster run, check the targets).
# Open http://localhost:4002/?sync=avro or http://localhost:4002/jobotron.
# Needs the stack up (Dagster on port 3000), the VPN on, and a devbox shell for the
# Postgres check. .env is sourced for the environment the checks read (the Jobotron
# topic check needs KAFKA_OUTPUT_*).

PORT=${PORT:-4002}
echo "=== Demos on http://localhost:$PORT/?sync=avro and http://localhost:$PORT/jobotron ==="
set -a
. "$(dirname "$0")/../.env"
set +a
export PYTHONUNBUFFERED=1 PORT
exec uv run python scripts/reverse_etl_demo_server.py
