#!/bin/bash

# Serve the reverse-ETL animation with working buttons (launch each step's Dagster run, check the targets).
# Open http://localhost:4002/?sync=avro (sync can be avro, cdf or orders).
# Needs the stack up (Dagster on port 3000), the VPN on, and a devbox shell for the Postgres check.

PORT=${PORT:-4002}
echo "=== Reverse-ETL demo on http://localhost:$PORT/?sync=avro ==="
export PYTHONUNBUFFERED=1 PORT
exec uv run python scripts/reverse_etl_demo_server.py
