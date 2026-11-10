#!/bin/bash

# Benchmark StarRocks Arrow Flight SQL vs the MySQL wire protocol.
#
# See docs/SR_POC_ARROW_FLIGHT_SQL_DEMO.md. The benchmark must run inside a
# container on the compose network (the FE advertises its Flight endpoint as
# grpc+tcp://starrocks:8010, which only resolves there), so this wraps a
# docker exec into the dagster-webserver. The ADBC driver is installed in the
# container's writable layer on each run (lost on image rebuild).
#
# Usage:
#   ./bin/5_starrocks_flight_benchmark.sh

set -e

echo "================================================================================"
echo "🚀 StarRocks Arrow Flight SQL vs MySQL benchmark (in dagster-webserver)"
echo "================================================================================"

if ! docker ps --format '{{.Names}}' | grep -q '^dagster-webserver$'; then
    echo "❌ dagster-webserver container is not running (docker compose up -d first)"
    exit 1
fi

docker exec -u root dagster-webserver python -m pip install -q adbc-driver-flightsql

exec docker exec -i dagster-webserver python - < scripts/starrocks_flight_benchmark.py
