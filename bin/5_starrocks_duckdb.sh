#!/bin/bash

# Query StarRocks from DuckDB over Arrow Flight SQL — the zero-copy pairing.
#
# See docs/SR_POC_ARROW_FLIGHT_SQL_DEMO.md ("Querying through DuckDB").
# Runs in the project venv on the host; that needs `127.0.0.1 starrocks` in
# /etc/hosts (the FE advertises its Flight endpoint by container name), which
# the script checks for. Dependencies: adbc-driver-flightsql + duckdb in the
# venv (uv sync'd via pyproject optional deps — see the doc if missing).
#
# Usage:
#   ./bin/5_starrocks_duckdb.sh

set -e

if [ -x ".venv/bin/python" ]; then
    PY=".venv/bin/python"
elif command -v uv &> /dev/null; then
    PY="uv run python"
else
    echo "❌ Error: no Python interpreter found (.venv or uv)"
    exit 1
fi

if ! grep -qE '^127\.0\.0\.1[[:space:]]+starrocks' /etc/hosts; then
    echo "⚠️  /etc/hosts has no '127.0.0.1 starrocks' entry — the FE advertises its"
    echo "   Flight endpoint by that name, so host runs need it (sudo edit). Trying"
    echo "   anyway; an in-container run (see the doc) needs no entry."
fi

exec $PY scripts/starrocks_duckdb_demo.py
