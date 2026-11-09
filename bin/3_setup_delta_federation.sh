#!/bin/bash

# Delta catalog federation (serving-layer doc §13): StarRocks reads the
# Databricks Delta source in place — no pipeline, no compute, just the HMS
# shim and the sync's service principal. Idempotent: re-run after every
# reset/rebuild (UC mints a new storage path for the source table each time).
# Prereqs: the hms container up (docker compose up -d hms) and StarRocks up
# (./bin/3_run_reverse_etl_demo.sh or the full 1_up.sh).
# Verified by a COUNT(*) through the federation at the end.

set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Delta federation: location -> HMS -> catalog -> verify ==="
.venv/bin/python scripts/setup_delta_federation.py
