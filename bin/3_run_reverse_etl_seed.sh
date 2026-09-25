#!/bin/bash

# Seed a handful of INSERT/UPDATE/DELETE rows into the reverse-ETL CDF POC
# source table, so reverse_etl_cdf_to_kafka has something to sync.
# See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md

echo "=== Seeding Reverse-ETL CDF POC table ==="
echo ""

export PYTHONUNBUFFERED=1
exec uv run python scripts/reverse_etl_poc_seed.py
