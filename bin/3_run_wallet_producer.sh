#!/bin/bash

# Script to run the synthetic wallet transaction producer (StarRocks upsert demo)
# See docs/SR_POC_WALLET_UPSERT_DEMO.md

TPS=${1:-1}

echo "=== Starting Wallet Transaction Producer with $TPS TPS ==="
echo ""

# Run the producer directly (script_runner handles output capture)
export PYTHONUNBUFFERED=1
exec uv run python scripts/wallet_producer.py --tps "$TPS"
