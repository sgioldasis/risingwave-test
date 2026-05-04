#!/bin/bash

# Script to run the producer with configurable TPS

TPS=${1:-1}

echo "=== Starting Producer with $TPS TPS ==="
echo ""

# Run the producer directly (script_runner handles output capture)
export PYTHONUNBUFFERED=1
exec uv run python scripts/producer.py --tps "$TPS"
