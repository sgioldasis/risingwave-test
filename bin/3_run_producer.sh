#!/bin/bash

# Script to run the producer with configurable TPS

TPS=${1:-1}

echo "=== Starting Producer with $TPS TPS ==="
echo ""

# Run the producer directly (script_runner handles output capture)
PYTHONUNBUFFERED=1 uv run python scripts/producer.py --tps "$TPS"
