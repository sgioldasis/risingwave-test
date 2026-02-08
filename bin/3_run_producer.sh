#!/bin/bash

# Script to run the producer with configurable TPS

TPS=${1:-1}

echo "=== Starting Producer with $TPS TPS ==="
echo ""

PYTHONUNBUFFERED=1 uv run python scripts/producer.py --tps "$TPS"
