#!/bin/bash

# Script to run the producer with configurable TPS

TPS=${1:-1}

echo "=== Starting Producer with $TPS TPS ==="
echo ""

# Run the producer and pipe output to producer.log
# Use -u for unbuffered output to ensure tee catches updates immediately
PYTHONUNBUFFERED=1 uv run python -u scripts/producer.py --tps "$TPS" 2>&1 | tee producer.log
