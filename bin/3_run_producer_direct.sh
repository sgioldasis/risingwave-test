#!/bin/bash

# Script to run the direct producer (no Kafka) with configurable TPS

TPS=${1:-1}

echo "=== Starting Direct Producer with $TPS TPS ==="
echo ""
echo "This producer sends data directly to RisingWave without using Kafka."
echo "Tables: users, devices, campaigns, sessions, page_catalog, clickstream_events"
echo ""

# Run the producer and pipe output to producer_direct.log
# Use -u for unbuffered output to ensure tee catches updates immediately
PYTHONUNBUFFERED=1 uv run python -u scripts/producer_direct.py --tps "$TPS" 2>&1 | tee producer_direct.log
