#!/bin/bash
# Launcher script for ML Serving with automatic mode detection

cd "$(dirname "$0")/.."

# Activate virtual environment
source .venv/bin/activate 2>/dev/null || true

# Parse parameters - if it looks like a port number (digits only), use it
# Otherwise default to 8001
if [[ "$1" =~ ^[0-9]+$ ]]; then
    PORT=$1
    # Validate port is in valid range (1024-65535)
    if [[ "$PORT" -lt 1024 ]] || [[ "$PORT" -gt 65535 ]]; then
        echo "⚠️  Invalid port: $PORT. Using default port 8001."
        PORT=8001
    fi
else
    PORT=8001
fi

echo "🚀 Starting ML Serving on port $PORT..."
echo "📊 API docs: http://localhost:$PORT/docs"
echo "🔄 Mode: Auto-detect (switches between Online Learning and Batch automatically)"
echo "🔍 Detection: Checks MinIO every 30s for batch models"
echo ""
echo "Behavior:"
echo "  - If batch models exist in MinIO → Uses Batch mode"
echo "  - If no batch models → Uses Online Learning (River + RisingWave)"
echo "  - Automatically switches when models become available"
echo ""

# Optional: Force online mode (uncomment if needed)
# export USE_ONLINE_LEARNING=true

# Optional: Use Kafka source instead of RisingWave polling (uncomment if needed)
# Enabled 2026-11-14: the learner's feed is StarRocks (Routine Load + async
# MV, RW-free) by default via ML_LEARNING_SOURCE, so predictions keep
# working (slightly stale, no errors) while RisingWave is paused. The
# RW-based predictor 500s whenever RW is down (round(None) crash in
# river_predictor.py), breaking the Predictions tab.
export USE_KAFKA_SOURCE=true
# starrocks = poll funnel windows maintained by StarRocks itself (Routine
# Load + async MV, no RW); kafka = consume the RW-fed funnel topic.
export ML_LEARNING_SOURCE=starrocks
# Minute-grain training windows (2026-11-14): the Predictions tab compares
# predictions against per-minute actuals (the dashboard's Kafka-fed
# windows), so the learner must train on the same grain -- a 20s-grain
# model predicts ~1/3 scale (~19 viewers vs 60 actual) and looks frozen.
# funnel_summary_direct is the minute-grain VIEW over the raw Routine-Load
# tables (computed live, no RW).
export STARROCKS_FUNNEL_SOURCE=funnel_summary_direct

# This process runs on the host (not in Docker), but .env sets
# RISINGWAVE_HOST="frontend-node-0" -- a Docker-network-internal hostname
# needed by in-container consumers (Trino, Grafana, dbt). devbox auto-loads
# .env for this host process too, so without this override it inherits that
# unreachable hostname and hangs (confirmed 2026-09-09: connection stuck in
# SYN_SENT against an unrelated external IP instead of failing fast, taking
# down the dashboard's Predictions tab). Same class of issue already
# documented for STARROCKS_URL in docs/SR_POC_LIVE_DEMO_RUNBOOK.md.
export RISINGWAVE_HOST=localhost

# --reload restricted to ml/: WatchFiles otherwise watches the whole repo,
# so editing an unrelated file (e.g. modern-dashboard/backend/api.py) would
# hot-reload this service and reset the online learner mid-replay
# (confirmed 2026-11-14: three dashboard edits reset the funnel-topic
# backfill three times).
exec uvicorn ml.serving.main:app --host 0.0.0.0 --port "$PORT" --reload --reload-dir ml
