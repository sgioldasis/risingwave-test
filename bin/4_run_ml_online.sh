#!/bin/bash
# Launcher script for ML Serving with automatic mode detection

cd "$(dirname "$0")/.."

# Activate virtual environment
source .venv/bin/activate 2>/dev/null || true

PORT=${1:-8001}

echo "🚀 Starting ML Serving on port $PORT..."
echo "📊 API docs: http://localhost:$PORT/docs"
echo "🔄 Mode: Auto-detect (switches between Online Learning and Batch automatically)"
echo "🔍 Detection: Checks MinIO every 30s for batch models"
echo ""
echo "Behavior:"
echo "  - If batch models exist in MinIO → Uses Batch mode"
echo "  - If no batch models → Uses Online Learning (River + RisingWave)"
echo "  - ONLINE_LEARNING_INTERVAL=5 (seconds between data polls)"
echo "  - CHECKPOINT_INTERVAL=60 (seconds between MinIO checkpoints)"
echo ""

# Auto-detection is now the default behavior
# The service will automatically switch to batch mode when models are available

exec uvicorn ml.serving.main:app --host 0.0.0.0 --port "$PORT" --reload