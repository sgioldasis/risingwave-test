#!/bin/bash
# Launcher script for ML Serving with River Online Learning

cd "$(dirname "$0")/.."

# Activate virtual environment
source .venv/bin/activate 2>/dev/null || true

PORT=${1:-8001}

echo "🌊 Starting ML Serving with River Online Learning on port $PORT..."
echo "📊 API docs: http://localhost:$PORT/docs"
echo "🔧 Mode: Online Learning (continuous incremental updates)"
echo ""
echo "Environment variables:"
echo "  USE_ONLINE_LEARNING=true (required)"
echo "  ONLINE_LEARNING_INTERVAL=5 (seconds between data polls)"
echo "  CHECKPOINT_INTERVAL=60 (seconds between MinIO checkpoints)"
echo ""

# Set required environment variable
export USE_ONLINE_LEARNING=true

exec uvicorn ml.serving.main:app --host 0.0.0.0 --port "$PORT" --reload