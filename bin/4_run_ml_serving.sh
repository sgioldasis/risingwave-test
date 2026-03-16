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
# export USE_KAFKA_SOURCE=true

exec uvicorn ml.serving.main:app --host 0.0.0.0 --port "$PORT" --reload
