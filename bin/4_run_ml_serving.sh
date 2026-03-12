#!/bin/bash
# Launcher script for ML Serving with River Online Learning (RisingWave source)

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

echo "🌊 Starting ML Serving with River Online Learning on port $PORT..."
echo "📊 API docs: http://localhost:$PORT/docs"
echo "🔧 Mode: Online Learning (RisingWave source)"
echo "📡 Source: RisingWave funnel_training view"
echo ""
echo "Environment variables:"
echo "  USE_ONLINE_LEARNING=true (required)"
echo "  USE_KAFKA_SOURCE=false (using RisingWave polling)"
echo "  CHECKPOINT_INTERVAL=60 (seconds between MinIO checkpoints)"
echo ""

# Set required environment variables
export USE_ONLINE_LEARNING=true
export USE_KAFKA_SOURCE=false

exec uvicorn ml.serving.main:app --host 0.0.0.0 --port "$PORT" --reload
