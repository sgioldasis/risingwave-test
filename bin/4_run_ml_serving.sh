#!/bin/bash
# Start ML Serving Service

PORT=${1:-8001}
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"

echo "🤖 Starting ML Serving Service on port $PORT..."

# Check if MinIO is accessible
echo "Checking MinIO connection..."
if ! curl -s http://localhost:9301/minio/health/live > /dev/null 2>&1; then
    echo "⚠️  Warning: MinIO may not be accessible at localhost:9301"
    echo "   Models will be loaded once MinIO is available."
fi

# Run the serving service using uvicorn
exec uvicorn ml.serving.main:app \
    --host 0.0.0.0 \
    --port "$PORT" \
    --reload \
    --reload-dir ml/serving \
    --log-level info
