# River Online Learning Integration Guide

## Overview

This project now supports **real-time incremental ML** using the [River](https://riverml.xyz/) library. Instead of batch retraining every few minutes, models learn continuously from each new record as it arrives from RisingWave.

## Key Benefits

| Feature | Batch Mode | Online Learning |
|---------|-----------|-----------------|
| **Training Latency** | 20s - 5min cycles | Instant per-record updates |
| **Adaptability** | Slow to concept drift | Immediate pattern adaptation |
| **Resource Usage** | High (retrain from scratch) | Low (incremental updates) |
| **Model Freshness** | Stale between cycles | Always current |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     RisingWave Stream                        │
│              (funnel_training materialized view)             │
└──────────────────────────┬──────────────────────────────────┘
                           │ poll every N seconds
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    ml/online/ Module                         │
│  ┌──────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  DataStreamer │  │ RiverModels │  │   CheckpointManager │ │
│  │  (polls RW)   │→ │ (learn_one) │→ │  (saves to MinIO)   │ │
│  └──────────────┘  └─────────────┘  └─────────────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │ predictions
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   ml/serving/main.py                         │
│              FastAPI with River predictor                    │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Install Dependencies

```bash
uv pip install -e .
```

This installs `river>=0.22.0` along with other dependencies.

### 2. Start Services

```bash
# Start RisingWave, MinIO, etc.
./bin/1_up.sh
```

### 3. Start ML Serving with Online Learning

```bash
# Via script runner: Click "🌊 ML Online Learning"
# Or manually:
./bin/4_run_ml_online.sh
```

Or set the environment variable with the regular serving script:

```bash
USE_ONLINE_LEARNING=true ./bin/4_run_ml_serving.sh
```

### 4. Verify It's Working

```bash
# Check health - should show mode: online
curl http://localhost:8001/health

# Get predictions - uses continuously updated models
curl http://localhost:8001/predict

# Check online learning status
curl http://localhost:8001/online/status
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_ONLINE_LEARNING` | `false` | Enable River online learning |
| `ONLINE_LEARNING_INTERVAL` | `5` | Seconds between data polls |
| `CHECKPOINT_INTERVAL` | `60` | Seconds between MinIO checkpoints |
| `ML_SERVING_PORT` | `8001` | API server port |

## API Endpoints

### Standard Endpoints (Both Modes)

- `GET /health` - Health check with mode indicator
- `GET /predict` - Get predictions for all metrics
- `GET /predict/{metric}` - Get prediction for specific metric
- `GET /models` - Get model status and statistics

### Online Learning Specific

- `GET /online/status` - Detailed online learning statistics
- `POST /learn` - Manually trigger learning from features/targets

### Batch Mode Specific

- `POST /reload` - Force model reload from MinIO

## Response Examples

### Online Learning Prediction Response

```json
{
  "predicted_at": "2026-03-12T10:30:00Z",
  "timestamp": "2026-03-12T10:31:00Z",
  "mode": "online",
  "viewers": {
    "value": 1523.5,
    "confidence": 0.87,
    "model_type": "river_online",
    "samples_learned": 156
  },
  "carters": {
    "value": 423.2,
    "confidence": 0.82,
    "model_type": "river_online",
    "samples_learned": 156
  }
}
```

### Online Status Response

```json
{
  "running": true,
  "started_at": "2026-03-12T10:00:00Z",
  "runtime_seconds": 1800,
  "records_learned": 156,
  "poll_interval": 5,
  "checkpoint_interval": 60,
  "models": {
    "viewers": {
      "learning_count": 156,
      "mae": 12.5,
      "r2": 0.89
    }
  }
}
```

## How It Works

### 1. Data Streaming

The [`DataStreamer`](ml/online/streamer.py) polls RisingWave's `funnel_training` view every 5 seconds (configurable) and yields new records.

### 2. Incremental Learning

For each new record, the [`OnlineLearner`](ml/online/learner.py) calls `learn_one()` on each River model:

```python
# For each metric, update the model incrementally
for metric, target in targets.items():
    models[metric].learn_one(features, target)
```

### 3. Model Types

Different River algorithms for different metrics:

- **Count metrics** (viewers, carters, purchasers): `LinearRegression` with `StandardScaler`
- **Rate metrics** (view_to_cart_rate, cart_to_buy_rate): `HoeffdingTreeRegressor`

### 4. Checkpointing

Models are periodically saved to MinIO bucket `ml-models-online` for durability. On restart, the latest checkpoint is loaded.

### 5. Predictions

Predictions use the in-memory models which are continuously updated. Confidence scores increase as more samples are learned.

## Switching Between Modes

### Batch Mode (Original)
```bash
# Default - uses Dagster-scheduled training
./bin/4_run_ml_serving.sh
```

### Online Learning Mode
```bash
# Continuous incremental learning
export USE_ONLINE_LEARNING=true
./bin/4_run_ml_serving.sh

# Or use the convenience script
./bin/4_run_ml_online.sh
```

## Files Added

```
ml/
├── online/                      # NEW: Online learning module
│   ├── __init__.py
│   ├── models.py               # River model definitions
│   ├── streamer.py             # RisingWave data streaming
│   ├── learner.py              # Continuous learning service
│   └── checkpoints.py          # MinIO checkpoint management
└── serving/
    ├── river_predictor.py      # NEW: River-based predictor
    └── main.py                 # MODIFIED: Dual-mode API

bin/
└── 4_run_ml_online.sh          # NEW: Online learning launcher

pyproject.toml                  # MODIFIED: Added river dependency
```

## Monitoring

Watch the online learner in action:

```bash
# Terminal 1: Watch RisingWave data
curl -s http://localhost:8001/online/status | jq

# Terminal 2: Get predictions
curl -s http://localhost:8001/predict | jq

# Terminal 3: Watch logs
docker logs -f risingwave-test-ml-serving-1
```

## Troubleshooting

### Models not predicting (confidence=0)
- Models need at least 5 samples before predicting
- Check `/online/status` to see `records_learned`
- Verify RisingWave has data: `SELECT COUNT(*) FROM funnel_training`

### High prediction errors
- Online models adapt quickly but may be unstable early on
- Wait for 50+ samples for stable predictions
- Check individual model MAE in `/online/status`

### Checkpoint not loading
- Check MinIO connection: `curl http://localhost:9301`
- Verify bucket exists: `ml-models-online`
- Models start fresh if no checkpoint found (expected behavior)

## Comparison with Batch Mode

| Scenario | Recommendation |
|----------|----------------|
| Demo/experiment | **Online Learning** - instant gratification |
| Production stability | **Batch** - proven, checkpointed models |
| Rapid concept drift | **Online** - adapts immediately |
| Audit/compliance | **Batch** - versioned model artifacts |
| Low latency | **Online** - no reload overhead |

## Further Reading

- [River Documentation](https://riverml.xyz/)
- [River API Reference](https://riverml.xyz/latest/api/)
- [Online Learning vs Batch Learning](https://riverml.xyz/latest/faq/)