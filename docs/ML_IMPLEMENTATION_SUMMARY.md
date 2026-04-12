# ML Training & Serving Separation - Implementation Summary

## Overview
This implementation separates ML model training and serving into two independent applications as requested:
- **Training**: Dagster-scheduled job (runs on cron schedule or sensor)
- **Serving**: Script-runner managed FastAPI service
- **Storage**: MinIO for model artifacts with versioning

## Architecture

### 1. Training Module (`ml/training/`)

#### Files Created:
- [`ml/training/__init__.py`](ml/training/__init__.py) - Module exports
- [`ml/training/model_registry.py`](ml/training/model_registry.py) - MinIO-based model storage
- [`ml/training/data_fetcher.py`](ml/training/data_fetcher.py) - RisingWave data fetching
- [`ml/training/trainer.py`](ml/training/trainer.py) - Core training logic

#### Key Features:
- **ModelRegistry**: Stores models in MinIO bucket `ml-models` with version format `vYYYYMMDD_HHMMSS`
- **DataFetcher**: Fetches last 1 minute of training data from RisingWave's `funnel_training` view
- **ModelTrainer**: Trains scikit-learn models (RandomForest/LinearRegression) for 5 metrics:
  - `viewers`, `carters`, `purchasers`, `view_to_cart_rate`, `cart_to_buy_rate`

### 2. Serving Module (`ml/serving/`)

#### Files Created:
- [`ml/serving/__init__.py`](ml/serving/__init__.py) - Module exports
- [`ml/serving/model_loader.py`](ml/serving/model_loader.py) - Model loading with hot-reload
- [`ml/serving/predictor.py`](ml/serving/predictor.py) - Prediction logic with caching
- [`ml/serving/main.py`](ml/serving/main.py) - FastAPI application

#### Key Features:
- **ModelLoader**: Loads models from MinIO with ETag-based change detection
- **ModelPredictor**: Caches predictions for 10 seconds, calculates confidence scores
- **FastAPI App** (port 8001):
  - `GET /predict` - Get predictions for all metrics
  - `GET /predict/{metric}` - Get prediction for specific metric
  - `GET /models` - Get model status with version timestamps
  - `POST /reload` - Force model reload
  - `GET /health` - Health check

### 3. Integration

#### Dagster Integration ([`orchestration/definitions.py`](orchestration/definitions.py))
- **Asset**: `ml_trained_models` - Trains models using last 1 minute of data
- **Job**: `ml_training_job` - Asset job for training
- **Schedule Options**:
  - Standard: 5-minute cron schedule (production)
  - Realtime: 20-second sensor for demos (set `ML_TRAINING_MODE=realtime`)

#### Script Runner Integration ([`scripts/script_runner.py`](scripts/script_runner.py))
- Added `4_run_ml_serving.sh` to SCRIPTS list with "🤖 ML Serving" label
- Added port 8001 to BACKGROUND_SERVICES_CONFIG for health checking

#### Launcher Script ([`bin/4_run_ml_serving.sh`](bin/4_run_ml_serving.sh))
```bash
#!/bin/bash
cd "$(dirname "$0")/.."
source .venv/bin/activate 2>/dev/null || true

PORT=${1:-8001}
echo "🤖 Starting ML Serving Service on port $PORT..."
echo "📊 API docs: http://localhost:$PORT/docs"
exec uvicorn ml.serving.main:app --host 0.0.0.0 --port "$PORT" --reload
```

#### Dependencies ([`pyproject.toml`](pyproject.toml))
Added `boto3>=1.34.0` for MinIO S3 API access.

## Usage

### Start ML Serving Service
```bash
# Via script runner: Click "🤖 ML Serving" button
# Or manually:
./bin/4_run_ml_serving.sh [PORT]
```

### Trigger Training
```bash
# Training runs automatically via Dagster schedule
# Or manually trigger via Dagster UI or API:
curl -X POST http://dagster:3000/graphql  # etc
```

### Get Predictions
```bash
# Get all predictions
curl http://localhost:8001/predict

# Get specific metric prediction
curl http://localhost:8001/predict/viewers

# Check model status (includes version timestamps)
curl http://localhost:8001/models

# Force model reload
curl -X POST http://localhost:8001/reload
```

### Enable Realtime Training (20-second intervals)
```bash
export ML_TRAINING_MODE=realtime
# Restart Dagster - the sensor will be enabled
```

## Model Version Format
The model version includes the actual training timestamp for frontend display:
```python
version = f"v{now.strftime('%Y%m%d_%H%M%S')}"  # e.g., v20260305_143022
```

This is returned in the `/models` endpoint:
```json
{
  "available_metrics": ["viewers", "carters", ...],
  "current_versions": {
    "viewers": "v20260305_143022",
    "carters": "v20260305_143022"
  }
}
```

## Training Data Window
Training uses only the last 1 minute of data:
```python
data = self.data_fetcher.fetch_training_data(minutes_back=1)
```

This enables very frequent retraining (every 20 seconds) for realtime demos.

## Files Created
```
ml/
├── __init__.py
├── training/
│   ├── __init__.py
│   ├── model_registry.py   # MinIO storage
│   ├── data_fetcher.py     # RisingWave queries
│   └── trainer.py          # Training logic
└── serving/
    ├── __init__.py
    ├── model_loader.py     # Model loading
    ├── predictor.py        # Prediction logic
    └── main.py             # FastAPI app

bin/
└── 4_run_ml_serving.sh     # Launcher script
```

## Files Modified
- [`orchestration/definitions.py`](orchestration/definitions.py) - Added ML training asset & schedules
- [`scripts/script_runner.py`](scripts/script_runner.py) - Added ML serving button & port config
- [`pyproject.toml`](pyproject.toml) - Added boto3 dependency

## Next Steps
1. Run `uv pip install -e .` to install the new boto3 dependency
2. Start services: `./bin/1_up.sh`
3. Start ML serving: Click "🤖 ML Serving" in script runner
4. Training starts automatically via Dagster (every 5 minutes by default)
5. Access predictions at http://localhost:8001/predict

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Dagster Orchestrator                      │
│  ┌──────────────────────┐                                        │
│  │ ml_trained_models    │  Trains models every 5 min (or 20s)   │
│  │ (Asset)              │  using last 1 minute of data          │
│  └──────────┬───────────┘                                        │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                        │
│  │ ml_training_job      │                                        │
│  │ (Job + Schedule)     │                                        │
│  └──────────┬───────────┘                                        │
└─────────────┼────────────────────────────────────────────────────┘
              │
              │ Trained models + metadata
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         MinIO Storage                            │
│  Bucket: ml-models                                               │
│  ├── models/v20260305_143022_viewers.pkl                        │
│  ├── models/v20260305_143022_carters.pkl                        │
│  ├── manifest.json  (current versions)                           │
│  └── metadata/    (training metrics)                             │
└─────────────────────────────────────────────────────────────────┘
              │
              │ Models loaded via S3 API
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ML Serving Service                          │
│                    FastAPI (port 8001)                           │
│                                                                  │
│  ┌─────────────────┐  ┌──────────────────┐                     │
│  │ ModelLoader     │  │ ModelPredictor   │                     │
│  │ - Loads models  │  │ - Caches preds   │                     │
│  │ - Hot reload    │  │ - Confidence     │                     │
│  └────────┬────────┘  └────────┬─────────┘                     │
│           │                     │                                │
│  ┌────────▼─────────────────────▼─────────┐                     │
│  │           FastAPI Endpoints            │                     │
│  │  GET /predict     → Predictions       │                     │
│  │  GET /models      → Status + Versions │                     │
│  │  POST /reload     → Force reload      │                     │
│  └─────────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
```
