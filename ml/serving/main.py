"""FastAPI application for ML Serving Service."""

import asyncio
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .predictor import ModelPredictor

# Optional River online learning integrations
try:
    from .river_predictor import RiverModelPredictor, get_river_predictor
    RIVER_AVAILABLE = True
except ImportError:
    RIVER_AVAILABLE = False

try:
    from .kafka_river_predictor import KafkaRiverModelPredictor, get_kafka_river_predictor
    KAFKA_RIVER_AVAILABLE = True
except ImportError:
    KAFKA_RIVER_AVAILABLE = False

app = FastAPI(
    title="ML Serving Service",
    description="Real-time ML predictions for funnel metrics",
    version="1.2.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global predictor instances
_predictor: Optional[ModelPredictor] = None
_river_predictor: Optional['RiverModelPredictor'] = None
_kafka_river_predictor: Optional['KafkaRiverModelPredictor'] = None
_reload_task: Optional[asyncio.Task] = None

# Configuration
USE_ONLINE_LEARNING = os.getenv('USE_ONLINE_LEARNING', 'false').lower() == 'true'
USE_KAFKA_SOURCE = os.getenv('USE_KAFKA_SOURCE', 'false').lower() == 'true'


def get_predictor() -> ModelPredictor:
    """Get or create the batch predictor instance."""
    global _predictor
    if _predictor is None:
        _predictor = ModelPredictor()
    return _predictor


def get_online_predictor() -> Optional[Any]:
    """Get or create the appropriate online learning predictor."""
    global _river_predictor, _kafka_river_predictor
    
    if USE_KAFKA_SOURCE and KAFKA_RIVER_AVAILABLE:
        if _kafka_river_predictor is None:
            _kafka_river_predictor = get_kafka_river_predictor(auto_start=True)
        return _kafka_river_predictor
    elif RIVER_AVAILABLE:
        if _river_predictor is None:
            _river_predictor = get_river_predictor(auto_start=True)
        return _river_predictor
    return None


async def auto_reload_loop():
    """Background task to check for batch model updates."""
    reload_interval = int(os.getenv('MODEL_RELOAD_INTERVAL_SECONDS', '60'))
    
    while True:
        try:
            await asyncio.sleep(reload_interval)
            # Only check batch models if not using online learning
            if not USE_ONLINE_LEARNING:
                predictor = get_predictor()
                updated = predictor.check_and_reload()
                if updated:
                    print(f"[{datetime.now(timezone.utc).isoformat()}] Batch models auto-reloaded")
        except Exception as e:
            print(f"Error in auto-reload: {e}")


@app.on_event("startup")
async def startup_event():
    """Initialize predictor on startup."""
    global _reload_task
    print("Starting ML Serving Service...")
    
    if USE_ONLINE_LEARNING:
        source = "Kafka" if USE_KAFKA_SOURCE else "RisingWave"
        print(f"🌊 Online learning mode enabled ({source})")
        # Initialize online predictor (starts the learner)
        get_online_predictor()
    else:
        print("📦 Batch mode enabled")
        # Initialize batch predictor
        get_predictor()
        
        # Start auto-reload task
        _reload_task = asyncio.create_task(auto_reload_loop())
        print(f"Auto-reload enabled (interval: {os.getenv('MODEL_RELOAD_INTERVAL_SECONDS', '60')}s)")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global _reload_task, _river_predictor, _kafka_river_predictor
    
    if _reload_task:
        _reload_task.cancel()
        try:
            await _reload_task
        except asyncio.CancelledError:
            pass
    
    # Stop online learners if running
    if _river_predictor and hasattr(_river_predictor, 'learner'):
        from ..online.learner import reset_learner
        reset_learner()
    
    if _kafka_river_predictor and hasattr(_kafka_river_predictor, 'learner'):
        from ..online.kafka_learner import reset_kafka_learner
        reset_kafka_learner()


class PredictionResponse(BaseModel):
    """Response model for predictions."""
    predicted_at: str
    timestamp: str
    mode: str
    source: str
    viewers: Optional[Dict[str, Any]]
    carters: Optional[Dict[str, Any]]
    purchasers: Optional[Dict[str, Any]]
    view_to_cart_rate: Optional[Dict[str, Any]]
    cart_to_buy_rate: Optional[Dict[str, Any]]


class ModelStatusResponse(BaseModel):
    """Response model for model status."""
    mode: str
    source: str
    models: Dict[str, Any]
    online_stats: Optional[Dict[str, Any]]
    last_reload: Optional[str]
    manifest: Optional[Dict[str, Any]]


class OnlineLearnRequest(BaseModel):
    """Request to manually trigger learning."""
    features: Dict[str, float]
    targets: Dict[str, float]


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    mode = "online" if USE_ONLINE_LEARNING else "batch"
    source = "kafka" if USE_KAFKA_SOURCE else "risingwave" if USE_ONLINE_LEARNING else "none"
    
    result = {
        "healthy": True,
        "service": "ml-serving",
        "mode": mode,
        "source": source,
        "river_available": RIVER_AVAILABLE,
        "kafka_river_available": KAFKA_RIVER_AVAILABLE,
        "checked_at": datetime.now(timezone.utc).isoformat()
    }
    
    if USE_ONLINE_LEARNING:
        online_pred = get_online_predictor()
        if online_pred:
            result["online_ready"] = online_pred.is_ready()
            if USE_KAFKA_SOURCE:
                result["kafka_connected"] = online_pred.is_kafka_connected()
    else:
        predictor = get_predictor()
        manifest = predictor.get_manifest()
        result["models_loaded"] = len(predictor.models)
        result["manifest_available"] = manifest is not None
    
    return result


@app.get("/predict", response_model=PredictionResponse)
async def predict_all():
    """Get predictions for all metrics."""
    predicted_at = datetime.now(timezone.utc).isoformat()
    next_minute = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    
    mode = "online" if USE_ONLINE_LEARNING else "batch"
    source = "kafka" if USE_KAFKA_SOURCE else "risingwave" if USE_ONLINE_LEARNING else "minio"
    
    result = {
        "predicted_at": predicted_at,
        "timestamp": next_minute.isoformat(),
        "mode": mode,
        "source": source,
        "viewers": None,
        "carters": None,
        "purchasers": None,
        "view_to_cart_rate": None,
        "cart_to_buy_rate": None
    }
    
    # Use online learning if enabled
    if USE_ONLINE_LEARNING:
        online_pred = get_online_predictor()
        if online_pred:
            predictions = online_pred.predict_all()
            for metric, pred in predictions.items():
                result[metric] = {
                    "value": pred.value,
                    "confidence": pred.confidence,
                    "model_type": pred.model_type,
                    "samples_learned": pred.samples_learned
                }
                # Add Kafka connection status if using Kafka
                if USE_KAFKA_SOURCE and hasattr(pred, 'kafka_connected'):
                    result[metric]["kafka_connected"] = pred.kafka_connected
        return result
    
    # Fall back to batch prediction
    predictor = get_predictor()
    predictor.check_and_reload()
    predictions = predictor.predict_all()
    
    for metric, pred in predictions.items():
        model_type = "unknown"
        if metric in predictor.models:
            model_data = predictor.models[metric]
            metadata = model_data.get("metadata", {})
            model_type = metadata.get("model_type", "unknown")
        elif pred.model_version == "moving_average":
            model_type = "MovingAverage"
        
        result[metric] = {
            "value": pred.value,
            "confidence": pred.confidence,
            "model_version": pred.model_version,
            "model_type": model_type
        }
    
    return result


@app.get("/predict/{metric}")
async def predict_metric(metric: str):
    """Get prediction for a specific metric."""
    
    # Validate metric
    valid_metrics = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
    if metric not in valid_metrics:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric}")
    
    predicted_at = datetime.now(timezone.utc).isoformat()
    
    # Use online learning if enabled
    if USE_ONLINE_LEARNING:
        online_pred = get_online_predictor()
        if online_pred:
            prediction = online_pred.predict(metric)
            if prediction:
                response = {
                    "metric": metric,
                    "value": prediction.value,
                    "confidence": prediction.confidence,
                    "model_type": prediction.model_type,
                    "samples_learned": prediction.samples_learned,
                    "predicted_at": predicted_at
                }
                if USE_KAFKA_SOURCE and hasattr(prediction, 'kafka_connected'):
                    response["kafka_connected"] = prediction.kafka_connected
                return response
    
    # Fall back to batch prediction
    predictor = get_predictor()
    predictor.check_and_reload()
    prediction = predictor.predict(metric)
    
    if prediction is None:
        raise HTTPException(status_code=503, detail=f"Prediction for {metric} not available")
    
    return {
        "metric": metric,
        "value": prediction.value,
        "confidence": prediction.confidence,
        "model_version": prediction.model_version,
        "predicted_at": predicted_at
    }


@app.get("/models", response_model=ModelStatusResponse)
async def get_models():
    """Get status of all loaded models."""
    mode = "online" if USE_ONLINE_LEARNING else "batch"
    source = "kafka" if USE_KAFKA_SOURCE else "risingwave" if USE_ONLINE_LEARNING else "minio"
    
    if USE_ONLINE_LEARNING:
        online_pred = get_online_predictor()
        if online_pred:
            status = online_pred.get_status()
            model_stats = status.get('models', {})
            
            # Format for response
            models_dict = {}
            for metric, stats in model_stats.items():
                models_dict[metric] = {
                    "learning_count": stats.get('learning_count', 0),
                    "mae": stats.get('mae'),
                    "r2": stats.get('r2'),
                    "ready": stats.get('learning_count', 0) >= 5
                }
            
            return {
                "mode": mode,
                "source": source,
                "models": models_dict,
                "online_stats": {
                    "running": status.get('running'),
                    "records_learned": status.get('records_learned'),
                    "runtime_seconds": status.get('runtime_seconds'),
                    "history_size": status.get('history_size'),
                    "kafka_connected": status.get('kafka_connected')
                },
                "last_reload": None,
                "manifest": None
            }
    
    # Batch mode status
    predictor = get_predictor()
    status = predictor.get_model_status()
    manifest = predictor.get_manifest()
    
    status_dict = {}
    for metric, model_status in status.items():
        status_dict[metric] = {
            "loaded": model_status.loaded,
            "version": model_status.version,
            "model_type": model_status.model_type,
            "loaded_at": model_status.loaded_at,
            "last_prediction": model_status.last_prediction
        }
    
    return {
        "mode": mode,
        "source": source,
        "models": status_dict,
        "online_stats": None,
        "last_reload": predictor.last_reload,
        "manifest": manifest
    }


@app.post("/reload")
async def reload_models():
    """Force reload models (batch mode only)."""
    if USE_ONLINE_LEARNING:
        return {
            "success": True,
            "message": "Online learning mode - models update continuously, no reload needed",
            "mode": "online",
            "source": "kafka" if USE_KAFKA_SOURCE else "risingwave"
        }
    
    predictor = get_predictor()
    success = predictor.reload_models()
    
    if success:
        return {
            "success": True,
            "message": f"Reloaded {len(predictor.models)} models",
            "reloaded_at": predictor.last_reload
        }
    else:
        raise HTTPException(status_code=503, detail="Failed to reload models")


@app.post("/learn")
async def manual_learn(request: OnlineLearnRequest):
    """
    Manually trigger learning from features and targets.
    Only available in online learning mode with RisingWave source.
    """
    if not USE_ONLINE_LEARNING or USE_KAFKA_SOURCE:
        raise HTTPException(
            status_code=400,
            detail="Manual learning only available when USE_ONLINE_LEARNING=true and USE_KAFKA_SOURCE=false"
        )
    
    if not RIVER_AVAILABLE:
        raise HTTPException(status_code=503, detail="River not available")
    
    river_pred = get_river_predictor()
    if not river_pred:
        raise HTTPException(status_code=503, detail="Online learner not available")
    
    river_pred.learner.learn_manual(request.features, request.targets)
    
    return {
        "success": True,
        "message": "Learning triggered",
        "features": request.features,
        "targets": request.targets
    }


@app.get("/online/status")
async def online_learning_status():
    """Get detailed online learning status."""
    if not USE_ONLINE_LEARNING:
        raise HTTPException(status_code=400, detail="Online learning not enabled")
    
    online_pred = get_online_predictor()
    if not online_pred:
        raise HTTPException(status_code=503, detail="Online predictor not initialized")
    
    return online_pred.get_status()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('ML_SERVING_PORT', '8001'))
    uvicorn.run(app, host="0.0.0.0", port=port)
