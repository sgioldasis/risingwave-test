"""FastAPI application for ML Serving Service."""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .predictor import ModelPredictor

logger = logging.getLogger(__name__)

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
_mode_check_task: Optional[asyncio.Task] = None

# Configuration - dynamic mode detection
_current_mode: str = "online"  # "online" or "batch"
_mode_switch_history: list = []  # Track mode switches for logging

# Optional environment variable overrides (for backward compatibility)
_FORCE_ONLINE = os.getenv('USE_ONLINE_LEARNING', '').lower() == 'true'
_FORCE_KAFKA = os.getenv('USE_KAFKA_SOURCE', '').lower() == 'true'


def get_predictor() -> ModelPredictor:
    """Get or create the batch predictor instance."""
    global _predictor
    if _predictor is None:
        _predictor = ModelPredictor()
    return _predictor


def get_online_predictor() -> Optional[Any]:
    """Get or create the appropriate online learning predictor."""
    global _river_predictor, _kafka_river_predictor
    
    if _FORCE_KAFKA and KAFKA_RIVER_AVAILABLE:
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
            # Only reload batch models when in batch mode
            if _current_mode == "batch":
                predictor = get_predictor()
                updated = predictor.check_and_reload()
                if updated:
                    logger.info(f"[{datetime.now(timezone.utc).isoformat()}] Batch models auto-reloaded")
        except Exception as e:
            logger.error(f"Error in auto-reload: {e}")


async def mode_detection_loop():
    """
    Background task to detect batch models and switch modes dynamically.
    
    Checks MinIO every 30 seconds for batch models:
    - If batch models found -> switch to batch mode
    - If no batch models -> stay in online mode
    """
    global _current_mode, _mode_switch_history
    check_interval = int(os.getenv('MODE_CHECK_INTERVAL_SECONDS', '30'))
    
    # Initialize model loader for checking
    from .model_loader import ModelLoader
    loader = ModelLoader()
    
    while True:
        try:
            await asyncio.sleep(check_interval)
            
            has_batch = loader.has_batch_models()
            previous_mode = _current_mode
            
            if has_batch and _current_mode != "batch":
                # Switch to batch mode
                _current_mode = "batch"
                _mode_switch_history.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "from": previous_mode,
                    "to": "batch",
                    "reason": "Batch models detected in MinIO"
                })
                logger.info(f"🔄 Mode switched: {previous_mode} -> batch (Batch models detected)")
                
                # Initialize batch predictor if not already done
                get_predictor()
                
            elif not has_batch and _current_mode != "online":
                # Switch to online mode
                _current_mode = "online"
                _mode_switch_history.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "from": previous_mode,
                    "to": "online",
                    "reason": "No batch models available"
                })
                logger.info(f"🔄 Mode switched: {previous_mode} -> online (No batch models)")
                
                # Initialize online predictor if not already done
                get_online_predictor()
                
        except Exception as e:
            logger.error(f"Error in mode detection: {e}")


@app.on_event("startup")
async def startup_event():
    """Initialize predictor on startup with dynamic mode detection."""
    global _reload_task, _mode_check_task, _current_mode
    logger.info("Starting ML Serving Service...")
    
    # Check for batch models on startup to determine initial mode
    from .model_loader import ModelLoader
    loader = ModelLoader()
    has_batch = loader.has_batch_models()
    
    if has_batch:
        _current_mode = "batch"
        logger.info("📦 Batch mode enabled (batch models detected)")
        # Initialize batch predictor
        get_predictor()
        # Start auto-reload task for batch models
        _reload_task = asyncio.create_task(auto_reload_loop())
        logger.info(f"Auto-reload enabled (interval: {os.getenv('MODEL_RELOAD_INTERVAL_SECONDS', '60')}s)")
    else:
        _current_mode = "online"
        source = "Kafka" if (_FORCE_KAFKA and KAFKA_RIVER_AVAILABLE) else "RisingWave"
        logger.info(f"🌊 Online learning mode enabled ({source}) - no batch models found")
        # Initialize online predictor (starts the learner)
        get_online_predictor()
    
    # Start mode detection background task (checks for batch models every 30s)
    _mode_check_task = asyncio.create_task(mode_detection_loop())
    logger.info("Mode detection enabled (interval: 30s)")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global _reload_task, _mode_check_task, _river_predictor, _kafka_river_predictor
    
    if _reload_task:
        _reload_task.cancel()
        try:
            await _reload_task
        except asyncio.CancelledError:
            pass
    
    if _mode_check_task:
        _mode_check_task.cancel()
        try:
            await _mode_check_task
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
    global _current_mode
    
    # Determine source based on current mode
    if _current_mode == "batch":
        source = "minio"
    elif _FORCE_KAFKA and KAFKA_RIVER_AVAILABLE:
        source = "kafka"
    else:
        source = "risingwave"
    
    result = {
        "healthy": True,
        "service": "ml-serving",
        "mode": _current_mode,
        "source": source,
        "river_available": RIVER_AVAILABLE,
        "kafka_river_available": KAFKA_RIVER_AVAILABLE,
        "mode_switches": len(_mode_switch_history),
        "checked_at": datetime.now(timezone.utc).isoformat()
    }
    
    if _current_mode == "online":
        online_pred = get_online_predictor()
        if online_pred:
            result["online_ready"] = online_pred.is_ready()
            if source == "kafka":
                result["kafka_connected"] = online_pred.is_kafka_connected()
    else:
        predictor = get_predictor()
        manifest = predictor.get_manifest()
        result["models_loaded"] = len(predictor.models)
        result["manifest_available"] = manifest is not None
    
    return result


@app.get("/predict", response_model=PredictionResponse)
async def predict_all():
    """Get predictions for all metrics using dynamically selected mode."""
    global _current_mode
    predicted_at = datetime.now(timezone.utc).isoformat()
    next_minute = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    
    # Determine source based on current mode and availability
    if _current_mode == "batch":
        source = "minio"
    elif _FORCE_KAFKA and KAFKA_RIVER_AVAILABLE:
        source = "kafka"
    else:
        source = "risingwave"
    
    result = {
        "predicted_at": predicted_at,
        "timestamp": next_minute.isoformat(),
        "mode": _current_mode,
        "source": source,
        "viewers": None,
        "carters": None,
        "purchasers": None,
        "view_to_cart_rate": None,
        "cart_to_buy_rate": None
    }
    
    # Use online learning if in online mode
    if _current_mode == "online":
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
                if source == "kafka" and hasattr(pred, 'kafka_connected'):
                    result[metric]["kafka_connected"] = pred.kafka_connected
        return result
    
    # Use batch prediction
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
    """Get prediction for a specific metric using dynamically selected mode."""
    global _current_mode
    
    # Validate metric
    valid_metrics = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
    if metric not in valid_metrics:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric}")
    
    predicted_at = datetime.now(timezone.utc).isoformat()
    
    # Determine source based on current mode
    if _current_mode == "batch":
        source = "minio"
    elif _FORCE_KAFKA and KAFKA_RIVER_AVAILABLE:
        source = "kafka"
    else:
        source = "risingwave"
    
    # Use online learning if in online mode
    if _current_mode == "online":
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
                    "mode": _current_mode,
                    "source": source,
                    "predicted_at": predicted_at
                }
                if source == "kafka" and hasattr(prediction, 'kafka_connected'):
                    response["kafka_connected"] = prediction.kafka_connected
                return response
    
    # Use batch prediction
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
        "mode": _current_mode,
        "source": source,
        "predicted_at": predicted_at
    }


@app.get("/models", response_model=ModelStatusResponse)
async def get_models():
    """Get status of all loaded models using dynamically selected mode."""
    global _current_mode
    
    # Determine source based on current mode
    if _current_mode == "batch":
        source = "minio"
    elif _FORCE_KAFKA and KAFKA_RIVER_AVAILABLE:
        source = "kafka"
    else:
        source = "risingwave"
    
    if _current_mode == "online":
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
                "mode": _current_mode,
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
        "mode": _current_mode,
        "source": source,
        "models": status_dict,
        "online_stats": None,
        "last_reload": predictor.last_reload,
        "manifest": manifest
    }


@app.post("/reload")
async def reload_models():
    """Force reload models (batch mode only)."""
    global _current_mode
    
    # Determine source for response
    if _current_mode == "batch":
        source = "minio"
    elif _FORCE_KAFKA and KAFKA_RIVER_AVAILABLE:
        source = "kafka"
    else:
        source = "risingwave"
    
    if _current_mode == "online":
        return {
            "success": True,
            "message": "Online learning mode - models update continuously, no reload needed",
            "mode": _current_mode,
            "source": source
        }
    
    predictor = get_predictor()
    success = predictor.reload_models()
    
    if success:
        return {
            "success": True,
            "message": f"Reloaded {len(predictor.models)} models",
            "mode": _current_mode,
            "source": source,
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
    global _current_mode
    
    # Determine source
    if _current_mode == "batch":
        source = "minio"
    elif _FORCE_KAFKA and KAFKA_RIVER_AVAILABLE:
        source = "kafka"
    else:
        source = "risingwave"
    
    if _current_mode != "online" or source == "kafka":
        raise HTTPException(
            status_code=400,
            detail="Manual learning only available in online mode with RisingWave source"
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
        "mode": _current_mode,
        "source": source,
        "features": request.features,
        "targets": request.targets
    }


@app.get("/online/status")
async def online_learning_status():
    """Get detailed online learning status."""
    global _current_mode
    
    if _current_mode != "online":
        raise HTTPException(status_code=400, detail="Online learning not enabled (currently in batch mode)")
    
    online_pred = get_online_predictor()
    if not online_pred:
        raise HTTPException(status_code=503, detail="Online predictor not initialized")
    
    return online_pred.get_status()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('ML_SERVING_PORT', '8001'))
    uvicorn.run(app, host="0.0.0.0", port=port)
