"""FastAPI application for ML Serving Service."""

import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Optional, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .predictor import ModelPredictor

app = FastAPI(
    title="ML Serving Service",
    description="Real-time ML predictions for funnel metrics",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global predictor instance
_predictor: Optional[ModelPredictor] = None
_reload_task: Optional[asyncio.Task] = None


def get_predictor() -> ModelPredictor:
    """Get or create the predictor instance."""
    global _predictor
    if _predictor is None:
        _predictor = ModelPredictor()
    return _predictor


async def auto_reload_loop():
    """Background task to check for model updates."""
    reload_interval = int(os.getenv('MODEL_RELOAD_INTERVAL_SECONDS', '5'))
    
    while True:
        try:
            await asyncio.sleep(reload_interval)
            predictor = get_predictor()
            updated = predictor.check_and_reload()
            if updated:
                print(f"[{datetime.now(timezone.utc).isoformat()}] Models auto-reloaded")
        except Exception as e:
            print(f"Error in auto-reload: {e}")


@app.on_event("startup")
async def startup_event():
    """Initialize predictor on startup."""
    global _reload_task
    print("Starting ML Serving Service...")
    
    # Initialize predictor
    get_predictor()
    
    # Start auto-reload task
    _reload_task = asyncio.create_task(auto_reload_loop())
    print(f"Auto-reload enabled (interval: {os.getenv('MODEL_RELOAD_INTERVAL_SECONDS', '60')}s)")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global _reload_task
    if _reload_task:
        _reload_task.cancel()
        try:
            await _reload_task
        except asyncio.CancelledError:
            pass


class PredictionResponse(BaseModel):
    """Response model for predictions."""
    predicted_at: str
    timestamp: str
    viewers: Optional[Dict[str, Any]]
    carters: Optional[Dict[str, Any]]
    purchasers: Optional[Dict[str, Any]]
    view_to_cart_rate: Optional[Dict[str, Any]]
    cart_to_buy_rate: Optional[Dict[str, Any]]


class ModelStatusResponse(BaseModel):
    """Response model for model status."""
    models: Dict[str, Any]
    last_reload: Optional[str]
    manifest: Optional[Dict[str, Any]]


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    predictor = get_predictor()
    manifest = predictor.get_manifest()
    
    return {
        "healthy": True,
        "service": "ml-serving",
        "models_loaded": len(predictor.models),
        "manifest_available": manifest is not None,
        "checked_at": datetime.now(timezone.utc).isoformat()
    }


@app.get("/predict", response_model=PredictionResponse)
async def predict_all():
    """Get predictions for all metrics."""
    predictor = get_predictor()
    
    # Check for model updates on each prediction request for faster pickup
    predictor.check_and_reload()
    
    # Predictions work even without ML models - falls back to moving average
    predictions = predictor.predict_all()
    
    predicted_at = datetime.now(timezone.utc).isoformat()
    next_minute = datetime.now(timezone.utc)
    next_minute = next_minute.replace(second=0, microsecond=0)
    # Add one minute
    from datetime import timedelta
    next_minute = next_minute + timedelta(minutes=1)
    
    result = {
        "predicted_at": predicted_at,
        "timestamp": next_minute.isoformat(),
        "viewers": None,
        "carters": None,
        "purchasers": None,
        "view_to_cart_rate": None,
        "cart_to_buy_rate": None
    }
    
    for metric, pred in predictions.items():
        # Get detailed model type from loaded model metadata
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
    predictor = get_predictor()
    
    if metric not in predictor.METRICS:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric}")
    
    # Check for model updates on each prediction request for faster pickup
    predictor.check_and_reload()
    
    prediction = predictor.predict(metric)
    
    if prediction is None:
        raise HTTPException(status_code=503, detail=f"Prediction for {metric} not available - no data in RisingWave")
    
    return {
        "metric": metric,
        "value": prediction.value,
        "confidence": prediction.confidence,
        "model_version": prediction.model_version,
        "predicted_at": datetime.now(timezone.utc).isoformat()
    }


@app.get("/models", response_model=ModelStatusResponse)
async def get_models():
    """Get status of all loaded models."""
    predictor = get_predictor()
    status = predictor.get_model_status()
    manifest = predictor.get_manifest()
    
    # Convert ModelStatus to dict
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
        "models": status_dict,
        "last_reload": predictor.last_reload,
        "manifest": manifest
    }


@app.post("/reload")
async def reload_models():
    """Force reload models from MinIO."""
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


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('ML_SERVING_PORT', '8001'))
    uvicorn.run(app, host="0.0.0.0", port=port)
