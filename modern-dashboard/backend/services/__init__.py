# Services module for ML prediction integration
from .ml_predictor import (
    FunnelMLPredictor,
    PredictionResult,
    get_next_minute_predictions
)

__all__ = [
    "FunnelMLPredictor",
    "PredictionResult",
    "get_next_minute_predictions"
]
