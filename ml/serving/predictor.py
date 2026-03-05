"""Model prediction with caching."""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Any, List

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

from .model_loader import ModelLoader


@dataclass
class Prediction:
    """A single prediction result."""
    value: float
    confidence: float
    model_version: str


@dataclass
class ModelStatus:
    """Status of a loaded model."""
    loaded: bool
    version: Optional[str]
    model_type: Optional[str]
    loaded_at: Optional[str]
    last_prediction: Optional[str]


class ModelPredictor:
    """Handles model predictions with caching."""
    
    METRICS = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
    
    FEATURE_COLUMNS = [
        'hour_of_day', 'minute_of_hour', 'day_of_week',
        'viewers_lag_1', 'viewers_lag_2', 'viewers_lag_3',
        'carters_lag_1', 'carters_lag_2', 'carters_lag_3',
        'purchasers_lag_1', 'purchasers_lag_2', 'purchasers_lag_3',
        'tps_viewers', 'tps_carters', 'tps_smoothed'
    ]
    
    # Database connection settings
    DB_HOST = os.getenv('RISINGWAVE_HOST', 'localhost')
    DB_PORT = int(os.getenv('RISINGWAVE_PORT', '4566'))
    DB_NAME = os.getenv('RISINGWAVE_DB', 'dev')
    DB_USER = os.getenv('RISINGWAVE_USER', 'root')
    DB_PASSWORD = os.getenv('RISINGWAVE_PASSWORD', '')
    
    def __init__(self):
        self.loader = ModelLoader()
        self.models: Dict[str, Dict[str, Any]] = {}
        self.last_reload: Optional[str] = None
        self._db_connection_params = {
            'host': self.DB_HOST,
            'port': self.DB_PORT,
            'database': self.DB_NAME,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD
        }
        self._load_models()
    
    def _load_models(self):
        """Load all models from MinIO."""
        print("Loading models from MinIO...")
        self.models = self.loader.load_latest_models()
        self.last_reload = datetime.now(timezone.utc).isoformat()
        print(f"Loaded {len(self.models)} models")
    
    def reload_models(self) -> bool:
        """Force reload models from MinIO."""
        print("Reloading models...")
        self._load_models()
        return len(self.models) > 0
    
    def check_and_reload(self) -> bool:
        """Check for updates and reload if needed."""
        if self.loader.check_for_updates():
            print("New models detected, reloading...")
            self._load_models()
            return True
        return False
    
    def predict(self, metric: str, features: Optional[Dict[str, float]] = None) -> Optional[Prediction]:
        """
        Make prediction for a specific metric.
        
        Uses a hybrid approach:
        - If ML model exists and has sufficient training samples, use ML prediction
        - Otherwise fall back to simple moving average for robustness
        
        Args:
            metric: Metric to predict
            features: Optional feature dict, if None uses current time features
            
        Returns:
            Prediction or None if prediction cannot be made
        """
        # First try to get recent average as a baseline
        recent_avg = self._fetch_recent_average(metric)
        
        # Check if we have a trained model with sufficient data
        if metric in self.models:
            model_data = self.models[metric]
            metadata = model_data.get("metadata", {})
            training_samples = metadata.get("training_samples", 0)
            
            # Only use ML model if it has enough training data (at least 60 samples)
            # With fewer samples, moving average is more reliable
            if training_samples >= 60:
                try:
                    ml_prediction = self._predict_with_model(metric, model_data, features)
                    if ml_prediction is not None:
                        # Blend ML prediction with recent average for stability
                        blended_value = (ml_prediction.value * 0.7) + (recent_avg * 0.3)
                        return Prediction(
                            value=round(blended_value, 2),
                            confidence=ml_prediction.confidence,
                            model_version=ml_prediction.model_version
                        )
                except Exception as e:
                    print(f"ML prediction failed for {metric}, using moving average: {e}")
        
        # Fall back to moving average prediction
        if recent_avg > 0:
            return Prediction(
                value=round(recent_avg, 2),
                confidence=0.6,  # Lower confidence for heuristic prediction
                model_version="moving_average"
            )
        
        return None
    
    def _predict_with_model(self, metric: str, model_data: Dict[str, Any], features: Optional[Dict[str, float]] = None) -> Optional[Prediction]:
        """Make prediction using the ML model."""
        model = model_data["model"]
        scaler = model_data["scaler"]
        version = model_data["version"]
        
        # Build feature vector
        if features is None:
            features = self._build_current_features()
        
        X = np.array([[
            float(features.get('hour_of_day', 0)),
            float(features.get('minute_of_hour', 0)),
            float(features.get('day_of_week', 0)),
            float(features.get('viewers_lag_1', 0)),
            float(features.get('viewers_lag_2', 0)),
            float(features.get('viewers_lag_3', 0)),
            float(features.get('carters_lag_1', 0)),
            float(features.get('carters_lag_2', 0)),
            float(features.get('carters_lag_3', 0)),
            float(features.get('purchasers_lag_1', 0)),
            float(features.get('purchasers_lag_2', 0)),
            float(features.get('purchasers_lag_3', 0)),
            float(features.get('tps_viewers', 0)),
            float(features.get('tps_carters', 0)),
            float(features.get('tps_smoothed', 0))
        ]])
        
        # Scale features
        X_scaled = scaler.transform(X)
        
        # Make prediction
        prediction_value = model.predict(X_scaled)[0]
        
        # Calculate confidence
        confidence = self._calculate_confidence(model, X_scaled, prediction_value)
        
        # Update last prediction time
        model_data["last_prediction"] = datetime.now(timezone.utc).isoformat()
        
        return Prediction(
            value=round(float(prediction_value), 2),
            confidence=round(float(confidence), 3),
            model_version=version
        )
    
    def _fetch_recent_average(self, metric: str) -> float:
        """
        Fetch the recent average of a metric from RisingWave.
        Uses a weighted approach favoring recent data for faster TPS change response.
        """
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Fetch last 3 windows with weighted average (favor recent data)
            cursor.execute("""
                SELECT """ + metric + """ as value, window_start
                FROM funnel_training
                WHERE window_end < NOW()
                ORDER BY window_start DESC
                LIMIT 3
            """)
            
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not rows:
                return 0.0
            
            if len(rows) == 1:
                return float(rows[0]['value'])
            
            # Weighted average: most recent window counts more
            # Weights: 50% for most recent, 30% for middle, 20% for oldest
            weights = [0.5, 0.3, 0.2]
            weighted_sum = 0.0
            total_weight = 0.0
            
            for i, row in enumerate(rows):
                weight = weights[i] if i < len(weights) else 0.0
                weighted_sum += float(row['value']) * weight
                total_weight += weight
            
            if total_weight > 0:
                return weighted_sum / total_weight
            return 0.0
            
        except Exception as e:
            print(f"Warning: Failed to fetch recent average for {metric}: {e}")
            return 0.0
    
    def predict_all(self, features: Optional[Dict[str, float]] = None) -> Dict[str, Prediction]:
        """
        Make predictions for all metrics.
        
        Args:
            features: Optional feature dict
            
        Returns:
            Dict mapping metric names to Predictions
        """
        results = {}
        for metric in self.METRICS:
            pred = self.predict(metric, features)
            if pred:
                results[metric] = pred
        return results
    
    def _get_db_connection(self):
        """Create and return a database connection."""
        return psycopg2.connect(**self._db_connection_params)
    
    def _fetch_latest_lag_values(self) -> Dict[str, float]:
        """
        Fetch the latest lag values and TPS from RisingWave funnel_training.
        Only uses completed windows (window_end < NOW()) to avoid partial data.
        With 20-second windows, we fetch 9 windows to get ~3 minutes of lag context.
        """
        default_values = {
            'viewers_lag_1': 0.0, 'viewers_lag_2': 0.0, 'viewers_lag_3': 0.0,
            'carters_lag_1': 0.0, 'carters_lag_2': 0.0, 'carters_lag_3': 0.0,
            'purchasers_lag_1': 0.0, 'purchasers_lag_2': 0.0, 'purchasers_lag_3': 0.0,
            'tps_viewers': 0.0, 'tps_carters': 0.0, 'tps_smoothed': 0.0
        }
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Fetch the 9 most recent completed windows (20-second windows = 3 minutes of context)
            # These will be used as lag_1, lag_2, lag_3 respectively (every 3rd window = 1 minute apart)
            cursor.execute("""
                SELECT
                    viewers,
                    carters,
                    purchasers
                FROM funnel_training
                WHERE window_end < NOW()
                ORDER BY window_start DESC
                LIMIT 9
            """)
            
            rows = cursor.fetchall()
            
            # With 20-second windows, lag features are 1 minute apart (every 3rd window)
            if len(rows) >= 1:
                default_values['viewers_lag_1'] = float(rows[0]['viewers'])
                default_values['carters_lag_1'] = float(rows[0]['carters'])
                default_values['purchasers_lag_1'] = float(rows[0]['purchasers'])
            
            if len(rows) >= 4:  # 3 windows back = 1 minute
                default_values['viewers_lag_2'] = float(rows[3]['viewers'])
                default_values['carters_lag_2'] = float(rows[3]['carters'])
                default_values['purchasers_lag_2'] = float(rows[3]['purchasers'])
            
            if len(rows) >= 7:  # 6 windows back = 2 minutes
                default_values['viewers_lag_3'] = float(rows[6]['viewers'])
                default_values['carters_lag_3'] = float(rows[6]['carters'])
                default_values['purchasers_lag_3'] = float(rows[6]['purchasers'])
            
            cursor.close()
            conn.close()
            
            # Calculate TPS from the lag values (20-second windows = change over 20 seconds)
            if len(rows) >= 2:
                # TPS based on latest 20-second change (convert to per-second rate)
                tps_v = abs(float(rows[0]['viewers']) - float(rows[1]['viewers'])) / 20.0
                tps_c = abs(float(rows[0]['carters']) - float(rows[1]['carters'])) / 20.0
                default_values['tps_viewers'] = tps_v
                default_values['tps_carters'] = tps_c
                
                # Smoothed TPS over last 3 windows (60 seconds total)
                if len(rows) >= 4:
                    change_1 = abs(float(rows[0]['viewers']) - float(rows[1]['viewers']))
                    change_2 = abs(float(rows[1]['viewers']) - float(rows[2]['viewers']))
                    change_3 = abs(float(rows[2]['viewers']) - float(rows[3]['viewers']))
                    default_values['tps_smoothed'] = (change_1 + change_2 + change_3) / 60.0
                else:
                    default_values['tps_smoothed'] = tps_v
            
            return default_values
            
        except Exception as e:
            print(f"Warning: Failed to fetch lag values from RisingWave: {e}")
            return default_values
    
    def _build_current_features(self) -> Dict[str, float]:
        """Build feature vector using current time and actual lag values from RisingWave."""
        now = datetime.now(timezone.utc)
        
        # Fetch actual lag values from RisingWave (last 3 completed windows)
        lag_values = self._fetch_latest_lag_values()
        
        return {
            'hour_of_day': float(now.hour),
            'minute_of_hour': float(now.minute),
            'day_of_week': float(now.weekday()),
            **lag_values  # Include fetched lag values
        }
    
    def _calculate_confidence(self, model, X_scaled, prediction) -> float:
        """Calculate confidence score for prediction."""
        confidence = 0.7  # Default confidence
        
        if hasattr(model, 'feature_importances_'):
            # RandomForest - use prediction variance from trees
            predictions = [tree.predict(X_scaled)[0] for tree in model.estimators_]
            variance = np.var(predictions)
            confidence = max(0.5, min(0.95, 1.0 - (variance / (abs(prediction) + 1))))
        elif hasattr(model, 'coef_'):
            # LinearRegression - use R² as proxy
            confidence = 0.75
        
        return confidence
    
    def get_model_status(self) -> Dict[str, ModelStatus]:
        """Return status of all loaded models."""
        status = {}
        
        for metric in self.METRICS:
            if metric in self.models:
                model_data = self.models[metric]
                metadata = model_data.get("metadata", {})
                status[metric] = ModelStatus(
                    loaded=True,
                    version=model_data.get("version"),
                    model_type=metadata.get("model_type"),
                    loaded_at=model_data.get("loaded_at"),
                    last_prediction=model_data.get("last_prediction")
                )
            else:
                status[metric] = ModelStatus(
                    loaded=False,
                    version=None,
                    model_type=None,
                    loaded_at=None,
                    last_prediction=None
                )
        
        return status
    
    def get_manifest(self) -> Optional[Dict[str, Any]]:
        """Get the current manifest from MinIO."""
        return self.loader.get_manifest()
