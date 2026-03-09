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
        # Use Simple Moving Average for stable predictions
        # Average of last 3 completed windows
        ma_prediction = self._fetch_recent_average(metric)
        
        # Get trained model info for display purposes
        trained_model_version = "unknown"
        if metric in self.models:
            model_data = self.models[metric]
            trained_model_version = model_data.get("version", "unknown")
        
        if ma_prediction is not None and ma_prediction > 0:
            display_version = trained_model_version if trained_model_version != "unknown" else "moving_average"
            return Prediction(
                value=round(ma_prediction, 2),
                confidence=0.80,  # Good confidence for moving average
                model_version=display_version
            )
        
        # Fallback to recent average if rate prediction unavailable
        recent_avg = self._fetch_recent_average(metric)
        if recent_avg > 0:
            display_version = trained_model_version if trained_model_version != "unknown" else "live_moving_average"
            return Prediction(
                value=round(recent_avg, 2),
                confidence=0.70,
                model_version=display_version
            )
        
        # Fall back to trained model if live data unavailable
        if metric in self.models:
            model_data = self.models[metric]
            metadata = model_data.get("metadata", {})
            model_type = metadata.get("model_type", "Unknown")
            version = model_data.get("version", "unknown")
            
            # Handle MovingAverage models
            if model_type == "MovingAverage":
                model = model_data.get("model", {})
                saved_avg = model.get("value", 0)
                if saved_avg > 0:
                    return Prediction(
                        value=round(saved_avg, 2),
                        confidence=0.5,  # Lower confidence for cached average
                        model_version=version
                    )
            
            # This path shouldn't be reached now, but kept for safety
            elif model_type in ["RandomForestRegressor", "LinearRegression"]:
                try:
                    ml_prediction = self._predict_with_model(metric, model_data, features)
                    if ml_prediction is not None:
                        # TPS-aware prediction scaling
                        # When TPS changes, scale the ML prediction proportionally
                        
                        # Get current TPS and historical TPS trend
                        tps_current = features.get('tps_smoothed', 0) if features else 0
                        tps_lag1 = features.get('tps_viewers', 0) if features else 0
                        tps_lag2 = features.get('tps_carters', 0) if features else 0  # Another TPS proxy
                        
                        # Calculate historical average TPS from lag features
                        historical_tps = (tps_lag1 + tps_lag2) / 2.0 if tps_lag2 > 0 else tps_lag1
                        
                        # TPS scaling factor: how much has TPS changed?
                        if historical_tps > 0 and tps_current > 0:
                            tps_ratio = tps_current / historical_tps
                        else:
                            tps_ratio = 1.0
                        
                        # Apply TPS scaling to ML prediction
                        # Cap scaling to prevent extreme predictions (0.5x to 3x)
                        scaled_tps_ratio = max(0.5, min(3.0, tps_ratio))
                        scaled_prediction = ml_prediction.value * scaled_tps_ratio
                        
                        # Blend scaled prediction with recent average for stability
                        # 60% scaled ML + 40% recent average (more weight to recent when TPS changes)
                        recent_weight = 0.4 if abs(tps_ratio - 1.0) > 0.3 else 0.2
                        ml_weight = 1.0 - recent_weight
                        
                        blended_value = (scaled_prediction * ml_weight) + (recent_avg * recent_weight)
                        
                        # Log significant TPS adjustments
                        if abs(tps_ratio - 1.0) > 0.2:
                            print(f"[{metric}] TPS ratio: {tps_ratio:.2f}x, ML: {ml_prediction.value:.1f} -> Scaled: {scaled_prediction:.1f} -> Final: {blended_value:.1f}")
                        
                        return Prediction(
                            value=round(blended_value, 2),
                            confidence=ml_prediction.confidence * 0.9,  # Slightly lower confidence when scaling
                            model_version=ml_prediction.model_version
                        )
                except Exception as e:
                    print(f"ML prediction failed for {metric}, using moving average: {e}")
        
        # Fall back to live moving average prediction
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
            
            # Ultra-responsive weighting: almost entirely based on most recent window
            # This allows predictions to track TPS spikes immediately
            # Weights: 90% for most recent, 8% for middle, 2% for oldest
            weights = [0.90, 0.08, 0.02]
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
    
    def _predict_rate_proportional(self, metric: str, features: Optional[Dict[str, float]] = None) -> Optional[float]:
        """
        Predict using Rate-Proportional Scaling.
        
        Formula: Prediction = (Metric_Value / TPS) × Current_TPS × 60_seconds
        
        This provides instant response to TPS changes because it calculates
        the conversion rate per event and scales linearly with TPS.
        
        Args:
            metric: Metric to predict (viewers, carters, purchasers, etc.)
            features: Optional feature dict with TPS values
            
        Returns:
            Predicted value or None if calculation not possible
        """
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Fetch the most recent complete window with TPS
            cursor.execute("""
                SELECT
                    viewers, carters, purchasers,
                    view_to_cart_rate, cart_to_buy_rate,
                    COALESCE(tps_viewers, 0) as tps,
                    COALESCE(tps_smoothed, 0) as tps_smoothed
                FROM funnel_training
                WHERE window_end < NOW()
                ORDER BY window_start DESC
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not row:
                return None
            
            # Get current TPS from features or use smoothed TPS from latest window
            if features:
                current_tps = features.get('tps_smoothed', 0) or features.get('tps_viewers', 0)
            else:
                current_tps = float(row['tps_smoothed']) if row['tps_smoothed'] else float(row['tps'])
            
            # Get historical metric value and TPS from the latest window
            historical_metric = float(row[metric]) if row[metric] is not None else 0
            historical_tps = float(row['tps']) if row['tps'] else current_tps
            
            if historical_tps <= 0 or historical_metric <= 0:
                return None
            
            # Calculate rate per event: how many metric units per TPS unit
            # e.g., 500 viewers / 100 TPS = 5 viewers per TPS
            rate_per_tps = historical_metric / historical_tps
            
            # Scale to 60 seconds for minute-level prediction
            # If window is 20 seconds, we multiply by 3 to get minute-level
            WINDOW_SECONDS = 20
            MINUTE_MULTIPLIER = 60.0 / WINDOW_SECONDS
            
            # Rate-Proportional Prediction
            # Prediction = Rate_per_TPS × Current_TPS × Minute_Multiplier
            prediction = rate_per_tps * current_tps * MINUTE_MULTIPLIER
            
            print(f"[RateProportional] {metric}: hist={historical_metric:.1f}/tps={historical_tps:.1f} "
                  f"→ rate={rate_per_tps:.3f} × curr_tps={current_tps:.1f} × {MINUTE_MULTIPLIER:.1f} "
                  f"= {prediction:.1f}")
            
            return prediction
            
        except Exception as e:
            print(f"Rate-Proportional prediction failed for {metric}: {e}")
            return None
    
    def _predict_ema(self, metric: str, alpha: float = 0.7) -> Optional[float]:
        """
        Predict using Exponential Moving Average (EMA) with current minute data.
        
        Includes the current (in-progress) window from the funnel view
        for maximum responsiveness to real-time traffic changes.
        
        Args:
            metric: Metric to predict (viewers, carters, purchasers, etc.)
            
        Returns:
            Predicted value scaled to minute-level, or None if calculation not possible
        """
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Fetch last 2 completed windows
            cursor.execute("""
                SELECT """ + metric + """ as value, window_start
                FROM funnel_training
                WHERE window_end < NOW() AND """ + metric + """ IS NOT NULL
                ORDER BY window_start DESC
                LIMIT 2
            """)
            
            completed_rows = cursor.fetchall()
            
            # Fetch current (in-progress) window from funnel view
            cursor.execute("""
                SELECT """ + metric + """ as value, window_start
                FROM funnel
                WHERE window_end > NOW()
                ORDER BY window_start DESC
                LIMIT 1
            """)
            
            current_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            values = []
            
            # Add completed windows (oldest first)
            if completed_rows:
                values.extend([float(row['value']) for row in reversed(completed_rows)])
            
            # Add current in-progress window (most recent)
            if current_row and current_row['value'] is not None:
                current_value = float(current_row['value'])
                values.append(current_value)
                latest_value = current_value
            elif values:
                latest_value = values[-1]
            else:
                return None
            
            # Calculate EMA with very high alpha for fast response
            alpha = 0.95  # 95% weight on latest
            ema = values[0]
            for value in values[1:]:
                ema = alpha * value + (1 - alpha) * ema
            
            # Use 90% current value + 10% EMA for maximum responsiveness
            blended = 0.90 * latest_value + 0.10 * ema
            
            # Scale to minute-level (20-second windows → multiply by 3)
            WINDOW_SECONDS = 20
            MINUTE_MULTIPLIER = 60.0 / WINDOW_SECONDS
            prediction = blended * MINUTE_MULTIPLIER
            
            print(f"[EMA-Current] {metric}: values={values}, latest={latest_value:.1f}, "
                  f"ema={ema:.1f}, blended={blended:.1f} → prediction={prediction:.1f}")
            
            return prediction
            
        except Exception as e:
            print(f"EMA prediction failed for {metric}: {e}")
            return None
    
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
