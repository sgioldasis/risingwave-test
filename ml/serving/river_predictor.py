"""River-based model predictor for online learning."""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from ..online.learner import get_learner, OnlineLearner
from ..online.streamer import record_to_features_target


@dataclass
class RiverPrediction:
    """A prediction result from River online learning models."""
    value: float
    confidence: float
    model_type: str  # 'river_online'
    samples_learned: int


class RiverModelPredictor:
    """
    Predictor that uses River online learning models.
    
    This predictor works with the OnlineLearner to provide real-time predictions
    from continuously updated models.
    """
    
    METRICS = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
    
    # Database connection settings
    DB_HOST = os.getenv('RISINGWAVE_HOST', 'localhost')
    DB_PORT = int(os.getenv('RISINGWAVE_PORT', '4566'))
    DB_NAME = os.getenv('RISINGWAVE_DB', 'dev')
    DB_USER = os.getenv('RISINGWAVE_USER', 'root')
    DB_PASSWORD = os.getenv('RISINGWAVE_PASSWORD', '')
    
    def __init__(self, auto_start: bool = True):
        """
        Initialize the River predictor.
        
        Args:
            auto_start: Whether to start the online learner automatically
        """
        self.learner = get_learner(auto_start=auto_start)
        self._db_connection_params = {
            'host': self.DB_HOST,
            'port': self.DB_PORT,
            'database': self.DB_NAME,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD
        }
    
    def _get_db_connection(self):
        """Create and return a database connection."""
        return psycopg2.connect(**self._db_connection_params)
    
    def predict(self, metric: str, features: Optional[Dict[str, float]] = None) -> Optional[RiverPrediction]:
        """
        Make prediction using River online learning model.
        
        Args:
            metric: Metric to predict
            features: Optional feature dict, if None fetches from RisingWave
            
        Returns:
            RiverPrediction or None if model not ready
        """
        # Build features if not provided
        if features is None:
            features = self._build_current_features()
        
        # Get model stats for tracking
        model_stats = self.learner.models.get_stats(metric)
        samples_learned = model_stats.get('learning_count', 0)
        
        # Always use Moving Average from RisingWave for predictions (fastest adaptation)
        # River model continues learning in background for potential future use
        ma_value = self._fetch_moving_average(metric)
        if ma_value is not None:
            print(f"[MA Live] {metric}: value={ma_value:.2f}, samples_learned={samples_learned}")
            return RiverPrediction(
                value=round(ma_value, 2),
                confidence=0.75,
                model_type="river_risingwave_online",
                samples_learned=samples_learned
            )
        
        # Fallback to River model only if MA unavailable
        value = self.learner.predict(metric, features)
        if value is not None and value >= 0:
            print(f"[River] {metric}: value={value:.2f}, samples={samples_learned}")
        
        # Calculate confidence based on samples learned
        confidence = min(0.95, 0.5 + (samples_learned / 100))
        
        return RiverPrediction(
            value=round(value, 2),
            confidence=round(confidence, 3),
            model_type="river_risingwave_online",
            samples_learned=samples_learned
        )
    
    def predict_all(self, features: Optional[Dict[str, float]] = None) -> Dict[str, RiverPrediction]:
        """
        Make predictions for all metrics.
        
        Args:
            features: Optional feature dict
            
        Returns:
            Dict mapping metric names to RiverPredictions
        """
        if features is None:
            features = self._build_current_features()
        
        results = {}
        for metric in self.METRICS:
            pred = self.predict(metric, features)
            if pred:
                results[metric] = pred
        return results
    
    def _build_current_features(self) -> Dict[str, float]:
        """Build feature vector using current time and latest values from RisingWave."""
        now = datetime.now(timezone.utc)
        lag_values = self._fetch_latest_lag_values()
        
        return {
            'hour_of_day': float(now.hour),
            'minute_of_hour': float(now.minute),
            'day_of_week': float(now.weekday()),
            **lag_values
        }
    
    def _fetch_latest_lag_values(self) -> Dict[str, float]:
        """Fetch lag features and TPS from RisingWave."""
        defaults = {
            'viewers_lag_1': 0.0, 'viewers_lag_2': 0.0, 'viewers_lag_3': 0.0,
            'carters_lag_1': 0.0, 'carters_lag_2': 0.0, 'carters_lag_3': 0.0,
            'purchasers_lag_1': 0.0, 'purchasers_lag_2': 0.0, 'purchasers_lag_3': 0.0,
            'tps_viewers': 0.0, 'tps_carters': 0.0, 'tps_smoothed': 0.0
        }
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Fetch last 3 windows (3 minutes of 1-minute windows)
            cursor.execute("""
                SELECT viewers, carters, purchasers
                FROM funnel_summary
                WHERE window_end < NOW()
                ORDER BY window_start DESC
                LIMIT 3
            """)
            
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            # With 1-minute windows, consecutive rows are already 1 minute apart
            if len(rows) >= 1:
                defaults['viewers_lag_1'] = float(rows[0]['viewers'])
                defaults['carters_lag_1'] = float(rows[0]['carters'])
                defaults['purchasers_lag_1'] = float(rows[0]['purchasers'])
            
            if len(rows) >= 2:
                defaults['viewers_lag_2'] = float(rows[1]['viewers'])
                defaults['carters_lag_2'] = float(rows[1]['carters'])
                defaults['purchasers_lag_2'] = float(rows[1]['purchasers'])
            
            if len(rows) >= 3:
                defaults['viewers_lag_3'] = float(rows[2]['viewers'])
                defaults['carters_lag_3'] = float(rows[2]['carters'])
                defaults['purchasers_lag_3'] = float(rows[2]['purchasers'])
            
            # Calculate TPS from 1-minute windows
            if len(rows) >= 2:
                tps_v = abs(float(rows[0]['viewers']) - float(rows[1]['viewers'])) / 60.0
                tps_c = abs(float(rows[0]['carters']) - float(rows[1]['carters'])) / 60.0
                defaults['tps_viewers'] = tps_v
                defaults['tps_carters'] = tps_c
                
                if len(rows) >= 3:
                    change_1 = abs(float(rows[0]['viewers']) - float(rows[1]['viewers']))
                    change_2 = abs(float(rows[1]['viewers']) - float(rows[2]['viewers']))
                    defaults['tps_smoothed'] = (change_1 + change_2) / 120.0
                else:
                    defaults['tps_smoothed'] = tps_v
            
            return defaults
            
        except Exception as e:
            print(f"Warning: Failed to fetch lag values: {e}")
            return defaults
    
    def _fetch_moving_average(self, metric: str, history: int = 2) -> Optional[float]:
        """Predict using rate-based extrapolation from the in-progress window.
        
        Core idea: measure the current event rate (events / elapsed seconds)
        from the in-progress window and project to 60 seconds. This adapts
        instantly to TPS changes.
        
        At the start of a new minute (< 5s elapsed), blends with the last
        completed window for stability until enough data accumulates.
        """
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT {metric},
                       EXTRACT(EPOCH FROM (NOW() - window_start)) as elapsed_seconds
                FROM funnel_summary
                WHERE {metric} IS NOT NULL
                ORDER BY window_start DESC
                LIMIT 2
            """)
            
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not rows:
                return None
            
            current_value = float(rows[0][0])
            elapsed = float(rows[0][1]) if rows[0][1] else 60.0
            
            if elapsed >= 60.0:
                # Current row is a completed window — just return it
                return current_value
            
            # In-progress window: extrapolate current rate to full minute
            if elapsed >= 5.0:
                # Enough data to extrapolate reliably
                projected = current_value * (60.0 / elapsed)
            else:
                # Very start of minute: not enough data to extrapolate
                # Use last completed window as baseline
                if len(rows) >= 2:
                    last_completed = float(rows[1][0])
                    if elapsed > 0:
                        projected = current_value * (60.0 / elapsed)
                        # Blend: ramp from 100% historical to 100% projected over 5 seconds
                        blend = elapsed / 5.0
                        projected = projected * blend + last_completed * (1.0 - blend)
                    else:
                        projected = last_completed
                else:
                    projected = current_value
            
            return projected
            
        except Exception as e:
            print(f"Error fetching moving average: {e}")
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """Get the status of online learning."""
        return self.learner.get_stats()
    
    def is_ready(self) -> bool:
        """Check if online models are ready for predictions."""
        return self.learner.is_ready(min_samples=5)


# Singleton instance
_global_river_predictor: Optional[RiverModelPredictor] = None


def get_river_predictor(auto_start: bool = True) -> RiverModelPredictor:
    """Get or create the global River predictor instance."""
    global _global_river_predictor
    if _global_river_predictor is None:
        _global_river_predictor = RiverModelPredictor(auto_start=auto_start)
    return _global_river_predictor