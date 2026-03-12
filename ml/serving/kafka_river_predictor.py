"""Kafka-based River predictor for online learning from funnel topic."""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from ..online.kafka_learner import get_kafka_learner, KafkaOnlineLearner


@dataclass
class KafkaRiverPrediction:
    """A prediction result from Kafka-based River online learning."""
    value: float
    confidence: float
    model_type: str
    samples_learned: int
    kafka_connected: bool


class KafkaRiverModelPredictor:
    """
    Predictor using Kafka-based River online learning models.
    
    This predictor consumes directly from the funnel Kafka topic,
    maintaining a local feature history to compute lag features.
    """
    
    METRICS = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
    
    # Database connection settings (for feature fetching fallback)
    DB_HOST = os.getenv('RISINGWAVE_HOST', 'localhost')
    DB_PORT = int(os.getenv('RISINGWAVE_PORT', '4566'))
    DB_NAME = os.getenv('RISINGWAVE_DB', 'dev')
    DB_USER = os.getenv('RISINGWAVE_USER', 'root')
    DB_PASSWORD = os.getenv('RISINGWAVE_PASSWORD', '')
    
    def __init__(self, auto_start: bool = True):
        """
        Initialize the Kafka-based River predictor.
        
        Args:
            auto_start: Whether to start the online learner automatically
        """
        self.learner = get_kafka_learner(auto_start=auto_start)
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
    
    def predict(self, metric: str, features: Optional[Dict[str, float]] = None) -> Optional[KafkaRiverPrediction]:
        """
        Make prediction using Kafka-based River online learning model.
        
        Args:
            metric: Metric to predict
            features: Optional feature dict, if None fetches from RisingWave
            
        Returns:
            KafkaRiverPrediction or None if model not ready
        """
        if features is None:
            features = self._build_current_features()
        
        # Get prediction from online learner
        value = self.learner.predict(metric, features)
        
        # Get model stats
        stats = self.learner.models.get_stats(metric)
        samples_learned = stats.get('learning_count', 0)
        
        # Debug logging
        import os
        if os.environ.get('ML_DEBUG'):
            print(f"[DEBUG] {metric}: value={value}, samples={samples_learned}, features={features}")
        
        # Use fallback only if not enough samples
        if value is None or samples_learned < 20:
            # Model not ready yet - fall back to moving average
            ma_value = self._fetch_moving_average(metric)
            if ma_value is not None:
                reason = "insufficient_samples" if samples_learned < 20 else "no_prediction"
                print(f"[MA Fallback] {metric}: reason={reason}, samples={samples_learned}, using MA={ma_value:.2f}")
                return KafkaRiverPrediction(
                    value=round(ma_value, 2),
                    confidence=0.6,
                    model_type="moving_average_fallback",
                    samples_learned=samples_learned,
                    kafka_connected=self.learner.streamer.is_connected()
                )
            return None
        
        # Check for negative predictions and fall back to MA if needed
        if value < 0:
            ma_value = self._fetch_moving_average(metric)
            if ma_value is not None:
                print(f"[MA Fallback] {metric}: reason=negative_prediction (value={value:.2f}), using MA={ma_value:.2f}")
                return KafkaRiverPrediction(
                    value=round(ma_value, 2),
                    confidence=0.6,
                    model_type="moving_average_fallback",
                    samples_learned=samples_learned,
                    kafka_connected=self.learner.streamer.is_connected()
                )
            # If no MA available, clip to 0 as last resort
            print(f"[River] {metric}: clipped {value:.2f} to 0, samples={samples_learned}")
            value = 0
        
        print(f"[River] {metric}: value={value:.2f}, samples={samples_learned}")
        
        # Calculate confidence based on samples learned
        confidence = min(0.95, 0.5 + (samples_learned / 100))
        
        return KafkaRiverPrediction(
            value=round(value, 2),
            confidence=round(confidence, 3),
            model_type="river_kafka_online",
            samples_learned=samples_learned,
            kafka_connected=self.learner.streamer.is_connected()
        )
    
    def predict_all(self, features: Optional[Dict[str, float]] = None) -> Dict[str, KafkaRiverPrediction]:
        """
        Make predictions for all metrics.
        
        Args:
            features: Optional feature dict
            
        Returns:
            Dict mapping metric names to KafkaRiverPredictions
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
        """Build feature vector using current time and latest values."""
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
            
            cursor.execute("""
                SELECT viewers, carters, purchasers
                FROM funnel_training
                WHERE window_end < NOW()
                ORDER BY window_start DESC
                LIMIT 9
            """)
            
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            # Extract lag values
            if len(rows) >= 1:
                defaults['viewers_lag_1'] = float(rows[0]['viewers'])
                defaults['carters_lag_1'] = float(rows[0]['carters'])
                defaults['purchasers_lag_1'] = float(rows[0]['purchasers'])
            
            if len(rows) >= 4:
                defaults['viewers_lag_2'] = float(rows[3]['viewers'])
                defaults['carters_lag_2'] = float(rows[3]['carters'])
                defaults['purchasers_lag_2'] = float(rows[3]['purchasers'])
            
            if len(rows) >= 7:
                defaults['viewers_lag_3'] = float(rows[6]['viewers'])
                defaults['carters_lag_3'] = float(rows[6]['carters'])
                defaults['purchasers_lag_3'] = float(rows[6]['purchasers'])
            
            # Calculate TPS
            if len(rows) >= 2:
                tps_v = abs(float(rows[0]['viewers']) - float(rows[1]['viewers'])) / 20.0
                tps_c = abs(float(rows[0]['carters']) - float(rows[1]['carters'])) / 20.0
                defaults['tps_viewers'] = tps_v
                defaults['tps_carters'] = tps_c
                
                if len(rows) >= 4:
                    change_1 = abs(float(rows[0]['viewers']) - float(rows[1]['viewers']))
                    change_2 = abs(float(rows[1]['viewers']) - float(rows[2]['viewers']))
                    change_3 = abs(float(rows[2]['viewers']) - float(rows[3]['viewers']))
                    defaults['tps_smoothed'] = (change_1 + change_2 + change_3) / 60.0
                else:
                    defaults['tps_smoothed'] = tps_v
            
            return defaults
            
        except Exception as e:
            print(f"Warning: Failed to fetch lag values: {e}")
            return defaults
    
    def _fetch_moving_average(self, metric: str, window: int = 3) -> Optional[float]:
        """Fetch responsive moving average from RisingWave as fallback.
        
        Uses a weighted average favoring recent data for faster response to TPS changes.
        """
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Fetch last 3 records with exponential weighting (more weight to recent)
            cursor.execute(f"""
                SELECT {metric}
                FROM funnel_training
                WHERE window_end < NOW()
                ORDER BY window_start DESC
                LIMIT %s
            """, (window,))
            
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not rows:
                return None
            
            # Heavy weight on most recent value for responsiveness
            values = [float(row[0]) for row in rows if row[0] is not None]
            if not values:
                return None
            
            if len(values) == 1:
                return values[0]
            elif len(values) == 2:
                return values[0] * 0.9 + values[1] * 0.1
            else:
                # Weight: 0.85, 0.10, 0.05 - heavily favor latest value
                return values[0] * 0.85 + values[1] * 0.10 + values[2] * 0.05
            
        except Exception as e:
            print(f"Error fetching moving average: {e}")
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """Get the status of online learning."""
        return self.learner.get_stats()
    
    def is_ready(self) -> bool:
        """Check if online models are ready for predictions."""
        return self.learner.is_ready(min_samples=5)
    
    def is_kafka_connected(self) -> bool:
        """Check if connected to Kafka."""
        return self.learner.streamer.is_connected()


# Singleton
_global_kafka_river_predictor: Optional[KafkaRiverModelPredictor] = None


def get_kafka_river_predictor(auto_start: bool = True) -> KafkaRiverModelPredictor:
    """Get or create the global Kafka River predictor instance."""
    global _global_kafka_river_predictor
    if _global_kafka_river_predictor is None:
        _global_kafka_river_predictor = KafkaRiverModelPredictor(auto_start=auto_start)
    return _global_kafka_river_predictor