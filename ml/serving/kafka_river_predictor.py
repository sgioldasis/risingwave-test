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


# Training source for the online learner: 'starrocks' polls the RW-free
# funnel windows StarRocks maintains via Routine Load + async MV (works
# while RisingWave is paused); 'kafka' consumes the RW-fed funnel topic.
ML_LEARNING_SOURCE = os.getenv('ML_LEARNING_SOURCE', 'starrocks')

# StarRocks connection for the recency-first prediction path (same envs
# the StarRocksDataStreamer trains through).
SR_HOST = os.getenv('STARROCKS_HOST', 'localhost')
SR_PORT = int(os.getenv('STARROCKS_PORT', '9030'))
SR_USER = os.getenv('STARROCKS_USER', 'root')
SR_PASSWORD = os.getenv('STARROCKS_PASSWORD', '')
SR_DB = os.getenv('STARROCKS_DB', 'sr_local_db_sr_local_db')

# Recency-first prediction (2026-09-09 behavior, ported 2026-11-14 to the
# StarRocks path): weighted average over the last N COMPLETED 20s windows,
# pro-rated to a minute. The original fix used 100% newest window; the
# 2026-11-14 tuning made the window count per metric -- wobble is a
# sample-size problem, not a weights problem: viewers see ~20 events per
# 20s slice (3 slices suffice), purchasers ~2 (even 3 slices is a coin
# flip, e.g. predicted 9 vs 16 actual on one thin slice). A stage's
# reaction window is sized to how fast its own volume can reveal a real
# change; geometric decay keeps the newest window dominant everywhere.
# The original lived in river_predictor.py reading RW's funnel view; this
# reads StarRocks's 20s view over the raw Routine-Load tables instead --
# same shape, no RW.
RECENT_TABLE = os.getenv('ML_RECENT_WINDOWS_TABLE', 'funnel_summary_direct_20s')
RECENT_DECAY = 0.7  # weight of each older window relative to the one after it
RECENT_WINDOWS_PER_METRIC = {
    'viewers': 3,            # ~20 events/slice -> 60s reaction is stable
    'carters': 5,            # ~5 events/slice
    'purchasers': 9,         # ~2 events/slice -> needs ~3 minutes of slices
    'view_to_cart_rate': 5,  # ratios inherit the small-count noise
    'cart_to_buy_rate': 5,
}
# A window is finalized once its window_end has passed plus ingest slack:
# Routine Load batches every max_batch_interval=5s, so a just-closed
# window can still be missing its last events for a few seconds.
FINALIZE_SLACK_SECONDS = 5


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
        if ML_LEARNING_SOURCE == 'starrocks':
            from ..online.starrocks_streamer import StarRocksDataStreamer
            streamer = StarRocksDataStreamer()
        else:
            streamer = None  # KafkaDataStreamer default
        self.learner = get_kafka_learner(auto_start=auto_start, streamer=streamer)
        self._db_connection_params = {
            'host': self.DB_HOST,
            'port': self.DB_PORT,
            'database': self.DB_NAME,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            # Bound the fallback paths (lag/MA fetch): RW paused must not
            # hang /predict -- the primary feature path no longer touches
            # RW at all (see _build_current_features).
            'connect_timeout': 2,
        }
    
    def _get_db_connection(self):
        """Create and return a database connection."""
        return psycopg2.connect(**self._db_connection_params)
    
    def _fetch_recent_prediction(self, metric: str) -> Optional[float]:
        """Predict the next minute from finalized 20s windows only.

        Port of the 2026-09-09 recency fix (see RECENT_WINDOWS_PER_METRIC):
        newest-weighted average over the last N COMPLETED windows (N sized
        to the stage's event volume), counts pro-rated x3 to per-minute,
        ratios averaged as-is. Reads StarRocks's live 20s view -- no RW
        anywhere in the path.
        """
        n_windows = RECENT_WINDOWS_PER_METRIC.get(metric, 3)
        try:
            import pymysql
            conn = pymysql.connect(
                host=SR_HOST, port=SR_PORT, user=SR_USER,
                password=SR_PASSWORD, database=SR_DB, connect_timeout=2,
            )
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT {metric}
                        FROM {RECENT_TABLE}
                        WHERE {metric} IS NOT NULL
                          AND window_end <= CURRENT_TIMESTAMP() - INTERVAL {FINALIZE_SLACK_SECONDS} SECOND
                        ORDER BY window_start DESC
                        LIMIT {n_windows}
                    """)
                    rows = cursor.fetchall()
            finally:
                conn.close()
        except Exception as e:
            print(f"Warning: recent-window fetch failed: {e}")
            return None

        values = [float(r[0]) for r in rows]  # newest first
        if not values:
            return None

        weights = [RECENT_DECAY ** i for i in range(len(values))]
        weighted_avg = sum(v * w for v, w in zip(values, weights)) / sum(weights)

        if metric in ('view_to_cart_rate', 'cart_to_buy_rate'):
            # Ratios don't accumulate across windows like counts do --
            # the weighted average is already the right shape.
            return weighted_avg

        # Counts are per-20s-window; project to per-minute.
        return weighted_avg * 3

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

        # Recency-first (2026-09-09 behavior restored 2026-11-14): the
        # newest completed window reacts to a rate change within one
        # 20s cycle, while the River regression needs several windows to
        # follow a level shift. Recent windows win whenever they exist;
        # the River model takes over only when they don't (StarRocks
        # unreachable / empty).
        recent_value = self._fetch_recent_prediction(metric)
        if recent_value is not None:
            samples_learned = self.learner.models.get_stats(metric).get('learning_count', 0)
            print(f"[Recent] {metric}: value={recent_value:.2f} "
                  f"(river fallback available, samples={samples_learned})")
            return KafkaRiverPrediction(
                value=round(recent_value, 2),
                confidence=0.9,
                model_type=f"recent_window_{ML_LEARNING_SOURCE}",
                samples_learned=samples_learned,
                kafka_connected=self.learner.streamer.is_connected()
            )

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
            model_type=f"river_{ML_LEARNING_SOURCE}_online",
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
        """Build feature vector for the next window.

        Prefers the learner's own training history (source-agnostic, no
        network call, matches the training distribution exactly) and only
        falls back to the RisingWave lag fetch when no history exists yet
        (fresh start, first poll not processed). 2026-11-14: with RW down
        the RW fetch returned all-zero lags and predictions collapsed.
        """
        features = self.learner.build_current_features()
        if features:
            return features

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