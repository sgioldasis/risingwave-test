"""
ML Prediction Service

This module provides ML prediction capabilities using scikit-learn.
It includes:
- FunnelMLPredictor class for scikit-learn based predictions

The FunnelMLPredictor:
- Fetches training data from RisingWave funnel_training view
- Trains RandomForestRegressor and LinearRegression models
- Uses StandardScaler for feature scaling
- Includes lag features (previous 1, 2, 3 values)
- Predicts next-minute values for viewers, carters, purchasers
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta, timezone
import logging
import os
from dataclasses import dataclass

import numpy as np
import psycopg2
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Window size for training (in minutes)
TRAINING_WINDOW = 5  # Reduced for faster demo setup
HORIZON = 1  # Predict 1 minute ahead


@dataclass
class PredictionResult:
    """Result of a single prediction."""
    metric: str
    predicted_value: float
    confidence: float
    model_type: str
    timestamp: str


class FunnelMLPredictor:
    """
    ML Predictor for funnel metrics using scikit-learn.

    This class provides:
    - Data fetching from RisingWave funnel_training view
    - Model training using RandomForestRegressor and LinearRegression
    - Feature scaling with StandardScaler
    - Lag features (previous 1, 2, 3 values)
    - Next-minute predictions for viewers, carters, purchasers
    """

    # Database connection settings
    DB_HOST = os.getenv('RISINGWAVE_HOST', 'localhost')
    DB_PORT = int(os.getenv('RISINGWAVE_PORT', '4566'))
    DB_NAME = os.getenv('RISINGWAVE_DB', 'dev')
    DB_USER = os.getenv('RISINGWAVE_USER', 'root')
    DB_PASSWORD = os.getenv('RISINGWAVE_PASSWORD', '')

    # Metrics to predict
    metrics = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']

    def __init__(self):
        """Initialize the predictor with empty models and scalers."""
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.prediction_history: List[Dict[str, Any]] = []
        self.last_training_data: Optional[List] = None
        self.training_timestamp: Optional[str] = None

        # Feature columns used for training
        self.feature_columns = [
            'hour_of_day', 'minute_of_hour', 'day_of_week',
            'viewers_lag_1', 'viewers_lag_2', 'viewers_lag_3',
            'carters_lag_1', 'carters_lag_2', 'carters_lag_3',
            'purchasers_lag_1', 'purchasers_lag_2', 'purchasers_lag_3'
        ]

        logger.info("FunnelMLPredictor initialized")

    def _get_db_connection(self):
        """Create and return a database connection to RisingWave."""
        return psycopg2.connect(
            host=self.DB_HOST,
            port=self.DB_PORT,
            dbname=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD
        )

    def fetch_training_data(self, minutes_back: int = 5) -> List[Dict[str, Any]]:
        """
        Fetch training data from RisingWave funnel_training view.

        Args:
            minutes_back: Number of minutes of historical data to fetch

        Returns:
            List of dictionaries containing training data
        """
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()

            # Query to fetch recent training data with lag features
            # Only use completed windows (window_end < NOW()) to avoid training on partial data
            query = """
                SELECT
                    window_start,
                    window_end,
                    viewers,
                    carters,
                    purchasers,
                    view_to_cart_rate,
                    cart_to_buy_rate,
                    hour_of_day,
                    minute_of_hour,
                    day_of_week,
                    COALESCE(viewers_lag_1, 0) as viewers_lag_1,
                    COALESCE(viewers_lag_2, 0) as viewers_lag_2,
                    COALESCE(viewers_lag_3, 0) as viewers_lag_3,
                    COALESCE(carters_lag_1, 0) as carters_lag_1,
                    COALESCE(carters_lag_2, 0) as carters_lag_2,
                    COALESCE(carters_lag_3, 0) as carters_lag_3,
                    COALESCE(purchasers_lag_1, 0) as purchasers_lag_1,
                    COALESCE(purchasers_lag_2, 0) as purchasers_lag_2,
                    COALESCE(purchasers_lag_3, 0) as purchasers_lag_3
                FROM (
                    SELECT *
                    FROM funnel_training
                    WHERE window_end < NOW()
                    ORDER BY window_start DESC
                    LIMIT %s
                ) sub
                ORDER BY window_start ASC
            """

            # Fetch more records to account for the lag features needing 3 prior minutes
            cursor.execute(query, (minutes_back + 5,))

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Fetch all rows and convert to dictionaries
            rows = cursor.fetchall()
            data = []
            for row in rows:
                record = dict(zip(columns, row))
                # Convert datetime objects to strings for JSON serialization
                if record.get('window_start'):
                    record['window_start'] = record['window_start'].isoformat()
                if record.get('window_end'):
                    record['window_end'] = record['window_end'].isoformat()
                data.append(record)

            cursor.close()
            conn.close()

            logger.info(f"Fetched {len(data)} training records from RisingWave (last {minutes_back} minutes)")
            if len(data) > 0:
                logger.info(f"First record: window_start={data[0].get('window_start')}, viewers={data[0].get('viewers')}")
                logger.info(f"Last record: window_start={data[-1].get('window_start')}, viewers={data[-1].get('viewers')}")
            else:
                logger.warning("No training data found - check if funnel_training view has data with completed windows (window_end < NOW())")
            return data

        except Exception as e:
            logger.error(f"Failed to fetch training data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def _prepare_features(self, data: List[Dict[str, Any]], metric: str) -> Tuple[np.ndarray, np.ndarray]:
        """
        Prepare feature matrix and target vector for training.

        Args:
            data: List of training records
            metric: Metric to predict (viewers, carters, purchasers, etc.)

        Returns:
            Tuple of (X, y) where X is feature matrix and y is target vector
        """
        X = []
        y = []

        for record in data:
            # Skip if target value is None
            if record.get(metric) is None:
                continue

            # Build feature vector
            features = [
                float(record.get('hour_of_day', 0)),
                float(record.get('minute_of_hour', 0)),
                float(record.get('day_of_week', 0)),
                float(record.get('viewers_lag_1', 0)),
                float(record.get('viewers_lag_2', 0)),
                float(record.get('viewers_lag_3', 0)),
                float(record.get('carters_lag_1', 0)),
                float(record.get('carters_lag_2', 0)),
                float(record.get('carters_lag_3', 0)),
                float(record.get('purchasers_lag_1', 0)),
                float(record.get('purchasers_lag_2', 0)),
                float(record.get('purchasers_lag_3', 0))
            ]

            X.append(features)
            y.append(float(record[metric]))

        return np.array(X), np.array(y)

    def train_models(self) -> bool:
        """
        Train ML models on recent funnel data.

        Uses RandomForestRegressor for primary predictions and
        LinearRegression as a fallback/comparison.

        Returns:
            True if training was successful, False otherwise
        """
        try:
            # Fetch recent training data (last few minutes)
            logger.info("Starting model training...")
            data = self.fetch_training_data(minutes_back=5)

            if len(data) < 2:
                logger.warning(f"Insufficient data for training: {len(data)} records (need at least 2)")
                return False

            logger.info(f"Training with {len(data)} records")

            self.last_training_data = data
            self.training_timestamp = datetime.now(timezone.utc).isoformat()

            # Train a model for each metric
            trained_count = 0
            for metric in self.metrics:
                X, y = self._prepare_features(data, metric)

                if len(X) < 2:
                    logger.warning(f"Insufficient samples for {metric}: {len(X)} (need at least 2)")
                    continue

                logger.info(f"Training {metric} with {len(X)} samples")

                # Initialize and fit scaler
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)

                # Train RandomForest model (primary)
                rf_model = RandomForestRegressor(
                    n_estimators=50,
                    max_depth=5,
                    min_samples_split=2,
                    random_state=42
                )
                rf_model.fit(X_scaled, y)

                # Train LinearRegression model (fallback)
                lr_model = LinearRegression()
                lr_model.fit(X_scaled, y)

                # Store models and scaler
                self.models[metric] = {
                    'random_forest': rf_model,
                    'linear': lr_model,
                    'primary': 'random_forest'
                }
                self.scalers[metric] = scaler

                # Calculate training metrics for validation
                y_pred = rf_model.predict(X_scaled)
                mae = mean_absolute_error(y, y_pred)
                r2 = r2_score(y, y_pred)

                logger.info(f"Trained model for {metric}: MAE={mae:.2f}, R²={r2:.3f}")
                trained_count += 1

            logger.info(f"Successfully trained {trained_count}/{len(self.metrics)} models")
            return trained_count > 0

        except Exception as e:
            logger.error(f"Error training models: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _build_prediction_features(self, data: List[Dict[str, Any]]) -> np.ndarray:
        """
        Build feature vector for prediction based on most recent data.

        Args:
            data: Recent training records

        Returns:
            Feature vector for next-minute prediction
        """
        if not data:
            return np.zeros((1, 12))

        # Use the most recent record as base
        latest = data[-1]

        # Get current time for time features - use UTC for consistency
        now = datetime.now(timezone.utc)

        features = [
            float(now.hour),  # hour_of_day
            float(now.minute),  # minute_of_hour
            float(now.weekday()),  # day_of_week
            # Lag features: use current values as lag_1, shift previous lags
            float(latest.get('viewers', 0)),  # viewers_lag_1 (current becomes lag)
            float(latest.get('viewers_lag_1', 0)),  # viewers_lag_2
            float(latest.get('viewers_lag_2', 0)),  # viewers_lag_3
            float(latest.get('carters', 0)),  # carters_lag_1
            float(latest.get('carters_lag_1', 0)),  # carters_lag_2
            float(latest.get('carters_lag_2', 0)),  # carters_lag_3
            float(latest.get('purchasers', 0)),  # purchasers_lag_1
            float(latest.get('purchasers_lag_1', 0)),  # purchasers_lag_2
            float(latest.get('purchasers_lag_2', 0))  # purchasers_lag_3
        ]

        return np.array([features])

    def predict_next(self, metric: str) -> Optional[PredictionResult]:
        """
        Predict the next-minute value for a specific metric.

        Args:
            metric: Metric to predict (viewers, carters, purchasers, etc.)

        Returns:
            PredictionResult with predicted value and confidence, or None if prediction fails
        """
        if metric not in self.models:
            logger.warning(f"No model trained for metric: {metric}")
            return None

        try:
            # Fetch recent data for feature building
            data = self.last_training_data or self.fetch_training_data(minutes_back=10)

            if not data:
                logger.warning("No data available for prediction")
                return None

            # Build features for prediction
            X_pred = self._build_prediction_features(data)

            # Scale features
            scaler = self.scalers[metric]
            X_pred_scaled = scaler.transform(X_pred)

            # Get model
            model_info = self.models[metric]
            model = model_info[model_info['primary']]

            # Make prediction
            prediction = model.predict(X_pred_scaled)[0]

            # Calculate confidence based on model's feature importances (for RF)
            # or R² score proxy for linear models
            confidence = 0.7  # Default confidence
            if hasattr(model, 'feature_importances_'):
                # RandomForest - use prediction variance from trees
                predictions = [tree.predict(X_pred_scaled)[0] for tree in model.estimators_]
                variance = np.var(predictions)
                confidence = max(0.5, min(0.95, 1.0 - (variance / (prediction + 1))))

            # Store prediction in history - use UTC for consistency
            utc_now = datetime.now(timezone.utc)
            result = PredictionResult(
                metric=metric,
                predicted_value=round(float(prediction), 2),
                confidence=round(float(confidence), 3),
                model_type=model_info['primary'],
                timestamp=(utc_now + timedelta(minutes=1)).isoformat()
            )

            self._store_prediction(result)

            return result

        except Exception as e:
            logger.error(f"Error predicting {metric}: {e}")
            return None

    def predict_all(self) -> Dict[str, PredictionResult]:
        """
        Predict next-minute values for all metrics.

        Returns:
            Dictionary mapping metric names to PredictionResult objects
        """
        results = {}

        for metric in self.metrics:
            result = self.predict_next(metric)
            if result:
                results[metric] = result

        return results

    def _store_prediction(self, result: PredictionResult):
        """Store prediction in history for later analysis."""
        self.prediction_history.append({
            'metric': result.metric,
            'predicted_value': result.predicted_value,
            'confidence': result.confidence,
            'model_type': result.model_type,
            'timestamp': result.timestamp,
            'predicted_at': datetime.now(timezone.utc).isoformat()
        })

        # Keep only last 1000 predictions
        if len(self.prediction_history) > 1000:
            self.prediction_history = self.prediction_history[-1000:]

    def get_prediction_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get historical predictions.

        Args:
            limit: Maximum number of predictions to return

        Returns:
            List of historical prediction records
        """
        return self.prediction_history[-limit:]

    def get_model_status(self) -> Dict[str, Any]:
        """
        Get status of all trained models.

        Returns:
            Dictionary with model status information
        """
        return {
            'models_trained': len(self.models),
            'is_trained': len(self.models) > 0 and self.training_timestamp is not None,
            'metrics': self.metrics,
            'trained_metrics': list(self.models.keys()),
            'training_timestamp': self.training_timestamp,
            'prediction_count': len(self.prediction_history)
        }


# =============================================================================
# Convenience functions for API usage
# =============================================================================

def get_next_minute_predictions() -> Dict[str, Any]:
    """
    Get predictions for the next minute from FunnelMLPredictor.

    Returns:
        Dictionary containing predictions for viewers, carters, purchasers,
        and derived conversion rates.
    """
    predictor = FunnelMLPredictor()

    # Ensure models are trained
    if not predictor.models:
        predictor.train_models()

    predictions = {
        "timestamp": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        "predicted_at": datetime.now(timezone.utc).isoformat(),
        "viewers": None,
        "carters": None,
        "purchasers": None,
        "view_to_cart_rate": None,
        "cart_to_buy_rate": None
    }

    try:
        results = predictor.predict_all()

        for metric, result in results.items():
            predictions[metric] = result.predicted_value

        return predictions

    except Exception as e:
        logger.error(f"Failed to get predictions: {e}")
        return {
            "error": str(e),
            **predictions
        }
