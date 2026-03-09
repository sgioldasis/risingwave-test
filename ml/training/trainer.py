"""ML model training logic."""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score

from .data_fetcher import DataFetcher
from .model_registry import ModelRegistry, ModelVersion


@dataclass
class TrainingResult:
    """Result of training a single metric model."""
    metric: str
    version: str
    model_type: str
    mae: float
    r2: float
    training_samples: int


@dataclass
class TrainingResults:
    """Results of training all models."""
    models: Dict[str, TrainingResult]
    trained_at: str
    total_models: int
    successful_models: int


class ModelTrainer:
    """Handles model training and evaluation."""
    
    # Metrics to predict
    METRICS = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
    
    # Feature columns used for training
    # TPS (transactions per second) features help the model understand current event rate
    FEATURE_COLUMNS = [
        'hour_of_day', 'minute_of_hour', 'day_of_week',
        'viewers_lag_1', 'viewers_lag_2', 'viewers_lag_3',
        'carters_lag_1', 'carters_lag_2', 'carters_lag_3',
        'purchasers_lag_1', 'purchasers_lag_2', 'purchasers_lag_3',
        'tps_viewers', 'tps_carters', 'tps_smoothed'
    ]
    
    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.registry = ModelRegistry()
    
    def train_all_metrics(
        self,
        minutes_back: int = 1,
        skip_if_no_new_data: bool = False,
        last_training_timestamp: Optional[str] = None
    ) -> TrainingResults:
        """
        Train models for all metrics.
        
        Args:
            minutes_back: Number of minutes of data to use
            skip_if_no_new_data: If True, skip training if no new data since last_training_timestamp
            last_training_timestamp: Timestamp to check against if skip_if_no_new_data is True
            
        Returns:
            TrainingResults with all training outcomes
        """
        from datetime import datetime, timezone
        
        # Check if we should skip due to no new data
        if skip_if_no_new_data and last_training_timestamp:
            if not self.data_fetcher.has_new_data(last_training_timestamp):
                print("No new data since last training, skipping...")
                return TrainingResults(
                    models={},
                    trained_at=datetime.now(timezone.utc).isoformat(),
                    total_models=0,
                    successful_models=0
                )
        
        # Fetch training data
        print(f"Fetching training data (last {minutes_back} minutes)...")
        data = self.data_fetcher.fetch_training_data(minutes_back=minutes_back)
        
        if len(data) < 2:
            print(f"Insufficient data for training: {len(data)} records (need at least 2)")
            return TrainingResults(
                models={},
                trained_at=datetime.now(timezone.utc).isoformat(),
                total_models=0,
                successful_models=0
            )
        
        print(f"Training with {len(data)} records")
        
        # Train a model for each metric
        results = {}
        trained_count = 0
        
        for metric in self.METRICS:
            result = self._train_metric(data, metric)
            if result:
                results[metric] = result
                trained_count += 1
        
        trained_at = datetime.now(timezone.utc).isoformat()
        print(f"Successfully trained {trained_count}/{len(self.METRICS)} models")
        
        return TrainingResults(
            models=results,
            trained_at=trained_at,
            total_models=len(self.METRICS),
            successful_models=trained_count
        )
    
    def _train_metric(
        self,
        data: List[Dict[str, Any]],
        metric: str
    ) -> Optional[TrainingResult]:
        """
        Train a model for a specific metric.
        
        Args:
            data: Training data
            metric: Metric to predict
            
        Returns:
            TrainingResult or None if training failed
        """
        try:
            # Use Moving Average for all predictions - simpler and more responsive to TPS changes
            print(f"Training {metric} with Moving Average (data-based)")
            return self._train_moving_average(data, metric)
            
        except Exception as e:
            print(f"Error training {metric}: {e}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def _train_moving_average(
        self,
        data: List[Dict[str, Any]],
        metric: str
    ) -> Optional[TrainingResult]:
        """
        Create a moving average 'model' for when insufficient data exists.
        This saves the average value to the model registry so it can be served.
        
        Args:
            data: Training data
            metric: Metric to create moving average for
            
        Returns:
            TrainingResult for the moving average model
        """
        try:
            # Calculate the moving average from the data
            values = [float(r[metric]) for r in data if r.get(metric) is not None]
            if not values:
                print(f"No values for {metric} moving average")
                return None
            
            moving_avg = sum(values) / len(values)
            
            # Save as a "model" to the registry
            # We use a simple dictionary to store the moving average
            version = self.registry.save_model(
                model={"type": "moving_average", "value": moving_avg, "window_size": len(values)},
                scaler=None,
                metric=metric,
                training_metrics={"mae": 0, "r2": 0, "samples": len(values)},
                model_type="MovingAverage",
                feature_columns=[],
                training_samples=len(values)
            )
            
            print(f"Created moving average for {metric}: {moving_avg:.2f} (n={len(values)}), version={version.version}")
            
            return TrainingResult(
                metric=metric,
                version=version.version,
                model_type="MovingAverage",
                mae=0.0,
                r2=0.0,
                training_samples=len(values)
            )
            
        except Exception as e:
            print(f"Error creating moving average for {metric}: {e}")
            return None
    
    def _prepare_features(
        self,
        data: List[Dict[str, Any]],
        metric: str
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Prepare feature matrix and target vector for training.
        
        Args:
            data: List of training records
            metric: Metric to predict
            
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
            # Include TPS features to help model understand current event rate
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
                float(record.get('purchasers_lag_3', 0)),
                float(record.get('tps_viewers', 0)),
                float(record.get('tps_carters', 0)),
                float(record.get('tps_smoothed', 0))
            ]
            
            X.append(features)
            y.append(float(record[metric]))
        
        return np.array(X), np.array(y)
