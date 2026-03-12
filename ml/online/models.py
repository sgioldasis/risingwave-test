"""River model definitions for online learning."""

from typing import Dict, Any, Optional
import river
from river import linear_model, tree, preprocessing, metrics, optim, stats, utils


class RiverModelManager:
    """Manages River models for each metric with incremental learning."""
    
    # Metrics to predict
    METRICS = ['viewers', 'carters', 'purchasers', 'view_to_cart_rate', 'cart_to_buy_rate']
    
    # Feature columns (same as batch training for compatibility)
    FEATURE_COLUMNS = [
        'hour_of_day', 'minute_of_hour', 'day_of_week',
        'viewers_lag_1', 'viewers_lag_2', 'viewers_lag_3',
        'carters_lag_1', 'carters_lag_2', 'carters_lag_3',
        'purchasers_lag_1', 'purchasers_lag_2', 'purchasers_lag_3',
        'tps_viewers', 'tps_carters', 'tps_smoothed'
    ]
    
    def __init__(self):
        self.models: Dict[str, Any] = {}
        self.metrics: Dict[str, metrics.Metric] = {}
        self.learning_counts: Dict[str, int] = {}
        self._init_models()
    
    def _init_models(self):
        """Initialize River models for each metric."""
        for metric in self.METRICS:
            self.models[metric] = self._create_model(metric)
            self.metrics[metric] = metrics.MAE() + metrics.R2()
            self.learning_counts[metric] = 0
    
    def _create_model(self, metric: str):
        """
        Create appropriate River model for the metric.
        
        Uses different algorithms based on metric characteristics:
        - Count metrics: Use PairedRegression (returns training data mean initially)
        - Rate metrics: HoeffdingTree for non-linear patterns
        """
        from river import dummy
        
        if metric in ['viewers', 'carters', 'purchasers']:
            # For count metrics, use Rolling mean with 3-period window (~1 min)
            # This adapts quickly to TPS changes while staying stable and non-negative
            model = dummy.StatisticRegressor(statistic=utils.Rolling(stats.Mean(), window_size=3))
        else:
            # Use HoeffdingTreeRegressor for rate metrics (handles non-linear relationships)
            model = tree.HoeffdingTreeRegressor(
                grace_period=10,
                max_depth=10,
                delta=1e-7,
                tau=0.05
            )
        
        return model
    
    def learn_one(self, metric: str, features: Dict[str, float], target: float) -> None:
        """
        Learn from a single example incrementally.
        
        Args:
            metric: Metric name to update
            features: Feature dictionary
            target: Target value to learn
        """
        if metric not in self.models:
            return
        
        # Extract features in order
        x = {col: float(features.get(col, 0)) for col in self.FEATURE_COLUMNS}
        y = float(target)
        
        # Update model
        self.models[metric].learn_one(x, y)
        
        # Update metrics
        prediction = self.models[metric].predict_one(x)
        self.metrics[metric].update(y, prediction)
        
        # Increment learning count
        self.learning_counts[metric] += 1
    
    def predict_one(self, metric: str, features: Dict[str, float]) -> Optional[float]:
        """
        Make prediction for a single example.
        
        Args:
            metric: Metric name to predict
            features: Feature dictionary
            
        Returns:
            Predicted value or None if model not available
        """
        if metric not in self.models:
            return None
        
        # Need minimum samples before predicting
        if self.learning_counts[metric] < 2:
            return None
        
        x = {col: float(features.get(col, 0)) for col in self.FEATURE_COLUMNS}
        return self.models[metric].predict_one(x)
    
    def get_stats(self, metric: str) -> Dict[str, Any]:
        """Get learning statistics for a metric."""
        if metric not in self.models:
            return {}
        
        metric_obj = self.metrics[metric]
        return {
            'learning_count': self.learning_counts[metric],
            'mae': metric_obj['MAE'].get() if 'MAE' in metric_obj else None,
            'r2': metric_obj['R2'].get() if 'R2' in metric_obj else None,
        }
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all metrics."""
        return {metric: self.get_stats(metric) for metric in self.METRICS}
    
    def is_ready(self, metric: str, min_samples: int = 5) -> bool:
        """Check if a model has enough training samples to make predictions."""
        return self.learning_counts.get(metric, 0) >= min_samples


def get_default_models() -> RiverModelManager:
    """Get a fresh instance of RiverModelManager with default models."""
    return RiverModelManager()