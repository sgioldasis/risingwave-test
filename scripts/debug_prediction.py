#!/usr/bin/env python3
"""Debug prediction to see what features are being used."""

import logging
import sys

sys.path.insert(0, 'modern-dashboard/backend')

from services.ml_predictor import FunnelMLPredictor

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

predictor = FunnelMLPredictor()

# Fetch training data
data = predictor.fetch_training_data(minutes_back=10)
logger.info(f"Fetched {len(data)} records for training/prediction")

if len(data) > 0:
    logger.info("\n=== Last 3 training records ===")
    for i, record in enumerate(data[-3:]):
        logger.info(f"Record {i}: window_start={record['window_start']}, viewers={record['viewers']}, hour={record['hour_of_day']}, minute={record['minute_of_hour']}")
    
    logger.info("\n=== Building prediction features ===")
    features = predictor._build_prediction_features(data)
    logger.info(f"Feature vector: {features}")
    
    logger.info("\n=== Feature breakdown ===")
    logger.info(f"  hour_of_day: {features[0][0]}")
    logger.info(f"  minute_of_hour: {features[0][1]}")
    logger.info(f"  day_of_week: {features[0][2]}")
    logger.info(f"  viewers_lag_1 (current): {features[0][3]}")
    logger.info(f"  viewers_lag_2: {features[0][4]}")
    logger.info(f"  viewers_lag_3: {features[0][5]}")
    logger.info(f"  carters_lag_1 (current): {features[0][6]}")
    logger.info(f"  purchasers_lag_1 (current): {features[0][9]}")
    
    # Train models
    logger.info("\n=== Training models ===")
    predictor.train_models()
    
    # Make prediction
    logger.info("\n=== Making prediction ===")
    result = predictor.predict_next('viewers')
    if result:
        logger.info(f"Predicted viewers: {result.predicted_value}")
        logger.info(f"Confidence: {result.confidence}")
        logger.info(f"Timestamp: {result.timestamp}")
