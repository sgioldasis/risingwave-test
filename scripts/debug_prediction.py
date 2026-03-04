#!/usr/bin/env python3
"""Debug prediction to see what features are being used."""

import sys
sys.path.insert(0, 'modern-dashboard/backend')

from services.ml_predictor import FunnelMLPredictor
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

predictor = FunnelMLPredictor()

# Fetch training data
data = predictor.fetch_training_data(minutes_back=10)
print(f"Fetched {len(data)} records for training/prediction")

if len(data) > 0:
    print("\n=== Last 3 training records ===")
    for i, record in enumerate(data[-3:]):
        print(f"Record {i}: window_start={record['window_start']}, viewers={record['viewers']}, hour={record['hour_of_day']}, minute={record['minute_of_hour']}")
    
    print("\n=== Building prediction features ===")
    features = predictor._build_prediction_features(data)
    print(f"Feature vector: {features}")
    
    print("\n=== Feature breakdown ===")
    print(f"  hour_of_day: {features[0][0]}")
    print(f"  minute_of_hour: {features[0][1]}")
    print(f"  day_of_week: {features[0][2]}")
    print(f"  viewers_lag_1 (current): {features[0][3]}")
    print(f"  viewers_lag_2: {features[0][4]}")
    print(f"  viewers_lag_3: {features[0][5]}")
    print(f"  carters_lag_1 (current): {features[0][6]}")
    print(f"  purchasers_lag_1 (current): {features[0][9]}")
    
    # Train models
    print("\n=== Training models ===")
    predictor.train_models()
    
    # Make prediction
    print("\n=== Making prediction ===")
    result = predictor.predict_next('viewers')
    if result:
        print(f"Predicted viewers: {result.predicted_value}")
        print(f"Confidence: {result.confidence}")
        print(f"Timestamp: {result.timestamp}")
