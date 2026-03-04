#!/usr/bin/env python3
"""Test ML training directly to see what's happening."""

import sys
sys.path.insert(0, 'modern-dashboard/backend')

from services.ml_predictor import FunnelMLPredictor
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

predictor = FunnelMLPredictor()

print("Testing training data fetch...")
data = predictor.fetch_training_data(minutes_back=5)
print(f"Fetched {len(data)} records")

if len(data) >= 2:
    print(f"First record: {data[0]}")
    print(f"Last record: {data[-1]}")
    
    print("\nTesting model training...")
    success = predictor.train_models()
    print(f"Training success: {success}")
    
    if success:
        print(f"Models trained: {list(predictor.models.keys())}")
        status = predictor.get_model_status()
        print(f"Status: {status}")
    else:
        print("Training failed - check logs above")
else:
    print("Not enough data for training (need at least 2 records)")
