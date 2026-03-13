#!/usr/bin/env python3
"""Test ML training directly to see what's happening."""

import logging
import sys

sys.path.insert(0, 'modern-dashboard/backend')

from services.ml_predictor import FunnelMLPredictor

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

predictor = FunnelMLPredictor()

logger.info("Testing training data fetch...")
data = predictor.fetch_training_data(minutes_back=5)
logger.info(f"Fetched {len(data)} records")

if len(data) >= 2:
    logger.info(f"First record: {data[0]}")
    logger.info(f"Last record: {data[-1]}")
    
    logger.info("\nTesting model training...")
    success = predictor.train_models()
    logger.info(f"Training success: {success}")
    
    if success:
        logger.info(f"Models trained: {list(predictor.models.keys())}")
        status = predictor.get_model_status()
        logger.info(f"Status: {status}")
    else:
        logger.error("Training failed - check logs above")
else:
    logger.warning("Not enough data for training (need at least 2 records)")
