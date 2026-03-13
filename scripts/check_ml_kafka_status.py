#!/usr/bin/env python3
"""Check ML Kafka Online status and debug predictions."""

import json
import logging
import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_status():
    """Check ML serving status."""
    logger.info("=" * 60)
    logger.info("ML Kafka Online Status Check")
    logger.info("=" * 60)
    
    # Check health
    try:
        r = requests.get("http://localhost:8001/health", timeout=5)
        logger.info(f"\n1. Health Check: {r.status_code}")
        logger.info(json.dumps(r.json(), indent=2))
    except Exception as e:
        logger.error(f"\n1. Health Check FAILED: {e}")
        return
    
    # Check models status
    try:
        r = requests.get("http://localhost:8001/models", timeout=5)
        logger.info(f"\n2. Models Status: {r.status_code}")
        data = r.json()
        logger.info(json.dumps(data, indent=2))
    except Exception as e:
        logger.error(f"\n2. Models Status FAILED: {e}")
    
    # Check predictions
    try:
        r = requests.get("http://localhost:8001/predict", timeout=5)
        logger.info(f"\n3. Predictions: {r.status_code}")
        data = r.json()
        logger.info(json.dumps(data, indent=2))
    except Exception as e:
        logger.error(f"\n3. Predictions FAILED: {e}")
    
    # Check online learning status
    try:
        r = requests.get("http://localhost:8001/online/status", timeout=5)
        logger.info(f"\n4. Online Learning Status: {r.status_code}")
        data = r.json()
        logger.info(json.dumps(data, indent=2))
    except Exception as e:
        logger.error(f"\n4. Online Learning Status FAILED: {e}")
    
    logger.info("\n" + "=" * 60)


if __name__ == "__main__":
    check_status()
