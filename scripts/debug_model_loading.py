#!/usr/bin/env python3
"""Debug script to trace model loading issues."""

import json
import logging
import os
import sys

# Add ml directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import boto3
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9301')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'hummockadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'hummockadmin')
BUCKET_NAME = "ml-models"


def debug_model_loading():
    logger.info(f"=== Debugging Model Loading ===\n")

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )

        # Import and test the ModelLoader directly
        from ml.serving.model_loader import ModelLoader

        logger.info("Creating ModelLoader instance...")
        loader = ModelLoader()
        logger.info(f"S3 client endpoint: {loader.s3_client._endpoint}")
        logger.info("")

        # Get manifest directly
        logger.info("=== Getting Manifest ===")
        manifest = loader._get_manifest()
        if manifest:
            logger.info(f"Manifest found:")
            logger.info(f"  Last updated: {manifest.get('last_updated')}")
            logger.info(f"  Latest versions: {manifest.get('latest_versions')}")
        else:
            logger.error("❌ Manifest is None!")
        logger.info("")

        # Try to load models
        logger.info("=== Loading Models ===")
        models = loader.load_latest_models()
        logger.info(f"Loaded {len(models)} models: {list(models.keys())}")
        logger.info("")

        # Try loading each model individually
        if manifest:
            for metric, version in manifest.get('latest_versions', {}).items():
                logger.info(f"\n--- Loading {metric} v{version} ---")
                try:
                    model_data = loader._load_model(metric, version)
                    if model_data:
                        logger.info(f"  ✅ Loaded successfully")
                        logger.info(f"     Model type: {model_data.get('metadata', {}).get('model_type')}")
                        logger.info(f"     Version: {model_data.get('version')}")
                    else:
                        logger.error(f"  ❌ _load_model returned None")
                except Exception as e:
                    logger.error(f"  ❌ Error: {e}")
                    import traceback
                    traceback.print_exc()

        # Check for updates
        logger.info(f"\n=== Checking for Updates ===")
        has_update = loader.check_for_updates()
        logger.info(f"Updates available: {has_update}")
        logger.info(f"Cached ETag: {loader._cached_manifest_etag}")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    debug_model_loading()
