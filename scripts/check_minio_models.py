#!/usr/bin/env python3
"""Debug script to check models in MinIO and verify connectivity."""

import json
import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO configuration - same as in model_registry.py
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9301')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'hummockadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'hummockadmin')
BUCKET_NAME = "ml-models"


def check_minio():
    logger.info(f"=== Checking MinIO Connection ===")
    logger.info(f"Endpoint: {MINIO_ENDPOINT}")
    logger.info(f"Access Key: {MINIO_ACCESS_KEY}")
    logger.info(f"Bucket: {BUCKET_NAME}")
    logger.info("")

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
            logger.info(f"✅ Bucket '{BUCKET_NAME}' exists")
        except ClientError as e:
            logger.error(f"❌ Bucket '{BUCKET_NAME}' does not exist or is not accessible: {e}")
            return

        # List all objects in the bucket
        logger.info(f"\n=== Objects in bucket '{BUCKET_NAME}' ===")
        try:
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
            if 'Contents' not in response:
                logger.info("No objects found in bucket")
                return

            objects = response['Contents']
            logger.info(f"Total objects: {len(objects)}")
            for obj in objects:
                logger.info(f"  - {obj['Key']} ({obj['Size']} bytes)")
        except ClientError as e:
            logger.error(f"❌ Error listing objects: {e}")
            return

        # Check manifest
        logger.info(f"\n=== Manifest Check ===")
        try:
            manifest_response = s3_client.get_object(Bucket=BUCKET_NAME, Key="manifest.json")
            manifest = json.loads(manifest_response['Body'].read())
            logger.info(f"✅ Manifest found")
            logger.info(f"  Last updated: {manifest.get('last_updated', 'N/A')}")
            logger.info(f"  Latest versions:")
            for metric, version in manifest.get('latest_versions', {}).items():
                logger.info(f"    - {metric}: {version}")
        except ClientError as e:
            logger.error(f"❌ Manifest not found: {e}")
            manifest = None

        # Check each metric's metadata
        if manifest:
            logger.info(f"\n=== Model Metadata ===")
            for metric, version in manifest.get('latest_versions', {}).items():
                try:
                    metadata_key = f"{metric}/{version}_metadata.json"
                    metadata_response = s3_client.get_object(Bucket=BUCKET_NAME, Key=metadata_key)
                    metadata = json.loads(metadata_response['Body'].read())
                    logger.info(f"\n  {metric}:")
                    logger.info(f"    Version: {metadata.get('version')}")
                    logger.info(f"    Trained at: {metadata.get('trained_at')}")
                    logger.info(f"    Model type: {metadata.get('model_type')}")
                    logger.info(f"    Model path: {metadata.get('file_path')}")
                    logger.info(f"    Scaler path: {metadata.get('scaler_path')}")
                    logger.info(f"    Metrics: {metadata.get('metrics', {})}")
                except ClientError as e:
                    logger.error(f"  ❌ {metric}: Error loading metadata - {e}")

        # Test ML Serving health
        logger.info(f"\n=== ML Serving Health Check ===")
        import urllib.request
        try:
            with urllib.request.urlopen('http://localhost:8001/health', timeout=5) as response:
                health = json.loads(response.read())
                logger.info(f"✅ ML Serving is healthy")
                logger.info(f"  Models loaded: {health.get('models_loaded', 'N/A')}")
                logger.info(f"  Manifest available: {health.get('manifest_available', 'N/A')}")
        except Exception as e:
            logger.error(f"❌ Cannot connect to ML Serving at localhost:8001: {e}")

        # Test ML Serving models endpoint
        logger.info(f"\n=== ML Serving Models Status ===")
        try:
            with urllib.request.urlopen('http://localhost:8001/models', timeout=5) as response:
                models = json.loads(response.read())
                logger.info(f"✅ Got models status")
                logger.info(f"  Last reload: {models.get('last_reload', 'N/A')}")
                logger.info(f"  Models: {models.get('models', {})}")
        except Exception as e:
            logger.error(f"❌ Cannot get models status: {e}")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    check_minio()
