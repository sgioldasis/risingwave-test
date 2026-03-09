#!/usr/bin/env python3
"""Debug script to trace model loading issues."""

import json
import os
import sys

# Add ml directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import boto3
from botocore.exceptions import ClientError

# MinIO configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9301')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'hummockadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'hummockadmin')
BUCKET_NAME = "ml-models"

def debug_model_loading():
    print(f"=== Debugging Model Loading ===\n")

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

        print("Creating ModelLoader instance...")
        loader = ModelLoader()
        print(f"S3 client endpoint: {loader.s3_client._endpoint}")
        print()

        # Get manifest directly
        print("=== Getting Manifest ===")
        manifest = loader._get_manifest()
        if manifest:
            print(f"Manifest found:")
            print(f"  Last updated: {manifest.get('last_updated')}")
            print(f"  Latest versions: {manifest.get('latest_versions')}")
        else:
            print("❌ Manifest is None!")
        print()

        # Try to load models
        print("=== Loading Models ===")
        models = loader.load_latest_models()
        print(f"Loaded {len(models)} models: {list(models.keys())}")
        print()

        # Try loading each model individually
        if manifest:
            for metric, version in manifest.get('latest_versions', {}).items():
                print(f"\n--- Loading {metric} v{version} ---")
                try:
                    model_data = loader._load_model(metric, version)
                    if model_data:
                        print(f"  ✅ Loaded successfully")
                        print(f"     Model type: {model_data.get('metadata', {}).get('model_type')}")
                        print(f"     Version: {model_data.get('version')}")
                    else:
                        print(f"  ❌ _load_model returned None")
                except Exception as e:
                    print(f"  ❌ Error: {e}")
                    import traceback
                    traceback.print_exc()

        # Check for updates
        print(f"\n=== Checking for Updates ===")
        has_update = loader.check_for_updates()
        print(f"Updates available: {has_update}")
        print(f"Cached ETag: {loader._cached_manifest_etag}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_model_loading()
