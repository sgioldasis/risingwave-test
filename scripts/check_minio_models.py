#!/usr/bin/env python3
"""Debug script to check models in MinIO and verify connectivity."""

import json
import os
import sys

import boto3
from botocore.exceptions import ClientError

# MinIO configuration - same as in model_registry.py
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9301')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'hummockadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'hummockadmin')
BUCKET_NAME = "ml-models"

def check_minio():
    print(f"=== Checking MinIO Connection ===")
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Access Key: {MINIO_ACCESS_KEY}")
    print(f"Bucket: {BUCKET_NAME}")
    print()

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
            print(f"✅ Bucket '{BUCKET_NAME}' exists")
        except ClientError as e:
            print(f"❌ Bucket '{BUCKET_NAME}' does not exist or is not accessible: {e}")
            return

        # List all objects in the bucket
        print(f"\n=== Objects in bucket '{BUCKET_NAME}' ===")
        try:
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
            if 'Contents' not in response:
                print("No objects found in bucket")
                return

            objects = response['Contents']
            print(f"Total objects: {len(objects)}")
            for obj in objects:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        except ClientError as e:
            print(f"❌ Error listing objects: {e}")
            return

        # Check manifest
        print(f"\n=== Manifest Check ===")
        try:
            manifest_response = s3_client.get_object(Bucket=BUCKET_NAME, Key="manifest.json")
            manifest = json.loads(manifest_response['Body'].read())
            print(f"✅ Manifest found")
            print(f"  Last updated: {manifest.get('last_updated', 'N/A')}")
            print(f"  Latest versions:")
            for metric, version in manifest.get('latest_versions', {}).items():
                print(f"    - {metric}: {version}")
        except ClientError as e:
            print(f"❌ Manifest not found: {e}")
            manifest = None

        # Check each metric's metadata
        if manifest:
            print(f"\n=== Model Metadata ===")
            for metric, version in manifest.get('latest_versions', {}).items():
                try:
                    metadata_key = f"{metric}/{version}_metadata.json"
                    metadata_response = s3_client.get_object(Bucket=BUCKET_NAME, Key=metadata_key)
                    metadata = json.loads(metadata_response['Body'].read())
                    print(f"\n  {metric}:")
                    print(f"    Version: {metadata.get('version')}")
                    print(f"    Trained at: {metadata.get('trained_at')}")
                    print(f"    Model type: {metadata.get('model_type')}")
                    print(f"    Model path: {metadata.get('file_path')}")
                    print(f"    Scaler path: {metadata.get('scaler_path')}")
                    print(f"    Metrics: {metadata.get('metrics', {})}")
                except ClientError as e:
                    print(f"  ❌ {metric}: Error loading metadata - {e}")

        # Test ML Serving health
        print(f"\n=== ML Serving Health Check ===")
        import urllib.request
        try:
            with urllib.request.urlopen('http://localhost:8001/health', timeout=5) as response:
                health = json.loads(response.read())
                print(f"✅ ML Serving is healthy")
                print(f"  Models loaded: {health.get('models_loaded', 'N/A')}")
                print(f"  Manifest available: {health.get('manifest_available', 'N/A')}")
        except Exception as e:
            print(f"❌ Cannot connect to ML Serving at localhost:8001: {e}")

        # Test ML Serving models endpoint
        print(f"\n=== ML Serving Models Status ===")
        try:
            with urllib.request.urlopen('http://localhost:8001/models', timeout=5) as response:
                models = json.loads(response.read())
                print(f"✅ Got models status")
                print(f"  Last reload: {models.get('last_reload', 'N/A')}")
                print(f"  Models: {models.get('models', {})}")
        except Exception as e:
            print(f"❌ Cannot get models status: {e}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_minio()
