#!/usr/bin/env python3
"""Clear ML online learning checkpoints from MinIO."""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from botocore.exceptions import ClientError


def clear_checkpoints():
    """Clear all checkpoints from MinIO."""
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9301'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'hummockadmin'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'hummockadmin'),
        region_name='us-east-1'
    )
    
    bucket_name = "ml-models-online"
    
    try:
        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' not in response:
            print("No checkpoints found.")
            return
        
        objects = response['Contents']
        
        if not objects:
            print("No checkpoints found.")
            return
        
        # Delete all objects
        delete_keys = [{'Key': obj['Key']} for obj in objects]
        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': delete_keys}
        )
        
        print(f"Cleared {len(objects)} checkpoint objects from MinIO.")
        
        # List what was deleted
        for obj in objects:
            print(f"  - Deleted: {obj['Key']}")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print("Checkpoint bucket doesn't exist yet.")
        else:
            print(f"Error clearing checkpoints: {e}")


if __name__ == "__main__":
    print("Clearing ML online learning checkpoints...")
    clear_checkpoints()
    print("\nDone! Restart ML Serving to start with fresh models.")
