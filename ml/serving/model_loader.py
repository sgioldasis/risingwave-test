"""Model loading from MinIO with hot-reload support."""

import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional, Any

import boto3
from botocore.exceptions import ClientError


class ModelLoader:
    """Loads models from MinIO with hot-reloading."""
    
    BUCKET_NAME = "ml-models"
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9301'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'hummockadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'hummockadmin'),
            region_name='us-east-1'
        )
        self._cached_manifest_etag: Optional[str] = None
    
    def load_latest_models(self) -> Dict[str, Dict[str, Any]]:
        """
        Load latest models from MinIO.
        
        Returns:
            Dict mapping metric names to model data
        """
        models = {}
        
        try:
            # Get manifest and update ETag cache to prevent redundant reloads
            try:
                response = self.s3_client.head_object(
                    Bucket=self.BUCKET_NAME,
                    Key="manifest.json"
                )
                self._cached_manifest_etag = response.get('ETag')
            except ClientError:
                pass
            
            manifest = self._get_manifest()
            if not manifest:
                print("No manifest found")
                return models
            
            latest_versions = manifest.get("latest_versions", {})
            
            # Load each model
            for metric, version in latest_versions.items():
                model_data = self._load_model(metric, version)
                if model_data:
                    models[metric] = model_data
                    print(f"Loaded model for {metric} version {version}")
            
            return models
            
        except Exception as e:
            print(f"Error loading models: {e}")
            import traceback
            print(traceback.format_exc())
            return models
    
    def _get_manifest(self) -> Optional[Dict[str, Any]]:
        """Get the global manifest from MinIO."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key="manifest.json"
            )
            return json.loads(response['Body'].read())
        except ClientError:
            return None
    
    def _load_model(self, metric: str, version: str) -> Optional[Dict[str, Any]]:
        """Load a specific model version."""
        try:
            # Get metadata
            metadata_response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key=f"{metric}/{version}_metadata.json"
            )
            metadata = json.loads(metadata_response['Body'].read())
            
            # Handle moving average models (stored as JSON, not pickle)
            model_type = metadata.get("model_type", "Unknown")
            if model_type == "MovingAverage":
                model_response = self.s3_client.get_object(
                    Bucket=self.BUCKET_NAME,
                    Key=f"{metric}/{version}_model.json"
                )
                model_data = json.loads(model_response['Body'].read())
                return {
                    "model": model_data,
                    "scaler": None,
                    "version": version,
                    "metadata": metadata
                }
            
            # Get model (pickle)
            import pickle
            model_response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key=metadata["file_path"]
            )
            model = pickle.loads(model_response['Body'].read())
            
            # Get scaler (pickle)
            scaler_response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key=metadata["scaler_path"]
            )
            scaler = pickle.loads(scaler_response['Body'].read())
            
            return {
                "model": model,
                "scaler": scaler,
                "version": version,
                "metadata": metadata,
                "loaded_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            print(f"Error loading model {metric} version {version}: {e}")
            return None
    
    def check_for_updates(self) -> bool:
        """
        Check if new models are available by comparing manifest ETag.
        
        Returns:
            True if updates are available
        """
        try:
            # Get current manifest ETag
            response = self.s3_client.head_object(
                Bucket=self.BUCKET_NAME,
                Key="manifest.json"
            )
            current_etag = response.get('ETag')
            
            if self._cached_manifest_etag is None:
                # No cached ETag means either:
                # 1. We checked before and there was no manifest (now there is - NEW MODELS!)
                # 2. This is the first check after startup
                # In both cases, we should trigger a reload to load/verify models.
                self._cached_manifest_etag = current_etag
                return True
            
            if current_etag != self._cached_manifest_etag:
                # Manifest has changed
                self._cached_manifest_etag = current_etag
                return True
            
            return False
            
        except ClientError:
            # Manifest doesn't exist
            self._cached_manifest_etag = None
            return False
    
    def get_manifest(self) -> Optional[Dict[str, Any]]:
        """Get the current manifest."""
        return self._get_manifest()
    
    def has_batch_models(self) -> bool:
        """
        Check if batch models are available in MinIO.
        
        Returns:
            True if manifest.json exists and has valid model entries
        """
        try:
            manifest = self._get_manifest()
            if not manifest:
                return False
            
            latest_versions = manifest.get("latest_versions", {})
            # Check if we have at least one model
            return len(latest_versions) > 0
            
        except Exception:
            return False
