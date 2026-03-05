"""Model registry for storing and versioning ML models in MinIO."""

import json
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import boto3
from botocore.exceptions import ClientError


class ModelVersion:
    """Represents a model version."""
    
    def __init__(self, metric: str, version: str, trained_at: str, metadata: Dict[str, Any]):
        self.metric = metric
        self.version = version
        self.trained_at = trained_at
        self.metadata = metadata


class ModelRegistry:
    """Manages model artifact storage to MinIO S3-compatible storage."""
    
    BUCKET_NAME = "ml-models"
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9301'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'hummockadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'hummockadmin'),
            region_name='us-east-1'
        )
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the ml-models bucket exists."""
        try:
            self.s3_client.head_bucket(Bucket=self.BUCKET_NAME)
        except ClientError:
            # Bucket doesn't exist, create it
            try:
                self.s3_client.create_bucket(Bucket=self.BUCKET_NAME)
                print(f"Created bucket: {self.BUCKET_NAME}")
            except ClientError as e:
                print(f"Error creating bucket: {e}")
    
    def _generate_version(self) -> str:
        """Generate version string from current timestamp: vYYYYMMDD_HHMMSS."""
        now = datetime.now(timezone.utc)
        return f"v{now.strftime('%Y%m%d_%H%M%S')}"
    
    def save_model(
        self,
        model: Any,
        scaler: Any,
        metric: str,
        training_metrics: Dict[str, float],
        model_type: str = "RandomForestRegressor",
        feature_columns: Optional[List[str]] = None,
        training_samples: int = 0
    ) -> ModelVersion:
        """
        Save model artifact to MinIO and update manifest.
        
        Args:
            model: Trained sklearn model
            scaler: Fitted StandardScaler
            metric: Metric name (viewers, carters, etc.)
            training_metrics: Dict with mae, r2, etc.
            model_type: Type of model
            feature_columns: List of feature column names
            training_samples: Number of training samples
            
        Returns:
            ModelVersion with version info
        """
        version = self._generate_version()
        now = datetime.now(timezone.utc).isoformat()
        
        # Handle moving average models (saved as JSON)
        if model_type == "MovingAverage":
            model_key = f"{metric}/{version}_model.json"
            scaler_key = None
            # Upload model as JSON
            self.s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=model_key,
                Body=json.dumps(model, indent=2)
            )
        else:
            # Serialize model and scaler as pickle for sklearn models
            model_bytes = pickle.dumps(model)
            scaler_bytes = pickle.dumps(scaler)
            
            model_key = f"{metric}/{version}.pkl"
            scaler_key = f"{metric}/{version}_scaler.pkl"
            
            # Upload model
            self.s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=model_key,
                Body=model_bytes
            )
            
            # Upload scaler
            self.s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=scaler_key,
                Body=scaler_bytes
            )
        
        # Create and upload metadata
        metadata_key = f"{metric}/{version}_metadata.json"
        metadata = {
            "metric": metric,
            "version": version,
            "trained_at": now,
            "model_type": model_type,
            "file_path": model_key,
            "scaler_path": scaler_key,
            "metrics": training_metrics,
            "training_samples": training_samples,
            "feature_columns": feature_columns or []
        }
        
        self.s3_client.put_object(
            Bucket=self.BUCKET_NAME,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2)
        )
        
        # Update global manifest
        self._update_manifest(metric, version, now)
        
        print(f"Saved model for {metric} version {version}")
        return ModelVersion(metric, version, now, metadata)
    
    def _update_manifest(self, metric: str, version: str, trained_at: str):
        """Update the global manifest with the latest version."""
        manifest_key = "manifest.json"
        
        # Try to load existing manifest
        try:
            response = self.s3_client.get_object(Bucket=self.BUCKET_NAME, Key=manifest_key)
            manifest = json.loads(response['Body'].read())
        except ClientError:
            # Manifest doesn't exist yet
            manifest = {
                "last_updated": trained_at,
                "latest_versions": {}
            }
        
        # Update manifest
        manifest["last_updated"] = trained_at
        manifest["latest_versions"][metric] = version
        
        # Save manifest
        self.s3_client.put_object(
            Bucket=self.BUCKET_NAME,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2)
        )
    
    def get_latest_model(self, metric: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve latest model for a metric from MinIO.
        
        Returns:
            Dict with model, scaler, and metadata, or None if not found
        """
        try:
            # Get manifest
            response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key="manifest.json"
            )
            manifest = json.loads(response['Body'].read())
            
            version = manifest["latest_versions"].get(metric)
            if not version:
                return None
            
            # Get metadata
            metadata_response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key=f"{metric}/{version}_metadata.json"
            )
            metadata = json.loads(metadata_response['Body'].read())
            
            # Get model
            model_response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key=metadata["file_path"]
            )
            model = pickle.loads(model_response['Body'].read())
            
            # Get scaler
            scaler_response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key=metadata["scaler_path"]
            )
            scaler = pickle.loads(scaler_response['Body'].read())
            
            return {
                "model": model,
                "scaler": scaler,
                "metadata": metadata
            }
            
        except ClientError as e:
            print(f"Error loading model for {metric}: {e}")
            return None
    
    def list_versions(self, metric: str) -> List[ModelVersion]:
        """List available model versions for a metric."""
        versions = []
        
        try:
            prefix = f"{metric}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.BUCKET_NAME,
                Prefix=prefix
            )
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('_metadata.json'):
                    # Fetch metadata
                    metadata_response = self.s3_client.get_object(
                        Bucket=self.BUCKET_NAME,
                        Key=key
                    )
                    metadata = json.loads(metadata_response['Body'].read())
                    
                    versions.append(ModelVersion(
                        metric=metadata['metric'],
                        version=metadata['version'],
                        trained_at=metadata['trained_at'],
                        metadata=metadata
                    ))
            
            # Sort by trained_at descending
            versions.sort(key=lambda v: v.trained_at, reverse=True)
            
        except ClientError as e:
            print(f"Error listing versions for {metric}: {e}")
        
        return versions
    
    def get_manifest(self) -> Optional[Dict[str, Any]]:
        """Get the global manifest."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key="manifest.json"
            )
            return json.loads(response['Body'].read())
        except ClientError:
            return None
