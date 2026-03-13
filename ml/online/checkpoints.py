"""Checkpoint management for River models using MinIO."""

import json
import logging
import os
import pickle
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages periodic checkpoints of River models to MinIO."""
    
    BUCKET_NAME = "ml-models-online"
    
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
        """Ensure the online models bucket exists."""
        try:
            self.s3_client.head_bucket(Bucket=self.BUCKET_NAME)
        except ClientError:
            try:
                self.s3_client.create_bucket(Bucket=self.BUCKET_NAME)
                logger.info(f"Created bucket: {self.BUCKET_NAME}")
            except ClientError as e:
                logger.error(f"Error creating bucket: {e}")
    
    def save_checkpoint(self, model_manager, stats: Dict[str, Any]) -> str:
        """
        Save a checkpoint of all models.
        
        Args:
            model_manager: RiverModelManager instance
            stats: Dictionary of learning statistics
            
        Returns:
            Checkpoint version string
        """
        version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        # Save each model
        for metric in model_manager.METRICS:
            model = model_manager.models[metric]
            metric_stats = model_manager.get_stats(metric)
            
            # Serialize model with pickle
            model_bytes = pickle.dumps(model)
            model_key = f"{metric}/{version}.pkl"
            
            self.s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=model_key,
                Body=model_bytes
            )
            
            # Save metadata
            metadata = {
                'metric': metric,
                'version': version,
                'saved_at': datetime.now(timezone.utc).isoformat(),
                'learning_count': metric_stats.get('learning_count', 0),
                'mae': metric_stats.get('mae'),
                'r2': metric_stats.get('r2'),
            }
            
            self.s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=f"{metric}/{version}_metadata.json",
                Body=json.dumps(metadata, indent=2)
            )
        
        # Save global manifest
        manifest = {
            'version': version,
            'saved_at': datetime.now(timezone.utc).isoformat(),
            'metrics': {m: model_manager.learning_counts[m] for m in model_manager.METRICS},
            'global_stats': stats
        }
        
        self.s3_client.put_object(
            Bucket=self.BUCKET_NAME,
            Key=f"manifest_{version}.json",
            Body=json.dumps(manifest, indent=2)
        )
        
        # Update latest manifest
        self.s3_client.put_object(
            Bucket=self.BUCKET_NAME,
            Key="manifest_latest.json",
            Body=json.dumps(manifest, indent=2)
        )
        
        logger.info(f"Checkpoint saved: {version}")
        return version
    
    def load_checkpoint(self, model_manager, version: Optional[str] = None) -> bool:
        """
        Load models from a checkpoint.
        
        Args:
            model_manager: RiverModelManager instance to populate
            version: Specific version to load, or None for latest
            
        Returns:
            True if successful
        """
        try:
            if version is None:
                # Load latest manifest
                try:
                    response = self.s3_client.get_object(
                        Bucket=self.BUCKET_NAME,
                        Key="manifest_latest.json"
                    )
                    manifest = json.loads(response['Body'].read())
                    version = manifest['version']
                except ClientError:
                    logger.info("No checkpoint manifest found, starting fresh")
                    return False
            
            # Load each model
            for metric in model_manager.METRICS:
                model_key = f"{metric}/{version}.pkl"
                
                try:
                    response = self.s3_client.get_object(
                        Bucket=self.BUCKET_NAME,
                        Key=model_key
                    )
                    model = pickle.loads(response['Body'].read())
                    
                    # Check if loaded model type matches expected type
                    expected_model = model_manager._create_model(metric)
                    loaded_type = type(model).__name__
                    expected_type = type(expected_model).__name__
                    
                    if loaded_type != expected_type:
                        logger.warning(f"[Checkpoint Skip] {metric}: checkpoint has {loaded_type}, "
                              f"expected {expected_type}. Starting fresh.")
                        continue
                    
                    # Model type matches, use the checkpoint
                    model_manager.models[metric] = model
                    
                    # Load metadata to restore counts
                    metadata_response = self.s3_client.get_object(
                        Bucket=self.BUCKET_NAME,
                        Key=f"{metric}/{version}_metadata.json"
                    )
                    metadata = json.loads(metadata_response['Body'].read())
                    model_manager.learning_counts[metric] = metadata.get('learning_count', 0)
                    
                    logger.info(f"Loaded {metric} model from checkpoint {version}")
                    
                except ClientError:
                    logger.warning(f"No checkpoint found for {metric} version {version}")
                    continue
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return False
    
    def list_checkpoints(self) -> list[Dict[str, Any]]:
        """List available checkpoints."""
        checkpoints = []
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.BUCKET_NAME,
                Prefix="manifest_"
            )
            
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key == "manifest_latest.json":
                    continue
                
                # Extract version from manifest_YYYYMMDD_HHMMSS.json
                version = key.replace("manifest_", "").replace(".json", "")
                checkpoints.append({
                    'version': version,
                    'last_modified': obj['LastModified'].isoformat()
                })
            
            # Sort by version (timestamp)
            checkpoints.sort(key=lambda x: x['version'], reverse=True)
            
        except ClientError as e:
            logger.error(f"Error listing checkpoints: {e}")
        
        return checkpoints
    
    def get_latest_manifest(self) -> Optional[Dict[str, Any]]:
        """Get the latest checkpoint manifest."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.BUCKET_NAME,
                Key="manifest_latest.json"
            )
            return json.loads(response['Body'].read())
        except ClientError:
            return None