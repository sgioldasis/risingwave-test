"""Online learning module using River for incremental ML."""

from .models import RiverModelManager, get_default_models
from .streamer import DataStreamer, record_to_features_target
from .learner import OnlineLearner, get_learner, reset_learner
from .checkpoints import CheckpointManager

# Kafka-based implementations
from .kafka_streamer import KafkaDataStreamer
from .kafka_learner import KafkaOnlineLearner, get_kafka_learner, reset_kafka_learner

__all__ = [
    # Core components
    "RiverModelManager",
    "get_default_models",
    "DataStreamer",
    "record_to_features_target",
    "OnlineLearner",
    "get_learner",
    "reset_learner",
    "CheckpointManager",
    # Kafka-based components
    "KafkaDataStreamer",
    "KafkaOnlineLearner",
    "get_kafka_learner",
    "reset_kafka_learner",
]