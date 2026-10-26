"""The Kafka topic asset for a reverse-ETL sync, in a demo-free home: the released
ReverseEtlCdfSync component builds it when create_topic is true, and the demo's
build_demo_defs imports the same builder.

The asset name is a caller argument, not a config field: the demo config owns its
infrastructure names explicitly (ReverseEtlSyncConfig.topic_asset), while the released
component derives it from the label (dagster_names) -- the builder stays out of the
naming policy either way.

A Debezium-style change topic wants compaction: the broker default (delete) risks
retention dropping events a lagging consumer has not read, while cleanup.policy=compact
keeps the latest event per key. It is the documented default; topic_config overrides it
per key.
"""

from confluent_kafka.admin import NewTopic
from dagster import AssetExecutionContext, asset

from . import reverse_etl_connections as connections
from .kafka_topics_setup import _DEFAULT_PARTITIONS, _DEFAULT_REPLICATION
from .reverse_etl_config import CdfSyncConfig


def build_topic_asset(
    cfg: CdfSyncConfig,
    asset_name: str,
    *,
    topic_partitions: int | None = None,
    topic_config: dict | None = None,
):
    """Build the asset that creates the sync's Kafka topic if it does not exist.

    `topic_partitions` defaults to the module default in kafka_topics_setup;
    `topic_config` entries override the defaults per key, so
    `{"cleanup.policy": "delete"}` restores the broker default."""
    config = {"cleanup.policy": "compact", **(topic_config or {})}

    @asset(
        name=asset_name,
        group_name=cfg.group_name,
        description=f"Create the {cfg.kafka_topic} Kafka topic if it does not exist.",
    )
    def kafka_topic(context: AssetExecutionContext):
        admin = connections.kafka().admin_client()
        if cfg.kafka_topic in admin.list_topics(timeout=15).topics:
            context.log.info(f"Topic {cfg.kafka_topic} already exists")
            return
        new_topic = NewTopic(
            cfg.kafka_topic,
            num_partitions=_DEFAULT_PARTITIONS if topic_partitions is None else topic_partitions,
            replication_factor=_DEFAULT_REPLICATION,
            config=config,
        )
        admin.create_topics([new_topic])[cfg.kafka_topic].result()
        context.log.info(f"Created topic {cfg.kafka_topic}")

    return kafka_topic
