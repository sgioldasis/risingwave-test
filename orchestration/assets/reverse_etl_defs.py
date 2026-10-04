"""Turn a ReverseEtlSyncConfig into Dagster definitions: the four assets (source
table setup, CDF -> Kafka, RisingWave table, Debezium JDBC sink), a setup job that
materializes them in order, and a reset job. Optionally also an asset that creates
the sync's Kafka topic (config.create_topic_asset).

    defs = Definitions.merge(base_defs, build_reverse_etl_defs(config))
"""

from confluent_kafka.admin import NewTopic
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Definitions,
    asset,
    define_asset_job,
    in_process_executor,
)

from .kafka_topics_setup import _DEFAULT_PARTITIONS, _DEFAULT_REPLICATION, _admin_client
from .reverse_etl_cdf_setup import build_seed_asset, build_sync_asset, build_table_setup_asset
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_debezium_sink import build_debezium_sink_asset
from .reverse_etl_reset import build_reset_job
from .reverse_etl_risingwave_setup import build_risingwave_table_asset


def _build_topic_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.topic_asset,
        group_name=cfg.group_name,
        description=f"Create the {cfg.kafka_topic} Kafka topic if it does not exist.",
    )
    def kafka_topic(context: AssetExecutionContext):
        admin = _admin_client()
        if cfg.kafka_topic in admin.list_topics(timeout=15).topics:
            context.log.info(f"Topic {cfg.kafka_topic} already exists")
            return
        new_topic = NewTopic(
            cfg.kafka_topic, num_partitions=_DEFAULT_PARTITIONS, replication_factor=_DEFAULT_REPLICATION
        )
        admin.create_topics([new_topic])[cfg.kafka_topic].result()
        context.log.info(f"Created topic {cfg.kafka_topic}")

    return kafka_topic


def build_reverse_etl_defs(cfg: ReverseEtlSyncConfig) -> Definitions:
    assets = [
        build_table_setup_asset(cfg),
        build_sync_asset(cfg),
        build_risingwave_table_asset(cfg),
        build_debezium_sink_asset(cfg),
    ]
    if cfg.create_topic_asset:
        assets.append(_build_topic_asset(cfg))
    if cfg.seed_statements:
        assets.append(build_seed_asset(cfg))

    # AssetKey selection (not asset objects) so the shared topic asset, which this
    # function does not build, is still part of the job.
    job_assets = [
        cfg.table_setup_asset,
        cfg.topic_asset,
        cfg.sync_asset,
        cfg.risingwave_asset,
        cfg.sink_asset,
    ]
    if cfg.seed_statements:
        job_assets.append(cfg.seed_asset)
    selection = AssetSelection.assets(*[AssetKey(name) for name in job_assets])
    setup_job = define_asset_job(
        name=cfg.setup_job,
        selection=selection,
        description=(
            f"Create {cfg.sync_name}'s Databricks tables and Kafka topic, run the first sync, then "
            "create the RisingWave table and the Debezium JDBC sink, and last seed the demo rows if the "
            "sync has seed statements."
        ),
        executor_def=in_process_executor,
    )
    return Definitions(assets=assets, jobs=[setup_job, build_reset_job(cfg)])
