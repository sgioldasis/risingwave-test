"""Turn a ReverseEtlSyncConfig into Dagster definitions: the four assets (source
table setup, CDF -> Kafka, RisingWave table, Debezium JDBC sink), a setup job that
materializes them in order, a reset job, and a sink health check with its job and schedule. Optionally also an asset that creates
the sync's Kafka topic (config.create_topic_asset).

    defs = Definitions.merge(base_defs, build_demo_defs(config))
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

from .kafka_topics_setup import _DEFAULT_PARTITIONS, _DEFAULT_REPLICATION
from .reverse_etl_cdf_setup import build_sync_asset
from .reverse_etl_demo_assets import build_seed_asset, build_table_setup_asset
from .reverse_etl_duckdb_target import build_duckdb_target_asset
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_demo_hooks import DemoSyncHooks
from .reverse_etl_debezium_sink import build_debezium_sink_asset
from .reverse_etl_health import build_sink_health_check, build_sink_health_job_and_schedule
from .reverse_etl_change import build_change_asset, build_change_job
from .reverse_etl_reset import build_reset_job
from .reverse_etl_resources import KafkaResource
from .reverse_etl_risingwave_setup import build_risingwave_table_asset


def _build_topic_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.topic_asset,
        group_name=cfg.group_name,
        description=f"Create the {cfg.kafka_topic} Kafka topic if it does not exist.",
    )
    def kafka_topic(context: AssetExecutionContext, kafka: KafkaResource):
        admin = kafka.admin_client()
        if cfg.kafka_topic in admin.list_topics(timeout=15).topics:
            context.log.info(f"Topic {cfg.kafka_topic} already exists")
            return
        new_topic = NewTopic(
            cfg.kafka_topic, num_partitions=_DEFAULT_PARTITIONS, replication_factor=_DEFAULT_REPLICATION
        )
        admin.create_topics([new_topic])[cfg.kafka_topic].result()
        context.log.info(f"Created topic {cfg.kafka_topic}")

    return kafka_topic


def build_demo_defs(cfg: ReverseEtlSyncConfig) -> Definitions:
    """The demo's builder: the sync asset plus everything the demo needs around it (table setup,
    topic, the selected targets, seed, make-change, the jobs and the health check). The reusable
    component builds only the sync asset itself (decoupling plan step 6). The targets are consumers
    of the sync's topic, selected by cfg.targets (plan section 12, option A): "risingwave" builds
    the RisingWave table, "postgres" the Debezium JDBC sink with its health check, "duckdb" a local
    file mirror."""
    assets = [
        build_table_setup_asset(cfg),
        # The demo's hooks reach RisingWave, Kafka Connect and the Postgres TARGET through
        # context.resources, so those are declared as required (postgres is needed by the hooks
        # whatever the state backend is -- the state's own postgres need is the core's, per
        # cfg.state_backend); the setup and topic assets are wired in as upstream deps.
        build_sync_asset(
            cfg,
            hooks=DemoSyncHooks(),
            extra_resource_keys=("risingwave", "http", "postgres"),
            deps=[AssetKey(cfg.table_setup_asset), AssetKey(cfg.topic_asset)],
        ),
        *([build_risingwave_table_asset(cfg)] if "risingwave" in cfg.targets else []),
        *([build_debezium_sink_asset(cfg)] if "postgres" in cfg.targets else []),
        *([build_duckdb_target_asset(cfg)] if "duckdb" in cfg.targets else []),
    ]
    if cfg.create_topic_asset:
        assets.append(_build_topic_asset(cfg))
    if cfg.seed_statements:
        assets.append(build_seed_asset(cfg))
    # The demo-change helper builds its SQL from source_columns and the demo key `id`; a sync on an
    # existing table (no source_columns) has no such shape, so it gets no make_change asset or job.
    change_asset = build_change_asset(cfg) if cfg.source_columns else None
    if change_asset is not None:
        assets.append(change_asset)

    # AssetKey selection (not asset objects) so the shared topic asset, which this
    # function does not build, is still part of the job.
    job_assets = [
        cfg.table_setup_asset,
        cfg.topic_asset,
        cfg.sync_asset,
        *([cfg.risingwave_asset] if "risingwave" in cfg.targets else []),
        *([cfg.sink_asset] if "postgres" in cfg.targets else []),
        *([f"{cfg.sync_name}_duckdb_target"] if "duckdb" in cfg.targets else []),
    ]
    if cfg.seed_statements:
        job_assets.append(cfg.seed_asset)
    selection = AssetSelection.assets(*[AssetKey(name) for name in job_assets])
    setup_job = define_asset_job(
        name=cfg.setup_job,
        selection=selection,
        description=(
            f"Create {cfg.sync_name}'s Databricks tables and Kafka topic, run the first sync, then "
            "build the selected targets (RisingWave table, Debezium JDBC sink, DuckDB mirror), and last "
            "seed the demo rows if the sync has seed statements."
        ),
        executor_def=in_process_executor,
    )
    # The health check watches the Debezium JDBC sink's connector and lag: it exists only with the
    # postgres target (plan section 12 -- the health check follows the selected targets).
    has_postgres_target = "postgres" in cfg.targets
    if has_postgres_target:
        health_check = build_sink_health_check(cfg)
        health_job, health_schedule = build_sink_health_job_and_schedule(cfg, health_check)
    return Definitions(
        assets=assets,
        asset_checks=[health_check] if has_postgres_target else [],
        jobs=[
            setup_job,
            build_reset_job(cfg),
            *([build_change_job(cfg, change_asset)] if change_asset is not None else []),
            *([health_job] if has_postgres_target else []),
        ],
        schedules=[health_schedule] if has_postgres_target else [],
    )
