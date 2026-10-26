"""Turn a ReverseEtlSyncConfig into Dagster definitions: the four assets (source
table setup, CDF -> Kafka, RisingWave table, Debezium JDBC sink), a setup job that
materializes them in order, a reset job, and a sink health check with its job and schedule. Optionally also an asset that creates
the sync's Kafka topic (config.create_topic_asset).

    defs = Definitions.merge(base_defs, build_demo_defs(config))
"""

from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    define_asset_job,
    in_process_executor,
)

from .reverse_etl_cdf_setup import build_sync_asset
from .reverse_etl_demo_assets import build_seed_asset, build_table_setup_asset
from .reverse_etl_duckdb_target import build_duckdb_target_asset
from .reverse_etl_config import (
    ReverseEtlSyncConfig,
    ReverseEtlTargetsConfig,
    duckdb_target_config,
    postgres_target_config,
    risingwave_target_config,
)
from .reverse_etl_demo_hooks import DemoSyncHooks
from .reverse_etl_debezium_sink import build_debezium_sink_asset
from .reverse_etl_health import build_sink_health_check, build_sink_health_job_and_schedule
from .reverse_etl_change import build_change_asset, build_change_job
from .reverse_etl_reset import build_reset_job
from .reverse_etl_risingwave_setup import build_risingwave_table_asset
from .reverse_etl_topic import build_topic_asset


def build_target_defs(cfg: ReverseEtlTargetsConfig, source_fqn: str = "") -> Definitions:
    """The downstream side of the sync/targets seam (decoupling plan section 13): the selected
    target assets plus the sink health check with its job and schedule (D19: the check watches
    the connector and the lag, so it lives on the targets side). Takes only the targets config --
    no field of it names the source -- plus the one composition input `source_fqn`, which the
    RisingWave builder requires: its CREATE reads the source's columns and types once, the same
    target-side schema care the demo hooks do inside the sync run.

    The demo bridge (release design step 4): each target builds from its slim config, composed
    here from the merged one — the label strips the minted prefix, the deps pin today's ordering
    (topic then sync) — so the demo's names and edges stay byte-identical while the released
    components compose their own slim configs from YAML."""
    has_postgres_target = "postgres" in cfg.targets
    if has_postgres_target:
        postgres_cfg = postgres_target_config(cfg)
        health_check = build_sink_health_check(postgres_cfg)
        health_job, health_schedule = build_sink_health_job_and_schedule(postgres_cfg, health_check)
    return Definitions(
        assets=[
            *([build_risingwave_table_asset(risingwave_target_config(cfg, source_fqn))] if "risingwave" in cfg.targets else []),
            *([build_debezium_sink_asset(postgres_cfg)] if has_postgres_target else []),
            *([build_duckdb_target_asset(duckdb_target_config(cfg))] if "duckdb" in cfg.targets else []),
        ],
        asset_checks=[health_check] if has_postgres_target else [],
        jobs=[health_job] if has_postgres_target else [],
        schedules=[health_schedule] if has_postgres_target else [],
    )


def build_demo_defs(cfg: ReverseEtlSyncConfig) -> Definitions:
    """The demo's builder: the sync asset plus everything the demo needs around it (table setup,
    topic, seed, make-change, the setup and reset jobs), composed with the downstream side via
    build_target_defs on the config's targets half. The reusable component builds only the sync
    asset itself (decoupling plan step 6). The targets are consumers of the sync's topic,
    selected by cfg.targets (plan section 12, option A): "risingwave" builds the RisingWave
    table, "postgres" the Debezium JDBC sink with its health check, "duckdb" a local file
    mirror."""
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
    ]
    if cfg.create_topic_asset:
        assets.append(build_topic_asset(cfg, cfg.topic_asset))
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
    return Definitions.merge(
        Definitions(
            assets=assets,
            jobs=[
                setup_job,
                build_reset_job(cfg),
                *([build_change_job(cfg, change_asset)] if change_asset is not None else []),
            ],
        ),
        # The downstream side, on the config's targets half: the source knowledge crosses the
        # seam only as this explicit composition argument (plan section 13.2).
        build_target_defs(cfg.targets_config(), source_fqn=cfg.source_fqn),
    )
