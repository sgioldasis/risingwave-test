"""The released target components (docs/poc/REVERSE_ETL_RELEASE_DESIGN.md): one per target, so a
team takes only what it wants — the presence of a YAML is the target selection.

Each component types a minimal attribute set and derives everything it mints as
reverse_etl_<name>[_<role>] (every derived field keeps an explicit override). They build from the
slim per-target configs (reverse_etl_config.py), so a target cannot know things its target does
not need: the postgres and duckdb targets carry no source knowledge at all, and the RisingWave
target alone holds `source_fqn` — the seam's single composition input.

The components declare no Dagster resources (the env contract): the builders take their
connections from the library's env-fed functions, and a team's Python footprint stays zero files.
"""

import dataclasses

import dagster as dg

from ..assets.reverse_etl_config import DuckdbTargetConfig, PostgresTargetConfig, RisingwaveTargetConfig
from ..assets.reverse_etl_debezium_sink import build_debezium_sink_asset
from ..assets.reverse_etl_duckdb_target import build_duckdb_target_asset
from ..assets.reverse_etl_health import HEALTH_CRON, build_sink_health_check, build_sink_health_job_and_schedule
from ..assets.reverse_etl_risingwave_setup import build_risingwave_table_asset


class ReverseEtlRisingwaveTarget(dg.Component, dg.Model, dg.Resolvable):
    """One RisingWave table ingesting a reverse-ETL sync's Kafka topic via FORMAT DEBEZIUM.

    The only target allowed `source_fqn`: its CREATE reads the source's columns and types once
    (the seam's single composition input), and its envelope schema name composes from `sync_name`
    and the source's last two components — the same formula the sync side uses, so an Avro
    registration byte-matches by arithmetic. Override `kafka_topic`/`sync_name`/`encoding`/
    `schema_registry_url` on one side only if you override on both (the wire contract).

    scan.startup.mode='earliest' covers message order, not topic existence: materialize the
    sync's bootstrap once before this target's first materialization, or add the sync's topic
    asset to `deps`.
    """

    name: str
    # The source's catalog.schema.table — read once for the CREATE, never written to.
    source_fqn: str
    # Must match the sync's key column: it is the RisingWave primary key.
    key_column: str
    # The envelope's prefix. Omitted it derives as reverse_etl_<name> — the same derivation the
    # sync uses, so the two match by construction.
    sync_name: str | None = None
    # Omitted it derives as reverse_etl_<name>_topic — matching the sync's topic by construction.
    kafka_topic: str | None = None
    risingwave_table: str | None = None
    encoding: str | None = None
    schema_registry_url: str | None = None
    # Upstream asset keys (names), so the target can order itself after the sync's bootstrap.
    deps: list[str] | None = None

    def to_config(self) -> RisingwaveTargetConfig:
        overrides = {
            field: getattr(self, field)
            for field in ("risingwave_table", "encoding", "schema_registry_url")
            if getattr(self, field) is not None
        }
        return RisingwaveTargetConfig(
            label=self.name,
            source_fqn=self.source_fqn,
            sync_name=self.sync_name or f"reverse_etl_{self.name}",
            kafka_topic=self.kafka_topic or f"reverse_etl_{self.name}_topic",
            key_column=self.key_column,
            deps=tuple(self.deps or ()),
            **overrides,
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[build_risingwave_table_asset(self.to_config())])


class ReverseEtlPostgresTarget(dg.Component, dg.Model, dg.Resolvable):
    """The Debezium JDBC sink into a Postgres table, plus the sink health check with its job and
    schedule (D19: the check watches the connector and its lag, so it exists only when this YAML
    does).

    This component carries no Postgres coordinates at all: the asset POSTs a connector config to
    Kafka Connect (its REST URL read from env) and the connector itself opens the JDBC connection,
    reading `HOST_POSTGRES_URL`/`POSTGRES_USER` from the environment and the password as
    `${env:POSTGRES_PASSWORD}` resolved by the Connect worker — the password never passes through
    Dagster, and one defs.yaml deploys to staging and production.
    """

    name: str
    # Must match the sync's key column: it is the sink's primary.key.fields (record_key mode).
    key_column: str
    kafka_topic: str | None = None
    connector_name: str | None = None
    postgres_table: str | None = None
    # tasks.max of the Debezium JDBC sink. One task drained 300,000 messages at about 35,000 to
    # 40,000 rows a second in a local test; raise it for a backfill.
    sink_tasks_max: int | None = None
    # The health check's cadence. Default: the library's 5-minute cron, stopped — start the
    # schedule from the Dagster UI when a sink should actually be watched.
    health_cron: str | None = None
    encoding: str | None = None
    schema_registry_url: str | None = None
    deps: list[str] | None = None

    def to_config(self) -> PostgresTargetConfig:
        config = PostgresTargetConfig(
            label=self.name,
            kafka_topic=self.kafka_topic or f"reverse_etl_{self.name}_topic",
            key_column=self.key_column,
            deps=tuple(self.deps or ()),
            **{
                field: getattr(self, field)
                for field in ("connector_name", "postgres_table", "encoding", "schema_registry_url")
                if getattr(self, field) is not None
            },
        )
        if self.sink_tasks_max is not None:
            config = dataclasses.replace(config, sink_tasks_max=self.sink_tasks_max)
        return config

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self.to_config()
        check = build_sink_health_check(cfg)
        job, schedule = build_sink_health_job_and_schedule(cfg, check, cron=self.health_cron or HEALTH_CRON)
        return dg.Definitions(
            assets=[build_debezium_sink_asset(cfg)],
            asset_checks=[check],
            jobs=[job],
            schedules=[schedule],
        )


class ReverseEtlDuckdbTarget(dg.Component, dg.Model, dg.Resolvable):
    """The stateless local mirror: drains the sync's topic into a DuckDB file, latest event per
    key, tombstones on op=="d", table named after the topic.

    The path is typed — a deployment decision, not a minted name. It is a dev mirror with a
    stated ceiling: it re-reads the whole topic every run (no committed offsets), so point it at
    PoC-volume topics, not a high-rate production one.
    """

    name: str
    key_column: str
    duckdb_target_path: str
    kafka_topic: str | None = None
    encoding: str | None = None
    schema_registry_url: str | None = None
    deps: list[str] | None = None

    def to_config(self) -> DuckdbTargetConfig:
        return DuckdbTargetConfig(
            label=self.name,
            kafka_topic=self.kafka_topic or f"reverse_etl_{self.name}_topic",
            key_column=self.key_column,
            duckdb_target_path=self.duckdb_target_path,
            deps=tuple(self.deps or ()),
            **{
                field: getattr(self, field)
                for field in ("encoding", "schema_registry_url")
                if getattr(self, field) is not None
            },
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[build_duckdb_target_asset(self.to_config())])
