from dataclasses import replace

import dagster as dg

from ..assets.reverse_etl_config import ReverseEtlSyncConfig
from ..assets.reverse_etl_defs import build_reverse_etl_defs


class ReverseEtlCdfSync(dg.Component, dg.Model, dg.Resolvable):
    """One Databricks Delta table synced to Kafka, then to Postgres and RisingWave.

    Builds the sync's assets (source table setup, CDF to Kafka, RisingWave table,
    Debezium JDBC sink), a setup job and a reset job. `name` is a short label such as
    "orders"; every name is derived from it as reverse_etl_<name>_<role> (see
    ReverseEtlSyncConfig.for_name), so syncs do not collide. See
    docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md, section 14.4.

    The reset job drops the source table, so `schema_name` must be a scratch schema,
    and every name it drops must start with one of `reset_name_prefixes`.

    The optional fields override a derived name, for example to point `source_table` at
    an existing table; leave them unset otherwise.
    """

    name: str
    catalog: str
    schema_name: str
    # Column DDL besides the key, used only if the setup asset has to create the
    # source table (for example "id BIGINT NOT NULL", "value STRING").
    source_columns: list[str]
    key_column: str = "rid"
    # Demo seed: SQL run against the source table as the last step of the setup job.
    # Each statement may use {source_table}. Leave unset for a real table.
    seed_statements: list[str] | None = None

    # Overrides. sync_name is the watermark key and the Debezium envelope prefix:
    # changing it for an existing sync restarts that sync from the beginning.
    sync_name: str | None = None
    source_table: str | None = None
    state_table: str | None = None
    kafka_topic: str | None = None
    connector_name: str | None = None
    postgres_table: str | None = None
    risingwave_table: str | None = None
    group_name: str | None = None
    table_setup_asset: str | None = None
    sync_asset: str | None = None
    risingwave_asset: str | None = None
    sink_asset: str | None = None
    # Asset that makes sure the topic exists. Set create_topic_asset to false when it is
    # built elsewhere (the POC uses the shared kafka_output_topics_setup).
    topic_asset: str | None = None
    create_topic_asset: bool | None = None
    setup_job: str | None = None
    reset_job: str | None = None
    reset_name_prefixes: list[str] | None = None
    reset_schema: str | None = None

    def to_config(self) -> ReverseEtlSyncConfig:
        config = ReverseEtlSyncConfig.for_name(
            self.name,
            catalog=self.catalog,
            schema=self.schema_name,
            source_columns=tuple(self.source_columns),
            key_column=self.key_column,
            seed_statements=tuple(self.seed_statements or ()),
        )
        overrides = {
            field: getattr(self, field)
            for field in (
                "sync_name",
                "source_table",
                "state_table",
                "kafka_topic",
                "connector_name",
                "postgres_table",
                "risingwave_table",
                "group_name",
                "table_setup_asset",
                "sync_asset",
                "risingwave_asset",
                "sink_asset",
                "topic_asset",
                "create_topic_asset",
                "setup_job",
                "reset_job",
                "reset_schema",
            )
            if getattr(self, field) is not None
        }
        if self.reset_name_prefixes is not None:
            overrides["reset_name_prefixes"] = tuple(self.reset_name_prefixes)
        return replace(config, **overrides)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return build_reverse_etl_defs(self.to_config())
