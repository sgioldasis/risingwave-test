from dataclasses import replace

import dagster as dg

from ..assets.reverse_etl_config import ReverseEtlSyncConfig, dagster_names
from ..assets.reverse_etl_defs import build_demo_defs


class ReverseEtlCdfDemo(dg.Component, dg.Model, dg.Resolvable):
    """The demo's reverse-ETL sync: one Databricks Delta table synced to Kafka, then to Postgres and
    RisingWave. The reusable sync (ReverseEtlCdfSync) plus everything the PoC creates around it.

    Builds the demo's assets (source table setup, CDF to Kafka through the demo hooks, RisingWave
    table, Debezium JDBC sink), a setup job and a reset job. The infrastructure names — the
    watermark key, the source and state tables, the topic, the connector and both target tables —
    are explicit required attributes, so the YAML reads as the inventory of what the sync touches.
    `name` is a short label such as "orders" that derives the Dagster object names as
    reverse_etl_<name>_<role>. See docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md, section 14.4.

    The reset job drops the source table, so `schema_name` must be a scratch schema,
    and every name it drops must start with one of `reset_name_prefixes`.

    The optional fields override a derived Dagster name; leave them unset otherwise.
    """

    name: str
    catalog: str
    schema_name: str
    # --- the infrastructure names, explicit (plan section 11): nothing here is derived, so the
    # YAML reads as the inventory of what this sync touches in every system. sync_name is the
    # watermark key and the Debezium envelope prefix: changing it for an existing sync restarts
    # that sync from the beginning.
    sync_name: str
    source_table: str
    state_table: str
    kafka_topic: str
    connector_name: str
    postgres_table: str
    risingwave_table: str
    # Column DDL besides the key, used only if the setup asset has to create the
    # source table (for example "id BIGINT NOT NULL", "value STRING"). Leave unset
    # for an existing table: the setup asset then only checks that it exists and
    # fails if it does not, instead of creating it.
    source_columns: list[str] | None = None
    key_column: str = "rid"
    # Demo seed: SQL run against the source table as the last step of the setup job.
    # Each statement may use {source_table}. Leave unset for a real table.
    seed_statements: list[str] | None = None

    # --- the Dagster object names, derived from the label `name` (reverse_etl_<name>_<role>),
    # each overridable.
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
    # tasks.max of the Debezium JDBC sink (default 1, enough for a daily load); raise it for a backfill.
    sink_tasks_max: int | None = None
    # False creates the source table as plain Delta (Change Data Feed only, no
    # UniForm/Iceberg) — see the delta sync's defs.yaml.
    uniform: bool | None = None
    # True propagates source column drops to RisingWave and Postgres at the next
    # sync (evidence-gated via the state table's last_schema).
    propagate_drops: bool | None = None
    # "json" (default) or "avro"; see ReverseEtlSyncConfig.encoding.
    encoding: str | None = None
    schema_registry_url: str | None = None

    def to_config(self) -> ReverseEtlSyncConfig:
        """The infrastructure names are the YAML's (required attributes above); the Dagster object
        names derive from the label, each overridable."""
        config = ReverseEtlSyncConfig(
            sync_name=self.sync_name,
            catalog=self.catalog,
            schema=self.schema_name,
            source_table=self.source_table,
            state_table=self.state_table,
            key_column=self.key_column,
            seed_statements=tuple(self.seed_statements or ()),
            source_columns=tuple(self.source_columns or ()),
            kafka_topic=self.kafka_topic,
            connector_name=self.connector_name,
            postgres_table=self.postgres_table,
            risingwave_table=self.risingwave_table,
            reset_name_prefixes=("reverse_etl_",),
            reset_schema=self.reset_schema or self.schema_name,
            create_topic_asset=True,
            **dagster_names(self.name),
        )
        overrides = {
            field: getattr(self, field)
            for field in (
                "group_name",
                "table_setup_asset",
                "sync_asset",
                "risingwave_asset",
                "sink_asset",
                "topic_asset",
                "create_topic_asset",
                "setup_job",
                "reset_job",
                "sink_tasks_max",
                "uniform",
                "propagate_drops",
                "encoding",
                "schema_registry_url",
            )
            if getattr(self, field) is not None
        }
        if self.reset_name_prefixes is not None:
            overrides["reset_name_prefixes"] = tuple(self.reset_name_prefixes)
        return replace(config, **overrides)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return build_demo_defs(self.to_config())
