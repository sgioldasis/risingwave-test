"""Configuration for one reverse-ETL CDF sync (APR-233): Databricks Delta table ->
Kafka -> Postgres (Debezium JDBC sink) and RisingWave.

ReverseEtlSyncConfig names everything that identifies a sync: the Databricks
source and watermark tables, the key column, the Kafka topic, the connector, the
two target tables, and the Dagster assets and jobs built for it. The modules in
this package take a config as a parameter; reverse_etl_defs.build_reverse_etl_defs
turns one into Definitions. Build one with ReverseEtlSyncConfig.for_name(), which
derives every name from a short label; the ReverseEtlCdfSync component does so from YAML.

The notebook (notebooks/reverse_etl_cdf_to_kafka.py) runs inside Databricks and
cannot import this module, so it repeats the Databricks-side values as widget
defaults; keep them equal to the config it serves.

No imports from the asset modules, so any of them can import this one.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ReverseEtlSyncConfig:
    # --- what is synced -------------------------------------------------------
    sync_name: str
    catalog: str
    schema: str
    source_table: str
    state_table: str
    # Surrogate key: an identity column Databricks assigns on insert and never
    # changes, so rows sharing a business `id` (the table does not enforce
    # uniqueness) stay distinct downstream. Must exist from table creation.
    key_column: str
    # Column DDL for the source table besides the key, used only when the setup
    # asset creates the table (the demo does; a real table already exists).
    source_columns: tuple[str, ...]
    # The single topic both consumers read: RisingWave (FORMAT DEBEZIUM) and the
    # Debezium JDBC sink.
    kafka_topic: str
    connector_name: str
    postgres_table: str
    risingwave_table: str

    # --- the Dagster definitions built for it ----------------------------------
    group_name: str
    table_setup_asset: str
    sync_asset: str
    risingwave_asset: str
    sink_asset: str
    # Name of the asset that makes sure the topic exists. If create_topic_asset is
    # False it must already exist elsewhere (the demo uses the shared
    # kafka_output_topics_setup); if True the sync's own asset of that name is built.
    topic_asset: str
    create_topic_asset: bool
    setup_job: str
    reset_job: str

    # --- reset safety allowlist ------------------------------------------------
    # The reset job drops tables, a connector and a topic, so it refuses to run
    # unless every name it touches starts with one of these prefixes and the
    # Databricks schema equals reset_schema. Set them deliberately.
    reset_name_prefixes: tuple[str, ...]
    reset_schema: str

    # --- optional demo seed ----------------------------------------------------
    # SQL run against the source table by the seed asset, after everything else in
    # the setup job, so the rows wait in Databricks until the sync is run. Each
    # statement may use {source_table}. Leave empty for a real table.
    seed_asset: str = ""
    seed_statements: tuple[str, ...] = ()

    @property
    def source_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.source_table}"

    @property
    def state_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.state_table}"

    @property
    def envelope_schema_name(self) -> str:
        # Debezium's `<server>.<schema>.<table>` convention, which is what the sink
        # uses to recognise a Debezium event.
        return f"{self.sync_name}.{self.schema}.{self.source_table}"

    @property
    def consumer_group(self) -> str:
        # Kafka Connect names a sink connector's consumer group connect-<name>.
        return f"connect-{self.connector_name}"

    @classmethod
    def for_name(
        cls,
        name: str,
        *,
        catalog: str,
        schema: str,
        source_columns: tuple[str, ...],
        key_column: str = "rid",
        seed_statements: tuple[str, ...] = (),
    ) -> "ReverseEtlSyncConfig":
        """A config for a sync labelled `name` (for example "cdf" or "orders"): every
        name is reverse_etl_<name>_<role>, so syncs never collide and the label shows
        in every system (Databricks, Kafka, Connect, Postgres, RisingWave, Dagster)."""
        base = f"reverse_etl_{name}"
        return cls(
            sync_name=base,
            catalog=catalog,
            schema=schema,
            source_table=f"{base}_source",
            state_table=f"{base}_state",
            key_column=key_column,
            source_columns=source_columns,
            kafka_topic=f"{base}_topic",
            connector_name=f"{base}_sink",
            postgres_table=f"{base}_target",
            risingwave_table=f"{base}_target",
            group_name=base,
            table_setup_asset=f"{base}_table_setup",
            sync_asset=f"{base}_to_kafka",
            risingwave_asset=f"{base}_risingwave_target",
            sink_asset=f"{base}_jdbc_sink",
            topic_asset=f"{base}_topic_setup",
            create_topic_asset=True,
            setup_job=f"{base}_setup_job",
            reset_job=f"{base}_reset_job",
            reset_name_prefixes=("reverse_etl_",),
            reset_schema=schema,
            seed_asset=f"{base}_seed",
            seed_statements=seed_statements,
        )

