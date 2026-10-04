"""Configuration for one reverse-ETL CDF sync (APR-233): Databricks Delta table ->
Kafka -> Postgres (Debezium JDBC sink) and RisingWave.

ReverseEtlSyncConfig names everything that identifies a sync: the Databricks
source and watermark tables, the key column, the Kafka topic, the connector, the
two target tables, and the Dagster assets and jobs built for it. The modules in
this package take a config as a parameter; reverse_etl_defs.build_reverse_etl_defs
turns one into Definitions. POC_SYNC is the demo's config, with the names the
demo has always used. For another table use ReverseEtlSyncConfig.for_name().

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
        source_table: str,
        source_columns: tuple[str, ...],
        key_column: str = "rid",
    ) -> "ReverseEtlSyncConfig":
        """A config for a new sync, with every other name derived from `name` so two
        syncs never collide (asset, job, topic, connector and table names)."""
        return cls(
            sync_name=name,
            catalog=catalog,
            schema=schema,
            source_table=source_table,
            state_table=f"{name}_state",
            key_column=key_column,
            source_columns=source_columns,
            kafka_topic=f"{name}_cdf",
            connector_name=f"{name}_jdbc_sink",
            postgres_table=name,
            risingwave_table=f"{name}_current",
            group_name=name,
            table_setup_asset=f"{name}_table_setup",
            sync_asset=f"{name}_cdf_to_kafka",
            risingwave_asset=f"{name}_risingwave_table",
            sink_asset=f"{name}_debezium_jdbc_sink",
            topic_asset=f"{name}_kafka_topic",
            create_topic_asset=True,
            setup_job=f"{name}_setup_job",
            reset_job=f"{name}_reset_job",
            reset_name_prefixes=(name,),
            reset_schema=schema,
        )


# The demo's sync. The "_jdbc" suffix on the topic is historical: it started as a
# second topic beside a schemaless one, since removed.
POC_SYNC = ReverseEtlSyncConfig(
    sync_name="reverse_etl_cdf_poc",
    catalog="de_dev",
    schema="sr_poc_external",
    source_table="reverse_etl_cdf_poc_source",
    state_table="reverse_etl_cdf_poc_state",
    key_column="rid",
    source_columns=("id BIGINT NOT NULL", "value STRING", "updated_at TIMESTAMP"),
    kafka_topic="rw_poc_reverse_etl_cdf_out_jdbc",
    connector_name="reverse_etl_cdf_jdbc_sink",
    postgres_table="reverse_etl_cdf_poc",
    risingwave_table="reverse_etl_cdf_poc_current",
    group_name="reverse_etl_poc",
    table_setup_asset="reverse_etl_poc_table_setup",
    sync_asset="reverse_etl_cdf_to_kafka",
    risingwave_asset="reverse_etl_cdf_risingwave_table",
    sink_asset="reverse_etl_debezium_jdbc_sink",
    topic_asset="kafka_output_topics_setup",
    create_topic_asset=False,
    setup_job="reverse_etl_poc_setup_job",
    reset_job="reverse_etl_poc_reset_job",
    reset_name_prefixes=("rw_poc_reverse_etl_", "reverse_etl_cdf_"),
    reset_schema="sr_poc_external",
)
