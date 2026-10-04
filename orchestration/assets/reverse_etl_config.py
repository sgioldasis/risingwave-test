"""Names for the reverse-ETL CDF sync (APR-233), in one place.

Everything that identifies a sync -- the Databricks source and watermark tables, the
key column, the Kafka topic, the connector and the two target tables -- lives in
ReverseEtlSyncConfig. The sync, RisingWave, connector, reset and topic modules read
POC_SYNC instead of repeating the strings. The notebook (notebooks/
reverse_etl_cdf_to_kafka.py) runs inside Databricks and cannot import this module,
so it repeats the Databricks-side values as widget defaults; keep them equal.

No imports from the asset modules, so any of them can import this one.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ReverseEtlSyncConfig:
    sync_name: str
    catalog: str
    schema: str
    source_table: str
    state_table: str
    # Surrogate key: an identity column Databricks assigns on insert and never
    # changes, so rows sharing a business `id` (the table does not enforce
    # uniqueness) stay distinct downstream. Must exist from table creation.
    key_column: str
    # The single topic both consumers read: RisingWave (FORMAT DEBEZIUM) and the
    # Debezium JDBC sink. The "_jdbc" suffix is historical: this started as a
    # second topic beside a schemaless one, since removed.
    kafka_topic: str
    connector_name: str
    postgres_table: str
    risingwave_table: str

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


POC_SYNC = ReverseEtlSyncConfig(
    sync_name="reverse_etl_cdf_poc",
    catalog="de_dev",
    schema="sr_poc_external",
    source_table="reverse_etl_cdf_poc_source",
    state_table="reverse_etl_cdf_poc_state",
    key_column="rid",
    kafka_topic="rw_poc_reverse_etl_cdf_out_jdbc",
    connector_name="reverse_etl_cdf_jdbc_sink",
    postgres_table="reverse_etl_cdf_poc",
    risingwave_table="reverse_etl_cdf_poc_current",
)
