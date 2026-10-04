import dagster as dg

from ..assets.reverse_etl_config import ReverseEtlSyncConfig
from ..assets.reverse_etl_defs import build_reverse_etl_defs


class ReverseEtlCdfSync(dg.Component, dg.Model, dg.Resolvable):
    """One Databricks Delta table synced to Kafka, then to Postgres and RisingWave.

    Builds the sync's assets (source table setup, CDF to Kafka, RisingWave table,
    Debezium JDBC sink), a setup job and a reset job. Every other name (topic,
    connector, target tables, assets, jobs) is derived from `name`, so syncs do not
    collide. See docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md, section 14.4.

    The reset job drops the source table, so `schema_name` must be a scratch schema
    and `name` must prefix everything it drops (the allowlist in the config).
    """

    name: str
    catalog: str
    schema_name: str
    source_table: str
    # Column DDL besides the key, used only if the setup asset has to create the
    # source table (for example "id BIGINT NOT NULL", "value STRING").
    source_columns: list[str]
    key_column: str = "rid"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return build_reverse_etl_defs(
            ReverseEtlSyncConfig.for_name(
                self.name,
                catalog=self.catalog,
                schema=self.schema_name,
                source_table=self.source_table,
                source_columns=tuple(self.source_columns),
                key_column=self.key_column,
            )
        )
