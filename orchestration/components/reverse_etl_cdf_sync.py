import dataclasses

import dagster as dg

from ..assets.reverse_etl_cdf_setup import build_sync_asset
from ..assets.reverse_etl_config import CdfSyncConfig


class ReverseEtlCdfSync(dg.Component, dg.Model, dg.Resolvable):
    """One Databricks Delta table's changes, read and written to Kafka as Debezium-style events.

    The reusable reverse-ETL component (docs/poc/REVERSE_ETL_COMPONENT_DECOUPLING_PLAN.md, section
    3.2): it builds exactly one Dagster asset, the sync. The sync reads an existing Delta table with
    delta-rs (no Spark, no Databricks compute), keeps its watermark in the Postgres table
    reverse_etl_sync_state, and produces Debezium-style events to an existing Kafka topic in Connect
    JSON or Confluent Avro.

    It creates nothing: the topic and the state table must already exist (a missing one fails the
    sync with what to create), and the asset touches only the postgres, databricks, kafka and
    registry resources. Schema changes on the target side — column adds, drop/rename propagation —
    are the demo's hooks (ReverseEtlCdfDemo); this component runs without them.

    `name` is a short label deriving the sync name (the watermark key and the Debezium envelope
    prefix) and the default asset and group names, as reverse_etl_<name>_<...>.
    """

    name: str
    catalog: str
    schema_name: str
    # The existing table. The component never creates it.
    source_table: str
    # The column that identifies a row downstream. No default: the demo's surrogate
    # key `rid` means nothing for a real table.
    key_column: str
    # The topic to produce to. It must already exist; the sync never creates one.
    kafka_topic: str

    # "json" (default: Kafka Connect JSON with the schema embedded in every message) or "avro"
    # (Confluent wire format, schemas in the registry under <topic>-key and <topic>-value).
    encoding: str | None = None
    schema_registry_url: str | None = None

    # Overrides for the derived Dagster names.
    group_name: str | None = None
    sync_asset: str | None = None

    # Upstream asset keys, so a team can order the sync after its own pipeline's assets.
    deps: list[str] | None = None

    def to_config(self) -> CdfSyncConfig:
        base = f"reverse_etl_{self.name}"
        config = CdfSyncConfig(
            sync_name=base,
            catalog=self.catalog,
            schema=self.schema_name,
            source_table=self.source_table,
            key_column=self.key_column,
            kafka_topic=self.kafka_topic,
            group_name=self.group_name or base,
            sync_asset=self.sync_asset or f"{base}_to_kafka",
        )
        overrides = {
            field: getattr(self, field)
            for field in ("encoding", "schema_registry_url")
            if getattr(self, field) is not None
        }
        return dataclasses.replace(config, **overrides)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        deps = tuple(dg.AssetKey(dep) for dep in self.deps or ())
        return dg.Definitions(assets=[build_sync_asset(self.to_config(), deps=deps)])
