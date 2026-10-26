from dataclasses import replace

import dagster as dg

from ..assets.reverse_etl_config import ReverseEtlSyncConfig, dagster_names
from ..assets.reverse_etl_defs import build_demo_defs


class SyncAttributes(dg.Model):
    """The sync's own attributes (the demo's `sync:` block): the source's location, the external
    names of what the sync touches, and the sync's behavior. The field names are the reusable
    component's (ReverseEtlCdfSync), so promoting this block verbatim into a team's own YAML is
    mechanical (decoupling plan section 13, D17) — only the demo-only table-shaping fields
    (source_columns, seed_statements, uniform, propagate_drops) would drop away."""

    catalog: str
    schema_name: str
    # sync_name is the watermark key and the Debezium envelope prefix: changing it for an
    # existing sync restarts that sync from the beginning.
    sync_name: str
    source_table: str
    state_table: str
    kafka_topic: str
    # Column DDL besides the key, used only if the setup asset has to create the
    # source table (for example "id BIGINT NOT NULL", "value STRING"). Leave unset
    # for an existing table: the setup asset then only checks that it exists and
    # fails if it does not, instead of creating it.
    source_columns: list[str] | None = None
    key_column: str = "rid"
    # Demo seed: SQL run against the source table as the last step of the setup job.
    # Each statement may use {source_table}. Leave unset for a real table.
    seed_statements: list[str] | None = None
    # "json" (default) or "avro"; see ReverseEtlSyncConfig.encoding.
    encoding: str | None = None
    schema_registry_url: str | None = None
    # The watermark state (reverse_etl_state.py): "postgres" (default, the shared table
    # `state_table`) or "duckdb" (a local file at `state_path`, created on first run).
    state_backend: str | None = None
    state_path: str | None = None
    # False creates the source table as plain Delta (Change Data Feed only, no
    # UniForm/Iceberg) — see the delta sync's defs.yaml.
    uniform: bool | None = None
    # True propagates source column drops to RisingWave and Postgres at the next
    # sync (evidence-gated via the state table's last_schema). Target-side care
    # that runs through the demo's hooks inside the sync run: it straddles the
    # sync/targets seam by composition, not by the seam itself (plan section 13.6).
    propagate_drops: bool | None = None


class TargetsAttributes(dg.Model):
    """The downstream attributes (the demo's `targets:` block): the target names (plan section 11)
    and the selection (section 12) — everything the targets need besides the topic, which the sync
    block names. The seam (section 13.2) is exactly the sync block's message contract; no field
    here names the source table."""

    connector_name: str
    postgres_table: str
    risingwave_table: str
    # tasks.max of the Debezium JDBC sink (default 1, enough for a daily load); raise it for a backfill.
    sink_tasks_max: int | None = None
    # Which target stacks to build, as consumers of the topic (plan section 12, option A):
    # any of "risingwave", "postgres", "duckdb". Default: risingwave and postgres, the
    # demo's shape so far. The Debezium sink's health check follows the postgres target.
    targets: list[str] | None = None
    # The DuckDB target's file (required when "duckdb" is selected); one target per file.
    duckdb_target_path: str | None = None


class ReverseEtlCdfDemo(dg.Component, dg.Model, dg.Resolvable):
    """The demo's reverse-ETL sync: one Databricks Delta table synced to Kafka, then to the
    selected targets. The reusable sync (ReverseEtlCdfSync) plus everything the PoC creates
    around it.

    The attributes split at the sync/targets seam (decoupling plan section 13, option C): the
    `sync:` block is the sync's own configuration, the `targets:` block the downstream's, and the
    top level keeps what spans both sides — the `name` label the Dagster object names derive from,
    their overrides, and the demo infrastructure's flags (topic asset, reset). The infrastructure
    names are explicit required attributes, so the YAML reads as the inventory of what the sync
    touches in every system.

    Builds the demo's assets (source table setup, CDF to Kafka through the demo hooks, the
    selected targets), a setup job and a reset job.

    The reset job drops the source table, so the `sync:` block's `schema_name` must be a scratch
    schema, and every name it drops must start with one of `reset_name_prefixes`.

    The optional fields override a derived Dagster name; leave them unset otherwise.
    """

    name: str
    sync: SyncAttributes
    targets: TargetsAttributes

    # --- the Dagster object names, derived from the label `name` (reverse_etl_<name>_<role>),
    # each overridable. They span both sides, so they stay at the composition level.
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
        """The infrastructure names are the YAML's (required attributes in the blocks); the
        Dagster object names derive from the label, each overridable."""
        config = ReverseEtlSyncConfig(
            sync_name=self.sync.sync_name,
            label=self.name,
            # The demo keeps the decoupling plan's naming rule: the watermark key is
            # explicit YAML (releases derive it and guard the rename case instead).
            sync_name_explicit=True,
            catalog=self.sync.catalog,
            schema=self.sync.schema_name,
            source_table=self.sync.source_table,
            state_table=self.sync.state_table,
            key_column=self.sync.key_column,
            seed_statements=tuple(self.sync.seed_statements or ()),
            source_columns=tuple(self.sync.source_columns or ()),
            kafka_topic=self.sync.kafka_topic,
            connector_name=self.targets.connector_name,
            postgres_table=self.targets.postgres_table,
            risingwave_table=self.targets.risingwave_table,
            reset_name_prefixes=("reverse_etl_",),
            reset_schema=self.reset_schema or self.sync.schema_name,
            create_topic_asset=True,
            # bootstrap_job is build-only (the released component's); the demo's setup job
            # orders topic then sync itself.
            **{k: v for k, v in dagster_names(self.name).items() if k != "bootstrap_job"},
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
            )
            if getattr(self, field) is not None
        }
        overrides.update(
            {
                field: getattr(self.sync, field)
                for field in ("uniform", "propagate_drops", "encoding", "schema_registry_url", "state_backend", "state_path")
                if getattr(self.sync, field) is not None
            }
        )
        targets_overrides = {
            field: getattr(self.targets, field)
            for field in ("sink_tasks_max", "duckdb_target_path")
            if getattr(self.targets, field) is not None
        }
        if self.targets.targets is not None:
            targets_overrides["targets"] = tuple(self.targets.targets)
        overrides.update(targets_overrides)
        if self.reset_name_prefixes is not None:
            overrides["reset_name_prefixes"] = tuple(self.reset_name_prefixes)
        return replace(config, **overrides)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return build_demo_defs(self.to_config())
