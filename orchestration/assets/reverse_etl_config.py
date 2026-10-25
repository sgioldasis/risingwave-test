"""Configuration for one reverse-ETL CDF sync (APR-233): Databricks Delta table ->
Kafka -> Postgres (Debezium JDBC sink) and RisingWave.

Two frozen dataclasses split the configuration by audience:

- `CdfSyncConfig` holds exactly what the sync asset needs: the Databricks source
  and key column, the Kafka topic, the Dagster names, and the message encoding.
  This is the configuration a team using the reverse-ETL component provides.
- `ReverseEtlSyncConfig(CdfSyncConfig)` adds everything the demo needs around the
  sync: the tables and connector it creates, the targets, the reset allowlist, the
  seed. The demo's modules take this one; the sync's pure helpers take the base.

Both are keyword-only, so the demo fields can carry defaults without ordering
constraints. The modules in this package take a config as a parameter;
reverse_etl_defs.build_demo_defs (the demo's builder) turns one into Definitions. Build the demo
config with ReverseEtlSyncConfig.for_name(), which derives every name from a short
label; the ReverseEtlCdfSync component does so from YAML.

The notebook (notebooks/reverse_etl_cdf_to_kafka.py) runs inside Databricks and
cannot import this module, so it repeats the Databricks-side values as widget
defaults; keep them equal to the config it serves.

No imports from the asset modules, so any of them can import this one.
"""

from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class CdfSyncConfig:
    # --- what is synced -------------------------------------------------------
    sync_name: str
    catalog: str
    schema: str
    source_table: str
    # Surrogate key: an identity column Databricks assigns on insert and never
    # changes, so rows sharing a business `id` (the table does not enforce
    # uniqueness) stay distinct downstream. Must exist from table creation.
    key_column: str
    # The single topic both consumers read: RisingWave (FORMAT DEBEZIUM) and the
    # Debezium JDBC sink.
    kafka_topic: str

    # --- the Dagster definitions built for it ----------------------------------
    group_name: str
    sync_asset: str

    # --- the watermark state (reverse_etl_state.py) -----------------------------
    # "postgres" (default): the postgres resource and the shared table `state_table`
    # (keyed by sync_name), which the team creates -- the sync's precondition fails with
    # the DDL when it is absent. "duckdb": a local file at `state_path`, one sync per
    # file (DuckDB allows one writer), created on first run (D15). The watermark
    # semantics are identical either way: append-only, freshest-row-wins.
    state_backend: str = "postgres"
    state_table: str = "reverse_etl_sync_state"
    state_path: str = ""

    # --- message encoding ------------------------------------------------------
    # "json": Kafka Connect JSON with the schema embedded in every message (the default).
    # "avro": Confluent wire format, schemas in the registry under <topic>-key and <topic>-value
    # (a Confluent-compatible API: the shared staging Apicurio's /apis/ccompat/v7). Section 14.1 of
    # docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md. The notebook gets `encoding` as a job parameter.
    encoding: str = "json"
    schema_registry_url: str = "http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7"

    def __post_init__(self) -> None:
        if self.encoding not in ("json", "avro"):
            raise ValueError(f"encoding must be 'json' or 'avro', got {self.encoding!r}")
        if self.state_backend not in ("postgres", "duckdb"):
            raise ValueError(f"state_backend must be 'postgres' or 'duckdb', got {self.state_backend!r}")
        if self.state_backend == "duckdb" and not self.state_path:
            raise ValueError("state_path is required for the duckdb state backend")

    @property
    def key_subject(self) -> str:
        return f"{self.kafka_topic}-key"

    @property
    def value_subject(self) -> str:
        return f"{self.kafka_topic}-value"

    @property
    def source_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.source_table}"

    @property
    def envelope_schema_name(self) -> str:
        # Debezium's `<server>.<schema>.<table>` convention, which is what the sink
        # uses to recognise a Debezium event.
        return f"{self.sync_name}.{self.schema}.{self.source_table}"


def dagster_names(label: str) -> dict:
    """The Dagster object names for a sync labelled `label` (reverse_etl_<label>_<role>): the
    asset group and names, and the job names. The demo component derives these from its `name`
    attribute. The infrastructure names are never derived — they are explicit in the YAML."""
    base = f"reverse_etl_{label}"
    return {
        "group_name": base,
        "table_setup_asset": f"{base}_table_setup",
        "sync_asset": f"{base}_to_kafka",
        "risingwave_asset": f"{base}_risingwave_target",
        "sink_asset": f"{base}_jdbc_sink",
        "topic_asset": f"{base}_topic_setup",
        "setup_job": f"{base}_setup_job",
        "reset_job": f"{base}_reset_job",
        "seed_asset": f"{base}_seed",
    }


@dataclass(frozen=True, kw_only=True)
class ReverseEtlSyncConfig(CdfSyncConfig):
    """The demo's configuration: `CdfSyncConfig` plus everything the PoC creates
    around the sync (source and state tables, connector, targets, reset, seed)."""

    # Column DDL for the source table besides the key, used only when the setup
    # asset creates the table (the demo does; a real table already exists).
    source_columns: tuple[str, ...]
    connector_name: str
    postgres_table: str
    risingwave_table: str

    # --- the rest of the Dagster definitions built for it ----------------------
    table_setup_asset: str
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

    # --- connector throughput --------------------------------------------------
    # tasks.max of the Debezium JDBC sink. One task drained 300,000 messages at about 35,000 to
    # 40,000 rows a second in a local test, far more than a daily load needs; raise it (the topic has 15
    # partitions) for a backfill. See docs/poc/REVERSE_ETL_DRT_COMPARISON.md section 4.8.
    sink_tasks_max: int = 1

    # --- source table format ----------------------------------------------------
    # True (default): the setup asset creates the source with UniForm (Iceberg
    # compat) so other engines — StarRocks via UC's Iceberg REST catalog — can read
    # it. False: plain Delta with only Change Data Feed, for showing the sync is
    # UniForm-agnostic (the delta sync). Only affects a table the setup asset
    # creates; an existing source table is never touched.
    uniform: bool = True

    # --- target schema reconciliation --------------------------------------------
    # False (default): targets only ever gain columns (add_missing_columns, and
    # the Debezium JDBC sink does the same on Postgres). True: a schema change
    # the source made — recorded in the state table's last_schema and absent
    # from the current schema read — is followed at the next sync: a dropped
    # column is dropped from RisingWave and Postgres, and a renamed column (its
    # delta.columnMapping.id survives the rename) is renamed on both with the
    # target data preserved. Opt-in because drops destroy target data
    # automatically; the evidence rule is what keeps a partial schema read from
    # ever cascading into changes (docs/poc/REVERSE_ETL_LIVE_DEMO.md,
    # drop-propagation section).
    propagate_drops: bool = False

    # --- target selection (plan section 12, option A) ----------------------------
    # Which target stacks the demo builds, as consumers of the sync's Kafka topic:
    # "risingwave" (the RisingWave table), "postgres" (the Debezium JDBC sink and its
    # health check) and "duckdb" (a local DuckDB file mirror at duckdb_target_path).
    # Default: both warehouse targets, the demo's shape so far. The sync itself is
    # untouched by this -- it produces to Kafka and its state, and nothing else.
    targets: tuple[str, ...] = ("risingwave", "postgres")
    # The DuckDB target's file. Explicit like every infrastructure name: it lands in
    # a real system. Local file, one target per file (DuckDB allows one writer).
    duckdb_target_path: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.sink_tasks_max < 1:
            raise ValueError(f"sink_tasks_max must be at least 1, got {self.sink_tasks_max}")
        unknown = set(self.targets) - {"risingwave", "postgres", "duckdb"}
        if unknown:
            raise ValueError(f"unknown target(s) {sorted(unknown)} -- targets may contain 'risingwave', 'postgres', 'duckdb'")
        if "duckdb" in self.targets and not self.duckdb_target_path:
            raise ValueError("duckdb_target_path is required when the duckdb target is selected")

    @property
    def state_fqn(self) -> str:
        """Legacy: the Delta state table's FQN, pre-Postgres. The watermark state
        lives in the Postgres table reverse_etl_sync_state now; this FQN remains
        only so the reset can drop a table created by an older demo run."""
        return f"{self.catalog}.{self.schema}.{self.state_table}"

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
        uniform: bool = True,
    ) -> "ReverseEtlSyncConfig":
        """A test helper: a config for a sync labelled `name` (for example "cdf" or "orders") with
        every name derived as reverse_etl_<name>_<role> and the default state backend. Production
        paths keep the infrastructure names explicit (the components' YAML); only the tests derive
        them, to stay short."""
        return cls(
            sync_name=f"reverse_etl_{name}",
            catalog=catalog,
            schema=schema,
            source_table=f"reverse_etl_{name}_source",
            key_column=key_column,
            source_columns=source_columns,
            kafka_topic=f"reverse_etl_{name}_topic",
            connector_name=f"reverse_etl_{name}_sink",
            postgres_table=f"reverse_etl_{name}_target",
            risingwave_table=f"reverse_etl_{name}_target",
            create_topic_asset=True,
            reset_name_prefixes=("reverse_etl_",),
            reset_schema=schema,
            seed_statements=seed_statements,
            uniform=uniform,
            **dagster_names(name),
        )
