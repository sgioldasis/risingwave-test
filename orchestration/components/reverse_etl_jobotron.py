import dagster as dg

from ..assets.reverse_etl_jobotron import (
    BASE,
    SCHEMA_REGISTRY_URL,
    JobotronConfig,
    build_jobotron_defs,
)


class JobotronProducer(dg.Component, dg.Model, dg.Resolvable):
    """The Jobotron RFC's upstream producer (docs/poc/REVERSE_ETL_JOBOTRON.md): run a
    candidate-selection SQL query against the lake's Delta tables and publish one Avro
    message per selected customer to its operator topic, schemas in Apicurio.

    No Spark, no JVM, no CDF: the sources are scanned with the service principal (engine
    per scan_engine: delta-rs column-pruned snapshots, or the DuckDB delta extension's
    streaming delta_scan), DuckDB runs the query, the full eligible set is published every
    run (the Jobotron worker owns deduplication and re-validation). The setup asset creates
    the demo tables (plain Delta, no Change Data Feed) and the per-operator topics; the
    reset job tears them down.
    """

    job_type: str = "kyc.unverified_pm"
    catalog: str = "de_dev"
    schema_name: str = "sr_poc_external"
    # The candidate-selection query, in DuckDB dialect. Table names must match the
    # customers_table/kyc_table values below (they are the DuckDB registrations).
    query: str
    operators: list[str] | None = None
    topic_template: str = "jobotron_demo_{operator}"
    key_columns: list[str] | None = None
    operator_column: str = "operator"
    # Columns each delta-rs scan fetches (pruning hint: scan only what the query selects).
    scan_columns: dict[str, list[str]] | None = None
    # Per-job scan engine (docs/poc/REVERSE_ETL_JOBOTRON.md §8.6): "delta_rs" (day one)
    # materializes column-pruned snapshots via delta-rs; "delta_extension" registers DuckDB
    # views over the delta extension's streaming delta_scan (verified live 2026-10-21).
    scan_engine: str = "delta_rs"
    customers_table: str = f"{BASE}_customers"
    kyc_table: str = f"{BASE}_kyc_cases"
    schema_registry_url: str | None = None

    def to_config(self) -> JobotronConfig:
        return JobotronConfig(
            job_type=self.job_type,
            catalog=self.catalog,
            schema=self.schema_name,
            sources={
                "customers": (
                    f"{self.catalog}.{self.schema_name}.{self.customers_table}",
                    tuple((self.scan_columns or {}).get("customers", ())),
                ),
                "kyc_cases": (
                    f"{self.catalog}.{self.schema_name}.{self.kyc_table}",
                    tuple((self.scan_columns or {}).get("kyc_cases", ())),
                ),
            },
            query=self.query,
            operators=tuple(self.operators or ["gr", "br"]),
            topic_template=self.topic_template,
            key_columns=tuple(self.key_columns or ["customer_id"]),
            operator_column=self.operator_column,
            scan_engine=self.scan_engine,
            customers_table=self.customers_table,
            kyc_table=self.kyc_table,
            schema_registry_url=self.schema_registry_url or SCHEMA_REGISTRY_URL,
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return build_jobotron_defs(self.to_config())
