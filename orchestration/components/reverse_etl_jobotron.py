import dagster as dg

from ..assets.reverse_etl_jobotron import (
    SCHEMA_REGISTRY_URL,
    JobotronConfig,
    build_jobotron_defs,
)


class JobotronProducer(dg.Component, dg.Model, dg.Resolvable):
    """One Jobotron producer instance (docs/poc/JOBOTRON_CONTRACT_PARITY.md): one query type
    x one operator, publishing the team's real 13-field envelope -- one JSON message per
    selected customer, keyed by CustomerId -- to the instance's topic. The candidate query
    runs on the scan_engine of choice: "databricks_sql" (the default) submits the YAML
    query verbatim to a SQL warehouse (DATABRICKS_WAREHOUSE_ID in the environment, never
    here); "delta_rs"/"delta_extension" scan Delta tables directly and need `sources`, the
    bronze-table transcription of what the query joins. dry_run (the default) runs the
    whole path -- scan, query, envelope, hashing, counting -- and sends nothing.
    """

    # D9: the instance identity -- names the folder and every Dagster object
    # (jobotron_<query_type>_<operator>[_<role>]).
    query_type: str
    operator: str
    # D2: the wire identity -- the envelope's JobName, typed, never derived.
    job_name: str
    catalog: str
    schema_name: str
    # The candidate-selection query, verbatim (tokens, not bytes: whitespace is the
    # paste's). Must emit CustomerId and CompanyId -- the envelope reads both from the
    # row, and a result missing them fails before any produce.
    query: str
    dry_run: bool = True
    schema_version: int = 1
    encoding: str = "json"
    scan_engine: str = "databricks_sql"
    topic: str | None = None
    # alias -> {table?, columns}: the zero-DBU engines' DuckDB registrations (the
    # pruning allowlist); `databricks_sql` needs none -- the warehouse resolves the views.
    sources: dict[str, dict] | None = None
    schema_registry_url: str | None = None

    def to_config(self) -> JobotronConfig:
        sources = {}
        for alias, spec in (self.sources or {}).items():
            columns = spec.get("columns") or []
            if not columns:
                raise ValueError(
                    f"sources.{alias}.columns is required -- it is the pruning allowlist "
                    "(the query may touch nothing else)"
                )
            table = spec.get("table") or alias
            # A 3-part name is absolute -- the demo's market shards live in sibling
            # schemas (de.voperdb_cy.cCustomer) and the verification dimension is
            # cross-catalog (de.metadatadb.mVerificationStatus); a 1-part name stays
            # relative to the instance's catalog.schema.
            fqn = table if table.count(".") == 2 else f"{self.catalog}.{self.schema_name}.{table}"
            sources[alias] = (fqn, tuple(columns))
        return JobotronConfig(
            query_type=self.query_type,
            operator=self.operator,
            job_name=self.job_name,
            catalog=self.catalog,
            schema=self.schema_name,
            query=self.query,
            dry_run=self.dry_run,
            schema_version=self.schema_version,
            encoding=self.encoding,
            scan_engine=self.scan_engine,
            topic=self.topic or "",
            sources=sources,
            schema_registry_url=self.schema_registry_url or SCHEMA_REGISTRY_URL,
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return build_jobotron_defs(self.to_config())
