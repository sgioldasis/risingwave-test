# kaizen-dagster-reverse-etl (README skeleton)

*The README the extracted library ships with (release design step 9). This skeleton lives in
the PoC so the eventual extraction is a move, not a rewrite: the env contract below is
already the released code's docstring contract
(`orchestration/assets/reverse_etl_connections.py`), and the boundary rule is already
enforced by the hygiene test (`tests/components/hygiene/test_import_hygiene.py`). At
extraction, replace this prose with the destination's real paths and versions.*

Reverse ETL for Kaizen teams: read a Databricks Delta table's changes, put them on Kafka as
Debezium-style events, and let per-target components consume them — with zero Python files
adopted per team and zero Dagster resources anywhere.

## What teams adopt

One YAML per concern under `src/<team>/defs/<name>/defs.yaml`:

- `ReverseEtlCdfSync` — the sync: reads the table's changes, produces to the topic, keeps the
  watermark. Builds the sync asset, the topic asset (optional) and the bootstrap job.
- `ReverseEtlRisingwaveTarget` / `ReverseEtlPostgresTarget` / `ReverseEtlDuckdbTarget` — one
  per target; the presence of the YAML is the selection. Postgres ships the sink health
  check with its job and schedule.

Minimal attributes only: the label `name`, the source's location, the key column, the state
backend. Everything `reverse_etl_<name>[_<role>]` is derived (naming policy: minted names
are derived, owned names are typed). The full attribute surface and examples:
docs/poc/REVERSE_ETL_COMPONENT_GUIDE.md.

## The env contract

The components declare **no Dagster resources** — one defs.yaml deploys to staging and
production, and the environment is the only layer that differs. Connections resolve through
env-fed functions that read `os.environ` **at call time** (never import time), so a
reconfigured process picks values up on the next call:

| Connection | Variables |
|---|---|
| State Postgres (the sync's watermark) | `POSTGRES_HOST` (**required** — no default), `POSTGRES_PORT` (5432), `POSTGRES_DB` (postgres), `POSTGRES_USER` (postgres), `POSTGRES_PASSWORD` ("") |
| Demo schema helpers (Postgres, `postgres` db) | the same variables, 10 s connect timeout |
| RisingWave | `RISINGWAVE_HOST` (frontend-node-0), `RISINGWAVE_PORT` (4566), `RISINGWAVE_DB` (dev), `RISINGWAVE_USER` (root), `RISINGWAVE_PASSWORD` ("") |
| Databricks (delta-rs storage + control plane) | `DBT_DATABRICKS_HOST`, `DATABRICKS_AZURE_TENANT_ID`, `DATABRICKS_AZURE_CLIENT_ID`, `DATABRICKS_AZURE_CLIENT_SECRET` — **service principal, never a SAS token** (a SAS token expires and silently broke every MV refresh once) |
| Kafka output cluster (sync producer, connector, targets) | `KAFKA_OUTPUT_BOOTSTRAP` (**required**), optional `KAFKA_OUTPUT_SASL_USERNAME`/`_MECHANISM`/`_PASSWORD` |
| Kafka demo cluster (topic asset) | `KAFKA_BOOTSTRAP_SERVERS` + the cluster's `KAFKA_*_SASL_*` |
| Connector JDBC coordinates | `HOST_POSTGRES_URL`, `POSTGRES_USER`; the password stays the literal `${env:POSTGRES_PASSWORD}`, resolved by the Connect worker — it never passes through Dagster |
| Kafka Connect REST | `KAFKA_CONNECT_URL` at the call site |
| Schema registry | none — the URL is a config field with an org-specific default |

The rule the table follows: **names and decisions in files, values in the environment.**
Everything env-adjacent that is not a credential stays in YAML or config; no
environment-specific value ever lives in a component YAML (one defs.yaml deploys to stg and
prd; a filled-in URL would make a branch deploy write to production's database).

## The boundary rule

The library imports nothing from any team's project and nothing demo-shaped — no demo
assets, no demo hooks, no change/reset scaffolding. Enforced in this repo by
`tests/components/hygiene/test_import_hygiene.py` (the released components' import graph
must not reach `reverse_etl_demo_*` / `reverse_etl_change` / `reverse_etl_reset`), and stated
as policy in the workspace: official `dagster-*` packages provide integration mechanics;
internal libraries provide Kaizen-specific behavior. The components are wiring-agnostic —
`build_defs()` runs identically under `Definitions.merge` + `load_defs` (this PoC) and
`@definitions` + `load_from_defs_folder` (the workspace), pinned by
`tests/components/compat/test_defs_folder_load.py`.
