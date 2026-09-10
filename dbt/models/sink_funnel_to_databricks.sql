{#
  Model: sink_funnel_to_databricks
  Purpose: Append finalized funnel windows to a Unity Catalog Managed Iceberg table.
  UC external writers must use append-only semantics; updates are collapsed downstream.

  partition_by='window_date' added 2026-09-07 -- funnel_summary_historical
  was migrated to a day-partitioned table (existing 7290 rows CTAS'd into
  funnel_summary_historical_v2, then swapped into place) so StarRocks/Trino
  queries filtered by date range can prune to matching partitions instead
  of always scanning the whole table (confirmed via EXPLAIN ANALYZE this
  was costing 1.2-3s of real Iceberg scan time per query even at today's
  small ~7k-row volume -- see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md).
  window_date is a plain column (funnel_for_iceberg.sql), not a hidden
  day(window_start) transform, because Databricks' managed-Iceberg
  CREATE TABLE rejects expression-based PARTITIONED BY clauses.
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['databricks', 'iceberg', 'funnel']
) }}

CREATE SINK IF NOT EXISTS sink_funnel_to_databricks
FROM {{ ref('funnel_for_iceberg') }}
WITH (
    connector = 'iceberg',
    type = 'append-only',
    force_append_only = 'true',
    catalog.type = 'rest',
    catalog.uri = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri = 'https://login.microsoftonline.com/{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}/oauth2/v2.0/token',
    catalog.credential = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}:{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    catalog.scope = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path = '{{ env_var("DATABRICKS_CATALOG", "de_dev") }}',
    database.name = 'sr_poc_external',
    table.name = 'funnel_summary_historical',
    partition_by = 'window_date',
    adlsgen2.account_name = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.tenant_id = '{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}',
    adlsgen2.client_id = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}',
    adlsgen2.client_secret = '{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    -- 15 checkpoints * (barrier_interval_ms=2000 * checkpoint_frequency=2 =
    -- 4s/checkpoint) = ~60s, aligning the commit cadence with
    -- funnel_summary's own 1-minute tumbling window instead of the
    -- previous commit_checkpoint_interval=20 (~80s, an arbitrary interval
    -- relative to the data's natural granularity). Changed 2026-09-10
    -- alongside the hot/cold cutoff fix in dashboard_funnel_serving.sql --
    -- see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md.
    commit_checkpoint_interval = 15
)
