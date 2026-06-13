{{ config(
    materialized='sink',
    tags=['databricks']
) }}

-- Append-only sink of the per-event rolling-turnover snapshots into Databricks
-- Unity Catalog. mv_casino_turnover_90d emits one immutable row per
-- (customer_id, event_ts): a later transaction never alters an earlier row's
-- rolling sum (each row's window is [t-window, t]), so force_append_only drops
-- nothing meaningful — it just coerces RisingWave's retract stream to INSERTs.
--
-- Upsert / "latest row per customer" semantics are applied downstream by the
-- Databricks view v_casino_turnover_latest (QUALIFY ROW_NUMBER), NOT here — UC
-- does not support Iceberg delete files, so an upsert sink stalls silently.
-- See docs/poc/DATABRICKS_ICEBERG_SINK.md §16.

CREATE SINK IF NOT EXISTS sink_casino_turnover_90d_databricks
FROM {{ ref('mv_casino_turnover_90d') }}
WITH (
    connector                            = 'iceberg',
    type                                 = 'append-only',
    force_append_only                    = 'true',
    catalog.type                         = 'rest',
    catalog.uri                          = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri            = 'https://login.microsoftonline.com/{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}/oauth2/v2.0/token',
    catalog.credential                   = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}:{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    catalog.scope                        = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path                       = '{{ env_var("DATABRICKS_CATALOG") }}',
    database.name                        = 'rw_poc',
    table.name                           = 'rw_casino_turnover_90d',
    adlsgen2.account_name                = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.account_key                 = '{{ env_var("ADLS_ACCOUNT_KEY") }}',
    commit_checkpoint_interval           = 5
)
