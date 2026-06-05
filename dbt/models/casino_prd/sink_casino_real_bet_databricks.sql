{{ config(
    materialized='sink',
    tags=['casino_uc1', 'databricks']
) }}

CREATE SINK IF NOT EXISTS sink_casino_real_bet_databricks
FROM {{ ref('mv_casino_real_bet') }}
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
    table.name                           = 'rw_casino_real_bet',
    adlsgen2.account_name                = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.account_key                 = '{{ env_var("ADLS_ACCOUNT_KEY") }}',
    commit_checkpoint_interval           = 20,
    compaction.write_parquet_compression = 'zstd'
)
