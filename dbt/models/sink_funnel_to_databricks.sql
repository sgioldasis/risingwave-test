{#
  Model: sink_funnel_to_databricks
  Purpose: Append finalized funnel windows to a Unity Catalog Managed Iceberg table.
  UC external writers must use append-only semantics; updates are collapsed downstream.
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
    adlsgen2.account_name = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.tenant_id = '{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}',
    adlsgen2.client_id = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}',
    adlsgen2.client_secret = '{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    commit_checkpoint_interval = 20
)
