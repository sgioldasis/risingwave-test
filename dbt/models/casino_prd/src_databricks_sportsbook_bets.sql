{{ config(
    materialized='risingwave_source',
    tags=['databricks'],
    schema='public',
    connector='iceberg',
    catalog_uri=env_var('DBT_DATABRICKS_HOST') ~ '/api/2.1/unity-catalog/iceberg-rest',
    warehouse_path=env_var('DATABRICKS_CATALOG'),
    database_name='rw_poc',
    table_name='rw_sportsbook_bets',
    oauth2_server_uri='https://login.microsoftonline.com/' ~ env_var('DATABRICKS_AZURE_TENANT_ID') ~ '/oauth2/v2.0/token',
    catalog_credential=env_var('DATABRICKS_AZURE_CLIENT_ID') ~ ':' ~ env_var('DATABRICKS_AZURE_CLIENT_SECRET'),
    catalog_scope='2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    adlsgen2_account_name=env_var('ADLS_ACCOUNT_NAME'),
    adlsgen2_account_key=env_var('ADLS_ACCOUNT_KEY')
) }}
-- depends_on: {{ ref('sink_sportsbook_bets_databricks') }}
SELECT 'bet_id' AS bet_id WHERE 1=0
