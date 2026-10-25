{#
  Materialization for a RisingWave Iceberg-backed TABLE with FULL_RELOAD mode.
  Use this for batch reference/lookup tables that need to be joined with streams.

  RisingWave 3.0 made DataFusion the default batch engine. CREATE SOURCE with
  connector='iceberg' is DataFusion-backed, which means CREATE VIEW over it fails:
    "Expected RisingWave plan in BatchPlanChoice, but got DataFusion plan"
  A FULL_RELOAD TABLE stores data inside RisingWave (native storage), so the batch
  planner queries it normally and VIEWs over it work. Use with temporal joins
  (FOR SYSTEM_TIME AS OF PROCTIME()) for canonical stream-batch unification.

  After creation, REFRESH TABLE is called immediately for the initial load.
  Set refresh_interval_sec to enable automatic periodic reloads.
#}
{% materialization risingwave_iceberg_table, adapter='risingwave' %}
  {%- set connector      = config.require('connector') -%}
  {%- set catalog_type   = config.get('catalog_type', 'rest') -%}
  {%- set catalog_uri    = config.require('catalog_uri') -%}
  {%- set warehouse_path = config.require('warehouse_path') -%}
  {%- set database_name  = config.require('database_name') -%}
  {%- set table_name     = config.require('table_name') -%}
  {%- set source_name    = config.get('source_name', this.identifier) -%}
  {%- set refresh_interval_sec = config.get('refresh_interval_sec') -%}
  {%- set primary_key          = config.get('primary_key') -%}

  {# S3 / MinIO params (optional) #}
  {%- set s3_endpoint          = config.get('s3_endpoint') -%}
  {%- set s3_region            = config.get('s3_region', 'us-east-1') -%}
  {%- set s3_access_key        = config.get('s3_access_key') -%}
  {%- set s3_secret_key        = config.get('s3_secret_key') -%}
  {%- set s3_path_style_access = config.get('s3_path_style_access', 'false') -%}

  {# Azure ADLS Gen2 / OAuth2 params (optional) #}
  {%- set oauth2_server_uri     = config.get('oauth2_server_uri') -%}
  {%- set catalog_credential    = config.get('catalog_credential') -%}
  {%- set catalog_scope         = config.get('catalog_scope') -%}
  {%- set adlsgen2_account_name = config.get('adlsgen2_account_name') -%}
  {%- set adlsgen2_account_key  = config.get('adlsgen2_account_key') -%}

  {%- set target_relation = this.incorporate(type='table') -%}

  {% call statement('drop_source') %}
    DROP SOURCE IF EXISTS {{ source_name }} CASCADE;
  {% endcall %}
  {% call statement('drop_table') %}
    DROP TABLE IF EXISTS {{ source_name }} CASCADE;
  {% endcall %}
  {{ log("Dropped source/table " ~ source_name ~ " (will recreate)", info=True) }}

  {% call statement('main') %}
    CREATE TABLE {{ source_name }}
    {%- if primary_key %}
    ( PRIMARY KEY ({{ primary_key }}) )
    {%- endif %}
    WITH (
        connector      = '{{ connector }}',
        catalog.type   = '{{ catalog_type }}',
        catalog.uri    = '{{ catalog_uri }}',
        warehouse.path = '{{ warehouse_path }}',
        database.name  = '{{ database_name }}',
        table.name     = '{{ table_name }}',
        refresh_mode   = 'FULL_RELOAD'
        {%- if refresh_interval_sec %},
        refresh_interval_sec = '{{ refresh_interval_sec }}'
        {%- endif %}
        {%- if s3_endpoint %},
        s3.endpoint              = '{{ s3_endpoint }}',
        s3.region                = '{{ s3_region }}',
        s3.access.key            = '{{ s3_access_key }}',
        s3.secret.key            = '{{ s3_secret_key }}',
        s3.path.style.access     = '{{ s3_path_style_access }}'
        {%- endif %}
        {%- if oauth2_server_uri %},
        catalog.oauth2_server_uri = '{{ oauth2_server_uri }}',
        catalog.credential        = '{{ catalog_credential }}',
        catalog.scope             = '{{ catalog_scope }}',
        adlsgen2.account_name     = '{{ adlsgen2_account_name }}',
        adlsgen2.account_key      = '{{ adlsgen2_account_key }}'
        {%- endif %}
    );
  {% endcall %}
  {{ log("✓ Table '" ~ source_name ~ "' created — triggering initial REFRESH", info=True) }}

  {% call statement('refresh') %}
    REFRESH TABLE {{ source_name }};
  {% endcall %}
  {{ log("✓ REFRESH TABLE '" ~ source_name ~ "' complete", info=True) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
