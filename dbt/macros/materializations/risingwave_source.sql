{#
  Custom materialization for RisingWave sources.
  Supports both S3/MinIO (local Lakekeeper) and Azure ADLS Gen2 (Databricks Unity Catalog).
  Pass s3_endpoint for S3; pass oauth2_server_uri for Azure — the unused block is omitted.
#}
{% materialization risingwave_source, adapter='risingwave' %}
  {%- set connector      = config.require('connector') -%}
  {%- set catalog_type   = config.get('catalog_type', 'rest') -%}
  {%- set catalog_uri    = config.require('catalog_uri') -%}
  {%- set warehouse_path = config.require('warehouse_path') -%}
  {%- set database_name  = config.require('database_name') -%}
  {%- set table_name     = config.require('table_name') -%}
  {%- set source_name    = config.get('source_name', this.identifier) -%}

  {# S3 / MinIO params (optional) #}
  {%- set s3_endpoint         = config.get('s3_endpoint') -%}
  {%- set s3_region           = config.get('s3_region', 'us-east-1') -%}
  {%- set s3_access_key       = config.get('s3_access_key') -%}
  {%- set s3_secret_key       = config.get('s3_secret_key') -%}
  {%- set s3_path_style_access = config.get('s3_path_style_access', 'false') -%}

  {# Azure ADLS Gen2 / OAuth2 params (optional) #}
  {%- set oauth2_server_uri      = config.get('oauth2_server_uri') -%}
  {%- set catalog_credential     = config.get('catalog_credential') -%}
  {%- set catalog_scope          = config.get('catalog_scope') -%}
  {%- set adlsgen2_account_name  = config.get('adlsgen2_account_name') -%}
  {%- set adlsgen2_account_key   = config.get('adlsgen2_account_key') -%}

  {%- set target_relation = this.incorporate(type='view') -%}

  {% if flags.FULL_REFRESH %}
    {% call statement('drop_source') %}
      DROP SOURCE IF EXISTS {{ source_name }} CASCADE;
    {% endcall %}
    {{ log("Dropped source " ~ source_name ~ " for full refresh", info=True) }}
  {% endif %}

  {% call statement('main') %}
    CREATE SOURCE IF NOT EXISTS {{ source_name }}
    WITH (
        connector      = '{{ connector }}',
        catalog.type   = '{{ catalog_type }}',
        catalog.uri    = '{{ catalog_uri }}',
        warehouse.path = '{{ warehouse_path }}',
        database.name  = '{{ database_name }}',
        table.name     = '{{ table_name }}'
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

  {{ log("✓ Source '" ~ source_name ~ "' created", info=True) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
