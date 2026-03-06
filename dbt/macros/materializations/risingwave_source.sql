{#
  Custom materialization for RisingWave sources.
  This creates a SOURCE in RisingWave that connects to external systems like Iceberg.
#}
{% materialization risingwave_source, adapter='risingwave' %}
  {# Get config values #}
  {%- set connector = config.require('connector') -%}
  {%- set catalog_type = config.get('catalog_type', 'rest') -%}
  {%- set catalog_uri = config.require('catalog_uri') -%}
  {%- set warehouse_path = config.require('warehouse_path') -%}
  {%- set database_name = config.require('database_name') -%}
  {%- set table_name = config.require('table_name') -%}
  {%- set s3_endpoint = config.require('s3_endpoint') -%}
  {%- set s3_region = config.get('s3_region', 'us-east-1') -%}
  {%- set s3_access_key = config.require('s3_access_key') -%}
  {%- set s3_secret_key = config.require('s3_secret_key') -%}
  {%- set source_name = config.get('source_name', this.identifier) -%}

  {# Get the relation for this model #}
  {%- set target_relation = this.incorporate(type='view') -%}

  {# Drop and recreate for full refresh #}
  {% if flags.FULL_REFRESH %}
    {% call statement('drop_source') %}
      DROP SOURCE IF EXISTS {{ source_name }} CASCADE;
    {% endcall %}
    {{ log("Dropped source " ~ source_name ~ " for full refresh", info=True) }}
  {% endif %}

  {# Create the source using statement block for proper tracking #}
  {% call statement('main') %}
    CREATE SOURCE IF NOT EXISTS {{ source_name }}
    WITH (
        connector = '{{ connector }}',
        catalog.type = '{{ catalog_type }}',
        catalog.uri = '{{ catalog_uri }}',
        warehouse.path = '{{ warehouse_path }}',
        database.name = '{{ database_name }}',
        table.name = '{{ table_name }}',
        s3.endpoint = '{{ s3_endpoint }}',
        s3.region = '{{ s3_region }}',
        s3.access.key = '{{ s3_access_key }}',
        s3.secret.key = '{{ s3_secret_key }}'
    );
  {% endcall %}

  {{ log("Created source " ~ source_name, info=True) }}

  {# Return the relation for dbt to track #}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
