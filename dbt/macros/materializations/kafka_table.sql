{#
  Materialization: kafka_table
  Purpose: Creates RisingWave tables with Kafka connector for modifiable streaming data
  Usage: {{ config(materialized='kafka_table') }}

  Unlike CREATE SOURCE, these tables:
  - Store data internally (not just a connector)
  - Support UPDATE and DELETE operations
  - Can have PRIMARY KEY for upserts

  Config options:
    - topic (required): Kafka topic name
    - primary_key (optional): Column name for PRIMARY KEY
    - bootstrap_servers (optional): Default 'redpanda:9092'
    - scan_startup_mode (optional): Default 'earliest'
    - table_name (optional): Override the table name
#}
{% materialization kafka_table, adapter='risingwave' %}

    {# Get config values #}
    {%- set topic = config.require('topic') -%}
    {%- set primary_key = config.get('primary_key', none) -%}
    {%- set bootstrap_servers = config.get('bootstrap_servers', 'redpanda:9092') -%}
    {%- set scan_startup_mode = config.get('scan_startup_mode', 'earliest') -%}
    {%- set table_name = config.get('table_name', this.identifier) -%}

    {# Get the target relation #}
    {%- set target_relation = this.incorporate(type='table') -%}

    {# Drop and recreate for full refresh #}
    {% if flags.FULL_REFRESH %}
        {% call statement('drop_table') %}
            DROP TABLE IF EXISTS {{ table_name }} CASCADE;
        {% endcall %}
        {{ log("Dropped table " ~ table_name ~ " for full refresh", info=True) }}
    {% endif %}

    {# Build the CREATE TABLE SQL from the model's SQL #}
    {%- set create_table_sql -%}
        {{ sql }}
    {%- endset -%}

    {{ log("Creating Kafka table: " ~ table_name, info=True) }}

    {# Execute the create table statement #}
    {% call statement('main') %}
        {{ create_table_sql }}
    {% endcall %}

    {{ log("✓ Kafka table '" ~ table_name ~ "' created successfully", info=True) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
