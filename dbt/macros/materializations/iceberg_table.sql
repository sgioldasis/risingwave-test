{#
  Materialization: iceberg_table
  Purpose: Creates Iceberg tables in RisingWave using the Lakekeeper catalog
  Usage: {{ config(materialized='iceberg_table') }}
#}

{% materialization iceberg_table, adapter='risingwave' %}

    {# Create the Iceberg connection first #}
    {{ create_iceberg_connection() }}

    {# Get the target relation #}
    {%- set target_relation = this.incorporate(type='table') -%}
    
    {# Check if the relation already exists #}
    {%- set existing_relation = load_relation(this) -%}
    
    {# Build the create table SQL #}
    {%- set create_table_sql -%}
        {{ sql }}
    {%- endset -%}
    
    {{ log("Creating Iceberg table: " ~ this.identifier, info=True) }}
    
    {# Execute the create table statement #}
    {% call statement('main') %}
        {{ create_table_sql }}
    {% endcall %}
    
    {{ log("✓ Iceberg table '" ~ this.identifier ~ "' created successfully", info=True) }}
    
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}