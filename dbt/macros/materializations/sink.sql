{#
  Materialization: sink
  Purpose: Creates RisingWave sinks from materialized views or tables to Iceberg tables
  Usage: {{ config(materialized='sink') }}
#}

{% materialization sink, adapter='risingwave' %}

    {# Ensure Iceberg connection exists #}
    {{ create_iceberg_connection() }}

    {# Get the target relation #}
    {%- set target_relation = this.incorporate(type='table') -%}
    
    {# Build the create sink SQL #}
    {%- set create_sink_sql -%}
        {{ sql }}
    {%- endset -%}
    
    {{ log("Creating sink: " ~ this.identifier, info=True) }}
    
    {# Execute the create sink statement #}
    {% call statement('main') %}
        {{ create_sink_sql }}
    {% endcall %}
    
    {{ log("✓ Sink '" ~ this.identifier ~ "' created successfully", info=True) }}
    
    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}