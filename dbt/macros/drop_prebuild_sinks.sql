{#
  Macro: drop_prebuild_sinks
  Purpose: Drops sink objects that must be recreated on each build to avoid stale
  IF NOT EXISTS definitions from previous runs.
#}

{% macro drop_prebuild_sinks() %}
    {% set drop_sinks_sql %}
        DROP SINK IF EXISTS iceberg_funnel_sink CASCADE;
        DROP SINK IF EXISTS rw_managed_funnel_sink CASCADE;
    DROP SINK IF EXISTS sink_hermes_features_to_iceberg CASCADE;
    DROP SINK IF EXISTS funnel_kafka_sink CASCADE;
    DROP SINK IF EXISTS funnel_postgres_sink CASCADE;
    {% endset %}

    {% do run_query(drop_sinks_sql) %}
    {{ log("✓ Dropped pre-build sinks (if they existed)", info=True) }}
{% endmacro %}
