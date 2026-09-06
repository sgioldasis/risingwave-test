{#
  Macro: drop_prebuild_sinks
  Purpose: Drops sink objects that must be recreated on each build to avoid stale
  IF NOT EXISTS definitions from previous runs.
#}

{% macro drop_prebuild_sinks(sink_names=none) %}
  {% set default_sink_names = [
    'rw_managed_funnel_sink',
    'sink_hermes_features_to_iceberg',
    'funnel_kafka_sink',
    'funnel_postgres_sink',
    'sink_funnel_to_databricks',
    'sink_casino_real_bet',
    'sink_turnover_percentage',
    'sink_casino_real_bet_kafka',
    'sink_turnover_percentage_kafka',
    'sink_casino_transactions_databricks',
    'sink_sportsbook_bets_databricks'
  ] %}
  {% set sinks_to_drop = sink_names if sink_names is not none else default_sink_names %}
    {% set drop_sinks_sql %}
    {% for sink_name in sinks_to_drop %}
    DROP SINK IF EXISTS {{ adapter.quote(sink_name) }} CASCADE;
    {% endfor %}
    {% endset %}

    {% do run_query(drop_sinks_sql) %}
  {{ log("✓ Dropped pre-build sinks: " ~ sinks_to_drop | join(", "), info=True) }}
{% endmacro %}
