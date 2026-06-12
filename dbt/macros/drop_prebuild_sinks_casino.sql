{#
  Macro: drop_prebuild_sinks_casino
  Purpose: Drops only casino-related sink objects that must be recreated on each
  casino build to refresh IF NOT EXISTS definitions from previous runs.
  Important: This intentionally excludes realtime funnel sinks such as
  funnel_kafka_sink and funnel_postgres_sink that power the modern dashboard.
#}

{% macro drop_prebuild_sinks_casino() %}
    {% set drop_sinks_sql %}
        DROP SINK IF EXISTS sink_casino_real_bet CASCADE;
        DROP SINK IF EXISTS sink_turnover_percentage CASCADE;
        DROP SINK IF EXISTS sink_casino_real_bet_kafka CASCADE;
        DROP SINK IF EXISTS sink_turnover_percentage_kafka CASCADE;

        DROP SINK IF EXISTS sink_casino_transactions_databricks CASCADE;
        DROP SINK IF EXISTS sink_sportsbook_bets_databricks CASCADE;
        DROP SINK IF EXISTS sink_casino_landing_databricks CASCADE;
        DROP SINK IF EXISTS sink_sportsbook_landing_databricks CASCADE;

        DROP SINK IF EXISTS sink_casino_transactions_lakekeeper CASCADE;
        DROP SINK IF EXISTS sink_sportsbook_bets_lakekeeper CASCADE;
        DROP SINK IF EXISTS sink_casino_landing_lakekeeper CASCADE;
        DROP SINK IF EXISTS sink_sportsbook_landing_lakekeeper CASCADE;
    {% endset %}

    {% do run_query(drop_sinks_sql) %}
    {{ log("✓ Dropped casino pre-build sinks (if they existed)", info=True) }}
{% endmacro %}