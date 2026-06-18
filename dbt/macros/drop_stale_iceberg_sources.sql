{#
  Macro: drop_stale_iceberg_sources
  Purpose: Drops the Iceberg FULL_RELOAD table and its dependents.
           Use on-demand to force recreation with new connection settings.
  Idempotent: Yes (uses IF EXISTS)
#}

{% macro drop_stale_iceberg_sources() %}
    {# Drop legacy objects from previous architectures (may or may not exist) #}
    {% do run_query("DROP VIEW IF EXISTS funnel_enriched CASCADE") %}
    {% do run_query("DROP VIEW IF EXISTS funnel_summary_with_country CASCADE") %}
    {% do run_query("DROP MATERIALIZED VIEW IF EXISTS funnel_summary_with_country CASCADE") %}
    {% do run_query("DROP VIEW IF EXISTS rw_countries CASCADE") %}
    {% do run_query("DROP MATERIALIZED VIEW IF EXISTS rw_countries CASCADE") %}
    {% do run_query("DROP TABLE IF EXISTS iceberg_countries_ref CASCADE") %}
    {# Drop the Iceberg source (FULL_RELOAD TABLE registers as table, plain SOURCE as source) #}
    {% do run_query("DROP SOURCE IF EXISTS src_iceberg_countries CASCADE") %}
    {% do run_query("DROP TABLE IF EXISTS src_iceberg_countries CASCADE") %}
    {{ log("✓ Dropped stale RisingWave Iceberg objects and dependents", info=True) }}
{% endmacro %}
