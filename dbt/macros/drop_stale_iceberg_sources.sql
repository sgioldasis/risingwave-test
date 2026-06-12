{#
  Macro: drop_stale_iceberg_sources
  Purpose: Drops RisingWave Iceberg sources and their dependent views.
           Use this only for explicit refreshes (e.g. endpoint/config migration).
           Normal runs should not drop sources on every materialization.
           Addresses issue: "failed to lookup address information: Name or service not known"
  
  When to use: Run on-demand to force source recreation with new connection settings.
  Idempotent: Yes (uses IF EXISTS)
  
  Flow:
  1. This macro drops RisingWave views and sources that depend on Iceberg
  2. On dbt build, on-run-start hooks recreate Iceberg connection + sources
#}

{% macro drop_stale_iceberg_sources() %}
    {% set drop_sources_sql %}
        -- Drop views that depend on Iceberg sources first
        DROP VIEW IF EXISTS rw_countries CASCADE;
        DROP VIEW IF EXISTS funnel_summary_with_country CASCADE;
        
        -- Drop RisingWave source so it is recreated on next dbt build
        DROP SOURCE IF EXISTS src_iceberg_countries CASCADE;
    {% endset %}

    {% do run_query(drop_sources_sql) %}
    {{ log("✓ Dropped stale RisingWave Iceberg sources and views", info=True) }}
{% endmacro %}
