{#
  Macro: create_watermark_task
  Purpose: (Re)create the StarRocks scheduled task that lands closed funnel
  windows into databricks_uc.sr_poc_external.funnel_summary_historical.
  This task is the RW-free data path for the historical table -- the Dagster
  watermark asset's schedule is retired (default STOPPED); the task owns it.

  Why a task and not a Dagster schedule: it lives in the StarRocks FE, so
  the historical table keeps updating even with Dagster down. dbt has no
  task materialization, so it's created idempotently here (DROP IF EXISTS +
  SUBMIT) on every dbt run -- which is what makes demo resets self-healing.

  The INSERT body is byte-identical to the one the Dagster asset
  (orchestration/assets/funnel_historical_watermark.py) runs: MAX-watermark
  + per-window NOT EXISTS, so the two writers are interchangeable and even
  coexist safely. The task name is db-qualified (sr_local_db)
  because SUBMIT TASK lands in the session database and dbt's session may
  not carry a USE context.
#}

{% macro create_watermark_task() %}
    {% set task_name = 'sr_local_db.funnel_historical_watermark' %}
    {% set target = 'databricks_uc.sr_poc_external.funnel_summary_historical' %}
    {% set source_view = 'sr_local_db.funnel_summary_direct' %}

    {% if execute %}
        {# The task's INSERT reads source_view -- SUBMIT TASK validates the
           body against the catalog, so on a fresh cluster (view not built
           yet in this same run) the submission must be skipped, not fired
           blind. information_schema.tables is NOT reliable for this on
           StarRocks 4.1.7 shared-data (it lists only a fraction of the
           relations -- verified 2026-11-17), so check via SHOW TABLES.
           Hook runs in on-run-end, so the view normally exists; the skip
           is only for failed/reordered runs -- the next successful dbt
           run (or the funnel_historical_watermark asset) recreates it. #}
        {% set exists_result = run_query("SHOW TABLES FROM sr_local_db LIKE 'funnel_summary_direct'") %}
        {% set view_exists = exists_result | length > 0 %}

        {% if view_exists %}
    {% set drop_sql %}
        DROP TASK IF EXISTS {{ task_name }}
    {% endset %}
    {% do run_query(drop_sql) %}

    {% set submit_sql %}
        SUBMIT TASK {{ task_name }} SCHEDULE EVERY(INTERVAL 1 MINUTE) AS
        INSERT INTO {{ target }}
        SELECT
            v.window_start,
            v.window_end,
            v.country,
            v.viewers,
            v.carters,
            v.purchasers,
            v.view_to_cart_rate,
            v.cart_to_buy_rate,
            DATE(v.window_start) AS window_date
        FROM {{ source_view }} v
        WHERE v.window_start > COALESCE(
                (SELECT MAX(window_start) FROM {{ target }}),
                TIMESTAMP('1970-01-01 00:00:00'))
          AND v.window_start < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
          AND NOT EXISTS (
                SELECT 1 FROM {{ target }} t
                WHERE t.window_start = v.window_start AND t.country = v.country)
    {% endset %}
    {% do run_query(submit_sql) %}

    {{ log("✓ watermark task 'funnel_historical_watermark' recreated (every 1 minute)", info=True) }}
        {% else %}
    {{ log("⚠ view sr_local_db.funnel_summary_direct does not exist yet -- watermark task not (re)created this run; the next successful dbt run or the funnel_historical_watermark asset will create it", info=True) }}
        {% endif %}
    {% endif %}
{% endmacro %}
