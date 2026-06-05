{#
  Macro: create_databricks_connection
  Purpose: Creates the Iceberg connection to Databricks Unity Catalog REST API.
  Usage: Called in on-run-start before any models execute.
#}

{% macro create_databricks_connection() %}
    {% set token = env_var("DATABRICKS_TOKEN", "") %}
    {% if not token %}
        {{ log("⚠ DATABRICKS_TOKEN not set — skipping Databricks connection (Databricks sinks will be excluded from this run)", info=True) }}
    {% else %}
        {% set create_connection_sql %}
            CREATE CONNECTION IF NOT EXISTS databricks_uc_conn
            WITH (
                type               = 'iceberg',
                catalog.type       = 'rest',
                catalog.uri        = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
                catalog.token      = '{{ token }}',
                warehouse.path     = '{{ env_var("DATABRICKS_CATALOG") }}'
            );
        {% endset %}
        {% do run_query(create_connection_sql) %}
        {{ log("✓ Databricks UC connection 'databricks_uc_conn' created/verified", info=True) }}
    {% endif %}
{% endmacro %}
