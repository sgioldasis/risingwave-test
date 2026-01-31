{#
  Macro: create_iceberg_connection
  Purpose: Creates the Iceberg connection to Lakekeeper catalog and sets it as default
  Usage: Run at the beginning of dbt execution or before creating Iceberg tables/sinks
#}

{% macro create_iceberg_connection() %}
    {# Create the Iceberg connection to Lakekeeper #}
    {% set create_connection_sql %}
        CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn
        WITH (
            type = 'iceberg',
            catalog.type = 'rest',
            catalog.uri = 'http://lakekeeper:8181/catalog/',
            warehouse.path = 'risingwave-warehouse',
            s3.access.key = 'hummockadmin',
            s3.secret.key = 'hummockadmin',
            s3.path.style.access = 'true',
            s3.endpoint = 'http://minio-0:9301',
            s3.region = 'us-east-1'
        );
    {% endset %}
    
    {% do run_query(create_connection_sql) %}
    {{ log("✓ Iceberg connection 'lakekeeper_catalog_conn' created/verified", info=True) }}
    
    {# Set the connection as default for this session #}
    {% set set_default_sql %}
        SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';
    {% endset %}
    
    {% do run_query(set_default_sql) %}
    {{ log("✓ Default Iceberg engine connection set", info=True) }}
{% endmacro %}