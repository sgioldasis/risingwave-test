{# This macro creates Python UDFs from a non-Jinja2 SQL file #}
{# Call with: dbt run-operation create_udfs_from_file #}

{% macro create_udfs_from_file() %}
    {% set sql_file_path = project_dir ~ '/sql/create_udfs.sql' %}
    
    {# Read file content - this happens at runtime, not parse time #}
    {% set udf_sql = modules.os.popen('cat "' ~ sql_file_path ~ '" 2>/dev/null').read() %}
    
    {% if udf_sql %}
        {% do run_query(udf_sql) %}
        {% do log("Python UDFs created successfully from " ~ sql_file_path, info=true) %}
    {% else %}
        {% do log("ERROR: Could not read " ~ sql_file_path, info=true) %}
    {% endif %}
{% endmacro %}

{% macro create_udfs_via_macro() %}
    {{ create_udfs_from_file() }}
{% endmacro %}
