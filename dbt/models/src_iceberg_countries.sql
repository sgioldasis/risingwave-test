{{ config(
    materialized='risingwave_source',
    tags=['risingwave'],
    schema='public',
    persist_docs={"relation": true, "columns": true},
    meta={
        "dagster": {
            "deps": ["iceberg_countries"]
        }
    },
    connector='iceberg',
    catalog_uri='http://lakekeeper:8181/catalog',
    warehouse_path='risingwave-warehouse',
    database_name='public',
    table_name='iceberg_countries',
    s3_endpoint='http://minio-0:9301',
    s3_access_key='hummockadmin',
    s3_secret_key='hummockadmin',
    s3_path_style_access='true'
) }}

SELECT
    'country' as country,
    'country_name' as country_name
WHERE 1=0
