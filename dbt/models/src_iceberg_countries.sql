{{ config(
    materialized='risingwave_source',
    tags=['risingwave'],
    schema='public',
    persist_docs={"relation": true, "columns": true},
    connector='iceberg',
    catalog_uri='http://lakekeeper:8181/catalog',
    warehouse_path='risingwave-warehouse',
    database_name='analytics',
    table_name='iceberg_countries',
    s3_endpoint='http://minio-0:9301',
    s3_access_key='hummockadmin',
    s3_secret_key='hummockadmin'
) }}

-- RisingWave native Iceberg source that reads from the Iceberg table
-- Created by the iceberg_countries Dagster asset via Trino
-- This source automatically reflects changes made to the Iceberg table

SELECT
    'country' as country,
    'country_name' as country_name
WHERE 1=0
