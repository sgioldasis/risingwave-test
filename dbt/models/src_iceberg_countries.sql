-- Disabled 2026-09-07: leftover RisingWave-native-Iceberg-source experiment
-- (see disabled companions rw_countries/mv_countries_from_iceberg in
-- .disabled/) with zero downstream consumers in the dbt graph (confirmed via
-- target/graph_summary.json -- no other model has this as a dependency).
-- Points at the local Lakekeeper-backed iceberg_countries table, which no
-- longer exists -- that table moved to Databricks Unity Catalog
-- (de_dev.sr_poc_external.iceberg_countries, see
-- docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md), and country lookups now go
-- through StarRocks' databricks_uc catalog instead of a RisingWave-native
-- source. Broke modern_dashboard_setup_job outright on a fresh stack
-- ("Failed to load iceberg table... Error getting tabular from catalog").
{{ config(
    enabled=false,
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
