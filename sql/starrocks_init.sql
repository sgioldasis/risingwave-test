-- StarRocks Initialization Script
-- Sets up connections to Iceberg datalake and RisingWave

-- =====================================================
-- ICEBERG CATALOG CONNECTION
-- =====================================================

-- Create external catalog for Iceberg via REST catalog (Lakekeeper)
CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://lakekeeper:8181/catalog",
    "iceberg.catalog.warehouse" = "risingwave-warehouse",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio-0:9301",
    "aws.s3.access_key" = "hummockadmin",
    "aws.s3.secret_key" = "hummockadmin",
    "aws.s3.region" = "us-east-1"
);

-- Create a database in the Iceberg catalog
CREATE DATABASE IF NOT EXISTS iceberg_catalog.analytics;

-- =====================================================
-- RISINGWAVE CATALOG CONNECTION (via PostgreSQL JDBC)
-- =====================================================

-- Note: RisingWave is PostgreSQL-compatible on port 4566
-- This requires the PostgreSQL JDBC driver to be available
CREATE EXTERNAL CATALOG risingwave_catalog
PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "root",
    "jdbc_uri" = "jdbc:postgresql://frontend-node-0:4566/dev",
    "driver_class" = "org.postgresql.Driver",
    "driver_url" = "file:///opt/starrocks/be/lib/postgresql-42.7.3.jar"
);

-- =====================================================
-- EXAMPLE: Create a materialized view in StarRocks
-- that queries data from Iceberg
-- =====================================================

-- Create a local StarRocks database for analytics
CREATE DATABASE starrocks_analytics;

-- Example: Create an external table pointing to Iceberg
-- (This assumes you have an 'iceberg_countries' table in Iceberg)
-- Uncomment after verifying the table exists:
/*
CREATE EXTERNAL TABLE starrocks_analytics.ext_iceberg_countries
(
    country VARCHAR(10),
    country_name VARCHAR(255)
)
ENGINE = ICEBERG
PROPERTIES (
    "resource" = "iceberg_catalog",
    "database" = "public",
    "table" = "iceberg_countries"
);
*/

-- =====================================================
-- EXAMPLE: Query federation across RisingWave and Iceberg
-- =====================================================

-- Create a view that combines RisingWave real-time data with Iceberg historical
-- Uncomment and modify based on your actual table structures:
/*
CREATE VIEW starrocks_analytics.combined_analytics AS
SELECT 
    r.*,
    i.historical_data
FROM risingwave_catalog.public.funnel_summary r
JOIN iceberg_catalog.public.iceberg_countries i
ON r.country = i.country;
*/

-- =====================================================
-- RISINGWAVE SINK TARGET TABLE
-- =====================================================

-- Create database for RisingWave sink data
CREATE DATABASE IF NOT EXISTS starrocks_analytics;

-- Create table to receive data from RisingWave JDBC sink
-- This table stores funnel analytics data from RisingWave
-- Note: Primary key columns must be first in StarRocks
CREATE TABLE IF NOT EXISTS starrocks_analytics.funnel_summary
(
    window_start DATETIME,
    country VARCHAR(10),
    window_end DATETIME,
    viewers INT,
    carters INT,
    purchasers INT,
    view_to_cart_rate DECIMAL(5,2),
    cart_to_buy_rate DECIMAL(5,2)
)
PRIMARY KEY(window_start, country)
DISTRIBUTED BY HASH(window_start) BUCKETS 10
PROPERTIES ('replication_num' = '1');

-- =====================================================
-- ROUTINE LOAD FROM KAFKA (RisingWave → StarRocks)
-- =====================================================

-- Create a routine load job to consume from RisingWave's Kafka sink
-- This consumes from the 'funnel' topic populated by sink_funnel_to_kafka.sql
CREATE ROUTINE LOAD starrocks_analytics.load_funnel_from_kafka
ON starrocks_analytics.funnel_summary
COLUMNS (window_start, country, window_end, viewers, carters, purchasers, view_to_cart_rate, cart_to_buy_rate)
PROPERTIES (
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5",
    "max_batch_rows" = "200000"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "funnel",
    "kafka_partitions" = "0",
    "kafka_offsets" = "OFFSET_BEGINNING"
);

-- Show configured catalogs
SHOW CATALOGS;
