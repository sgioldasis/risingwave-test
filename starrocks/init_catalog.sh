#!/bin/sh
set -e

: "${DATABRICKS_AZURE_CLIENT_ID:?DATABRICKS_AZURE_CLIENT_ID must be set}"
: "${DATABRICKS_AZURE_TENANT_ID:?DATABRICKS_AZURE_TENANT_ID must be set}"
: "${DATABRICKS_AZURE_CLIENT_SECRET:?DATABRICKS_AZURE_CLIENT_SECRET must be set}"

apk add --no-cache mysql-client >/dev/null

# Unset any injected proxy so requests to Databricks go direct
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY
export NO_PROXY='*' no_proxy='*'

echo "Waiting for StarRocks MySQL port (starrocks:9030)..."
for i in $(seq 1 30); do
  if mysql -h starrocks -P 9030 -u root --connect-timeout=3 -e "SELECT 1" >/dev/null 2>&1; then
    echo "StarRocks is ready."
    break
  fi
  [ "$i" -eq 30 ] && echo "StarRocks never became ready after 30 attempts." >&2 && exit 1
  sleep 5
done

echo "Creating default storage volume (shared-data, backed by MinIO)..."
mysql -h starrocks -P 9030 -u root <<SQL
CREATE STORAGE VOLUME IF NOT EXISTS minio_default
    TYPE = S3
    LOCATIONS = ('s3://starrocks/')
    PROPERTIES (
        "aws.s3.endpoint" = "http://minio-0:9301",
        "aws.s3.region" = "us-east-1",
        "aws.s3.access_key" = "hummockadmin",
        "aws.s3.secret_key" = "hummockadmin",
        "aws.s3.enable_path_style_access" = "true",
        "aws.s3.use_aws_sdk_default_behavior" = "false",
        "enabled" = "true"
    );
SET minio_default AS DEFAULT STORAGE VOLUME;
SQL

echo "Creating external catalog databricks_uc..."
# The heredoc is unquoted (<<SQL) so ${VAR} expands from the container environment.
mysql -h starrocks -P 9030 -u root <<SQL
DROP CATALOG IF EXISTS databricks_uc;
CREATE EXTERNAL CATALOG databricks_uc
COMMENT "Databricks Unity Catalog de_dev via Iceberg REST"
PROPERTIES (
    "type"                          = "iceberg",
    "iceberg.catalog.type"          = "rest",
    "iceberg.catalog.uri"           = "https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest",
    "iceberg.catalog.warehouse"     = "de_dev",
    "iceberg.catalog.credential"    = "${DATABRICKS_AZURE_CLIENT_ID}:${DATABRICKS_AZURE_CLIENT_SECRET}",
    "iceberg.catalog.oauth2-server-uri" = "https://login.microsoftonline.com/${DATABRICKS_AZURE_TENANT_ID}/oauth2/v2.0/token",
    "iceberg.catalog.scope"         = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
    "iceberg_meta_cache_ttl_sec"    = "0"
);
SQL

echo "Creating local Lakekeeper catalog (lakekeeper_local)..."
mysql -h starrocks -P 9030 -u root <<SQL
DROP CATALOG IF EXISTS lakekeeper_local;
CREATE EXTERNAL CATALOG lakekeeper_local
COMMENT "Local Lakekeeper REST catalog backed by MinIO"
PROPERTIES (
    "type"                      = "iceberg",
    "iceberg.catalog.type"      = "rest",
    "iceberg.catalog.uri"       = "http://lakekeeper:8181/catalog/",
    "iceberg.catalog.warehouse" = "risingwave-warehouse",
    "aws.s3.endpoint"           = "http://minio-0:9301",
    "aws.s3.access_key"         = "hummockadmin",
    "aws.s3.secret_key"         = "hummockadmin",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.region"             = "us-east-1"
);
SQL

echo "Creating RisingWave JDBC catalog (risingwave)..."
mysql -h starrocks -P 9030 -u root <<SQL
DROP CATALOG IF EXISTS risingwave;
CREATE EXTERNAL CATALOG risingwave
COMMENT "RisingWave PostgreSQL JDBC federation"
PROPERTIES (
  "type"            = "jdbc",
  "user"            = "root",
  "password"        = "root",
  "jdbc_uri"        = "jdbc:postgresql://frontend-node-0:4566/dev",
  "driver_url"      = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar",
  "driver_class"    = "org.postgresql.Driver",
  "schema_resolver" = "postgresql"
);
SQL

# NOTE: this project tried three different mechanisms to get "hot" RisingWave
# data into StarRocks without a live per-query JDBC touch: a 10s-refresh
# local MV mirror, a StarRocks Routine Load job off RisingWave's `funnel`
# Kafka topic (hard 5s minimum batch interval, StarRocks-enforced), and a
# native RisingWave StarRocks sink (Stream Load-based, no floor, but
# every-checkpoint flushing overloaded compaction on the receiving table and
# made query latency worse). All were reverted 2026-09-07 in favor of the
# original live JDBC SELECT (see dbt_starrocks/models/dashboard_funnel_serving.sql
# and docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md for the full history) --
# do not re-add a hot_funnel_kafka table/Routine Load job/native sink here
# without reading that history first.

echo "Verifying catalogs..."
mysql -h starrocks -P 9030 -u root -e "SHOW CATALOGS LIKE 'databricks_uc'; SHOW CATALOGS LIKE 'lakekeeper_local'; SHOW CATALOGS LIKE 'risingwave';"
echo "StarRocks init complete."
