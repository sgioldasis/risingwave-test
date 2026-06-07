#!/bin/sh
set -e
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

echo "Creating external catalog databricks_uc..."
# The heredoc is unquoted (<<SQL) so ${VAR} expands from the container environment.
# azure.adls2 properties are included as fallback if credential vending doesn't pick up
# the SAS token automatically; remove them once vending is confirmed working.
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
    "iceberg.catalog.oauth2-server-uri" = "https://login.microsoftonline.com/78395483-9425-447a-ba64-60b90f6bb16e/oauth2/v2.0/token",
    "iceberg.catalog.scope"         = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
    "hadoop.fs.azure.account.key.stkznneurwpoccdddevstd.dfs.core.windows.net" = "${ADLS_ACCOUNT_KEY}"
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

echo "Verifying catalogs..."
mysql -h starrocks -P 9030 -u root -e "SHOW CATALOGS LIKE 'databricks_uc'; SHOW CATALOGS LIKE 'lakekeeper_local';"
echo "StarRocks init complete."
