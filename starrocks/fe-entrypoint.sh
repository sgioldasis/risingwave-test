#!/bin/bash
set -e

# Shared-data FE entrypoint (starrocks/fe-ubuntu image). Adapted from the old
# allin1-ubuntu docker-entrypoint.sh (starrocks/docker-entrypoint.sh, kept for
# reference next to the commented-out shared-nothing service in
# docker-compose.yml) -- same ADLS credential + tuning logic, different conf
# paths (/opt/starrocks/fe/conf instead of /data/deploy/starrocks/fe/conf) and
# no BE-side steps (those now live in cn-entrypoint.sh).

# --- ABFS authentication, so StarRocks can read the actual Databricks Unity
# Catalog table data files (physically stored in ADLS Gen2) -- separate from
# and in addition to the OAuth credential the databricks_uc REST catalog uses
# for metadata (see starrocks/init_catalog.sh). Without this, queries against
# databricks_uc fail with "Failed to get file system for path: abfss://...".
: "${ADLS_ACCOUNT_NAME:?ADLS_ACCOUNT_NAME must be set}"

ADLS_AUTH_MODE="${STARROCKS_ADLS_AUTH_MODE:-oauth}"

if [ "$ADLS_AUTH_MODE" = "sas" ]; then
  : "${ADLS_PROTO_SAS_TOKEN:?ADLS_PROTO_SAS_TOKEN must be set for SAS authentication}"
  ADLS_HOST="${ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
  SAS_TOKEN_XML="$(printf '%s' "$ADLS_PROTO_SAS_TOKEN" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g')"
  AZURE_XML="
<configuration>
  <property>
    <name>fs.azure.account.auth.type.${ADLS_HOST}</name>
    <value>SAS</value>
  </property>
  <property>
    <name>fs.azure.sas.token.provider.type.${ADLS_HOST}</name>
    <value>com.risingwave.starrocks.FixedSASTokenProvider</value>
  </property>
  <property>
    <name>fs.azure.sas.fixed.token.${ADLS_HOST}</name>
    <value>${SAS_TOKEN_XML}</value>
  </property>
</configuration>"
elif [ "$ADLS_AUTH_MODE" = "oauth" ]; then
  : "${ADLS_CLIENT_ID:?ADLS_CLIENT_ID must be set for ADLS OAuth}"
  : "${ADLS_CLIENT_SECRET:?ADLS_CLIENT_SECRET must be set for ADLS OAuth}"
  : "${ADLS_TENANT_ID:?ADLS_TENANT_ID must be set for ADLS OAuth}"
  ADLS_HOST="${ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
  AZURE_XML="
<configuration>
  <property>
    <name>fs.azure.account.auth.type.${ADLS_HOST}</name>
    <value>OAuth</value>
  </property>
  <property>
    <name>fs.azure.account.oauth.provider.type.${ADLS_HOST}</name>
    <value>org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider</value>
  </property>
  <property>
    <name>fs.azure.account.oauth2.client.id.${ADLS_HOST}</name>
    <value>${ADLS_CLIENT_ID}</value>
  </property>
  <property>
    <name>fs.azure.account.oauth2.client.secret.${ADLS_HOST}</name>
    <value>${ADLS_CLIENT_SECRET}</value>
  </property>
  <property>
    <name>fs.azure.account.oauth2.client.endpoint.${ADLS_HOST}</name>
    <value>https://login.microsoftonline.com/${ADLS_TENANT_ID}/oauth2/token</value>
  </property>
</configuration>"
elif [ "$ADLS_AUTH_MODE" = "account_key" ]; then
  : "${ADLS_ACCOUNT_KEY:?ADLS_ACCOUNT_KEY must be set for account-key authentication}"
  AZURE_XML="
<configuration>
  <property>
    <name>fs.azure.account.key.${ADLS_ACCOUNT_NAME}.dfs.core.windows.net</name>
    <value>${ADLS_ACCOUNT_KEY}</value>
  </property>
</configuration>"
else
  echo "Unsupported STARROCKS_ADLS_AUTH_MODE: ${STARROCKS_ADLS_AUTH_MODE}" >&2
  exit 1
fi

echo "$AZURE_XML" > /opt/starrocks/fe/conf/core-site.xml

# --- shared-data mode, storage on MinIO (see docs/SR_POC_STARROCKS_SHARED_DATA_MINIO.md) ---
grep -q '^run_mode' /opt/starrocks/fe/conf/fe.conf || echo "run_mode = shared_data" >> /opt/starrocks/fe/conf/fe.conf
grep -q '^cloud_native_storage_type' /opt/starrocks/fe/conf/fe.conf || echo "cloud_native_storage_type = S3" >> /opt/starrocks/fe/conf/fe.conf

# --- Iceberg metadata cache + background refresh (see old docker-entrypoint.sh for full rationale) ---
grep -q 'iceberg_metadata_memory_cache_capacity' /opt/starrocks/fe/conf/fe.conf || cat >> /opt/starrocks/fe/conf/fe.conf <<'EOF'

iceberg_metadata_memory_cache_capacity = 268435456
background_refresh_metadata_interval_millis = 300000
EOF

# --- reduce JVM heap ceiling from the image's default (was -Xmx8192m on allin1;
# fe-ubuntu's own default may differ, force to the validated 3072m either way) ---
if grep -q -- '-Xmx' /opt/starrocks/fe/conf/fe.conf; then
  sed -i -E 's/-Xmx[0-9]+[mMgG]/-Xmx3072m/' /opt/starrocks/fe/conf/fe.conf
else
  echo 'JAVA_OPTS="-Xmx3072m"' >> /opt/starrocks/fe/conf/fe.conf
fi

# --- disable query-triggered connector-table analyze concurrency (avoids the
# known stats-cache-refresher IllegalStateException tax on JDBC/Iceberg catalog
# queries, see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md) ---
grep -q 'connector_table_query_trigger_analyze_max_running_task_num' /opt/starrocks/fe/conf/fe.conf || cat >> /opt/starrocks/fe/conf/fe.conf <<'EOF'

connector_table_query_trigger_analyze_max_running_task_num = 0
EOF

# --- lower the async MV minimum refresh interval (default 60s -> 10s) ---
grep -q 'materialized_view_min_refresh_interval' /opt/starrocks/fe/conf/fe.conf || cat >> /opt/starrocks/fe/conf/fe.conf <<'EOF'

materialized_view_min_refresh_interval = 10
EOF

exec /opt/starrocks/fe/bin/start_fe.sh --host_type FQDN
