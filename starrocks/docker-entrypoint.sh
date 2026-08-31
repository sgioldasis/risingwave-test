#!/bin/bash
set -e

# Configure ABFS authentication for both FE and BE before StarRocks starts.
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

echo "$AZURE_XML" > /data/deploy/starrocks/fe/conf/core-site.xml
echo "$AZURE_XML" > /data/deploy/starrocks/be/conf/core-site.xml

# --- FE: Iceberg metadata cache + background refresh ---
# Cache Iceberg snapshot/manifest metadata in FE memory to avoid re-fetching from Lakekeeper
# on each query. Requires snapshot count to be kept low (Trino expire_snapshots in Dagster);
# with 100+ snapshots this produces a multi-minute cold start per container restart.
# Refresh metadata every 1 min (default: 10 min) so new RisingWave sink commits are visible.
grep -q 'iceberg_metadata_memory_cache_capacity' /data/deploy/starrocks/fe/conf/fe.conf || cat >> /data/deploy/starrocks/fe/conf/fe.conf <<'EOF'

iceberg_metadata_memory_cache_capacity = 268435456
background_refresh_metadata_interval_millis = 300000
EOF

# --- BE: data cache (Parquet block cache) ---
# Explicitly enable and size the data cache so Parquet blocks fetched from MinIO/ADLS
# are held in BE memory across queries. Without this the auto-sized quota is ~16% of
# container RAM; setting it explicitly ensures the value survives a memory-limit change.
grep -q 'datacache_enable' /data/deploy/starrocks/be/conf/be.conf || cat >> /data/deploy/starrocks/be/conf/be.conf <<'EOF'

datacache_enable = true
datacache_mem_size = 671088640
EOF

exec /data/deploy/entrypoint.sh
