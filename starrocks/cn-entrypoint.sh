#!/bin/bash
set -e

# Shared-data CN entrypoint (starrocks/cn-ubuntu image). CN replaces BE in
# shared-data clusters and does the actual data scanning, so it needs the
# same ADLS credentials as the FE (see fe-entrypoint.sh) plus the data-cache
# tuning the old allin1 setup applied to its BE conf.

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

echo "$AZURE_XML" > /opt/starrocks/cn/conf/core-site.xml

# --- data cache (Parquet block cache), same values as the old BE conf ---
grep -q 'datacache_enable' /opt/starrocks/cn/conf/cn.conf || cat >> /opt/starrocks/cn/conf/cn.conf <<'EOF'

datacache_enable = true
datacache_mem_size = 671088640
EOF

sleep 15
ulimit -u 65535 || true
ulimit -n 65535 || true
mysql --connect-timeout 2 -h starrocks -P9030 -uroot -e "ALTER SYSTEM ADD COMPUTE NODE \"starrocks-cn:9050\";" || true
exec /opt/starrocks/cn/bin/start_cn.sh
