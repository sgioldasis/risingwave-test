#!/bin/sh
set -eu

: "${ADLS_CLIENT_ID:?ADLS_CLIENT_ID must be set}"
: "${ADLS_CLIENT_SECRET:?ADLS_CLIENT_SECRET must be set}"
: "${ADLS_TENANT_ID:?ADLS_TENANT_ID must be set}"

account_host="stkznneusrpoccdddevstd.dfs.core.windows.net"
config_file="/opt/hadoop/etc/hadoop/core-site.xml"

cat > "$config_file" <<EOF
<configuration>
  <property>
    <name>fs.azure.account.auth.type.${account_host}</name>
    <value>OAuth</value>
  </property>
  <property>
    <name>fs.azure.account.oauth.provider.type.${account_host}</name>
    <value>org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider</value>
  </property>
  <property>
    <name>fs.azure.account.oauth2.client.id.${account_host}</name>
    <value>${ADLS_CLIENT_ID}</value>
  </property>
  <property>
    <name>fs.azure.account.oauth2.client.secret.${account_host}</name>
    <value>${ADLS_CLIENT_SECRET}</value>
  </property>
  <property>
    <name>fs.azure.account.oauth2.client.endpoint.${account_host}</name>
    <value>https://login.microsoftonline.com/${ADLS_TENANT_ID}/oauth2/token</value>
  </property>
</configuration>
EOF

exec /entrypoint.sh
