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

# --- FE: reduce JVM heap ceiling from the image's default -Xmx8192m ---
# Confirmed via fe.gc.log 2026-09-07: G1GC was producing "Pause Young (Normal)
# (G1 Evacuation Pause)" events over 1 SECOND long (MMU target violated:
# 201.0ms(200.0ms/201.0ms)), even for trivial connection-handshake queries
# ("select @@version_comment limit 1") with zero data access -- i.e. this
# was blocking ALL FE query processing for over a second at a time,
# regardless of what query ran. Actual live heap usage in the same GC log
# oscillated around 260-320MB post-collection, nowhere near the 8192MB
# ceiling -- G1GC sizes its regions/generations off the *configured* max
# heap, so an 8GB ceiling for a workload that only ever uses a few hundred
# MB causes oversized, infrequent-but-massive pauses instead of small,
# frequent, fast ones. Increasing the container memory limit
# (docker-compose.yml, 3G->5G) alone did NOT fix this -- the JVM heap
# ceiling is independent of the container's cgroup limit and needs to be
# reduced directly.
#
# Bumped 2048m -> 3072m on 2026-09-07: the mv_unified_funnel_summary
# background refresh (runs every ~5min, pulls historical data from
# Databricks over the network) allocates up to ~210MB of FE heap per run
# by itself (QueryFEAllocatedMemory=220553112 observed in fe.audit.log).
# Against a 2048m ceiling that single query is >10% of the heap, and
# interactive dashboard queries landing during/just after a refresh window
# were seeing 600-900ms instead of the validated ~150-430ms baseline
# (CpuCostNs on those slow queries was only ~15-30ms -- the extra time was
# GC/allocation pressure, not real work). 3072m gives more headroom above
# the refresh's peak allocation while still being far below the original
# 8192m that caused the multi-second pauses.
sed -i -E 's/-Xmx[0-9]+[mMgG]/-Xmx3072m/' /data/deploy/starrocks/fe/conf/fe.conf

# --- FE: disable query-triggered connector-table analyze concurrency ---
# Confirmed via fe.log 2026-09-07: dashboard queries hitting the `risingwave`
# JDBC catalog (funnel_summary) were intermittently failing with
# "StarRocksPlannerException: StarRocks planner use long time 4000+ ms in
# logical phase" (hitting the new_planner_optimize_timeout=3000ms ceiling),
# then auto-retrying successfully -- i.e. every affected query paid a ~4.2s
# tax. Root cause: StarRocks auto-triggers ANALYZE jobs on connector
# (external/JDBC catalog) tables when they're queried; those jobs spawn
# stats-cache-refresher threads that fail with IllegalStateException in
# StatisticsUtils.getTableByUUID (a known-unfixed StarRocks limitation --
# see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md). Setting max running
# tasks to 0 stops new analyze jobs from being queued during planning,
# which eliminates the 4s timeout failures. It does NOT eliminate the
# underlying stats-cache exception (still fires on every query, harmlessly
# -- the planner falls back to default cost estimates) or the residual
# ~500-900ms per-query planning cost documented in that same file.
grep -q 'connector_table_query_trigger_analyze_max_running_task_num' /data/deploy/starrocks/fe/conf/fe.conf || cat >> /data/deploy/starrocks/fe/conf/fe.conf <<'EOF'

connector_table_query_trigger_analyze_max_running_task_num = 0
EOF

# --- FE: lower the async MV minimum refresh interval ---
# Default is 60s (Config.materialized_view_min_refresh_interval). Lowered to
# 10s on 2026-09-07 so mv_iceberg_countries_cache (see
# dbt_starrocks/models/mv_iceberg_countries_cache.sql) can refresh every 10s
# instead of 60s, so a Databricks-side country rename reaches the dashboard
# faster. This is a global FE setting -- it lowers the floor for any other
# async MV in this project too, not just that one.
grep -q 'materialized_view_min_refresh_interval' /data/deploy/starrocks/fe/conf/fe.conf || cat >> /data/deploy/starrocks/fe/conf/fe.conf <<'EOF'

materialized_view_min_refresh_interval = 10
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
