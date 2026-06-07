#!/bin/bash
set -e

# Write ABFS key into core-site.xml for both FE and BE before StarRocks starts.
# StarRocks' Iceberg REST catalog doesn't forward catalog PROPERTIES to the
# Hadoop Configuration, so the account key must be in core-site.xml which
# Hadoop loads from the classpath automatically (FE for metadata, BE for scans).
AZURE_XML="
<configuration>
  <property>
    <name>fs.s3.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.azure.account.key.stkznneurwpoccdddevstd.dfs.core.windows.net</name>
    <value>${ADLS_ACCOUNT_KEY}</value>
  </property>
</configuration>"

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
