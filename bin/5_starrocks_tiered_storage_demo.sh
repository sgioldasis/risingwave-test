#!/bin/bash

# Tiered-storage demo — what "tiered storage" means on this stack: the cluster
# has run in SHARED-DATA mode since 2026-09-08 (run_mode = shared_data, FE +
# CN, see docs/SR_POC_STARROCKS_SHARED_DATA_MINIO.md), so every table's data
# files live in the MinIO "starrocks" bucket (durable cold tier) while the CN's
# local block cache (datacache) serves hot reads (hot tier). There is no
# per-table hot/cold toggle in shared-data mode — the tiering IS the cache.
#
# This script makes that visible: the storage volume, the physical MinIO
# files, the CN cache stats, and an EXPLAIN ANALYZE warm scan showing reads
# served from the cache tier.

set -e

MYSQL="docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root"
TABLE="page_views_direct"

cat <<'INTRO'

"Tiered storage" in StarRocks means the cluster keeps durable data in cheap
object storage (S3/MinIO) and serves hot reads from a local cache on the
compute node. This stack has ALREADY been running that way since 2026-09-08:
the cluster is in SHARED-DATA mode (run_mode = shared_data, FE + CN instead
of the classic shared-nothing FE + BEs), with a MinIO-backed storage volume
as the single source of truth. Discovery of the session: we went looking for
a per-table hot/cold toggle and found none -- in shared-data mode the tiering
IS the cache, automatically, for every table.

The four steps make that architecture visible on this live stack:

  Step 1  the mode itself   -> run_mode = shared_data in fe.conf + the
                               storage volume every table lives on
  Step 2  the cold tier     -> the physical files in the MinIO bucket: this
                               is where durable data actually is; delete it
                               and the cluster's data is gone
  Step 3  the hot tier      -> the CN's datacache (block cache) stats: disk
                               quota 10GB + mem quota 640MB -- durable data
                               is NOT here, only cached copies
  Step 4  the tiers working -> the same scan twice via EXPLAIN ANALYZE:
                               watch IOTaskExecTime collapse once the blocks
                               are cached (first run may still touch MinIO,
                               second is fully warm)

INTRO
echo "=== 1. Cluster runs shared-data (run_mode = shared_data) ==="
docker exec starrocks sh -c 'grep "^run_mode" /opt/starrocks/fe/conf/fe.conf'
$MYSQL -e "SHOW STORAGE VOLUMES;"

echo ""
echo "=== 2. Cold tier: every table's data files live in MinIO (bucket 'starrocks') ==="
echo "    (the du below runs INSIDE the MinIO container -- this is the durable copy;"
echo "     deleting it deletes the cluster's data)"
docker exec minio-0 sh -c 'du -sh /data/starrocks 2>/dev/null; V=$(ls /data/starrocks | head -1); ls /data/starrocks/$V | head -4; echo "  ..."'
echo "Storage volume DDL (from docs/SR_POC_STARROCKS_SHARED_DATA_MINIO.md):"
echo "  CREATE STORAGE VOLUME minio_default TYPE = S3 LOCATIONS = ('s3://starrocks/')"
echo "  PROPERTIES ('aws.s3.endpoint' = 'http://minio-0:9301', ...)"

echo ""
echo "=== 3. Hot tier: the CN's local block cache (datacache) ==="
echo "    (information_schema.be_datacache_metrics = the cache itself: usage vs"
echo "     quota, one row per CN -- nothing here is the source of truth)"
$MYSQL -e "SELECT BE_ID, STATUS,
    round(DISK_USED_BYTES/1048576, 1) AS disk_used_mb,
    round(DISK_QUOTA_BYTES/1048576, 0) AS disk_quota_mb,
    round(MEM_USED_BYTES/1048576, 1) AS mem_used_mb,
    round(MEM_QUOTA_BYTES/1048576, 0) AS mem_quota_mb
  FROM information_schema.be_datacache_metrics\G" 2>/dev/null | grep -v '\*\*\*'
echo "Cache directory on the CN (durable data is NOT here — only cached blocks):"
docker exec starrocks-cn sh -c 'du -sh /opt/starrocks/cn/storage/datacache 2>/dev/null; ls /opt/starrocks/cn/storage 2>/dev/null'

echo ""
echo "=== 4. Cache in action: EXPLAIN ANALYZE warm scan of ${TABLE} (twice) ==="
echo "    (input both times: EXPLAIN ANALYZE SELECT page_id, COUNT(*) FROM"
echo "     sr_local_db.${TABLE} GROUP BY page_id -- compare the two IOTaskExecTime)"
$MYSQL -e "EXPLAIN ANALYZE SELECT page_id, COUNT(*) FROM sr_local_db.${TABLE} GROUP BY page_id;" 2>&1 | grep -E 'IOTaskExecTime' | sed -E 's/([0-9])us/\1µs/g' | head -3
echo "  --- second (fully warm) run ---"
$MYSQL -e "EXPLAIN ANALYZE SELECT page_id, COUNT(*) FROM sr_local_db.${TABLE} GROUP BY page_id;" 2>&1 | grep -E 'IOTaskExecTime' | sed -E 's/([0-9])us/\1µs/g' | head -3

echo ""
echo "=== 5. Honest scale note ==="
echo "    (what these numbers do and don't prove at demo scale --)"
echo "At demo scale every segment fits in the 640MB mem + 10GB disk cache, so"
echo "warm scans are cache-served. The MinIO round-trip cost is only visible"
echo "with a cold cache (CN restart) or far more data than the cache quota."
echo "The Iceberg-side analogue (cold vs warm against the lake) is"
echo "bin/5_starrocks_lake_cache_benchmark.sh."
