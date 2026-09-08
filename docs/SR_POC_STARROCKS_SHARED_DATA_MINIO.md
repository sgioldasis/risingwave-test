---
title: StarRocks Shared-Data Mode on MinIO — Now the Real Storage Backend
description: How the main StarRocks service was migrated from shared-nothing to shared-data storage on the stack's own MinIO, and how it's wired in
---

<!-- markdownlint-disable-file -->

## What this is

The main `starrocks` service's actual storage architecture, as of
2026-09-08. StarRocks now runs in **shared-data mode** (compute/storage
separation) with its table data stored in the stack's own MinIO instance
(`minio-0`, S3-compatible) instead of local BE disk. This is separate from
the existing Iceberg-via-REST-catalog integration (`databricks_uc`,
`lakekeeper_local`) — those let StarRocks *read* Iceberg tables that live
elsewhere; this is about where StarRocks's *own* native tables (the
`dbt_starrocks`-managed views/MVs the dashboard serves from) physically
store their data.

This started as a standalone feasibility test (mirroring the earlier,
credential-blocked [Azure ADLS attempt](SR_POC_STARROCKS_SHARED_DATA_AZURE.md)),
confirmed working end-to-end, then wired in as the real `starrocks` service
the same day. The standalone test cluster (`starrocks-shared-fe`/
`starrocks-shared-cn`, separate ports) is now commented out in
`docker-compose.yml`, superseded by the real thing.

## Why this needed a real service swap, not a config change

The old `starrocks` service used the `starrocks/allin1-ubuntu` image, which
is **shared-nothing only** (FE+BE combined in one process). Shared-data
mode requires the dedicated `starrocks/fe-ubuntu` + `starrocks/cn-ubuntu`
images (CN = Compute Node, replaces BE in shared-data clusters), and
`run_mode` is set at FE bootstrap — not something you flip on a running
shared-nothing cluster. The old allin1-based service is commented out in
`docker-compose.yml`, kept for reference/revert, not deleted.

**Why MinIO succeeded where Azure didn't**: MinIO uses plain static
access-key/secret auth, which is exactly what StarRocks's S3 storage-volume
properties expect — no OAuth/client-secret gap like ADLS2 had (StarRocks's
ADLS2 storage-volume only supports Managed/Workload Identity, not a plain
client secret).

## How it's wired in

Kept the service key, `hostname`, and `container_name` as `starrocks`
(unchanged from the old shared-nothing setup) so nothing downstream needed
to change — the dbt_starrocks profile, `orchestration/*.py`,
`modern-dashboard/backend/api.py`, and `starrocks/init_catalog.sh` all
still connect via hostname `starrocks` port 9030.

* **`starrocks`** (service) — `starrocks/fe-ubuntu:4.0-latest`, entrypoint
  [starrocks/fe-entrypoint.sh](../starrocks/fe-entrypoint.sh). Sets
  `run_mode = shared_data`, `cloud_native_storage_type = S3`, and carries
  forward every fix accumulated on the old allin1 setup: the ADLS
  core-site.xml credential (needed to read the Databricks Unity Catalog
  table's actual data files, physically stored in ADLS — separate from the
  REST catalog's own OAuth metadata credential), the JVM heap ceiling fix
  (`-Xmx3072m`, see [SR_POC_ICEBERG_COUNTRIES_MIGRATION.md](SR_POC_ICEBERG_COUNTRIES_MIGRATION.md)
  for why the image default caused multi-second GC pauses),
  `connector_table_query_trigger_analyze_max_running_task_num = 0`, the 10s
  MV refresh floor, and the Iceberg metadata cache settings.
* **`starrocks-cn`** (service) — `starrocks/cn-ubuntu:4.0-latest`,
  entrypoint [starrocks/cn-entrypoint.sh](../starrocks/cn-entrypoint.sh).
  Registers itself via `ALTER SYSTEM ADD COMPUTE NODE`, carries the same
  ADLS credential (CN does the actual data scanning, same as BE did
  before), and the BE data-cache tuning (`datacache_enable`,
  `datacache_mem_size = 671088640`).
* **`starrocks-init`** — unchanged responsibility (register the three
  external catalogs), plus one new step: `CREATE STORAGE VOLUME
  minio_default TYPE = S3 ... ; SET minio_default AS DEFAULT STORAGE
  VOLUME;` against bucket `s3://starrocks/`, run once FE is healthy. Now
  also depends on `starrocks-cn` being healthy, not just `starrocks`.
* **MinIO** — provisions the `starrocks` bucket via the same
  `mkdir -p "/data/starrocks"` entrypoint trick used for `hummock001`.

## Verified end-to-end (2026-09-08)

* Storage volume creation and default-volume assignment succeed with no
  credential errors.
* Internal StarRocks table write/read round-trips through the volume,
  independently confirmed by inspecting MinIO's own backing filesystem
  directly (`docker exec minio-0` — the actual `.dat`/`meta`/`log` objects
  StarRocks wrote are there under `/data/starrocks/`).
* All three external catalogs (`databricks_uc`, `lakekeeper_local`,
  `risingwave`) register cleanly via `starrocks-init` on the new FE.
* The Databricks Unity Catalog query that failed before the ADLS
  core-site.xml was ported over (`Failed to get file system for path:
  abfss://...`) now succeeds.
* `modern_dashboard_setup_job` (Dagster) ran `RUN_SUCCESS` end-to-end on
  the fresh cluster — both the RisingWave-side dbt models and all 7
  `dbt_starrocks` models (`hot_funnel_summary`, `dashboard_funnel_serving`,
  `mv_funnel_daily_country_rollup`, `mv_iceberg_countries_cache`,
  `mv_unified_funnel_summary`, `dashboard_funnel_enriched`,
  `dashboard_funnel_serving_cached`) built with zero errors.
* The dashboard's own FastAPI backend was started and hit directly —
  `/api/query/funnel`, `/api/query/funnel/aggregate`, and
  `/api/query/funnel/cached` all returned real rows (`degraded: false`)
  reading through the new StarRocks cluster.

## Operational notes

* **Rebuilding after a fresh stack start**: use `modern_dashboard_setup_job`
  in Dagster, not `dbt_build_job`/`dbt_starrocks_build_job` run unscoped
  (the unscoped path can hit an unrelated `sink_funnel_to_postgres`
  build-order failure — see the Dagster job-choice note in memory / project
  history).
* **Data persistence**: the `starrocks` MinIO bucket lives in the same
  `minio-0` Docker volume as `hummock001` — it survives normal container
  restarts/recreates, but is wiped by `bin/6_down.sh`
  (`docker compose down --volumes`), same as before. Shared-data mode
  doesn't change this project's teardown behavior; it only changes *where*
  the data physically sits while the stack is up.
* **Reverting**: the old allin1-ubuntu `starrocks` service definition is
  commented out (not deleted) directly above the new one in
  `docker-compose.yml`, in case a revert is ever needed. Reverting would
  mean re-losing shared-data storage and going back to ephemeral local BE
  disk.

## Storage-volume SQL (for reference)

```sql
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
```

`aws.s3.enable_path_style_access = true` is required for MinIO — without it
StarRocks would try virtual-hosted-style addressing
(`starrocks.minio-0:9301`), which MinIO's `MINIO_DOMAIN=minio-0` config
doesn't resolve the same way AWS S3 does. This is run automatically by
`starrocks/init_catalog.sh` — the block above is for reference only.
