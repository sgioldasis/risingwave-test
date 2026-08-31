# Always-On StarRocks Integration Plan

## Goal

Add StarRocks to the default Docker Compose startup so the normal `./bin/1_up.sh` command starts RisingWave and StarRocks together. StarRocks should query both local Lakekeeper/MinIO Iceberg tables and Databricks Unity Catalog Iceberg tables.

The repository already has most of the raw pieces:

- `docker-compose.yml` contains a commented StarRocks service scaffold.
- `starrocks/docker-entrypoint.sh` injects Hadoop configuration for FE and BE and tunes StarRocks metadata/data caches.
- `starrocks/init_catalog.sh` creates the StarRocks external catalogs.
- `docs/poc/STARROCKS_SERVING_LAYER.md` documents the serving-layer design and previous validation.

The implementation should activate and harden those existing pieces rather than creating a parallel integration path.

## Architecture

StarRocks should serve as an ad-hoc OLAP query layer over Iceberg:

```text
Kafka -> RisingWave -> materialized views/sinks -> Iceberg
                                            |       |
                                            |       +-> Lakekeeper/MinIO
                                            |       +-> Databricks Unity Catalog/ADLS
                                            |
                                            +-> StarRocks external catalogs
```

RisingWave remains responsible for streaming ingestion, transformation, and Iceberg writes. StarRocks should not consume Kafka independently for this test path.

## Compose Integration

Convert the commented StarRocks services in `docker-compose.yml` into active, always-on services. Do not add a Compose profile: the existing default `docker compose up` path and `./bin/1_up.sh` must start StarRocks automatically.

The `starrocks` service should keep:

- Image: `starrocks/allin1-ubuntu:4.1.4`
- Container name: `starrocks`
- MySQL protocol port: `9030:9030`
- HTTP UI/API port: `8030:8030`
- Custom entrypoint: `/starrocks-entrypoint.sh`
- Proxy-clearing environment variables
- Healthcheck using the StarRocks MySQL protocol
- Memory limit around `3G`
- Network: `iceberg_net`

The `starrocks-init` service should:

- Be active in the default Compose project
- Depend on healthy `starrocks`
- Depend on completed `lakekeeper-bootstrap` and healthy `minio-0` where needed, so the local catalog does not race startup
- Mount `starrocks/init_catalog.sh`
- Create and verify both `databricks_uc` and `lakekeeper_local`

## Credential Handling

The Databricks UC path needs credentials from the environment. Do not commit secrets.

Required variables:

```bash
DATABRICKS_AZURE_CLIENT_ID
DATABRICKS_AZURE_CLIENT_SECRET
ADLS_ACCOUNT_KEY
```

`DATABRICKS_AZURE_CLIENT_ID` already has a repo default in several places:

```text
3b7f531f-db93-4186-af75-6566c12c076b
```

`DATABRICKS_AZURE_CLIENT_SECRET` and `ADLS_ACCOUNT_KEY` must come from the user's shell or an untracked local environment file.

`starrocks/docker-entrypoint.sh` should fail fast if `ADLS_ACCOUNT_KEY` is missing. A blank key can let metadata operations appear healthy while ADLS file scans fail later.

## Catalogs To Create

### `databricks_uc`

StarRocks external catalog over Databricks Unity Catalog Iceberg REST:

```sql
CREATE EXTERNAL CATALOG databricks_uc
COMMENT "Databricks Unity Catalog de_dev via Iceberg REST"
PROPERTIES (
    "type"                              = "iceberg",
    "iceberg.catalog.type"              = "rest",
    "iceberg.catalog.uri"               = "https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest",
    "iceberg.catalog.warehouse"         = "de_dev",
    "iceberg.catalog.credential"        = "<client-id>:<client-secret>",
    "iceberg.catalog.oauth2-server-uri" = "https://login.microsoftonline.com/78395483-9425-447a-ba64-60b90f6bb16e/oauth2/v2.0/token",
    "iceberg.catalog.scope"             = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
    "hadoop.fs.azure.account.key.stkznneurwpoccdddevstd.dfs.core.windows.net" = "<adls-account-key>"
);
```

### `lakekeeper_local`

StarRocks external catalog over local Lakekeeper/MinIO:

```sql
CREATE EXTERNAL CATALOG lakekeeper_local
COMMENT "Local Lakekeeper REST catalog backed by MinIO"
PROPERTIES (
    "type"                              = "iceberg",
    "iceberg.catalog.type"              = "rest",
    "iceberg.catalog.uri"               = "http://lakekeeper:8181/catalog/",
    "iceberg.catalog.warehouse"         = "risingwave-warehouse",
    "aws.s3.endpoint"                   = "http://minio-0:9301",
    "aws.s3.access_key"                 = "hummockadmin",
    "aws.s3.secret_key"                 = "hummockadmin",
    "aws.s3.enable_path_style_access"   = "true",
    "aws.s3.region"                     = "us-east-1"
);
```

## Known Databricks Tables

The working Databricks Iceberg tables are under `de_dev.rw_poc`.

Use these StarRocks table references through the `databricks_uc` catalog:

- `databricks_uc.rw_poc.rw_casino_transactions`
- `databricks_uc.rw_poc.rw_sportsbook_bets`
- `databricks_uc.rw_poc.rw_casino_turnover_90d`
- `databricks_uc.rw_poc.rw_casino_landing`
- `databricks_uc.rw_poc.rw_sportsbook_landing`

The corresponding Unity Catalog names are:

- `de_dev.rw_poc.rw_casino_transactions`
- `de_dev.rw_poc.rw_sportsbook_bets`
- `de_dev.rw_poc.rw_casino_turnover_90d`
- `de_dev.rw_poc.rw_casino_landing`
- `de_dev.rw_poc.rw_sportsbook_landing`

These names are confirmed by the dbt Databricks sink models and `sql/databricks_setup.sql`.

## Lifecycle Helper

Add `bin/4_run_starrocks.sh` as an optional diagnostic/query helper, not as the primary startup path. The primary user-facing command should remain `./bin/1_up.sh`.

Responsibilities:

1. Run from the project root.
2. Check Docker is available.
3. Check required Databricks/ADLS environment variables.
4. If StarRocks is not already running, start it with `docker compose up -d starrocks starrocks-init`.
5. Wait for the StarRocks MySQL protocol to become available.
6. Run smoke queries from inside a container so the host does not need a MySQL client.
7. Print connection details:

   ```text
   StarRocks MySQL: mysql -h 127.0.0.1 -P 9030 -u root
   StarRocks UI/API: http://localhost:8030
   ```

## Smoke Test

Add either `starrocks/smoke_test.sh` or `sql/starrocks_smoke.sql`.

The smoke test should run in layers, because each layer proves a different part of the integration.

### 1. Catalog Exists

```sql
SHOW CATALOGS LIKE 'databricks_uc';
SHOW CATALOGS LIKE 'lakekeeper_local';
```

### 2. Databricks Namespace Access

```sql
SHOW DATABASES FROM databricks_uc;
SHOW TABLES FROM databricks_uc.rw_poc;
```

### 3. Databricks Table Metadata

```sql
DESCRIBE databricks_uc.rw_poc.rw_casino_transactions;
```

This proves StarRocks can authenticate with Unity Catalog and read Iceberg metadata.

### 4. Databricks Data Scan

```sql
SELECT *
FROM databricks_uc.rw_poc.rw_casino_transactions
LIMIT 5;
```

or:

```sql
SELECT COUNT(*)
FROM databricks_uc.rw_poc.rw_casino_transactions;
```

This proves StarRocks BE can read the underlying ADLS Parquet files. If metadata works but this query fails, check `ADLS_ACCOUNT_KEY` and the generated FE/BE `core-site.xml` files inside the StarRocks container.

### 5. Local Lakekeeper Check

```sql
SHOW DATABASES FROM lakekeeper_local;
SHOW TABLES FROM lakekeeper_local.public;
```

If local tables exist, run a bounded query against a known table, for example:

```sql
SELECT COUNT(*)
FROM lakekeeper_local.public.rw_managed_funnel;
```

or one of the casino Lakekeeper tables if the production casino demo has populated them:

```sql
SELECT COUNT(*)
FROM lakekeeper_local.public.rw_casino_transactions;
```

## Validation Commands

Run these from `devbox shell`.

Validate the default Compose model:

```bash
docker compose config
```

Start the full default stack, including StarRocks:

```bash
./bin/1_up.sh
```

The script should start StarRocks automatically after the StarRocks services have been activated in `docker-compose.yml`. No `COMPOSE_PROFILES` setting is required.

Export credentials without committing them:

```bash
export DATABRICKS_AZURE_CLIENT_SECRET='<client-secret>'
export ADLS_ACCOUNT_KEY='<storage-account-key>'
```

If the base stack is already running and only StarRocks needs to be restarted:

```bash
docker compose up -d starrocks starrocks-init
```

Check container status:

```bash
docker compose ps starrocks starrocks-init
```

Open a StarRocks SQL shell if needed:

```bash
docker exec -it starrocks mysql -h 127.0.0.1 -P 9030 -u root
```

## Documentation Updates

After implementation, update:

- `docs/poc/STARROCKS_SERVING_LAYER.md` with the always-on startup workflow.
- `README.md` with a short StarRocks section and start/query commands.
- `bin/show_links.sh` with StarRocks UI and MySQL connection details.

## Cleanup

`docker compose down --volumes` should stop StarRocks as part of the default Compose project. Only add explicit cleanup for ports `8030` and `9030` in `bin/6_down.sh` if stale local processes become common.

## Follow-Up Options

- Add `STARROCKS_ENABLE_DATABRICKS=0` later if local-only testing should be possible without Databricks credentials. Since StarRocks starts by default, this may be useful for development environments without cloud credentials.
- Reduce StarRocks memory limits or document partial-stack testing if the full stack plus StarRocks is too heavy on the local machine.
- SAS authentication using `ADLS_PROTO_SAS_TOKEN` is enabled by default through a small compatibility wrapper in `starrocks/FixedSASTokenProvider.java`. The wrapper adapts Hadoop 3.4.3's constructor-based provider to StarRocks' no-argument provider loader. The custom image is built with `docker compose build starrocks`.
