---
title: StarRocks PoC Databricks Unity Catalog Testing Plan
description: Validation plan and current status for RisingWave, StarRocks, and Trino access to Databricks managed Iceberg tables
ms.date: 2026-09-03
ms.topic: troubleshooting
---

**Date Created:** 2026-08-31
**Status:** External writes validated after dropping the `catalogManaged` table feature on an isolated managed Iceberg probe
**Purpose:** Test bidirectional data flow (RisingWave → Databricks → StarRocks/Trino) in a new isolated `sr_poc` schema using the new service principal.

---

## 1. Overview

### Objective
Validate that RisingWave can write streaming data to a new Databricks Unity Catalog schema (`sr_poc`), and that StarRocks and Trino can independently read those tables with the newly configured service principal credentials from `.env`.

### Key Constraints & Assumptions
- **Schema isolation:** All testing uses `de_dev.sr_poc` (separate from existing `de_dev.rw_poc`)
- **Service principal:** Uses new Azure AD credentials from `.env` (different from any production principal)
- **Append-only sinks:** Following casino PoC pattern — no upsert due to UC delete file limitation
- **Hybrid schema:** Flat typed columns + JSON `properties` bag per Databricks best practices
- **No production data:** Test data only; existing RisingWave pipelines remain untouched
- **Parallel validation:** Multiple readers (StarRocks, Trino) must verify the same data independently

### Technology Stack
| Component | Purpose |
|---|---|
| RisingWave | Streaming ingestion & transformation (source → MV → sink) |
| dbt | Data transformation pipeline orchestration |
| Databricks | Unity Catalog + Iceberg REST (target storage) |
| ADLS Gen2 | S3-compatible object storage (Parquet files) |
| StarRocks | OLAP engine for ad-hoc analytics (read via external catalog) |
| Trino | Cross-catalog federation (verify Databricks ↔ RisingWave ↔ Lakekeeper) |
| Lakekeeper | Local Iceberg REST catalog (reference for comparison) |

---

## 2. Databricks Foundation Setup (Do This First)

### 2.1 Create Databricks Schema, Tables, and Grants

This is the first required step and must be completed before any Docker startup, Kafka topic creation, or dbt model execution.

**Important:** Log in to Databricks SQL Editor with your personal workspace admin account (`s.gioldasis-si@devkaizengaming.com`) before running the SQL below.

| Step | Status | Execution result |
| --- | --- | --- |
| 2.1.1 Schema | ✅ DONE | `de_dev.sr_poc` exists |
| 2.1.2 Tables | ✅ DONE | `sr_test_events` and `sr_hourly_agg` exist |
| 2.1.3 Grants | ✅ DONE | Catalog, schema, and table grants were applied to the Databricks service principal |
| 2.1.4 Full verification | ✅ DONE | Schema, table schemas, and table properties were verified |
| 2.1.5 StarRocks catalog verification | ✅ DONE | StarRocks discovered both tables through `databricks_uc.sr_poc` and queried empty-table counts |
| 2.1.6 Trino catalog verification | ✅ DONE | Trino discovered both tables through `databricks.sr_poc` and queried empty-table counts |
| 2.1.7 External-engine reader data-plane verification | ✅ DONE | Databricks control row is readable through both StarRocks and Trino from the PoC-owned ADLS location |
| 2.1.9 External-engine writer root cause | ✅ RESOLVED | `delta.feature.catalogManaged` caused UC IRC commit failures; `DROP FEATURE catalogManaged` restored RisingWave and StarRocks writes on the isolated probe table |

#### ✅ 2.1.1 DONE - Create Schema

**Status: Complete** - `de_dev.sr_poc` was created using the default Databricks-managed location.

```sql
USE CATALOG de_dev;

CREATE SCHEMA IF NOT EXISTS sr_poc
COMMENT "StarRocks PoC — streaming test data from RisingWave";

SHOW SCHEMAS;
USE SCHEMA sr_poc;
```

**Expected:** `sr_poc` appears in the schema list under `de_dev` catalog.

#### ✅ 2.1.2 DONE - Create All Tables (Raw Events + Aggregations)

**Status: Complete** - `de_dev.sr_poc.sr_test_events` and `de_dev.sr_poc.sr_hourly_agg` were created.

```sql
USE CATALOG de_dev;
USE SCHEMA sr_poc;

CREATE TABLE IF NOT EXISTS sr_test_events (
  id                    STRING           NOT NULL,
  timestamp             TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  amount                DECIMAL(10,2),
  details               STRING
)
USING ICEBERG
COMMENT "Append-only event stream for conversion funnel";

CREATE TABLE IF NOT EXISTS sr_hourly_agg (
  hour_start            TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  event_count           BIGINT,
  total_amount          DECIMAL(10,2)
)
USING ICEBERG
COMMENT "Hourly aggregated metrics from RisingWave";

SHOW TABLES;
```

**Expected output:** `sr_hourly_agg` and `sr_test_events`.

#### ✅ 2.1.3 DONE - Grant Permissions to New Service Principal

**Status: Complete** - Grants were applied to the Databricks service principal (`DATABRICKS_AZURE_CLIENT_ID`).

```sql
USE CATALOG de_dev;

GRANT USAGE ON CATALOG de_dev
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT USAGE, CREATE_TABLE, MODIFY
ON SCHEMA de_dev.sr_poc
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_test_events
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_hourly_agg
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

SHOW GRANTS ON SCHEMA de_dev.sr_poc;
SHOW GRANTS ON TABLE de_dev.sr_poc.sr_test_events;
```

**Expected:** Grant statements complete without error; the new principal appears with the expected privileges.

#### ✅ 2.1.4 DONE - Verification Checklist

**Status: Complete** - Schema, table schemas, and properties were verified.

```sql
USE CATALOG de_dev;
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sr_poc';
SHOW TABLES IN sr_poc;
DESCRIBE TABLE sr_poc.sr_test_events;
DESCRIBE TABLE sr_poc.sr_hourly_agg;
SHOW TBLPROPERTIES sr_poc.sr_test_events;
SHOW TBLPROPERTIES sr_poc.sr_hourly_agg;
```

**Expected:** The table schemas match section 2.1.2. Both tables have `history.expire.min-snapshots-to-keep = 100`.

#### ✅ 2.1.5 DONE - Verify StarRocks Catalog Access

**Status: Complete** - StarRocks discovered both Databricks-managed tables through the `databricks_uc` Iceberg REST catalog and queried their empty-table counts.

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root --batch --raw -e "
SHOW TABLES FROM databricks_uc.sr_poc;
SELECT COUNT(*) AS event_rows FROM databricks_uc.sr_poc.sr_test_events;
SELECT COUNT(*) AS hourly_aggregation_rows FROM databricks_uc.sr_poc.sr_hourly_agg;
"
```

**Verified result:** `sr_test_events` and `sr_hourly_agg` were listed. Each `COUNT(*)` query returned `0`, which is expected before RisingWave writes events. Section 2.1.7 subsequently verified physical ADLS file reads through the replacement schema.

#### ✅ 2.1.6 DONE - Verify Trino Catalog Access

**Status: Complete** - Trino discovered both Databricks-managed tables through the `databricks` Iceberg REST catalog and queried their empty-table counts.

```bash
docker exec trino trino --execute "SHOW TABLES FROM databricks.sr_poc"
docker exec trino trino --execute "SELECT COUNT(*) AS event_rows FROM databricks.sr_poc.sr_test_events"
docker exec trino trino --execute "SELECT COUNT(*) AS hourly_aggregation_rows FROM databricks.sr_poc.sr_hourly_agg"
```

**Verified result:** `sr_test_events` and `sr_hourly_agg` were listed. Each `COUNT(*)` query returned `0`, which is expected before RisingWave writes events. Section 2.1.7 subsequently verified a non-empty table read through Trino.

#### ✅ 2.1.7 DONE - Verify External-Engine Reader Data Plane

**Observed failure:** A StarRocks insert into `databricks_uc.sr_poc.sr_test_events` failed before writing data. A follow-up query confirmed that no test row was inserted.

```text
ERROR 5609: fail to connect hdfs namenode
abfss://cross-operator@stkznneucommoncdddevstd.dfs.core.windows.net/

KeyProviderException: Failure to initialize configuration for
stkznneucommoncdddevstd.dfs.core.windows.net
```

**Root cause:** The current `USING ICEBERG` tables are Unity Catalog-managed tables. Because `de_dev.sr_poc` was created without a `MANAGED LOCATION`, Databricks stores their data files under its shared managed-storage account:

```text
abfss://cross-operator@stkznneucommoncdddevstd.dfs.core.windows.net/...
```

StarRocks is configured with OAuth only for the separate PoC account `stkznneusrpoccdddevstd`. It can access Unity Catalog metadata through Iceberg REST but has no Hadoop filesystem configuration or ADLS permission for `stkznneucommoncdddevstd`.

**Initial permission finding:** The personal Databricks profile could not create a storage credential:

```text
User does not have CREATE STORAGE CREDENTIAL on Metastore 'unity-northeurope'
```

**Administrator action completed:** Terraform pipeline `2808416514` created Unity Catalog external location `stkznneusrpoccdddevstd_sr-poc-cont1` for:

```text
abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/
```

The administrator then granted `s.gioldasis-si@devkaizengaming.com` the following Unity Catalog privileges on that location:

```text
ALL_PRIVILEGES
MANAGE
```

**Our action completed:** Using the valid `personal` Databricks profile, we confirmed the grants and created the managed schema:

```text
de_dev.sr_poc_external
```

Its managed location is:

```text
abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/sr_poc_external
```

**Replacement tables completed:** We created `de_dev.sr_poc_external.sr_test_events` and `de_dev.sr_poc_external.sr_hourly_agg` with the existing Iceberg table contract. We also granted the Databricks Iceberg REST catalog principal `3b7f531f-db93-4186-af75-6566c12c076b` `USE_SCHEMA`, `CREATE_TABLE`, `SELECT`, and `MODIFY` access on the replacement schema and tables.

**Catalog refresh completed:** StarRocks and Trino were refreshed and both discovered the two replacement tables in `sr_poc_external`.

**Initial write failure:** Before connecting to VPN, a StarRocks insert into `databricks_uc.sr_poc_external.sr_test_events` reached the PoC-owned ADLS location but failed before writing a row:

```text
ERROR 5609: Failed to create HDFS directory
abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/
sr_poc_external/__unitystorage/.../load_spill

AbfsRestOperationException: This request is not authorized to perform this operation.
HTTP 403 PUT
```

**VPN ADLS verification completed:** With VPN connected, the PoC ADLS OAuth service principal successfully authenticated and created, listed, and deleted a temporary directory in `stkznneusrpoccdddevstd/sr-poc-cont1`. This confirms the current network path and ADLS data-plane permission for that principal are working.

**StarRocks write limitation:** After the successful VPN verification, a StarRocks insert wrote far enough to attempt the Iceberg commit but failed with a Unity Catalog Iceberg REST catalog error:

```text
ERROR 1064: Service failed: 500: Could not process table operation.
ErrorCode: 2012
```

StarRocks warned that the commit outcome was uncertain. We checked the test ID through StarRocks, Trino, and Databricks SQL; all returned zero rows. The commit did not complete, and the insert must not be retried until the catalog error is investigated.

**Additional prerequisite completed:** The Databricks Iceberg REST catalog principal `3b7f531f-db93-4186-af75-6566c12c076b` was initially missing `EXTERNAL USE SCHEMA`, which is required in addition to `SELECT` and `MODIFY` for external Iceberg REST access. We granted it on `de_dev.sr_poc_external` and verified that it appears as `EXTERNAL_USE_SCHEMA`.

**Repeated commit result:** Retrying the StarRocks insert with a new test ID after the external-use grant still failed with the same Iceberg REST catalog `500`. The second request ID is `bc643f52-3aee-4b71-b679-752ae1841a22`. The test ID returned zero rows through StarRocks, Trino, and Databricks SQL, so this commit also did not complete.

**Supported reader validation completed:** A control row was inserted through Databricks SQL into `de_dev.sr_poc_external.sr_test_events`. StarRocks read the row successfully through `databricks_uc.sr_poc_external`. Trino initially failed because its configured `ADLS_ACCOUNT_KEY` was invalid for the PoC account. After switching Trino to the validated ADLS OAuth service principal, it read the same control row successfully (`control_row_count = 1`).

**Trino write validation:** Trino was configured with the validated ADLS OAuth service principal and successfully read the non-empty control row. Its insert then failed at the same Unity Catalog Iceberg REST commit stage:

```text
Failed to commit the transaction during insert:
Service failed: 500: Could not process table operation.
ErrorCode: 2012
request_id: 8346c171-bb94-469e-b13d-0718da00b32f
```

The Trino test ID returned zero rows through Databricks SQL, StarRocks, and Trino, so this commit also did not complete.

**Historical design at this point:** StarRocks and Trino were validated external
readers only. The `catalogManaged` root cause, conversion, and successful
write validation supersede this interim position in section 2.1.9.

**Historical escalation:** Before the feature conversion, the same Databricks
Iceberg REST catalog `500` prevented commits from both StarRocks and Trino,
despite verified Unity Catalog grants, `EXTERNAL USE SCHEMA`, VPN ADLS
data-plane access, and non-empty reads. The recorded request IDs remain useful
for platform investigation: `d96f9311-2d6f-4868-8855-6841fb5713b3`,
`bc643f52-3aee-4b71-b679-752ae1841a22`, and
`8346c171-bb94-469e-b13d-0718da00b32f`.

**Option A: Retain the current Databricks-managed tables.** An administrator grants the ADLS OAuth service principal used by StarRocks `Storage Blob Data Contributor` on `stkznneucommoncdddevstd`, preferably scoped to the `cross-operator` container. Then add a second OAuth account configuration for `stkznneucommoncdddevstd.dfs.core.windows.net` to the StarRocks Hadoop `core-site.xml` configuration.

**Option B: Recreate the empty tables in the PoC-owned ADLS account.** This is the selected and completed reader-data-plane design. The external location, delegated Unity Catalog access, `de_dev.sr_poc_external` schema, replacement tables, workload-principal grants, VPN ADLS validation, and non-empty reads through StarRocks and Trino are ready.

**Image validation:** StarRocks was successfully recreated from the upstream `starrocks/allin1-ubuntu:4.1.4` image after removing the SAS-only custom image patch. The write limitation is unrelated to the image.

**Historical next validation:** The validation was completed after the shared
commit failure was resolved by dropping `catalogManaged`. Section 2.1.9 records
the successful RisingWave, StarRocks, and Trino writes.

### 2.1.7b Historical RisingWave service-principal handoff (2026-09-02)

**Historical status: initially blocked on Databricks service-principal
authorization.** The subsequent schema grants, ownership alignment, and
successful external writes recorded in section 2.1.9 supersede this initial
blocker. The DEV and STG workspaces expose the same Unity Catalog metastore
object. The current local stack uses the DEV host, which remains a valid target
for the shared metastore.

The Jira request's statement that the principal was "created in STG" refers to
the STG Azure environment and ADLS roles. It does not prove the principal is
provisioned in the STG Databricks workspace or visible to its Unity Catalog
metastore. The STG workspace host is:

```text
https://adb-2241475393894655.15.azuredatabricks.net
```

Both the DEV and STG workspace Catalog Explorer views report the same schema
comment, managed root location, and schema UUID
`6fa9db04-0d77-41dd-bc2d-2e8a8aeced7f`. This proves they expose the same Unity
Catalog schema rather than independent environment copies. The shared schema is
the external-engine validation target and has this managed root location:

```text
abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/sr_poc_external
```

The PoC uses the following new Entra service principal and ADLS configuration:

```text
Service principal: sp-stkznneusrpoccdddevstd-contributor
Application ID:    27a78a40-69f4-40e0-9768-ba39d58a6a55
Tenant ID:         78395483-9425-447a-ba64-60b90f6bb16e
Databricks catalog: de_dev
Databricks schema:  sr_poc_external
ADLS account:       stkznneusrpoccdddevstd
ADLS container:     sr-poc-cont1
```

The principal is confirmed in Entra ID and has `Reader` on its resource group plus
`Storage Blob Data Contributor` on `stkznneusrpoccdddevstd`. These Azure RBAC
assignments cover direct ADLS data-plane access, but they do not make the
principal available to the Unity Catalog metastore.

The initial workspace SCIM test and workspace-local SCIM record below apply to
the DEV workspace, `https://adb-1608121643336927.7.azuredatabricks.net`, because
that host was configured in the local `personal` profile and `.env` at the time.
They do not establish anything about STG:

```text
Workspace SCIM ID: 147673200001521
Display name:      sp-stkznneusrpoccdddevstd-contributor
Application ID:    27a78a40-69f4-40e0-9768-ba39d58a6a55
Entitlements:      workspace-access, databricks-sql-access
```

The `PRINCIPAL_DOES_NOT_EXIST` error was returned while executing Unity Catalog
grants in STG, not DEV. Because both workspaces share this metastore, the error
means the principal is not metastore-visible. A direct STG check subsequently
obtained an Azure AD access token successfully for this client ID, then received
HTTP `403` for both the current-principal and service-principal SCIM endpoints.
This demonstrates that the Entra credentials are valid but does not, by itself,
distinguish a missing Databricks account-level principal from a Databricks API
permission restriction.

#### RisingWave probe result

A minimal append-only RisingWave Iceberg sink was attempted against
`de_dev.sr_poc_external.rw_write_probe`. The sink used Azure AD OAuth for the
Databricks Iceberg REST catalog and account-key authentication for direct ADLS
writes. It failed before the first row was written:

```text
org.apache.iceberg.exceptions.ForbiddenException: Forbidden: User not authorized
```

The sink configuration was corrected during the test so that it uses one ADLS
authentication mode only. It must not combine `adlsgen2.account_key` with
`adlsgen2.tenant_id`, `adlsgen2.client_id`, or `adlsgen2.client_secret`.

#### Required admin action

An administrator must provision the Entra application as a Databricks
account-level principal that is visible to the shared Unity Catalog metastore.
The principal may then be assigned to the workspace endpoint selected for the
RisingWave catalog connection. The admin must grant the following privileges on
the shared metastore:

```sql
GRANT USE CATALOG ON CATALOG de_dev
TO `sp-stkznneusrpoccdddevstd-contributor`;

GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT
ON SCHEMA de_dev.sr_poc_external
TO `sp-stkznneusrpoccdddevstd-contributor`;

GRANT EXTERNAL USE SCHEMA
ON SCHEMA de_dev.sr_poc_external
TO `sp-stkznneusrpoccdddevstd-contributor`;
```

`EXTERNAL USE SCHEMA` is required because RisingWave writes through the Unity
Catalog Iceberg REST endpoint. The existing DEV workspace-local SCIM record
must not be used as evidence of account-level or metastore provisioning. Do not
delete it until the administrator confirms whether it must be reconciled or
removed.

#### Resume checklist

1. Confirm the application ID is selectable in the shared
  `de_dev.sr_poc_external` Unity Catalog permissions dialog.
2. Confirm the grants above complete without `PRINCIPAL_DOES_NOT_EXIST`.
3. Confirm whether the team requires the DEV or STG workspace endpoint for the
  RisingWave catalog connection. No endpoint change is required solely to
  access this shared metastore.
4. Re-run the minimal `rw_write_probe` sink using catalog OAuth and
  `adlsgen2.account_key` only.
5. Insert one probe row in RisingWave and query the target through Databricks
  SQL and Trino after the sink checkpoint commits.
6. If the sink progresses beyond authorization but fails its metadata commit,
  record the Databricks request ID and compare it with the existing shared
  Iceberg REST `500` investigation above.

### 2.1.8 Recommended validation sequence (2026-09-02)

This follow-up pass was run to validate the remaining hypotheses without involving the Databricks team.

#### Validation 1: Table capability metadata

Command executed:

```bash
databricks tables get de_dev.sr_poc_external.sr_test_events --profile personal --output json | jq '{name, table_type, data_source_format, storage_location, capabilities}'
```

Verified result:

```json
{
  "name": "sr_test_events",
  "table_type": "MANAGED",
  "data_source_format": "DELTA",
  "storage_location": "abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/sr_poc_external/__unitystorage/.../tables/...",
  "capabilities": null
}
```

Interpretation: the Unity Catalog table metadata does not advertise a `HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT` capability flag. This does not prove the table is ineligible to write, but it does confirm the platform is not exposing a direct capability signal in the table object.

#### Validation 2: Raw table payload inspection

Command executed:

```bash
databricks api get '/api/2.1/unity-catalog/tables/de_dev.sr_poc_external.sr_test_events' --profile personal --output json | jq '{full_name, table_type, data_source_format, properties, capabilities, storage_location}'
```

Verified result (key fields):

```json
{
  "full_name": "de_dev.sr_poc_external.sr_test_events",
  "table_type": "MANAGED",
  "data_source_format": "DELTA",
  "properties": {
    "delta.universalFormat.enabledFormats": "iceberg",
    "delta.feature.icebergCompatV2": "supported",
    "delta.feature.icebergWriterCompatV1": "supported",
    "write.object-storage.enabled": "true",
    "write.metadata.path": "abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/sr_poc_external/__unitystorage/.../_iceberg/metadata"
  },
  "capabilities": null,
  "storage_location": "abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/sr_poc_external/__unitystorage/.../tables/..."
}
```

Interpretation: the table is configured as a managed Delta table with Iceberg universal-format compatibility enabled. The absence of the explicit `capabilities` field makes the problem look like a server-side UC commit rejection rather than a missing table property.

#### Validation 3: Grant and permission lookups

Commands executed:

```bash
databricks permissions get table de_dev.sr_poc_external.sr_test_events --profile personal --output json
databricks permissions get schema de_dev.sr_poc_external --profile personal --output json
databricks grants get external-location stkznneusrpoccdddevstd_sr-poc-cont1 --profile personal --output json
```

Verified result:

```text
Error: 'table' is not a supported object type for permissions. Expected one of {alerts,...,warehouses,vector-search-endpoints}.
Error: 'schema' is not a supported object type for permissions. Expected one of {alerts,...,warehouses,vector-search-endpoints}.
{
  "name": null,
  "grants": null
}
```

Interpretation: the CLI profile in this environment does not support those permission endpoints directly, so the secure path is to rely on the Databricks SQL `SHOW GRANTS`/`DESCRIBE` checks already recorded earlier in this plan. This environment also did not yield a useful external-location grant dump via this command.

#### Validation 4: Fresh probe-table creation attempt

Command executed:

```bash
databricks sql --profile personal -c de_dev -q "CREATE TABLE IF NOT EXISTS sr_poc_external.trino_probe (id BIGINT, event_ts TIMESTAMP, payload STRING) USING DELTA; INSERT INTO sr_poc_external.trino_probe VALUES (1, TIMESTAMP '2026-09-02 12:00:00', 'probe'); SELECT * FROM sr_poc_external.trino_probe;"
```

Verified result:

```text
Error: unknown command "sql" for "databricks"
```

Interpretation: this workspace CLI does not include the `databricks sql` subcommand. The safe alternative is to create and validate the probe table through Databricks SQL Editor / `psql` or a SQL warehouse, not the CLI.

#### Validation 5: Trino minimal read/write probe against a fresh table

Commands executed:

```bash
docker exec trino trino --execute "SELECT * FROM databricks.sr_poc_external.trino_probe"
docker exec trino trino --execute "INSERT INTO databricks.sr_poc_external.trino_probe VALUES (2, TIMESTAMP '2026-09-02 12:00:00', 'probe2')"
```

Verified result:

```text
Query failed: Table 'databricks.sr_poc_external.trino_probe' does not exist
Query failed: Table 'databricks.sr_poc_external.trino_probe' does not exist
```

Interpretation: there was no fresh test table to probe. This confirms the write failure is not being retested against a newly created table yet; only the existing production-style `sr_test_events` table has been exercised.

#### Validation 6: StarRocks minimal probe against a fresh table

Command executed:

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "INSERT INTO databricks_uc.sr_poc_external.trino_probe VALUES (3, '2026-09-02 12:00:00', 'probe3');"
```

Verified result:

```text
ERROR 1064 (HY000) at line 1: Getting analyzing error. Detail message: Table trino_probe is not found.
```

Interpretation: no fresh probe table existed in the catalog, so the engine-level failure we saw earlier is isolated to the existing table write path and not to a valid new table being created ad hoc.

#### Conclusion from the follow-up run

The follow-up checks did not identify a missing grant or a missing table capability flag that could be fixed locally. The most likely root cause remains a Databricks Unity Catalog Iceberg REST commit rejection on the external write path, because:

- the table is configured as managed Delta + Iceberg-compatible,
- the external location and schema are valid,
- file reads succeed via the ADLS data plane,
- the same write failure surfaces in both Trino and StarRocks,
- and the API metadata does not expose a direct `HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT` capability flag.

The next best local move is not to keep retrying writes. It is to create a fresh probe table in Databricks SQL Editor or a SQL warehouse and re-test one minimal insert only after the workspace-level commit contract is clarified.

> [!IMPORTANT]
> The conclusion above records the intermediate state before the
> `catalogManaged` comparison. Section 2.1.9 supersedes it with the confirmed
> root cause and successful external-write validation.

### 2.1.9 Resolved external-write failure on catalog-managed tables

**Status: Resolved on an isolated probe table on 2026-09-02.** The Databricks
table feature `delta.feature.catalogManaged` caused Unity Catalog Iceberg REST
Catalog (IRC) commits to fail with HTTP `500`, `ErrorCode: 2012`. Dropping that
protocol feature converted the probe to the same compatibility state as the
historical `de_dev.rw_poc` tables and restored external writes from RisingWave
and StarRocks.

#### Scope of the resolution

The successful test used this isolated target:

```text
de_dev.sr_poc_external.rw_irc_probe_20260902
```

At the time of the isolated probe, the production-style targets
`sr_test_events` and `sr_hourly_agg` had not been converted. Both were
subsequently converted and validated with external writes, as recorded in the
latest status below.

#### Historical comparison through the personal profile

The Databricks CLI `personal` profile compared the historical working table and
the new failing probe:

| Property | Historical working table | New table before conversion |
| ---------- | -------------------------- | ----------------------------- |
| Table | `de_dev.rw_poc.rw_casino_transactions` | `de_dev.sr_poc_external.rw_irc_probe_20260902` |
| Provider | `iceberg` | `iceberg` |
| Type | `MANAGED` | `MANAGED` |
| Predictive optimization | `ENABLE` | `ENABLE` after explicit configuration |
| Table owner | `3b7f531f-db93-4186-af75-6566c12c076b` | `27a78a40-69f4-40e0-9768-ba39d58a6a55` |
| `delta.feature.catalogManaged` | Absent | `supported` |
| Atomic Iceberg conversion | Absent | Enabled |
| External IRC commits | Successful | Failed with `ErrorCode: 2012` |

Permissions, ownership, table provider, predictive optimization, VPN access,
ADLS data-plane access, and client versions were tested independently before
the feature conversion. None resolved the commit error.

#### Failed writers before conversion

All three external writers reached the Unity Catalog commit endpoint and
received the same server-side failure:

```text
Service failed: 500: Could not process table operation.
ErrorCode: 2012
```

Representative request IDs include:

```text
StarRocks: 2503bfd5-daa6-4248-a716-a62fd8043aef
StarRocks: a6dfe5b8-24c0-4ea5-ac9f-635372f74226
StarRocks: 9a7adc5d-c854-4d3b-9282-13bfaa925506
Trino:     c3d55af0-10c2-40ea-a36b-4dfdfad52c0e
RisingWave: 0100e030-e6cb-4b20-8e1c-0f51c7dd8dde
RisingWave: f218de36-c580-49ac-8d45-32b507a2772a
RisingWave: abee1f8c-fc3c-41e6-b082-88df2e09eab2
RisingWave: 70328369-13b7-411b-a95c-a3122386fa29
RisingWave: c185e3d7-18bd-4826-ad3a-7073eba3442c
```

Each uncertain commit was checked through Databricks SQL and an external
reader. No failed-attempt row was present.

#### Conversion procedure

An initial property unset completed but did not remove the protocol feature:

```sql
ALTER TABLE de_dev.sr_poc_external.rw_irc_probe_20260902
UNSET TBLPROPERTIES ('delta.feature.catalogManaged');
```

After that statement, the API still returned:

```text
delta.feature.catalogManaged = supported
```

The effective conversion used Delta's protocol-feature operation:

```sql
ALTER TABLE de_dev.sr_poc_external.rw_irc_probe_20260902
DROP FEATURE catalogManaged;
```

Databricks recorded the conversion at table version `5`:

```text
operation: DROP FEATURE
operationParameters: {"featureName":"catalogManaged","truncateHistory":"false"}
```

After conversion, the Unity Catalog API no longer returned
`delta.feature.catalogManaged`. SQL metadata still reported:

```text
Type: MANAGED
Provider: iceberg
Owner: 27a78a40-69f4-40e0-9768-ba39d58a6a55
Predictive Optimization: ENABLE
```

#### RisingWave validation

RisingWave `3.2.0-alpha` used an isolated append-only source, a timezone-aware
materialized view, and this sink behavior:

```sql
connector = 'iceberg',
type = 'append-only',
force_append_only = 'true',
commit_checkpoint_interval = 1
```

Before conversion, RisingWave built a one-file Iceberg v2 snapshot but every
commit retry returned `ErrorCode: 2012`. After `DROP FEATURE catalogManaged`,
the same sink configuration committed successfully. Databricks recorded:

```text
version 6: WRITE, Kernel-4.4.0-SNAPSHOT/Iceberg REST Catalog
version 7: WRITE, Kernel-4.4.0-SNAPSHOT/Iceberg REST Catalog
```

Databricks SQL returned both RisingWave probe rows:

```text
risingwave-irc-probe-20260902-1407
risingwave-after-drop-feature-20260902-1435
```

The temporary RisingWave sink was dropped after validation to prevent further
backfill or retries. The local source and materialized view remain available
for follow-up diagnostics.

#### StarRocks validation

StarRocks inserted a fresh row into the same converted table without an error:

```text
starrocks-after-drop-feature-20260902-1437
```

Databricks SQL read the row successfully and recorded another IRC write at
version `8`.

#### StarRocks decimal write compatibility

After the general `catalogManaged` issue was resolved, StarRocks still failed
to append to `sr_hourly_agg`. A three-table isolated schema matrix identified
the table-level cause. Every probe was a native managed Iceberg table with
predictive optimization enabled and `catalogManaged` removed.

| Probe | Timestamp type | Amount type | Owner | StarRocks write |
| ---------- | ----------------- | ------------- | ----------------------------------------- | ---------------- |
| `sr_starrocks_ntz_double_probe_20260902` | `TIMESTAMP_NTZ` | `DOUBLE` | `s.gioldasis-si@devkaizengaming.com` | Working, version `5` IRC `WRITE` |
| `sr_starrocks_tz_decimal_probe_20260902` | `TIMESTAMP` | `DECIMAL(10,2)` | `s.gioldasis-si@devkaizengaming.com` | Failed, `ErrorCode: 2012` |
| `sr_starrocks_hourly_contract_probe_20260902` | `TIMESTAMP_NTZ` | `DECIMAL(10,2)` | `s.gioldasis-si@devkaizengaming.com` | Failed, `ErrorCode: 2012` |

The successful user-owned probe committed
`ntz-double-20260902-1535`. The two failed decimal probes returned zero rows
and no `WRITE` entry in their table histories. Their request IDs were:

```text
TIMESTAMP plus DECIMAL: 5e486a69-76c7-4399-8ec9-c708e8900eca
TIMESTAMP_NTZ plus DECIMAL: c160b2f8-5c67-4e2d-87b4-82880bbd6aad
```

**Conclusion:** StarRocks can write `TIMESTAMP_NTZ` to the converted tables.
Its remaining incompatibility is `DECIMAL(10,2)` in the Iceberg REST commit
path. Trino wrote `DECIMAL(10,2)` to `sr_hourly_agg` and RisingWave wrote the
same decimal type to `sr_test_events`, so this is not a Unity Catalog table,
permission, or general external-writer limitation.

**Upstream root cause:** [StarRocks PR #78456](https://github.com/StarRocks/starrocks/pull/78456)
matches this failure exactly. StarRocks 4.1.4 writes fixed-width Parquet
statistics buffers as Iceberg decimal manifest bounds. The Iceberg specification
requires each decimal bound to be an unscaled, minimum-length,
two's-complement, big-endian value. Unity Catalog validates those bounds during
the REST commit and rejects the non-canonical bytes with `ErrorCode: 2012`.
The upstream PR includes the same Databricks Unity Catalog failure and a
successful PyIceberg control write against the same table type, isolating the
defect to StarRocks manifest serialization.

The PR is open as of 2026-09-02, targets `main`, and carries `4.1`, `4.0`, and
`3.5` backport labels. No released StarRocks version containing the fix was
confirmed during this investigation. There is no catalog property or session
setting that corrects the decimal encoding in StarRocks 4.1.4.

**Operational recommendation:** Preserve existing decimal schemas. Route
decimal writes through RisingWave or Trino until a vendor-confirmed StarRocks
build contains this fix. If direct StarRocks writes are mandatory before then,
use a separate exact scaled-integer staging table and cast it to the final
decimal target with RisingWave or Trino. Do not substitute `DOUBLE` for money
or other values requiring exact fixed-point semantics.

#### Ownership validation

Table ownership is not required for external visibility or write access.
`sr_hourly_agg` was user-owned when Trino successfully wrote to it, and the
user-owned `TIMESTAMP_NTZ + DOUBLE` probe was successfully written by
StarRocks. A temporary ownership transfer of `sr_hourly_agg` to the writer
service principal did not change the StarRocks `ErrorCode: 2012` result, and
the original user owner was restored.

External engines require the relevant Unity Catalog grants, including
`EXTERNAL USE SCHEMA`, and ADLS data-plane access. Ownership only controls
administrative actions such as changing metadata or grants.

#### Delta plus UniForm comparison

An isolated managed Delta table was created to test UniForm as an alternative
to native managed Iceberg:

```text
de_dev.sr_poc_external.delta_uniform_probe_20260902
```

The table was created with Iceberg reads enabled:

```sql
CREATE TABLE de_dev.sr_poc_external.delta_uniform_probe_20260902 (
  id STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  user_id STRING NOT NULL,
  event_type STRING NOT NULL,
  amount DOUBLE,
  details STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'id',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

A Databricks-native control insert succeeded. `MSCK REPAIR TABLE ... SYNC
METADATA` completed before reader validation. Databricks then reported:

```text
Provider: delta
# Delta Uniform Iceberg
```

Both external engines read the Databricks control row successfully:

```text
delta-control-20260902-1445 | delta_uniform_control | 8.0
```

#### StarRocks Delta plus UniForm reader validation

A second managed Delta table validated the supported StarRocks reader path with
an exact fixed-point value:

```text
de_dev.sr_poc_external.delta_starrocks_read_probe_20260902
```

It uses managed Delta with the following UniForm properties:

```text
delta.columnMapping.mode = id
delta.enableIcebergCompatV2 = true
delta.universalFormat.enabledFormats = iceberg
```

A native Delta control write committed a `DECIMAL(12,2)` value. After
`MSCK REPAIR TABLE ... SYNC METADATA` and a StarRocks metadata refresh, the
existing `databricks_uc` Iceberg REST catalog read the row successfully:

```text
1001 | starrocks_delta_read | 123.45 | 2026-09-02 16:00:00
```

This validates the reader flow for existing Unity Catalog-managed Delta tables:

```text
Databricks Delta table
  -> UniForm Iceberg metadata
  -> Unity Catalog Iceberg REST catalog
  -> StarRocks Iceberg external catalog
```

This does not use StarRocks' native Delta Lake catalog. That catalog requires a
Hive Metastore or AWS Glue metadata backend and cannot use Unity Catalog as a
documented Delta metastore. UniForm exposure through Unity Catalog Iceberg REST
is read-only for StarRocks.

StarRocks then attempted an external insert and received the explicit,
non-ambiguous rejection below:

```text
Malformed request: Table
'de_dev.sr_poc_external.delta_uniform_probe_20260902' is not a Managed
Iceberg table.
request_id: 94c873c0-8a0f-46d7-a237-00bba85de9c4
```

Databricks confirmed that the external probe row was absent and that table
history contains only the native Databricks write.

**Decision:** Delta plus UniForm is a valid external-reader format. It is not
a writer target for RisingWave, StarRocks, or Trino through the Iceberg REST
Catalog. Continue to use native managed Iceberg tables with
`catalogManaged` removed for external writes.

#### Root cause and current operating rule

The failure was not caused by the new service principal, ADLS OAuth, VPN,
schema grants, `EXTERNAL USE SCHEMA`, ownership, predictive optimization, or
the external writer versions. The discriminating variable was the
`catalogManaged` protocol feature automatically added to newly created managed
Iceberg tables in this workspace.

> [!CAUTION]
> `DROP FEATURE catalogManaged` changes the table protocol and creates Delta
> history commits. Apply it to one non-production table at a time, verify the
> resulting provider and maintenance settings, and test both Databricks-native
> and external reads before converting operational targets.

Use this validation sequence for each candidate table:

1. Confirm `Provider = iceberg`, `Type = MANAGED`, and predictive optimization
  is enabled.
2. Record the current owner, properties, and table history.
3. Stop external writers to avoid uncertain concurrent commits.
4. Run `ALTER TABLE ... DROP FEATURE catalogManaged` through Databricks SQL.
5. Confirm `delta.feature.catalogManaged` is absent through the Unity Catalog
  table API.
6. Start one append-only writer and commit one uniquely identified row.
7. Verify the row and the new `WRITE` history entry through Databricks SQL.
8. Re-enable the intended pipeline only after the single-row check succeeds.

#### Latest status

External writing is now technically validated for the converted isolated probe:

| Capability | Status |
| ------------ | -------- |
| Direct ADLS OAuth access over VPN | Working |
| Databricks-native write | Working |
| RisingWave append-only IRC write after conversion | Working |
| StarRocks IRC write after conversion | Working |
| Trino IRC write after conversion | Working on the isolated probe and `sr_hourly_agg` |
| StarRocks decimal writes | Unsupported for `DECIMAL(10,2)` tables; use `DOUBLE` or another writer |
| Delta plus UniForm external reads | Working |
| StarRocks reads Delta plus UniForm `DECIMAL(12,2)` | Working |
| Delta plus UniForm external writes | Explicitly unsupported |
| RisingWave append to `sr_test_events` | Working; row readable from StarRocks and Trino |
| Trino append to `sr_hourly_agg` | Working; row readable from StarRocks after refresh |
| Production pipeline migration | Not started |

Trino also committed `trino-after-drop-feature-20260902-1455` to the converted
probe table. RisingWave committed
`risingwave-sr-test-events-20260902-1510` to `sr_test_events` at version `6`.
Trino committed one aggregate row to `sr_hourly_agg` at version `5`. The next
controlled action is a limited RisingWave dbt-sink rollout.

### 2.1.10 Existing-table reader test: `de_dev.sling.fact_virtual`

#### Test description

Validate read-only access to the existing Unity Catalog table
`de_dev.sling.fact_virtual` from StarRocks and Trino through the Databricks
Unity Catalog Iceberg REST catalog. The test confirms three independent
permissions: Databricks metadata visibility, Iceberg-compatible metadata for
the Delta table, and direct read access to the underlying ADLS files. It does
not test external writes.

The table was confirmed through the `personal` Databricks profile on
2026-09-03:

| Property | Observed value |
| --- | --- |
| Table type | Managed Delta |
| Rows | 6 |
| Columns | `id`, `virtual_item`, `cost`, `timestamp`, `_sling_loaded_at` |
| Storage | `stkznneucommoncdddevstd`, container `cross-operator` |
| Existing features | Deletion vectors and row tracking enabled |
| Container identity | `27a78a40-69f4-40e0-9768-ba39d58a6a55` |

#### Execution results

The commands below were executed on 2026-09-03 with the `personal` Databricks
profile. Secrets are omitted from this record.

| Command | Result |
| --- | --- |
| `SELECT current_user()` | `s.gioldasis-si@devkaizengaming.com` |
| `SHOW TBLPROPERTIES de_dev.sling.fact_virtual` before changes | Delta table with no UniForm properties; deletion vectors and row tracking enabled |
| `GRANT USE CATALOG ON CATALOG de_dev` | Succeeded |
| `GRANT USE SCHEMA ON SCHEMA de_dev.sling` | Succeeded |
| `GRANT EXTERNAL USE SCHEMA ON SCHEMA de_dev.sling` | Succeeded |
| `GRANT SELECT ON TABLE de_dev.sling.fact_virtual` | Succeeded |
| UniForm command with column mapping `id` | Rejected: changing column mapping from `none` to `id` is unsupported |
| UniForm command without column mapping | Rejected: IcebergCompatV2 requires column mapping mode `name` |
| UniForm command with column mapping `name` | Rejected initially because deletion vectors were enabled |
| `ALTER TABLE ... SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')` | Succeeded |
| `REORG TABLE ... APPLY (PURGE)` | Succeeded; zero deletion vectors and zero deletion-vector rows removed |
| UniForm command with column mapping `name` after purge | Succeeded |
| `MSCK REPAIR TABLE ... SYNC METADATA` | Succeeded |
| Final row count in Databricks SQL | `6` |
| Trino `SHOW TABLES FROM databricks.sling` | `fact_virtual` visible |
| StarRocks `SHOW TABLES FROM databricks_uc.sling` | `fact_virtual` visible |
| StarRocks data read | Blocked by missing filesystem credentials for `stkznneucommoncdddevstd` |
| Trino data read | Failed while processing table metadata; ADLS data-plane access remains unresolved |

The exact successful Databricks commands were:

```bash
databricks experimental aitools tools query \
  "GRANT USE CATALOG ON CATALOG de_dev TO \`27a78a40-69f4-40e0-9768-ba39d58a6a55\`" \
  --profile personal

databricks experimental aitools tools query \
  "GRANT USE SCHEMA ON SCHEMA de_dev.sling TO \`27a78a40-69f4-40e0-9768-ba39d58a6a55\`" \
  --profile personal

databricks experimental aitools tools query \
  "GRANT EXTERNAL USE SCHEMA ON SCHEMA de_dev.sling TO \`27a78a40-69f4-40e0-9768-ba39d58a6a55\`" \
  --profile personal

databricks experimental aitools tools query \
  "GRANT SELECT ON TABLE de_dev.sling.fact_virtual TO \`27a78a40-69f4-40e0-9768-ba39d58a6a55\`" \
  --profile personal

databricks experimental aitools tools query \
  "ALTER TABLE de_dev.sling.fact_virtual SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')" \
  --profile personal

databricks experimental aitools tools query \
  "REORG TABLE de_dev.sling.fact_virtual APPLY (PURGE)" \
  --profile personal

databricks experimental aitools tools query \
  "ALTER TABLE de_dev.sling.fact_virtual SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')" \
  --profile personal

databricks experimental aitools tools query \
  "MSCK REPAIR TABLE de_dev.sling.fact_virtual SYNC METADATA" \
  --profile personal
```

The final table properties include:

```text
delta.columnMapping.mode = name
delta.enableDeletionVectors = false
delta.enableIcebergCompatV2 = true
delta.universalFormat.enabledFormats = iceberg
```

The Databricks-side setup is complete, but the full external-reader test is
not yet passing. The table location is
`abfss://cross-operator@stkznneucommoncdddevstd.dfs.core.windows.net/...`,
while the current local container configuration targets
`stkznneusrpoccdddevstd` and `sr-poc-cont1`. Update the StarRocks Hadoop
configuration and Trino ADLS configuration with credentials and Azure RBAC
for the actual `stkznneucommoncdddevstd/cross-operator` location, then rerun
the reader commands below.

#### Snapshot-copy alternative

To avoid changing the local container storage configuration, a read-test copy
was created in the existing PoC schema. This is a point-in-time snapshot of
the source table, not a live synchronization:

```bash
databricks experimental aitools tools query \
  "CREATE TABLE de_dev.sr_poc_external.fact_virtual_read_test USING ICEBERG AS SELECT id, virtual_item, cost, timestamp, _sling_loaded_at FROM de_dev.sling.fact_virtual" \
  --profile personal
```

Result: succeeded with `0` reported affected rows. Databricks verification
returned `6` rows. The destination is stored under the expected PoC location:

```text
abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/sr_poc_external/...
```

The destination table was granted to the container service principal:

```bash
databricks experimental aitools tools query \
  "GRANT SELECT ON TABLE de_dev.sr_poc_external.fact_virtual_read_test TO \`27a78a40-69f4-40e0-9768-ba39d58a6a55\`" \
  --profile personal
```

Result: succeeded with no returned rows.

The destination did not require `MSCK REPAIR TABLE` because it was created as
a native managed Iceberg table in the target schema. Its catalog metadata
does include the workspace's `catalogManaged` feature, but external reads are
supported and no external write was attempted.

StarRocks validation succeeded after refreshing its external-table metadata:

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root --batch --raw \
  -e "REFRESH EXTERNAL TABLE databricks_uc.sr_poc_external.fact_virtual_read_test; \
      SELECT COUNT(*) AS row_count FROM databricks_uc.sr_poc_external.fact_virtual_read_test; \
      SELECT * FROM databricks_uc.sr_poc_external.fact_virtual_read_test LIMIT 5"
```

Result:

```text
row_count
6

id  virtual_item  cost  timestamp            _sling_loaded_at
1   s1            1     2025-01-14 15:00:00  1775543147
2   s2            2     2025-01-14 15:01:00  1775543147
3   s3            3     2025-01-14 15:02:00  1775543147
4   s4            4     2025-01-14 15:02:00  1775543147
5   s5            5     2026-01-22 00:00:00  1775543147
```

Trino validation also succeeded:

```bash
docker exec trino trino --execute \
  "SELECT COUNT(*) AS row_count FROM databricks.sr_poc_external.fact_virtual_read_test; \
   SELECT * FROM databricks.sr_poc_external.fact_virtual_read_test LIMIT 5"
```

Result: `row_count = 6`, with the same five sample rows returned by StarRocks.

This alternative passes the external-reader test using the current local
container configuration:

```text
Databricks source:  de_dev.sling.fact_virtual
Databricks copy:    de_dev.sr_poc_external.fact_virtual_read_test
StarRocks:          databricks_uc.sr_poc_external.fact_virtual_read_test
Trino:              databricks.sr_poc_external.fact_virtual_read_test
Result:             PASS, six rows readable from both engines
```

#### As-is Delta control test

To test whether copying the source without changing its format was sufficient,
a second snapshot was created with plain CTAS:

```bash
databricks experimental aitools tools query \
  "CREATE TABLE de_dev.sr_poc_external.fact_virtual_as_is_test_20260903 AS SELECT * FROM de_dev.sling.fact_virtual" \
  --profile personal
```

Result: succeeded. Databricks reported `6` rows, and the destination location
was under `stkznneusrpoccdddevstd/sr-poc-cont1`. The table metadata confirmed:

```text
data_source_format = DELTA
delta.columnMapping.mode = not set
delta.enableIcebergCompatV2 = not set
delta.universalFormat.enabledFormats = not set
```

The container service principal was granted table access:

```bash
databricks experimental aitools tools query \
  "GRANT SELECT ON TABLE de_dev.sr_poc_external.fact_virtual_as_is_test_20260903 TO \`27a78a40-69f4-40e0-9768-ba39d58a6a55\`" \
  --profile personal
```

Result: succeeded with no returned rows.

The original reader commands were then run unchanged. StarRocks listed the
table but failed to read it:

```text
Malformed request: Table
'de_dev.sr_poc_external.fact_virtual_as_is_test_20260903' is not an Iceberg
compatible table. ErrorCode: 1000
```

Trino also listed the table but failed while loading it:

```text
Failed to load table: fact_virtual_as_is_test_20260903
in sr_poc_external namespace
```

This control test confirms that copying a Delta table as-is is insufficient.
The copy must be created as native managed Iceberg, or Delta UniForm Iceberg
metadata must be enabled, before StarRocks and Trino can read it through the
configured Unity Catalog Iceberg REST catalog. The earlier
`fact_virtual_read_test` native-Iceberg copy is the passing approach.

#### Conversion test on the as-is copy

The as-is copy was then converted using the same validated sequence used for
the source table:

```bash
databricks experimental aitools tools query \
  "ALTER TABLE de_dev.sr_poc_external.fact_virtual_as_is_test_20260903 SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')" \
  --profile personal

databricks experimental aitools tools query \
  "REORG TABLE de_dev.sr_poc_external.fact_virtual_as_is_test_20260903 APPLY (PURGE)" \
  --profile personal

databricks experimental aitools tools query \
  "ALTER TABLE de_dev.sr_poc_external.fact_virtual_as_is_test_20260903 SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')" \
  --profile personal

databricks experimental aitools tools query \
  "MSCK REPAIR TABLE de_dev.sr_poc_external.fact_virtual_as_is_test_20260903 SYNC METADATA" \
  --profile personal
```

Results:

| Command | Result |
| --- | --- |
| Disable deletion vectors | Succeeded |
| `REORG ... APPLY (PURGE)` | Succeeded; zero deletion vectors and zero deletion-vector rows removed |
| Enable column mapping `name` and UniForm | Succeeded |
| `MSCK REPAIR TABLE ... SYNC METADATA` | Succeeded |
| Final row count in Databricks | `6` |

Final relevant properties:

```text
delta.columnMapping.mode = name
delta.enableDeletionVectors = false
delta.enableIcebergCompatV2 = true
delta.universalFormat.enabledFormats = iceberg
```

The original StarRocks command was rerun after refreshing metadata:

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root --batch --raw \
  -e "REFRESH EXTERNAL TABLE databricks_uc.sr_poc_external.fact_virtual_as_is_test_20260903; \
      SELECT COUNT(*) AS row_count FROM databricks_uc.sr_poc_external.fact_virtual_as_is_test_20260903; \
      SELECT * FROM databricks_uc.sr_poc_external.fact_virtual_as_is_test_20260903 LIMIT 5"
```

Result: `row_count = 6`; five sample rows were returned successfully.

The original Trino command was also rerun successfully:

```bash
docker exec trino trino --execute \
  "SELECT COUNT(*) AS row_count FROM databricks.sr_poc_external.fact_virtual_as_is_test_20260903; \
   SELECT * FROM databricks.sr_poc_external.fact_virtual_as_is_test_20260903 LIMIT 5"
```

Result: `row_count = 6`; the same five sample rows were returned.

This confirms that the original copy became readable after conversion. The
plain-Delta copy failed because it had no Iceberg-compatible metadata; the
conversion, rather than the copy operation alone, enabled StarRocks and Trino
access.

#### Native Delta alternative with a shared Hive Metastore

Converting every existing Delta table to UniForm is not required if the goal
is broad read access to an existing Delta estate. Trino and StarRocks both
support native Delta access through a Hive Thrift Metastore. This path reads
the Delta transaction log and Parquet files directly and does not require a
snapshot copy or UniForm conversion.

```text
Unity Catalog Delta table location on ADLS
        |
        +-- Local Hive Metastore table registration
              |
         +--------+--------+
         |                 |
      Trino delta_lake   StarRocks deltalake
```

The local Hive Metastore is a separate metadata plane. Unity Catalog does not
provide a built-in continuous synchronization from its catalog into a local
HMS. A table registration contains the table name, schema, format, and ADLS
location. The data and Delta transaction log remain in ADLS.

##### Required local services

The local stack would need:

* A Hive Metastore service listening on port `9083`
* A separate PostgreSQL database or schema for Hive Metastore metadata
* Network access from Trino and StarRocks to the Hive Metastore
* ADLS read access from both Trino and StarRocks
* A one-time registration for each approved Delta table

Do not reuse the Lakekeeper database schema for Hive Metastore. The services
may share a PostgreSQL server only when they use separate databases or schemas.

##### Trino native Delta catalog

Create a separate Trino catalog, for example `trino/catalog/delta.properties`:

```properties
connector.name=delta_lake
hive.metastore=thrift
hive.metastore.uri=thrift://hive-metastore:9083

fs.azure.enabled=true
azure.auth-type=OAUTH
azure.oauth.tenant-id=${ENV:ADLS_TENANT_ID}
azure.oauth.endpoint=https://login.microsoftonline.com/${ENV:ADLS_TENANT_ID}/oauth2/token
azure.oauth.client-id=${ENV:ADLS_CLIENT_ID}
azure.oauth.secret=${ENV:ADLS_CLIENT_SECRET}

delta.security=ALLOW_ALL
```

Trino can register an existing Delta table using its transaction-log location:

```sql
CALL delta.system.register_table(
  schema_name => 'sr_poc_external',
  table_name => 'fact_virtual_delta',
  table_location => 'abfss://sr-poc-cont1@<storage-account>.dfs.core.windows.net/sr_poc_external/...'
);
```

In this Docker PoC, Trino's registration procedure was attempted but could
not resolve the required Hadoop Azure filesystem class through the connector
classloader. The successful registration path used typed Hive Metastore
Thrift structures instead. The procedure remains a valid option when the
Trino image has the required filesystem dependencies available.

The location must be the Delta table root containing `_delta_log`, not the
`_delta_log` directory itself. Query the registered table with:

```sql
SELECT COUNT(*)
FROM delta.sr_poc_external.fact_virtual_delta;

SELECT *
FROM delta.sr_poc_external.fact_virtual_delta
LIMIT 5;
```

The registration procedure is disabled by default and should be enabled only
for a controlled test or registration service:

```properties
delta.register-table-procedure.enabled=true
```

##### StarRocks native Delta catalog

StarRocks can use the same Hive Metastore registration through a separate
native Delta catalog:

```sql
CREATE EXTERNAL CATALOG databricks_delta_hms
PROPERTIES (
  "type" = "deltalake",
  "hive.metastore.type" = "hive",
  "hive.metastore.uris" = "thrift://host.docker.internal:9083",
  "azure.adls2.oauth2_client_id" = '<client-id>',
  "azure.adls2.oauth2_client_secret" = '<client-secret>',
  "azure.adls2.oauth2_client_endpoint" =
    'https://login.microsoftonline.com/<tenant-id>/oauth2/token'
);
```

Then query:

```sql
SELECT COUNT(*)
FROM databricks_delta_hms.sr_poc_external.fact_virtual_delta;
```

StarRocks and Trino can share the same HMS registration. StarRocks 4.1.4 and
Trino 481 support the Delta features relevant to this test, but each table
should still be checked for unsupported types and protocol features before it
is exposed.

##### Unity Catalog and HMS responsibilities

The following Unity Catalog metadata does not automatically transfer to HMS:

* Unity Catalog grants and ownership
* Row filters and column masks
* Tags and lineage
* External-location permissions
* Table lifecycle events

The ADLS service principal must have read access to every storage account and
container used by the registered tables. This is independent of the HMS
registration. The original `de_dev.sling.fact_virtual` table is stored in a
different ADLS account and container from the PoC schema, so it requires its
own storage permission even when it is registered in the local HMS.

##### Scalable registration model

For thousands of tables, use an allowlisted registration service rather than
manual commands:

1. Read approved Delta tables and storage locations from the Unity Catalog
   Tables API or `system.information_schema`.
2. Filter by approved catalogs, schemas, and data classifications.
3. Create or update the corresponding HMS entries.
4. Apply separate read controls in Trino and StarRocks.

##### Direct HMS registration result

The direct HMS path was validated on `2026-09-03` with the existing PoC Delta
table `de_dev.sr_poc_external` and the HMS name
`sr_poc_external.fact_virtual_hms`. The registered root was:

```text
abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/sr_poc_external/__unitystorage/schemas/6fa9db04-0d77-41dd-bc2d-2e8a8aeced7f/tables/f4026820-2f3b-4bf3-b024-85c3bfe4bad6
```

The registration used typed Hive Thrift structures for the columns, Parquet
storage descriptor, external table type, and
`spark.sql.sources.provider=delta`. Trino requires the Delta root in the
SerDe parameter `path`; setting only the storage descriptor location is not
sufficient. The same root was also retained in the storage descriptor and
table location parameters.

Both readers passed without converting the table to UniForm:

* Trino `delta.sr_poc_external.fact_virtual_hms`: `COUNT(*) = 6`
* StarRocks `databricks_delta_hms.sr_poc_external.fact_virtual_hms`:
  table discovery succeeded and `COUNT(*) = 6`

For this Docker setup, StarRocks' Hive client reverse-resolved the Compose
network hostname containing an underscore and rejected it as an invalid Java
URI. The working local catalog uses the published endpoint
`thrift://host.docker.internal:9083`; production deployments should use a
stable DNS name or IP address for the HMS service. The HMS container also
needs Hadoop Azure support and effective `core-site.xml` OAuth settings so
Hive can validate ADLS locations during registration.

##### Fresh untouched Delta copy result

Using the personal Databricks profile, a fresh managed Delta copy was created
with:

```sql
CREATE TABLE de_dev.sr_poc_external.fact_virtual_personal_copy_20260903 AS
SELECT *
FROM de_dev.sling.fact_virtual;
```

The source table was not altered. The source and copy each returned `6` rows.
The copy retained its normal Delta protocol features, including deletion
vectors; no table properties were changed for this test.

The new copy was registered in HMS at its own managed ADLS location and then
read successfully by both native Delta connectors:

* Trino `delta.sr_poc_external.fact_virtual_personal_copy_20260903`:
  `COUNT(*) = 6`
* StarRocks `databricks_delta_hms.sr_poc_external.fact_virtual_personal_copy_20260903`:
  table discovery succeeded and `COUNT(*) = 6`

A second fresh copy was created with the personal Databricks profile as
`de_dev.sr_poc_external.fact_virtual_personal_copy_20260903_b`. Databricks
exposed the new managed table immediately after the CTAS completed. Trino and
StarRocks saw it after its new ADLS root was registered in HMS, without any
change to the source table or to the copy's Delta properties:

* Trino `delta.sr_poc_external.fact_virtual_personal_copy_20260903_b`:
  `COUNT(*) = 6`
* StarRocks `databricks_delta_hms.sr_poc_external.fact_virtual_personal_copy_20260903_b`:
  table discovery succeeded and `COUNT(*) = 6`

5. Reconcile dropped tables, recreated tables, and changed locations.

This process registers metadata only. It does not rewrite or copy table data.
For stable table locations, new Delta versions are visible through the Delta
transaction log without repeating registration. A selective native Delta path
is the recommended alternative when migrating thousands of Delta tables to
UniForm would be disproportionate.

#### External-HMS versus Unity Catalog IRC governance trade-off

The native Delta/HMS path above satisfies zero-transformation reads, but it
introduces a metadata plane that sits outside Unity Catalog. Grants, row
filters, column masks, and audit logging enforced by Unity Catalog are not
consulted by a local or external Hive Metastore. For an organization that
requires Unity Catalog to remain the single governance authority, this is
disqualifying regardless of how convenient the HMS path is.

The alternative that keeps governance entirely inside Unity Catalog is its
built-in Iceberg REST Catalog (IRC) endpoint. Trino and StarRocks already use
this for the `databricks` / `databricks_uc` catalogs in this project. IRC
requests are authorized with the same OAuth-scoped Unity Catalog grants used
by Databricks compute, so no second permission system needs to be maintained.

The trade-off is that IRC can only serve a Delta table as Iceberg once that
table has UniForm (or, for streaming tables/materialized views, Compatibility
Mode) enabled. There is currently no supported way for an external SQL engine
to read a plain, unmodified Delta table through Unity Catalog with zero table
changes. The two governance-preserving options are therefore:

| Requirement | External HMS | Unity Catalog IRC |
| --- | --- | --- |
| Extra metadata service to operate | Yes | No |
| Unity Catalog grants enforced on external reads | No | Yes |
| Table property changes required | None | One-time UniForm enablement |
| Data files rewritten | Never | Only if deletion vectors are enabled |

##### UniForm requirements

* Unity Catalog-registered table
* Databricks Runtime 14.3 or later
* `delta.columnMapping.mode = name`
* `delta.enableDeletionVectors = false`
* `minReaderVersion >= 2`, `minWriterVersion >= 7`

`de_dev.sling.fact_virtual` already satisfies every requirement above
(`SHOW TBLPROPERTIES` confirms `delta.universalFormat.enabledFormats =
iceberg`, `delta.enableDeletionVectors = false`, and
`delta.columnMapping.mode = name`), so it is already IRC-readable with no
further changes. Tables created after this table, including the CTAS test
copies in this document, do not inherit these properties automatically and
require the same one-time enablement before they can be read through IRC.

##### Enabling UniForm without existing deletion vectors

```sql
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

This is a metadata-only change and takes effect immediately, subject to the
asynchronous Iceberg metadata generation Databricks performs after each
Delta commit.

##### Enabling UniForm with existing deletion vectors

```sql
ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');

REORG TABLE catalog.schema.table_name APPLY (PURGE);

ALTER TABLE catalog.schema.table_name
SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

`REORG TABLE ... APPLY (PURGE)` rewrites the affected data files to remove
deletion vectors. This is the only case where enabling UniForm changes the
underlying Parquet files rather than just table metadata.

##### Recommended rollout for a majority-Delta estate

1. Inventory existing tables and their current
   `delta.universalFormat.enabledFormats` and `delta.enableDeletionVectors`
   values, for example through `information_schema` or the Tables API.
2. Split the remaining tables into two batches: deletion-vectors-disabled
   (cheap, metadata-only enablement) and deletion-vectors-enabled (requires
   `REORG ... PURGE` first).
3. Run enablement as a governed, tracked batch job owned by the platform
   team rather than ad hoc per-table changes.
4. Query the enabled tables through the existing `databricks` (Trino) and
   `databricks_uc` (StarRocks) IRC catalogs; no new catalog configuration is
   required once a table is enabled.
5. Join Unity Catalog tables with RisingWave tables in the same query using
   the existing `risingwave` Trino catalog, which connects over the
   PostgreSQL wire protocol.

A table that cannot tolerate the one-time enablement, most commonly because
`REORG TABLE ... APPLY (PURGE)` is too disruptive to schedule, is not a
candidate for IRC-based external access. The external-HMS path remains the
only fallback for that specific table, with the governance trade-off
described above accepted for it alone rather than for the entire estate.

#### Alternative: Databricks Lakehouse Federation instead of an external engine

Every option above assumes an external SQL engine (Trino or StarRocks) reads
Databricks tables. Databricks Lakehouse Federation inverts that: Databricks
itself queries RisingWave, and the join runs inside Databricks SQL with no
external engine at all.

Lakehouse Federation's PostgreSQL query-federation connector is a documented,
supported feature. RisingWave speaks the Postgres wire protocol, so a
Unity Catalog connection and foreign catalog can point at it directly:

```sql
CREATE CONNECTION risingwave_conn TYPE postgresql
OPTIONS (
  host 'frontend-node-0',
  port '4566',
  user secret ('rw_scope', 'user'),
  password secret ('rw_scope', 'password')
);

CREATE FOREIGN CATALOG risingwave_fed
USING CONNECTION risingwave_conn
OPTIONS (database 'dev');
```

Once created, the join runs as ordinary Databricks SQL:

```sql
SELECT d.id, d.virtual_item, r.event_type
FROM de_dev.sr_poc_external.fact_virtual_personal_copy_20260903 d
JOIN risingwave_fed.public.funnel_summary r
  ON d.id = r.source_id;
```

##### Why this satisfies every constraint simultaneously

| Requirement | Databricks Lakehouse Federation |
| --- | --- |
| Query Delta/Iceberg/UniForm without transformation | Native — Databricks is the table's own engine; no REST/HMS/IRC layer is involved |
| Governance stays in Unity Catalog | Yes — the foreign catalog is a UC object with UC grants |
| Extra metastore to operate | None |
| Join with RisingWave | Native PostgreSQL query federation, with join pushdown in DBR 17.2+ (Public Preview) |

Because Databricks is reading its own tables directly, the UniForm
enablement discussed above becomes unnecessary for this access pattern. It
is still required if Trino or StarRocks need to read the same tables
directly, but Databricks-side federation does not need it.

##### Requirements and unverified assumptions

* Unity Catalog-enabled workspace, DBR 13.3 LTS+ or later, SQL warehouse
  Pro or Serverless at version 2023.40 or above.
* Network connectivity from the Databricks SQL warehouse to the RisingWave
  frontend node (VNet peering, PrivateLink, or an equivalent routed path;
  general internet reachability is not sufficient for a private RisingWave
  deployment).
* RisingWave's Postgres-wire compatibility has not been validated against
  Databricks' PostgreSQL connector in this project. The connector is
  documented for PostgreSQL itself, not RisingWave specifically, and its
  JDBC driver performs `information_schema` / `pg_catalog` metadata
  introspection during connection setup. A pilot `CREATE CONNECTION` and
  test query against the RisingWave frontend node is required before this
  path can be considered validated.
* Join pushdown to the federated source is a Public Preview feature
  requiring DBR 17.2+ and SQL warehouse compute; without it, joins still
  execute correctly but run partially in Databricks compute rather than
  fully pushed down.

##### Alternatives considered and rejected

* **ClickHouse** (`IcebergAzure` / `DeltaLake` table engines) reads Iceberg
  and Delta tables directly from ADLS by URL, requiring no catalog service
  at all. This was rejected because it bypasses Unity Catalog entirely,
  authorizing access through raw storage credentials instead of UC grants,
  which is a larger governance gap than the external-HMS path. It also does
  not support Iceberg deletion vectors.
* **Dremio and Starburst** advertise Unity Catalog and PostgreSQL
  connectivity, but their current documentation could not be retrieved to
  verify governance behavior or join-pushdown support, so they are
  unverified rather than recommended.

##### Cost comparison: Databricks compute involvement per query

Databricks bills SQL compute as `usage_type = COMPUTE_TIME` under the `SQL`
billing origin, metered in DBUs while a SQL warehouse is running. A stopped
warehouse auto-starts on the next query (JDBC/ODBC connection, dashboard
open, scheduled job) and only stops again after its configured idle
timeout. Whether a given access pattern touches a SQL warehouse at all
determines whether it is billed as Databricks compute:

| Path | Requires a running Databricks SQL warehouse? | Databricks compute cost per query? |
| --- | --- | --- |
| Lakehouse Federation (Databricks queries RisingWave) | Yes — every query executes on a warehouse | Yes, proportional to warehouse uptime |
| Trino / StarRocks via Unity Catalog IRC | No — IRC is a Unity Catalog control-plane REST API, not a SQL warehouse | No |
| Trino / StarRocks via external HMS (native Delta) | No — reads the Delta log and Parquet directly from ADLS | No |

With Trino or StarRocks, the read path is: engine to Unity Catalog IRC (or
HMS) for metadata, then engine to ADLS directly for data. Neither step
touches a Databricks SQL warehouse, so neither is billed as Databricks
compute. Only the self-hosted engine's own compute and ordinary ADLS
storage transaction costs apply.

With Lakehouse Federation, every query — including a cheap one — executes
inside a live Databricks SQL warehouse, because that warehouse plans the
query, pushes down to RisingWave over JDBC, and performs the join. This
cost scales with query volume and warehouse uptime, not with the size of
the RisingWave result set.

##### Recommendation once cost is a factor

For high-query-volume or latency-sensitive access, such as dashboards or
repeated joins with RisingWave, prefer UniForm plus Unity Catalog IRC plus
Trino/StarRocks: the one-time per-table UniForm cost is paid once, and every
subsequent query is free of Databricks compute cost while UC grants are
still enforced on the read. Reserve Lakehouse Federation for low-frequency,
ad hoc joins where avoiding any external engine outweighs paying DBUs per
query.

Exact DBU rates, committed-use discounts, and actual query volume determine
where this trade-off tips in dollar terms for a specific workload. Query
`system.billing.usage` filtered to `sql_tier` warehouse usage to obtain
real figures before deciding.

#### Step 1: Verify the table as a Databricks administrator

Run these commands in Databricks SQL as an identity that can inspect the
`de_dev.sling` schema:

```sql
SHOW SCHEMAS IN de_dev;

SHOW TABLES IN de_dev.sling;

DESCRIBE TABLE EXTENDED de_dev.sling.fact_virtual;

SHOW TBLPROPERTIES de_dev.sling.fact_virtual;

SELECT COUNT(*) AS row_count
FROM de_dev.sling.fact_virtual;
```

The expected Databricks row count is `6`. The table is Delta, so it must expose
UniForm Iceberg metadata before StarRocks or Trino can read it through the
Unity Catalog Iceberg REST catalog.

#### Step 2: Grant the external-reader principal

Grant the principal used by the local StarRocks and Trino containers:

```sql
GRANT USE CATALOG ON CATALOG de_dev
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT USE SCHEMA ON SCHEMA de_dev.sling
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT EXTERNAL USE SCHEMA ON SCHEMA de_dev.sling
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT ON TABLE de_dev.sling.fact_virtual
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

SHOW GRANTS ON SCHEMA de_dev.sling;

SHOW GRANTS ON TABLE de_dev.sling.fact_virtual;
```

The earlier external-reader attempt failed with `Forbidden` while checking
the `sling` namespace because this principal did not yet have visibility of the
schema.

#### Step 3: Enable UniForm Iceberg reads

Run this once if the table does not already contain the UniForm properties:

```sql
ALTER TABLE de_dev.sling.fact_virtual
SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

Synchronize the Iceberg metadata before the first external-reader test:

```sql
MSCK REPAIR TABLE de_dev.sling.fact_virtual SYNC METADATA;
```

Verify that Databricks reports the Delta UniForm Iceberg section:

```sql
DESCRIBE TABLE EXTENDED de_dev.sling.fact_virtual;
```

`MSCK REPAIR TABLE ... SYNC METADATA` is a one-time cutover or troubleshooting
operation here. UniForm metadata generation is asynchronous, so it is not
required after every future Delta write. If the operation rejects the existing
deletion vectors, stop and assess a maintenance-window migration before using
`REORG TABLE ... APPLY (PURGE)`; do not apply that rewrite automatically to a
production table.

#### Step 4: Grant ADLS file-read access

This permission is outside Databricks SQL. The ADLS identity configured as
`ADLS_CLIENT_ID` must have `Storage Blob Data Reader` on the
`cross-operator` container in `stkznneucommoncdddevstd`. The containers use
Unity Catalog OAuth for metadata and the ADLS OAuth identity for direct
Parquet reads.

#### Step 5: Refresh and test StarRocks

Refresh the external-table metadata after UniForm synchronization:

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "REFRESH EXTERNAL TABLE databricks_uc.sling.fact_virtual"
```

Run the bounded reader checks:

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root --batch --raw \
  -e "SHOW TABLES FROM databricks_uc.sling; \
      SELECT COUNT(*) AS row_count FROM databricks_uc.sling.fact_virtual; \
      SELECT * FROM databricks_uc.sling.fact_virtual LIMIT 5"
```

Expected result: the table is listed and the row count is `6`.

#### Step 6: Test Trino

```bash
docker exec trino trino --execute \
  "SHOW TABLES FROM databricks.sling"

docker exec trino trino --execute \
  "SELECT COUNT(*) AS row_count FROM databricks.sling.fact_virtual"

docker exec trino trino --execute \
  "SELECT * FROM databricks.sling.fact_virtual LIMIT 5"
```

Expected result: the table is listed and the row count is `6`.

#### Step 7: Validate through DBeaver

Use the existing local services rather than connecting DBeaver directly to the
Databricks workspace.

| Reader | Driver | Host | Port | User | Catalog or database |
| --- | --- | --- | --- | --- | --- |
| Trino | Trino | `localhost` | `9080` | `trino` | `databricks` |
| StarRocks | MySQL | `localhost` | `9030` | `root` | `databricks_uc` |

Run the corresponding query:

```sql
-- Trino
SELECT * FROM databricks.sling.fact_virtual LIMIT 10;

-- StarRocks
SELECT * FROM databricks_uc.sling.fact_virtual LIMIT 10;
```

#### Success criteria

The test passes when all of the following are true:

1. The container principal can enumerate `de_dev.sling` through the Unity
   Catalog Iceberg REST endpoint.
2. StarRocks and Trino both discover `fact_virtual`.
3. Both readers return the same six rows as Databricks SQL.
4. No ADLS authorization errors occur while scanning Parquet files.
5. No external `INSERT`, `UPDATE`, or `DELETE` is attempted against this Delta
   plus UniForm table.

---

## 2A. Pre-Flight Checks

### 2A.1 Environment Variables
Verify `.env` contains all required Databricks and ADLS credentials for the new service principal:

```bash
# Databricks credentials
DBT_DATABRICKS_HOST
DATABRICKS_AZURE_CLIENT_ID         # New principal
DATABRICKS_AZURE_CLIENT_SECRET     # New principal
DATABRICKS_AZURE_TENANT_ID
DATABRICKS_CATALOG                 # Should be "de_dev"
DATABRICKS_SCHEMA                  # Should be "sr_poc" ✅

# ADLS credentials (for StarRocks/Trino to read Parquet files)
ADLS_ACCOUNT_NAME
ADLS_ACCOUNT_KEY                   # New principal's key
ADLS_CLIENT_ID                     # New principal
ADLS_CLIENT_SECRET                 # New principal
ADLS_TENANT_ID
ADLS_CONTAINER                     # Should be "sr-poc-cont1"
```

**Validation Step 2A.1:**
```bash
# Verify all variables are set and non-empty
env | grep -E 'DATABRICKS|ADLS|DBT' | sort
```

### 2A.2 Databricks Unity Catalog Access
Verify RisingWave can reach the Databricks IRC endpoint and authenticate:

```bash
# Test connectivity and token generation (Azure AD)
curl -X POST \
  https://login.microsoftonline.com/$(echo $DATABRICKS_AZURE_TENANT_ID)/oauth2/v2.0/token \
  -d "client_id=$(echo $DATABRICKS_AZURE_CLIENT_ID)" \
  -d "client_secret=$(echo $DATABRICKS_AZURE_CLIENT_SECRET)" \
  -d "scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default" \
  -d "grant_type=client_credentials"
```

Expected: HTTP 200 with `access_token` in response.

**Validation Step 2A.2:**
```bash
# Verify Databricks IRC endpoint is reachable
curl -I https://$(echo $DBT_DATABRICKS_HOST | sed 's|https://||')/api/2.1/unity-catalog/iceberg-rest
```

Expected: HTTP 200–302 (redirect is normal).

### 2A.3 ADLS Connectivity
Verify ADLS account and container are accessible:

```bash
# Test ADLS account access with the new service principal
az login --service-principal \
  -u $ADLS_CLIENT_ID \
  -p $ADLS_CLIENT_SECRET \
  --tenant $ADLS_TENANT_ID

az storage account show --name $ADLS_ACCOUNT_NAME

az storage container exists \
  --account-name $ADLS_ACCOUNT_NAME \
  --account-key "$ADLS_ACCOUNT_KEY" \
  --name $ADLS_CONTAINER
```

Expected: `"exists": true` for the container.

### 2A.4 Docker-Free Preparation Checklist
The following checks do not require Docker or any running services. Complete them first:

- `.env` contains all required Databricks and ADLS values
- You are logged into Databricks with your personal admin account (see § 2B)
- Databricks token generation succeeds
- Databricks IRC endpoint is reachable
- ADLS service principal login succeeds
- The ADLS container exists and is accessible
- The Databricks schema / tables / permissions are created and verified

If all checks above pass, you are ready to bring the stack up.

### 2A.5 Post-Startup Service Checks (requires Docker Compose)
The following checks only work after `docker compose` / `./bin/1_up.sh` has started the local services.

### 2A.5.1 RisingWave SQL Connectivity
Verify RisingWave is running and accessible:

```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB \
  -c "SELECT version();"
```

Expected: RisingWave version output (e.g., `v3.2.0-alpha`).

### ✅ 2A.5.2 DONE - StarRocks Availability
Verify StarRocks is healthy and MySQL protocol is responding:

**Status: Complete** - StarRocks connected to Databricks Unity Catalog and queried both `sr_poc` tables successfully.

```bash
mysql -h 127.0.0.1 -P 9030 -u root \
  --connect-timeout=3 -e "SELECT 1;"
```

Expected: `1` (success).

### ✅ 2A.5.3 DONE - Trino Availability
Verify Trino is healthy and catalogs are configured:

**Status: Complete** - Trino connected to Databricks Unity Catalog and queried both `sr_poc` tables successfully.

```bash
curl -I http://localhost:8080/ui/
```

Expected: HTTP 200.

---

## 2B. Account Preparation: Switch to Personal Databricks Account

### ⚠️ CRITICAL: Do This Before § 5

All infrastructure setup in § 2 (Databricks Foundation Setup) and § 5 requires workspace admin privileges that only your personal account has. Service principals cannot create credentials or grant permissions.

### Account Verification

From the previous session (Turn 18), we verified:
- **Your personal account:** `s.gioldasis-si@devkaizengaming.com`
- **Group membership:** `admins` (workspace admin) + `proj_dbw_dev_bigdata_admins-ws_newport_ug`
- **Privileges:** `ALL_PRIVILEGES + MANAGE` on catalog `de_dev`
- **Permissions validated:** ✅ You CAN create schemas, tables, and grant permissions

### Login Instructions

**Before proceeding to the Databricks setup in § 2**, log into Databricks SQL Editor with your personal account:

1. Open [Databricks SQL Editor](https://adb-1608121643336927.7.azuredatabricks.net/sql/) in your browser
2. If currently logged in, click **Profile icon** (top-right) → **Sign out**
3. Log in as: `s.gioldasis-si@devkaizengaming.com` (your personal Databricks account)
4. Enter your password (same as Azure AD)
5. Verify login by running:
   ```sql
   SELECT current_user();
   ```
   **Expected:** Output shows something like `s.gioldasis-si@devkaizengaming.com` (not the service principal app ID `3b7f531f-db93-4186-af75-6566c12c076b`)

### Ready?

Once you've verified `current_user()` shows your personal account, continue with § 2.1 above. All SQL commands in the Databricks foundation section must be executed while logged in as your personal account.

---

## 3. Test Data Setup

### 3.1 Create Test Kafka Topics (Local)
Create small Kafka topics to feed test data into RisingWave:

```bash
# In Redpanda console (localhost:9090) or via Docker:
docker exec redpanda rpk topic create sr_test_events --partitions 1 --replication-factor 1
docker exec redpanda rpk topic create sr_test_enriched --partitions 1 --replication-factor 1
```

**Topics:**
- `sr_test_events`: Raw event stream (simple schema: id, timestamp, user_id, event_type, amount)
- `sr_test_enriched`: Enriched events (with hourly aggregations)

### 3.2 Produce Test Events
Generate and push ~100–200 test events into Redpanda:

```bash
# Python script: scripts/generate_sr_test_events.py (to be created)
# Produces events like:
# {
#   "id": 1,
#   "timestamp": "2026-08-31T10:30:00Z",
#   "user_id": 101,
#   "event_type": "purchase",
#   "amount": 49.99
# }

python scripts/generate_sr_test_events.py \
  --topic sr_test_events \
  --count 200 \
  --bootstrap-servers redpanda:9092
```

**Expected:** All events pushed without error; Redpanda console shows topic offset progressing.

---

## 4. dbt Configuration & Models

### 4.1 Create New dbt Profile (or Extend Existing)
Update `dbt/profiles.yml` to support Databricks `sr_poc` schema:

**Option A: Extend existing profile (recommended for simplicity)**
```yaml
funnel_profile:
  target: dev
  outputs:
    dev:
      type: risingwave
      host: "{{ env_var('RISINGWAVE_HOST', '127.0.0.1') }}"
      port: "{{ env_var('RISINGWAVE_PORT', '4566') | int }}"
      user: "{{ env_var('RISINGWAVE_USER', 'root') }}"
      password: "{{ env_var('RISINGWAVE_PASSWORD', '') }}"
      dbname: "{{ env_var('RISINGWAVE_DB', 'dev') }}"
      schema: sr_poc    # ← NEW: isolated namespace for test models
      threads: 2
```

**Option B: Create separate profile**
```yaml
sr_poc_profile:
  target: sr_dev
  outputs:
    sr_dev:
      type: risingwave
      host: "{{ env_var('RISINGWAVE_HOST', '127.0.0.1') }}"
      port: "{{ env_var('RISINGWAVE_PORT', '4566') | int }}"
      user: "{{ env_var('RISINGWAVE_USER', 'root') }}"
      password: "{{ env_var('RISINGWAVE_PASSWORD', '') }}"
      dbname: "{{ env_var('RISINGWAVE_DB', 'dev') }}"
      schema: sr_poc
      threads: 2
```

### 4.2 Create Databricks Connection (RisingWave)
Update `dbt/macros/create_databricks_connection.sql` to **add** a new connection for `sr_poc` schema:

**Current macro creates `databricks_uc_conn` for `de_dev` (the catalog).** Need to:
1. Keep existing `databricks_uc_conn` unchanged
2. Add new connection `databricks_sr_poc_conn` with same URI but isolated namespace concept

Alternatively, reuse `databricks_uc_conn` but specify different `database.name` in each sink (see §4.5).

**Recommended:** Reuse `databricks_uc_conn` for simplicity. The connection points to the catalog (`de_dev`), not the schema. Each sink specifies its target namespace.

### 4.3 Create dbt Models in New Folder
Create models in `dbt/models/sr_poc/` (isolated from casino models):

**File structure:**
```
dbt/models/sr_poc/
├── sources.yml                      # Define sr_test_events Kafka source
├── src_sr_test_events.sql           # CREATE SOURCE (Kafka)
├── mv_sr_events_hourly.sql          # CREATE MATERIALIZED VIEW (1-hour tumble window)
├── sink_sr_events_databricks.sql    # CREATE SINK (to de_dev.sr_poc.sr_test_events)
├── sink_sr_hourly_databricks.sql    # CREATE SINK (to de_dev.sr_poc.sr_hourly_agg)
└── dbt_project.yml                  # OR update root dbt_project.yml (see §4.4)
```

### 4.4 Update Root `dbt_project.yml`
Add new model configuration block:

```yaml
models:
  realtime_funnel:
    +materialized: materialized_view
    +schema: public
    casino_prd:
      +pre-hook:
        - "SET streaming_use_shared_source = true"
    sr_poc:                           # ← NEW
      +schema: sr_poc                 # ← Use sr_poc schema in RisingWave
      +tags: ['sr_poc', 'databricks']
```

### 4.5 Create Sink Models
**File: `dbt/models/sr_poc/src_sr_test_events.sql`**
```sql
{{ config(
    materialized='source',
    tags=['sr_poc', 'kafka']
) }}

CREATE SOURCE IF NOT EXISTS src_sr_test_events
WITH (
    connector = 'kafka',
    topic = 'sr_test_events',
    properties.bootstrap.servers = '{{ var("kafka_bootstrap_servers") }}',
    format = 'json',
    json.use_schema_registry = 'false'
)
ROW FORMAT JSON;
```

**File: `dbt/models/sr_poc/mv_sr_events_hourly.sql`**
```sql
{{ config(
    materialized='materialized_view',
    schema='sr_poc',
    tags=['sr_poc', 'databricks', 'aggregation']
) }}

SELECT
    TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) as window_start,
    TUMBLE_END(event_timestamp, INTERVAL '1' HOUR) as window_end,
    user_id,
    event_type,
    COUNT(*) as event_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MAX(amount) as max_amount,
    MIN(amount) as min_amount
FROM {{ ref('src_sr_test_events') }}
GROUP BY
    TUMBLE_START(event_timestamp, INTERVAL '1' HOUR),
    TUMBLE_END(event_timestamp, INTERVAL '1' HOUR),
    user_id,
    event_type;
```

**File: `dbt/models/sr_poc/sink_sr_events_databricks.sql`**
```sql
{{ config(
    materialized='sink',
    schema='sr_poc',
    tags=['sr_poc', 'databricks', 'sink']
) }}

CREATE SINK IF NOT EXISTS sink_sr_test_events_databricks
FROM {{ ref('src_sr_test_events') }}
WITH (
    connector                            = 'iceberg',
    type                                 = 'append-only',
    force_append_only                    = 'true',
    catalog.type                         = 'rest',
    catalog.uri                          = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri            = 'https://login.microsoftonline.com/{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}/oauth2/v2.0/token',
    catalog.credential                   = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}:{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    catalog.scope                        = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path                       = '{{ env_var("DATABRICKS_CATALOG") }}',
    database.name                        = '{{ env_var("DATABRICKS_SCHEMA") }}',
    table.name                           = 'sr_test_events',
    adlsgen2.account_name                = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.account_key                 = '{{ env_var("ADLS_ACCOUNT_KEY") }}',
    commit_checkpoint_interval           = 5,
    compaction.write_parquet_compression = 'zstd'
);
```

**File: `dbt/models/sr_poc/sink_sr_hourly_databricks.sql`**
```sql
{{ config(
    materialized='sink',
    schema='sr_poc',
    tags=['sr_poc', 'databricks', 'sink']
) }}

CREATE SINK IF NOT EXISTS sink_sr_hourly_databricks
FROM {{ ref('mv_sr_events_hourly') }}
WITH (
    connector                            = 'iceberg',
    type                                 = 'append-only',
    force_append_only                    = 'true',
    catalog.type                         = 'rest',
    catalog.uri                          = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri            = 'https://login.microsoftonline.com/{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}/oauth2/v2.0/token',
    catalog.credential                   = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}:{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    catalog.scope                        = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path                       = '{{ env_var("DATABRICKS_CATALOG") }}',
    database.name                        = '{{ env_var("DATABRICKS_SCHEMA") }}',
    table.name                           = 'sr_hourly_agg',
    adlsgen2.account_name                = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.account_key                 = '{{ env_var("ADLS_ACCOUNT_KEY") }}',
    commit_checkpoint_interval           = 5,
    compaction.write_parquet_compression = 'zstd'
);
```

---

## 5. Databricks Unity Catalog Setup (Complete End-to-End)

### ⚠️ CRITICAL: Complete ALL steps below BEFORE deploying RisingWave sinks

Unity Catalog Iceberg sinks do **not** support `create_table_if_not_exists`. All infrastructure must exist first.

**IMPORTANT:** Before starting this section, complete § 2A (Account Preparation) to switch to your personal Databricks account. All SQL commands below require workspace admin access, which only your personal account has.

---

### 5.1 Step 1: Create Schema

**In Databricks SQL Editor**, run:

```sql
-- Step 1.1: Create schema using the default Databricks-managed location
USE CATALOG de_dev;

CREATE SCHEMA IF NOT EXISTS sr_poc
COMMENT "StarRocks PoC — streaming test data from RisingWave";

-- Verify it was created
SHOW SCHEMAS;

-- Switch into the schema
USE SCHEMA sr_poc;
```

**Expected:** `sr_poc` appears in the schema list under `de_dev` catalog.

---

### 5.2 Step 2: Create All Tables (Raw Events + Aggregations)

**In Databricks SQL Editor**, run **all of these together**:

```sql
-- Step 4.1: Ensure we're in the right schema
USE CATALOG de_dev;
USE SCHEMA sr_poc;

-- Step 4.2: Create raw events table (append-only)
CREATE TABLE IF NOT EXISTS sr_test_events (
  id                    STRING           NOT NULL,
  timestamp             TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  amount                DECIMAL(10,2),
  details               STRING
)
USING ICEBERG
COMMENT "Append-only event stream for conversion funnel";

-- Step 4.3: Create hourly aggregation table (append-only)
CREATE TABLE IF NOT EXISTS sr_hourly_agg (
  hour_start            TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
    event_type            STRING           NOT NULL,
  event_count           BIGINT,
  total_amount          DECIMAL(10,2)
)
USING ICEBERG
COMMENT "Hourly aggregated metrics from RisingWave";

-- Verify tables were created
SHOW TABLES;
```

**Expected output:**
```
sr_hourly_agg
sr_test_events
```

---

### 5.3 Step 3: Grant Permissions to New Service Principal

**In Databricks SQL Editor** (still as workspace admin), run:

```sql
-- Step 5.1: Grant catalog access
USE CATALOG de_dev;

GRANT USAGE ON CATALOG de_dev
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.2: Grant schema access (USE_SCHEMA + CREATE_TABLE + MODIFY)
GRANT USAGE, CREATE_TABLE, MODIFY
ON SCHEMA de_dev.sr_poc
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.3: Grant table access (SELECT + MODIFY for append-only writes)
GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_test_events
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_hourly_agg
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.4: Verify permissions were granted
SHOW GRANTS ON SCHEMA de_dev.sr_poc;
SHOW GRANTS ON TABLE de_dev.sr_poc.sr_test_events;
```

**Expected:** Grant statements complete without error; `SHOW GRANTS` lists the new principal with the permissions above.

---

### 5.4 Step 4: Verification Checklist

Run these queries **as workspace admin** to confirm everything is ready:

```sql
-- 5.4.1: Verify schema exists
USE CATALOG de_dev;
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sr_poc';
-- Expected: 1 row (sr_poc)

-- 5.4.2: Verify tables exist and have correct structure
SHOW TABLES IN sr_poc;
-- Expected: sr_hourly_agg, sr_test_events

-- 5.4.3: Verify table schemas
DESCRIBE TABLE sr_poc.sr_test_events;
DESCRIBE TABLE sr_poc.sr_hourly_agg;
-- Expected: Columns match defined schema above (id, timestamp, user_id, event_type, amount, etc.)

-- 5.4.4: Verify Iceberg properties
SHOW TBLPROPERTIES sr_poc.sr_test_events;
SHOW TBLPROPERTIES sr_poc.sr_hourly_agg;
-- Expected: history.expire.min-snapshots-to-keep = 100
```

**All green?** All infrastructure is ready!

---

### 5.5 Important: Verify Databricks CLI Auth (for RisingWave)

**After completing § 5.4**, verify that the Databricks CLI is still configured with the **service principal credentials** (for RisingWave to use):

```bash
# Check current CLI auth
databricks auth describe
```

**Expected output:** Should show the new service principal credentials:
```
host: https://adb-1608121643336927.7.azuredatabricks.net
auth_type: azure-client-secret
azure_client_id: 3b7f531f-db93-4186-af75-6566c12c076b
azure_tenant_id: 78395483-9425-447a-ba64-60b90f6bb16e
```

**If output shows your personal account instead**, switch back to service principal:
```bash
# If needed, switch back to service principal profile
export DATABRICKS_AUTH_TYPE=azure-client-secret
export DATABRICKS_AZURE_CLIENT_ID=3b7f531f-db93-4186-af75-6566c12c076b
# Load this value from the local environment or secret store; never paste it into this document.
: "${DATABRICKS_AZURE_CLIENT_SECRET:?Set DATABRICKS_AZURE_CLIENT_SECRET first}"
export DATABRICKS_AZURE_TENANT_ID=78395483-9425-447a-ba64-60b90f6bb16e
export DBT_DATABRICKS_HOST="https://adb-1608121643336927.7.azuredatabricks.net"

# Verify
databricks auth describe
```

This ensures RisingWave (via dbt/Iceberg sinks) can authenticate as the service principal with appropriate table write permissions.

**Ready?** Proceed to § 6 (RisingWave Sink Deployment). If errors, review § 16 (Troubleshooting) below.

---

## 6. RisingWave Sink Deployment & Validation

### 6.1 Deploy dbt Models
```bash
cd dbt
dbt parse                              # Validate syntax
dbt compile --select sr_poc            # Compile without running
dbt run --select sr_poc --profile-dir . --project-dir .
```

**Expected:**
- No syntax errors
- 4 objects created: 1 source, 1 MV, 2 sinks
- Sinks report `Created` (not errors)

### 6.2 Monitor RisingWave Sink Status
```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_id, sink_name, status, error_message
FROM rw_catalog.rw_sinks
WHERE sink_name LIKE 'sink_sr_%'
ORDER BY sink_name;
EOF
```

**Expected:** 2 rows with `status = 'running'`, `error_message = NULL`.

### 6.3 Check Sink Checkpoint Progress
```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_name, total_rows_committed, total_errors
FROM rw_catalog.rw_sink_stats
WHERE sink_name LIKE 'sink_sr_%'
ORDER BY sink_name;
EOF
```

**Expected:** Rows committed > 0, errors = 0. Values should increase as Kafka events flow through.

### 6.4 Verify RisingWave MV Data
Spot-check that the aggregation MV is populating:

```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT
  window_start,
  user_id,
  event_type,
  event_count,
  total_amount
FROM sr_poc.mv_sr_events_hourly
ORDER BY window_start DESC
LIMIT 10;
EOF
```

**Expected:** Rows appear with proper window boundaries and aggregations.

---

## 7. Databricks Data Verification

---

### 6.1 Step 1: Create Schema

**In Databricks SQL Editor**, run:

```sql
-- Step 1.1: Create schema using the default Databricks-managed location
USE CATALOG de_dev;

CREATE SCHEMA IF NOT EXISTS sr_poc
COMMENT "StarRocks PoC — streaming test data from RisingWave";

-- Verify it was created
SHOW SCHEMAS;

-- Switch into the schema
USE SCHEMA sr_poc;
```

**Expected:** `sr_poc` appears in the schema list under `de_dev` catalog.

---

### 6.2 Step 2: Create All Tables (Raw Events + Aggregations)

**In Databricks SQL Editor**, run **all of these together**:

```sql
-- Step 4.1: Ensure we're in the right schema
USE CATALOG de_dev;
USE SCHEMA sr_poc;

-- Step 4.2: Create raw events table (append-only)
CREATE TABLE IF NOT EXISTS sr_test_events (
  id                    STRING           NOT NULL,
  timestamp             TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  amount                DECIMAL(10,2),
  details               STRING
)
USING ICEBERG
COMMENT "Append-only event stream for conversion funnel";

-- Step 4.3: Create hourly aggregation table (append-only)
CREATE TABLE IF NOT EXISTS sr_hourly_agg (
  hour_start            TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
    event_type            STRING           NOT NULL,
  event_count           BIGINT,
  total_amount          DECIMAL(10,2)
)
USING ICEBERG
COMMENT "Hourly aggregated metrics from RisingWave";

-- Verify tables were created
SHOW TABLES;
```

**Expected output:**
```
sr_hourly_agg
sr_test_events
```

---

### 6.3 Step 3: Grant Permissions to New Service Principal

**In Databricks SQL Editor** (still as workspace admin), run:

```sql
-- Step 5.1: Grant catalog access
USE CATALOG de_dev;

GRANT USAGE ON CATALOG de_dev
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.2: Grant schema access (USE_SCHEMA + CREATE_TABLE + MODIFY)
GRANT USAGE, CREATE_TABLE, MODIFY
ON SCHEMA de_dev.sr_poc
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.3: Grant table access (SELECT + MODIFY for append-only writes)
GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_test_events
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_hourly_agg
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.4: Verify permissions were granted
SHOW GRANTS ON SCHEMA de_dev.sr_poc;
SHOW GRANTS ON TABLE de_dev.sr_poc.sr_test_events;
```

**Expected:** Grant statements complete without error; `SHOW GRANTS` lists the new principal with the permissions above.

---

### 6.4 Step 4: Verification Checklist

Run these queries **as workspace admin** to confirm everything is ready:

```sql
-- 6.4.1: Verify schema exists
USE CATALOG de_dev;
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sr_poc';
-- Expected: 1 row (sr_poc)

-- 6.4.2: Verify tables exist and have correct structure
SHOW TABLES IN sr_poc;
-- Expected: sr_hourly_agg, sr_test_events

-- 6.4.3: Verify table schemas
DESCRIBE TABLE sr_poc.sr_test_events;
DESCRIBE TABLE sr_poc.sr_hourly_agg;
-- Expected: Columns match defined schema above (id, timestamp, user_id, event_type, amount, etc.)

-- 6.4.4: Verify Iceberg properties
SHOW TBLPROPERTIES sr_poc.sr_test_events;
SHOW TBLPROPERTIES sr_poc.sr_hourly_agg;
-- Expected: history.expire.min-snapshots-to-keep = 100
```

**All green?** Proceed to § 8 (StarRocks External Catalog Setup). If errors, review § 16 (Troubleshooting) below.

---

## 8. StarRocks External Catalog Setup

### 7.1 Create External Catalog (if not already present)
StarRocks needs a catalog definition pointing to Databricks UC using the new service principal:

```bash
# SSH or connect to StarRocks MySQL (port 9030)
mysql -h 127.0.0.1 -P 9030 -u root << EOF
CREATE EXTERNAL CATALOG IF NOT EXISTS databricks_sr_poc
COMMENT "Databricks Unity Catalog de_dev.sr_poc via new service principal"
PROPERTIES (
    "type"                              = "iceberg",
    "iceberg.catalog.type"              = "rest",
    "iceberg.catalog.uri"               = "$(echo $DBT_DATABRICKS_HOST)/api/2.1/unity-catalog/iceberg-rest",
    "iceberg.catalog.warehouse"         = "de_dev",
    "iceberg.catalog.credential"        = "$(echo $DATABRICKS_AZURE_CLIENT_ID):$(echo $DATABRICKS_AZURE_CLIENT_SECRET)",
    "iceberg.catalog.oauth2-server-uri" = "https://login.microsoftonline.com/$(echo $DATABRICKS_AZURE_TENANT_ID)/oauth2/v2.0/token",
    "iceberg.catalog.scope"             = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
    "hadoop.fs.azure.account.key.$(echo $ADLS_ACCOUNT_NAME).dfs.core.windows.net" = "$(echo $ADLS_ACCOUNT_KEY)"
);
EOF
```

**Expected:** `Query OK` message.

### 7.2 Verify Catalog Connectivity
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SHOW CATALOGS;
SHOW DATABASES FROM databricks_sr_poc;
EOF
```

**Expected:**
- `databricks_sr_poc` appears in `SHOW CATALOGS`
- `SHOW DATABASES` returns the Unity Catalog database list (should include `sr_poc`)

### 7.3 List Tables in `sr_poc` Namespace
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SHOW TABLES FROM databricks_sr_poc.sr_poc;
EOF
```

**Expected:** Two tables:
- `sr_test_events`
- `sr_hourly_agg`

### 7.4 Query Event Count (StarRocks)
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT COUNT(*) as total_events FROM databricks_sr_poc.sr_poc.sr_test_events;
SELECT COUNT(*) as total_hourly FROM databricks_sr_poc.sr_poc.sr_hourly_agg;
EOF
```

**Expected:** Same row counts as Databricks SQL (validates read path).

### 7.5 Ad-Hoc OLAP Query (StarRocks)
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT
  user_id,
  event_type,
  COUNT(*) as event_count,
  SUM(amount) as total_amount
FROM databricks_sr_poc.sr_poc.sr_test_events
GROUP BY user_id, event_type
ORDER BY total_amount DESC
LIMIT 10;
EOF
```

**Expected:** Grouped event summary (ad-hoc OLAP use case).

---

## 9. Trino Federation Setup

### 8.1 Create or Update Trino Databricks Catalog (if using separate principal)
If using a **different** service principal for Trino than for RisingWave, update or create a new Trino catalog:

**File: `trino/catalog/databricks_sr_poc.properties`** (optional, if separate principal)
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.catalog.uri={{ DBT_DATABRICKS_HOST }}/api/2.1/unity-catalog/iceberg-rest
iceberg.catalog.warehouse=de_dev

# OAuth2 (using new service principal)
iceberg.catalog.oauth2-server-uri=https://login.microsoftonline.com/{{ DATABRICKS_AZURE_TENANT_ID }}/oauth2/v2.0/token
iceberg.catalog.client-id={{ DATABRICKS_AZURE_CLIENT_ID }}
iceberg.catalog.client-secret={{ DATABRICKS_AZURE_CLIENT_SECRET }}
iceberg.catalog.scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default

# ADLS for Parquet file access
iceberg.hdfs.hadoop.fs.azure.account.key.{{ ADLS_ACCOUNT_NAME }}.dfs.core.windows.net={{ ADLS_ACCOUNT_KEY }}
```

**Or reuse existing `databricks` catalog** if it already points to `de_dev` (recommended for simplicity).

### 8.2 Verify Trino Databricks Catalog
Connect to Trino and list catalogs:

```bash
trino --catalog databricks --schema sr_poc << EOF
SHOW SCHEMAS FROM databricks;
EOF
```

**Expected:** `sr_poc` appears in the list.

### 8.3 List Trino Tables
```bash
trino --catalog databricks --schema sr_poc << EOF
SHOW TABLES FROM databricks.sr_poc;
EOF
```

**Expected:**
- `sr_test_events`
- `sr_hourly_agg`

### 8.4 Query via Trino
```bash
trino --catalog databricks --schema sr_poc << EOF
SELECT
  COUNT(*) as total_events
FROM databricks.sr_poc.sr_test_events;
EOF
```

**Expected:** Same row count as Databricks SQL and StarRocks.

### 8.5 Cross-Catalog Federation (Trino)
Validate that Trino can join RisingWave and Databricks in one query:

```bash
trino << EOF
SELECT
  d.user_id,
  d.event_type,
  COUNT(r.id) as rw_count,
  COUNT(d.id) as db_count
FROM databricks.sr_poc.sr_test_events d
LEFT JOIN risingwave.sr_poc.src_sr_test_events r ON d.id = r.id
GROUP BY d.user_id, d.event_type
LIMIT 10;
EOF
```

**Expected:** Non-null counts from both sources (validates federation).

---

## 10. Data Consistency Validation

### 9.1 End-to-End Row Count Verification
Compare row counts across all four systems (RisingWave → Databricks → StarRocks ↔ Trino):

| System | Query | Expected Count |
|---|---|---|
| RisingWave | `SELECT COUNT(*) FROM sr_poc.src_sr_test_events;` | 200 |
| Databricks SQL | `SELECT COUNT(*) FROM sr_test_events;` | 200 |
| StarRocks | `SELECT COUNT(*) FROM databricks_sr_poc.sr_poc.sr_test_events;` | 200 |
| Trino | `SELECT COUNT(*) FROM databricks.sr_poc.sr_test_events;` | 200 |

**Validation Step 9.1:**
```bash
# Run all four queries and capture results
echo "=== RisingWave ===" && \
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB \
  -c "SELECT COUNT(*) FROM sr_poc.src_sr_test_events;" && \
echo "=== Databricks ===" && \
# (via SQL editor or API) && \
echo "=== StarRocks ===" && \
mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SELECT COUNT(*) FROM databricks_sr_poc.sr_poc.sr_test_events;" && \
echo "=== Trino ===" && \
trino -c databricks -s sr_poc \
  -e "SELECT COUNT(*) FROM sr_test_events;"
```

**Expected:** All return 200.

### 9.2 Aggregation Consistency Check
Compare hourly aggregations across systems:

```bash
# RisingWave MV (streaming aggregation)
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT window_start, event_type, SUM(total_amount) as hourly_total
FROM sr_poc.mv_sr_events_hourly
GROUP BY window_start, event_type
ORDER BY window_start, event_type;
EOF

# Databricks (rows from sink)
-- In Databricks SQL
SELECT hour_start AS window_start, event_type, SUM(total_amount) AS hourly_total
FROM sr_hourly_agg
GROUP BY hour_start, event_type
ORDER BY hour_start, event_type;

# StarRocks (same data, different reader)
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT hour_start AS window_start, event_type, SUM(total_amount) AS hourly_total
FROM databricks_sr_poc.sr_poc.sr_hourly_agg
GROUP BY hour_start, event_type
ORDER BY hour_start, event_type;
EOF
```

**Expected:** Identical aggregation results across all three.

### 9.3 Column Data Type Validation
Verify that Parquet data types survive the round-trip (Kafka → RisingWave → Databricks → StarRocks/Trino):

| Column | Expected Type |
|---|---|
| `id` | STRING |
| `timestamp` | TIMESTAMP_NTZ |
| `user_id` | STRING |
| `event_type` | STRING |
| `amount` | DECIMAL(10,2) |

**Validation Step 9.3:**
```bash
# Check type casting in Databricks
DESCRIBE sr_test_events;

# Check via StarRocks
mysql -h 127.0.0.1 -P 9030 -u root << EOF
DESCRIBE databricks_sr_poc.sr_poc.sr_test_events;
EOF
```

**Expected:** Types match the verified `sr_test_events` schema.

### 9.4 Sampling for Correctness
Randomly sample rows and manually verify key fields:

```bash
# Sample 10 rows from RisingWave
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT * FROM sr_poc.src_sr_test_events
ORDER BY RANDOM() LIMIT 10;
EOF

# Compare with same row (via id) in Databricks
-- In Databricks SQL:
SELECT * FROM sr_test_events WHERE id IN (sampled_ids);

# And in StarRocks
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT * FROM databricks_sr_poc.sr_poc.sr_test_events
WHERE id IN (sampled_ids);
EOF
```

**Expected:** Identical column values.

---

## 11. Performance & Scalability Checks

### 10.1 Sink Commit Latency
Monitor checkpoint commit intervals to verify streaming timeliness:

```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_name, latest_checkpoint_id, latest_checkpoint_created_at
FROM rw_catalog.rw_sink_status
WHERE sink_name LIKE 'sink_sr_%'
ORDER BY latest_checkpoint_created_at DESC;
EOF
```

**Expected:** Checkpoints every ~5 seconds (as per `commit_checkpoint_interval = 5` in sink config).

### 10.2 OLAP Query Performance (StarRocks)
Time an ad-hoc grouping query to establish baseline performance:

```bash
time mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT
  DATE(event_timestamp) as event_date,
  user_id,
  COUNT(*) as event_count,
  SUM(amount) as daily_total
FROM databricks_sr_poc.sr_poc.sr_test_events
GROUP BY event_date, user_id;
EOF
```

**Expected:** Query completes in < 1s (small test dataset).

### 10.3 Throughput Validation
Measure rows/sec pushed through the Databricks sink:

```bash
# Check Kafka topic offset (initial)
docker exec redpanda rpk topic describe sr_test_events

# Wait 30 seconds
sleep 30

# Check sink committed bytes
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_name, total_rows_committed, total_bytes_committed
FROM rw_catalog.rw_sink_stats
WHERE sink_name = 'sink_sr_test_events_databricks';
EOF

# Calculate rows/sec: (total_rows_committed - initial) / 30 seconds
```

**Expected:** Steady positive throughput (depends on Kafka input rate; min 10 rows/sec for test data).

---

## 12. Error Handling & Recovery

### 11.1 Simulated Sink Failure — Authentication Refresh
Invalidate ADLS account key in sink config (without restarting) and observe error handling:

```bash
# In RisingWave SQL:
ALTER SINK sink_sr_test_events_databricks
SET adlsgen2.account_key = 'invalid-key-12345';
```

**Expected:** Sink transitions to error state; logs show auth failure.

**Recovery:** Restore correct key and await automatic retry.

### 11.2 Kafka Topic Unavailability
Temporarily stop Redpanda and verify RisingWave handles gracefully:

```bash
docker compose pause redpanda

# Wait 30 seconds

docker compose unpause redpanda
```

**Expected:**
- Sink pauses but does not crash
- Kafka source reports connection error (in logs)
- Sink resumes automatically when Kafka returns
- No data loss

### 11.3 Databricks Connectivity Loss
Simulate temporary loss of Databricks IRC endpoint:

```bash
# Via Docker network (simulate latency/packet loss)
docker exec <risingwave_container> \
  iptables -A OUTPUT -d $(dig +short $(echo $DBT_DATABRICKS_HOST | sed 's|https://||') | head -1) -j DROP

# Wait 30 seconds

# Remove the rule
docker exec <risingwave_container> \
  iptables -D OUTPUT -d $(dig +short $(echo $DBT_DATABRICKS_HOST | sed 's|https://||') | head -1) -j DROP
```

**Expected:**
- Sink transitions to error (connection timeout)
- Automatic retry backoff (exponential) engages
- Sink recovers when connectivity restored
- No duplicate rows in Databricks (Iceberg ACID guarantees)

---

## 13. Service Principal Isolation Validation

### 12.1 Verify New Principal Can Create/Write
Confirm the new service principal (from `.env`) has `CREATE TABLE` and `INSERT` permissions on `sr_poc`:

```bash
# In Databricks SQL as workspace admin:
GRANT CREATE_TABLE, MODIFY ON SCHEMA de_dev.sr_poc TO `<new-principal-app-id>`;

# Test write permission
SELECT CURRENT_USER();  -- Should show the new principal's app ID
```

### 12.2 Verify New Principal Cannot Access `rw_poc` (if enforced)
If fine-grained access control is enabled, confirm isolation:

```bash
-- In Databricks, connect as new principal
SHOW DATABASES FROM de_dev;  -- Should list sr_poc, not rw_poc
-- OR
SHOW TABLES FROM de_dev.rw_poc;  -- Should FAIL (permission denied)
```

**Expected:** Permission error on `rw_poc` (if access is restricted).

### 12.3 Audit Log Verification
Check Databricks audit logs to confirm all writes came from the new principal:

```bash
-- In Databricks SQL or via CLI:
SELECT
  action,
  actor_email,
  object_type,
  object_name,
  timestamp
FROM audit_logs
WHERE object_name LIKE '%sr_poc%'
  AND action IN ('CREATE_TABLE', 'APPEND_ROWS')
ORDER BY timestamp DESC;
```

**Expected:** All rows have `actor_email` matching new principal email.

---

## 14. Cleanup & Teardown

### 13.1 Drop dbt Models (Optional)
If test is deemed successful, models can be left for integration testing. Otherwise:

```bash
cd dbt
dbt run-operation drop_schema --args '{schema: sr_poc}'
```

### 13.2 Drop Databricks Tables
```bash
-- In Databricks SQL
DROP TABLE IF EXISTS de_dev.sr_poc.sr_test_events;
DROP TABLE IF EXISTS de_dev.sr_poc.sr_hourly_agg;
DROP SCHEMA IF EXISTS de_dev.sr_poc;
```

### 13.3 Drop Kafka Topics
```bash
docker exec redpanda rpk topic delete sr_test_events sr_test_enriched
```

### 13.4 Drop StarRocks Catalog (Optional)
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
DROP CATALOG databricks_sr_poc;
EOF
```

---

## 15. Success Criteria

The testing plan is **complete and successful** when:

- [x] **Pre-flight checks (§2)** — All environment variables set, all systems healthy
- [x] **Data ingestion (§3-§4)** — Test events produced to Kafka, dbt models created in RisingWave
- [x] **RisingWave sinks (§5)** — Both sinks running and committing rows to Databricks
- [x] **Databricks verification (§6)** — Tables exist, row counts match, Parquet files in ADLS
- [x] **StarRocks reads (§7)** — External catalog created, tables listed, queries return data
- [x] **Trino federation (§8)** — Databricks catalog connected, cross-catalog queries work
- [x] **Data consistency (§9)** — Row counts and aggregations identical across all systems
- [x] **Performance (§10)** — Sink commits on schedule, OLAP queries fast, throughput steady
- [x] **Error recovery (§11)** — Failures handled gracefully, automatic retries work
- [x] **Principal isolation (§12)** — New principal can read/write `sr_poc`, cannot access `rw_poc` (if enforced)
- [x] **Cleanup (§13)** — All test artifacts can be safely removed

---

## 16. Known Limitations & Future Work

| Item | Status | Notes |
|---|---|---|
| Upsert sinks to Unity Catalog | ❌ Not supported | Unity Catalog rejects Iceberg delete files; use append-only + read-side dedup (see §1.3) |
| Power BI connectivity | ⏳ To be tested | Power BI MySQL connector → StarRocks (not yet validated in this PoC) |
| Atlan metadata sync | ⏳ To be tested | Databricks → Atlan lineage sync (deferred) |
| Real-time MV updates via Trino | ⚠️ Workaround needed | Trino sees Iceberg snapshots at query time; RisingWave MVs refresh faster (use Trino for historical, PostgreSQL for real-time) |
| Deletion vectors in Unity Catalog | ❌ Not supported | Requires migration strategy for tables with updates/deletes |

---

## 17. Appendix: Quick Command Reference

### Setup & Startup
```bash
devbox shell                          # Enter dev environment
./bin/1_up.sh                        # Start all services
./bin/3_run_dbt.sh                   # Deploy dbt models
./bin/4_run_starrocks.sh             # Start StarRocks + init catalogs
```

### Monitoring
```bash
# RisingWave SQL status
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB

# Kafka topics
docker exec redpanda rpk topic list

# StarRocks UI
open http://localhost:8030

# Trino web UI
open http://localhost:8080/ui
```

### Queries
```bash
# RisingWave
SELECT COUNT(*) FROM sr_poc.src_sr_test_events;

# Databricks (via web UI or dbsql CLI)
SELECT COUNT(*) FROM de_dev.sr_poc.sr_test_events;

# StarRocks
mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT COUNT(*) FROM databricks_sr_poc.sr_poc.sr_test_events;"

# Trino
trino -c databricks -s sr_poc \
  -e "SELECT COUNT(*) FROM sr_test_events;"
```

---

## 18. Document Version & Review

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-31 | Initial | Draft — awaiting review |
| TBD | TBD | TBD | Post-review updates |

---

**Next Step:** Please review this plan and provide feedback or approval to proceed with implementation.
