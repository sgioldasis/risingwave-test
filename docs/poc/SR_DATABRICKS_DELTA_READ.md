---
title: StarRocks Reads of Databricks Delta Tables
description: Runbook for exposing existing Databricks Unity Catalog Delta tables to StarRocks through UniForm and the Iceberg REST catalog
ms.date: 2026-09-02
ms.topic: how-to
---

## Scope

This runbook covers StarRocks reads of existing Databricks Unity Catalog-managed
Delta tables. It uses Delta UniForm Iceberg metadata and the existing
`databricks_uc` Iceberg REST catalog. It does not use StarRocks' native Delta
Lake catalog.

The path is read-only from StarRocks:

```text
Databricks Delta table
  -> UniForm Iceberg metadata
  -> Unity Catalog Iceberg REST catalog
  -> StarRocks Iceberg external catalog
```

StarRocks' native Delta Lake catalog requires a Hive Metastore or AWS Glue
metadata backend. It has no documented Unity Catalog metastore mode.

## Validated Configuration

On 2026-09-02, StarRocks 4.1.4 read this managed Delta table through the
existing Unity Catalog Iceberg REST catalog:

```text
de_dev.sr_poc_external.delta_starrocks_read_probe_20260902
```

The table used the following properties:

```text
delta.columnMapping.mode = id
delta.enableIcebergCompatV2 = true
delta.universalFormat.enabledFormats = iceberg
```

After a native Delta control write, `MSCK REPAIR TABLE ... SYNC METADATA`, and
`REFRESH EXTERNAL TABLE`, StarRocks returned the row below, including its
`DECIMAL(12,2)` value:

```text
1001 | starrocks_delta_read | 123.45 | 2026-09-02 16:00:00
```

## Production Process for a Large Existing Table

### 1. Assess Before Changing the Table

Run these commands in Databricks SQL and retain the output with the change
record:

```sql
DESCRIBE TABLE EXTENDED catalog_name.schema_name.table_name;
DESCRIBE HISTORY catalog_name.schema_name.table_name LIMIT 20;
SHOW TBLPROPERTIES catalog_name.schema_name.table_name;
```

Record the table size, file count, recent write rate, table owner, current
protocol versions, column-mapping mode, deletion-vector state, and enabled
features. Identify every Delta writer that may access the table. Enabling
UniForm upgrades the Delta reader and writer protocol, so older Delta clients
must be checked for compatibility before the change.

### 2. Confirm StarRocks and Unity Catalog Access

The StarRocks identity requires `USE CATALOG`, `USE SCHEMA`, `SELECT`, and
`EXTERNAL USE SCHEMA` on the target. The metastore must allow external data
access. StarRocks must reach the Unity Catalog Iceberg REST endpoint and the
ADLS location, either with supported credential vending or separately
configured ADLS credentials.

### 3. Enable or Upgrade UniForm

Use the metadata-only path when the table already satisfies the UniForm feature
requirements. This does not copy the data files:

```sql
ALTER TABLE catalog_name.schema_name.table_name
SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'id',
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

Use the rewrite path when deletion vectors must be purged, the table has legacy
UniForm metadata, or a feature check requires a physical upgrade:

```sql
REORG TABLE catalog_name.schema_name.table_name
APPLY (UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = 2));
```

Do not run the rewrite path concurrently with uncontrolled production writes.
Schedule it in a maintenance window and confirm available compute capacity,
storage headroom, and a rollback plan before execution.

### 4. Verify UniForm Metadata

UniForm generates Iceberg metadata asynchronously after a Delta commit. For a
controlled cutover, or if the conversion state is behind the Delta version,
run:

```sql
MSCK REPAIR TABLE catalog_name.schema_name.table_name SYNC METADATA;
```

Verify `DESCRIBE TABLE EXTENDED` includes the `Delta Uniform Iceberg` section
and its converted Delta version covers the expected source version. Compare row
counts and representative business aggregates before enabling StarRocks users.

### 5. Refresh and Validate StarRocks Reads

Refresh the StarRocks Iceberg REST metadata, then begin with bounded checks:

```sql
REFRESH EXTERNAL TABLE databricks_uc.schema_name.table_name;

SELECT COUNT(*)
FROM databricks_uc.schema_name.table_name;

SELECT *
FROM databricks_uc.schema_name.table_name
WHERE event_date >= DATE '2026-09-01'
LIMIT 100;
```

Compare narrow-range counts and representative aggregates with Databricks SQL.
Define a refresh expectation because StarRocks can retain an older Iceberg
snapshot in its metadata cache after a Delta write.

## Time and Cost Model

There is no safe fixed duration for a billion-row table. Cost and elapsed time
depend on the selected path, file count, table history, storage throughput,
compute capacity, and concurrent writes.

| Path | Expected Duration and Cost | Data-File Impact |
| ------ | ---------------------------- | ------------------ |
| Metadata-only enablement | Usually lower cost. Iceberg metadata generation can take from minutes to a longer asynchronous run for a very large history. | No permanent data copy and normally no Parquet rewrite |
| `REORG ... UPGRADE UNIFORM` | Potentially substantial compute, I/O, and temporary storage cost. For billions of rows, plan it as a large maintenance workload. | May rewrite data files to remove incompatible features or upgrade metadata |
| Ongoing UniForm use | Incremental cost on every Delta write because Iceberg metadata must be generated. | Same data files are shared; metadata grows over time |

UniForm does not normally duplicate Delta Parquet data. It adds Iceberg metadata
and can increase compute demand and write latency for high-frequency Delta
writes. Measure on a representative non-production table first and set a cost
guardrail before converting a large production target.

## Effects on Existing Databricks Workloads

| Area | Effect and Mitigation |
| ------ | ----------------------- |
| Delta protocol | UniForm upgrades the reader and writer protocol. Inventory all Delta clients and verify their supported protocol versions before changing the table. |
| Deletion vectors | IcebergCompatV2 cannot coexist with deletion vectors. Use the planned `REORG` path when they must be removed. |
| Write latency | Databricks continues to read and write the table as Delta, but generates Iceberg metadata after Delta commits. Monitor write latency and metadata lag. |
| External freshness | StarRocks may not immediately see the newest Delta commit. Refresh the external table for controlled freshness checks. |
| Delta features | Validate the existing feature set before conversion. If a required feature is incompatible, use Delta Sharing, CDC, or a separate read model instead. |
| Rollback | Unsetting UniForm stops new Iceberg-read exposure, but protocol upgrades are not automatically reversed. Treat enablement as a compatibility change, not a reversible toggle. |

> [!IMPORTANT]
> This process makes the table readable from StarRocks. It does not make it
> writable from StarRocks. Continue to write the Delta table with Databricks
> Delta-compatible clients.

## Operational Limits

* StarRocks can read UniForm Delta decimal columns. The validated table included
  `DECIMAL(12,2)`.
* StarRocks must not write to a UniForm Delta table through Iceberg REST.
* The StarRocks native Delta Lake catalog is not a substitute for this path with
  Unity Catalog-managed Delta tables.
* For native managed Iceberg write behavior and the separate StarRocks decimal
  write defect, see [Databricks Iceberg Sinks](DATABRICKS_ICEBERG_SINK.md).

## Related Documentation

* [Databricks Iceberg Read Guide](DATABRICKS_ICEBERG_READ.md)
* [StarRocks PoC Testing Plan](../SR_POC_TESTING_PLAN.md)
* [StarRocks Serving Layer](STARROCKS_SERVING_LAYER.md)
