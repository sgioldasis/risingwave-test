---
title: StarRocks Access to Databricks Managed Delta Tables
description: Research findings for StarRocks 4.1.4 access to Unity Catalog managed Delta tables on Azure ADLS
ms.date: 2026-09-02
ms.topic: reference
---

## Status

Complete

## Research Questions

1. Does StarRocks 4.1.4 provide a Databricks-specific connector or catalog that reads Unity Catalog managed Delta transaction logs directly?
2. Can the StarRocks Delta Lake catalog use Databricks Unity Catalog as its metastore?
3. Which supported StarRocks patterns apply to existing managed Delta tables on Azure ADLS?
4. What does the verified UniForm plus Databricks Iceberg REST route require and limit?

## Confirmed Findings

### No documented Databricks-native Delta catalog

No StarRocks 4.1 documentation identified a `databricks` catalog type, a Databricks connector, or a Unity Catalog metastore implementation for the Delta Lake catalog. The documented StarRocks Delta Lake catalog is created with `"type" = "deltalake"` and lists only two metadata backends: Hive Metastore and AWS Glue. The documentation explicitly states that non-S3 storage, including Azure Storage, can use only Hive Metastore.

Therefore, a Unity Catalog-managed Delta table on ADLS cannot be discovered through the documented StarRocks Delta Lake catalog path. Although StarRocks can read Delta transaction logs for a supported Delta catalog, the missing supported metadata/discovery integration is Unity Catalog itself.

The StarRocks Unified Catalog does not change this result. It likewise supports only Hive Metastore and AWS Glue, and its Delta support follows the same format-specific constraints.

### The supported route is generic Iceberg REST over UniForm

StarRocks supports an Iceberg external catalog with `"iceberg.catalog.type" = "rest"`, OAuth2 credentials or a bearer token, a REST URI, and a warehouse identifier. StarRocks documents Azure ADLS Gen2 authentication options and, for REST catalogs with vended credentials from v4.0 onward, says no storage credentials are needed.

Databricks confirms that Unity Catalog implements the Iceberg REST Catalog API at:

```text
https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest
```

Databricks explicitly lists StarRocks as an integration supported through the Iceberg REST catalog. Its supported-table matrix lists managed Delta and external Delta tables as readable only when Iceberg reads are enabled. This is the supported representation of a Unity Catalog Delta table for Iceberg clients, not direct Delta-log access.

The user's successful StarRocks read through UniForm and the Unity Catalog Iceberg REST endpoint is consistent with this documented pattern.

### Exact supported configuration shape

For a Unity Catalog catalog named `<uc-catalog>`, this is the source-backed StarRocks Iceberg REST catalog shape. The OAuth token endpoint and `all-apis` scope are taken from Databricks' Iceberg REST OAuth configuration; property names are taken from StarRocks' Iceberg REST catalog documentation.

```sql
CREATE EXTERNAL CATALOG databricks_uc_iceberg
PROPERTIES
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" =
        "https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" =
        "<oauth-client-id>:<oauth-client-secret>",
    "iceberg.catalog.oauth2.server-uri" =
        "https://<workspace-url>/oidc/v1/token",
    "iceberg.catalog.oauth2.scope" = "all-apis",
    "iceberg.catalog.warehouse" = "<uc-catalog>"
);
```

For a previously obtained short-lived bearer token or PAT, use `"iceberg.catalog.oauth2.token" = "<token>"` instead of the OAuth client-credentials fields. This avoids putting a token into the SQL example above. On Azure, prefer credential vending when the Databricks configuration and StarRocks deployment support it. Otherwise, configure StarRocks ADLS Gen2 access separately using one of its documented managed identity, service principal, shared-key, or workload identity options.

Databricks prerequisites are also confirmed: external data access must be enabled on the metastore; the configuring principal requires `EXTERNAL USE SCHEMA`; and the client authenticates with OAuth or a PAT.

### UniForm requirements and limitations

To expose an existing managed Delta table to StarRocks through the route above, Databricks requires all of the following:

* The Delta table is registered in Unity Catalog; managed tables are supported
* Column mapping is enabled
* `minReaderVersion >= 2` and `minWriterVersion >= 7`
* Writes use Databricks Runtime 14.3 LTS or later
* `delta.enableIcebergCompatV2 = true`
* `delta.universalFormat.enabledFormats = iceberg`

Deletion vectors cannot be enabled on a table with Iceberg reads. Databricks documents `REORG TABLE ... APPLY (UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = 2))` as the path when deletion vectors must be purged or legacy UniForm v1 needs upgrading. Iceberg metadata generation is asynchronous after Delta commits, so Iceberg readers can lag Delta and Delta version numbers/timestamps do not necessarily align with Iceberg versions. Databricks documents `MSCK REPAIR TABLE <table> SYNC METADATA` as a recovery/manual synchronization operation.

The important semantic limit is read-only: Databricks' Iceberg REST matrix permits readers to read managed Delta tables with Iceberg reads enabled but does not permit Iceberg clients to write them. StarRocks may generally support Iceberg writes, but it must not be used to write a UniForm-managed Delta table through the REST endpoint.

## Assessment

| Question | Answer | Confidence |
| --- | --- | --- |
| Databricks-specific StarRocks connector/catalog for direct Delta logs? | No documented support found. The documented external catalog types do not include Databricks or Unity Catalog for Delta. | High |
| Can StarRocks Delta Lake Catalog use Unity Catalog as metastore? | No. Its supported metadata services are HMS and AWS Glue; Azure storage is paired with HMS only. | High |
| Can StarRocks read existing Unity Catalog managed Delta tables? | Yes, through Databricks UniForm Iceberg reads and a StarRocks Iceberg REST catalog, subject to requirements and read-only behavior. | Very high |
| Does UniForm require rewriting Delta Parquet data? | Normally no. Databricks generates Iceberg metadata asynchronously alongside Delta metadata. `REORG` rewrites when needed for deletion vectors, legacy v1, or engines lacking Hive-style Parquet support. | High |

## Recommendation

Use the existing UniForm plus Unity Catalog Iceberg REST design for existing managed Delta tables. It is the only source-confirmed StarRocks path that preserves Unity Catalog discovery and governance without copying data or introducing an unsupported metastore bridge.

Enable Iceberg reads per eligible Delta table, wait for or verify metadata generation, and give StarRocks only read access through the Iceberg REST catalog. Keep Delta writes in Databricks-compatible Delta clients. Treat a proposed Delta Lake catalog pointed at the ADLS paths as unsupported unless the tables are independently registered in a Hive Metastore that StarRocks can use; such a registration is not equivalent to Unity Catalog and was not validated here.

## Assumptions and Unverified Items

* The stated StarRocks version is 4.1.4. The source evidence is the current StarRocks 4.1 documentation rather than a 4.1.4-specific compatibility matrix. The REST catalog properties and Delta metastore constraints are documented at the 4.1 level.
* This research did not execute a live catalog-creation or read test against the user's Azure workspace. The user has already verified the key read path.
* The exact Databricks external-data-access and Azure credential-vending policy configuration is workspace-specific and requires an administrator check.

## References

* [StarRocks Delta Lake catalog](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/)
* [StarRocks Iceberg catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg/)
* [StarRocks Unified catalog](https://docs.starrocks.io/docs/data_source/catalog/unified_catalog/)
* [Databricks access through Iceberg REST](https://docs.databricks.com/aws/en/external-access/iceberg)
* [Databricks UniForm Iceberg reads](https://docs.databricks.com/aws/en/delta/iceberg-reads)
* [Databricks Unity Catalog integrations](https://docs.databricks.com/aws/en/external-access/integrations)
