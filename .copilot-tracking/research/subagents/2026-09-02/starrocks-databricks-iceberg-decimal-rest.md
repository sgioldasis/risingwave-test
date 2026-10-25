# Research: StarRocks DECIMAL writes to Databricks Unity Catalog Iceberg REST

## Status
Complete

## Questions

1. Does StarRocks support writing `DECIMAL` values to Apache Iceberg tables through an Iceberg REST catalog?
2. Is there a documented limitation or defect involving StarRocks 4.1.4, Databricks Unity Catalog-managed Iceberg, and commit-time decimal writes?
3. Are table properties, client settings, or type mappings required?
4. Did later StarRocks releases contain relevant fixes?
5. What architectures preserve existing Databricks `DECIMAL` tables?

## User-reported facts to validate

- StarRocks 4.1.4 `INSERT` through Iceberg REST into Databricks Unity Catalog managed Iceberg succeeds for `DOUBLE`.
- The equivalent `DECIMAL(10,2)` write fails during commit: `Service failed: 500: Could not process table operation. [ErrorCode: 2012]`.
- Tables are native managed Iceberg and have `catalogManaged` removed.
- The same decimal tables work with Trino and RisingWave.

## Confirmed Findings

### Root cause: StarRocks 4.1.4 emits invalid Iceberg decimal manifest bounds

StarRocks upstream PR [#78456](https://github.com/StarRocks/starrocks/pull/78456), opened 2026-09-01 and still open at the time of this research, is an exact match for the reported failure.

- Its title is **"Encode Iceberg decimal manifest bounds using minimum-length two's-complement"**.
- The PR states that Iceberg Appendix D requires `DECIMAL(P,S)` lower/upper bounds to be the unscaled value in minimum-length, two's-complement, big-endian bytes.
- It identifies that StarRocks FE previously passed fixed-width Parquet statistics into the Iceberg manifest: decimal32/64 were only byte-reversed and decimal128 was not converted. This leaves padded/non-canonical values.
- Crucially, it reports: "confirmed against Databricks Unity Catalog's Iceberg REST endpoint, where every INSERT into a table with a non-null DECIMAL column fails at commit (`500 / ErrorCode 2012`, `CommitStateUnknownException`), across every precision/scale tested."
- The author committed a decimal through PyIceberg into the same StarRocks-created table using the same REST endpoint, narrowing the defect to StarRocks' emitted bound bytes, not the table definition or Unity Catalog access configuration.
- The fix changes `IcebergApiConverter.buildDataFileMetrics` to normalize all decimal bound bytes and adds tests for `DECIMAL(9,2)`, `DECIMAL(18,5)`, and `DECIMAL(38,5)`, including an end-to-end Iceberg commit-path regression test.

The PR is labelled for `4.1` (also `4.0` and `3.5`), but its merge destination is `main`; it is not evidence that any released version currently contains the correction.

### DECIMAL is valid for Iceberg and the REST catalog

- The Apache Iceberg [table specification](https://iceberg.apache.org/spec/#primitive-types) defines `decimal(P,S)` as a core primitive type, with precision <= 38.
- Its [Parquet mapping](https://iceberg.apache.org/spec/#parquet) defines `DECIMAL(P,S)` as `INT32` for precision <= 9, `INT64` for precision <= 18, and fixed-length bytes above that. This is exactly the three physical representations covered by the StarRocks fix.
- The specification's [single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization) requires a decimal lower/upper bound to use the unscaled two's-complement big-endian representation with the *minimum* required byte count.
- This is table-format metadata behavior, not a value coercion or SQL type-mapping option in the REST API.

### Databricks supports the intended architecture

- Databricks [documents](https://docs.databricks.com/aws/en/external-access/iceberg) that Unity Catalog implements the Iceberg REST catalog API and that supported clients can "read from and write to Unity Catalog-registered Iceberg tables." Its requirements table lists **Managed Iceberg: Yes / Yes** for read/write.
- Databricks [lists StarRocks](https://docs.databricks.com/aws/en/external-access/integrations) as supporting the Iceberg REST catalog.
- Databricks' [Iceberg documentation](https://docs.databricks.com/aws/en/iceberg/#external-systems) states that external Iceberg engines can create managed tables and that the REST API supports read and write operations. It recommends Iceberg client version 1.9.2 or newer.
- Databricks says Unity Catalog manages `write.location-provider.impl`, `write.data.path`, `write.metadata.path`, `write.format.default`, and `write.delete.format.default` on managed tables. Those properties cannot be manually set, so they are not a viable workaround for this bug.

### No relevant configuration workaround was found

The exact-match upstream PR implements a Java code correction in `IcebergApiConverter.buildDataFileMetrics`; it does not introduce a catalog property, session variable, table property, or DECIMAL type-mapping setting. The only configuration guidance found concerns normal REST endpoint/authentication/storage credential setup, not decimal manifest serialization. `catalogManaged` removal does not affect the bad outbound decimal bounds and is therefore not the cause or remediation.

### Related but non-causal evidence

- StarRocks issue [#54969](https://github.com/StarRocks/starrocks/issues/54969) contains maintainers' Unity Catalog REST setup guidance and confirms that Iceberg-managed Unity Catalog tables should use `/api/2.1/unity-catalog/iceberg-rest`, not the legacy endpoint. It also notes older endpoint and OAuth issues. This does not explain a deterministic decimal-only commit failure after successful `DOUBLE` commits.
- StarRocks issue [#60043](https://github.com/StarRocks/starrocks/issues/60043) documents a historical REST OAuth token-expiry problem. It is unrelated to numeric type serialization.
- Searches of Apache Iceberg issues/PRs did not identify an upstream REST-catalog decimal commit defect matching this symptom. This is expected because the concrete failure is in StarRocks' conversion of Parquet statistics to Iceberg manifest metadata.

## Assessment

| Question | Answer | Confidence |
|---|---|---:|
| Does StarRocks support writing `DECIMAL` to Iceberg REST catalogs? | Intended support: yes. Databricks lists StarRocks as supported and `DECIMAL` is a standard Iceberg type. StarRocks 4.1.4 is defective against strict REST catalog validation for non-null decimal writes. | High |
| Is the current error a Databricks managed-table or `catalogManaged` limitation? | No. Upstream reproduced the exact UC error and demonstrated PyIceberg success against the same table/endpoint. | Very high |
| Are special table properties, client settings, or mappings required? | No source-backed workaround exists. Managed-table write-path properties are UC-managed and cannot be set. | High |
| Is there a newer released StarRocks fix? | No confirmed released fix as of 2026-09-02. PR #78456 is open, targets `main`, and has `4.1` backport labeling. | High |

## Ranked Actionable Recommendation

1. **Treat StarRocks 4.1.4 decimal writes to UC Iceberg REST as blocked by upstream defect #78456; preserve native `DECIMAL` columns.** Do not migrate or cast the existing Databricks tables to `DOUBLE`. This maintains financial precision and is supported by the exact error/causality match. Track PR #78456 and its 4.1 backport/release.
2. **Use Trino, RisingWave, or an Apache Iceberg 1.9.2+ compatible writer for the StarRocks-to-Databricks write leg until the fix is released and validated.** The reported Trino/RisingWave success, plus the PR's PyIceberg control, supports this as the lowest-risk production architecture. StarRocks can remain the read/serving layer.
3. **If immediate StarRocks-originated writes are mandatory, build/test a StarRocks image containing the merged/cherry-picked #78456 correction, after vendor confirmation.** Test a non-null `DECIMAL(10,2)` append into a disposable UC managed Iceberg table, then test a realistic schema. Do not call this production-ready until the PR merges and a 4.1 backport is available; an automated review notes a remaining risk for nested decimal leaves in the current PR revision.
4. **Only as a tactical, lossy alternative, cast a StarRocks output copy to `DOUBLE` or scaled `BIGINT`, then have a compatible writer cast back to `DECIMAL(10,2)`.** Avoid writing such values directly to the existing `DECIMAL` table from StarRocks 4.1.4 because that still causes StarRocks to generate decimal manifest metrics at commit. This is not recommended for currency or regulated values.

## Verification Plan for a Released Fix

1. Record StarRocks version/build and confirm #78456 appears in its release notes or source revision.
2. Append positive, negative, zero, boundary, and null values to a disposable Unity Catalog managed Iceberg table with top-level `DECIMAL(10,2)`.
3. Confirm a new Iceberg snapshot exists, then read the values and scale through Databricks SQL, Trino, RisingWave, and StarRocks.
4. Repeat for `DECIMAL(9,2)`, `DECIMAL(18,5)`, and `DECIMAL(38,5)` because the physical Parquet layouts differ.
5. If schemas contain nested decimals, add nested `STRUCT`/`ARRAY`/`MAP` coverage: the current open PR has a review comment identifying that its first revision processes only top-level fields.

## Follow-on Questions

- Are any affected decimal fields nested rather than top-level? If yes, wait for PR #78456's nested-field review concern to be addressed or validate a revised patch.
- Is a vendor-supported patched StarRocks image available before the 4.1 release backport?
- Does the workload require direct writes from StarRocks, or can StarRocks remain a read/serving engine while an existing compatible writer owns UC commits?

## References

- StarRocks exact-match defect/fix PR: https://github.com/StarRocks/starrocks/pull/78456
- Apache Iceberg table specification: https://iceberg.apache.org/spec/
- Apache Iceberg primitive types: https://iceberg.apache.org/spec/#primitive-types
- Apache Iceberg Parquet type mapping: https://iceberg.apache.org/spec/#parquet
- Apache Iceberg decimal bound serialization: https://iceberg.apache.org/spec/#binary-single-value-serialization
- Databricks REST catalog access: https://docs.databricks.com/aws/en/external-access/iceberg
- Databricks Iceberg external-system support and limitations: https://docs.databricks.com/aws/en/iceberg/#external-systems
- Databricks Unity Catalog integrations: https://docs.databricks.com/aws/en/external-access/integrations
- StarRocks Unity Catalog issue: https://github.com/StarRocks/starrocks/issues/54969
- StarRocks REST OAuth issue: https://github.com/StarRocks/starrocks/issues/60043
