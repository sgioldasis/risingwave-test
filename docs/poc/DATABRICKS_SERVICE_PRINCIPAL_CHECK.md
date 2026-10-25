# Databricks Service Principal Access Check

## Service Principal Used

The integration uses the Azure/Entra service principal identified by:

```text
Client/Application ID: 3b7f531f-db93-4186-af75-6566c12c076b
Tenant ID:             78395483-9425-447a-ba64-60b90f6bb16e
```

The client secret is stored locally in the environment and is intentionally not included here.

The client ID is configured in the project as both:

```text
DATABRICKS_AZURE_CLIENT_ID
ADLS_CLIENT_ID
```

The credentials have been verified with the Azure AD service-principal client
credentials flow. The service principal can access the Databricks Unity Catalog
REST API and the `de_dev.rw_poc` schema.

## Databricks Unity Catalog Check

In Databricks, open **Catalog Explorer** and check permissions for:

```text
de_dev
de_dev.rw_poc
```

The service principal should have access to the catalog and schema.

Alternatively, run the following in a Databricks SQL editor:

```sql
SHOW GRANTS ON CATALOG de_dev;
SHOW GRANTS ON SCHEMA de_dev.rw_poc;
```

Look for the principal with application ID:

```text
3b7f531f-db93-4186-af75-6566c12c076b
```

The required permissions should include:

```text
USE CATALOG
USE SCHEMA
SELECT
MODIFY
```

The relevant Databricks tables are:

```text
de_dev.rw_poc.rw_casino_transactions
de_dev.rw_poc.rw_sportsbook_bets
de_dev.rw_poc.rw_casino_turnover_90d
de_dev.rw_poc.rw_casino_landing
de_dev.rw_poc.rw_sportsbook_landing
```

The `.env` value `DATABRICKS_SCHEMA=risingwave_poc` refers to a different
schema. It is not the schema used by the tables listed above. For this StarRocks
test, use `rw_poc`:

```sql
SHOW TABLES FROM databricks_uc.rw_poc;
```

The verified `rw_poc` namespace contains eight tables, including the five
tables listed above plus `casino_real_bet`, `turnover_percentage`, and
`v_casino_turnover_latest`.

The service principal can also be searched in the Databricks workspace under:

```text
Workspace Settings
-> Identity and access
-> Service principals
```

Search by the client/application ID.

## Azure ADLS Storage Check

In Azure Portal, open the storage account:

```text
stkznneurwpoccdddevstd
```

Navigate to:

```text
Access control (IAM)
-> Role assignments
```

Search for the same application ID:

```text
3b7f531f-db93-4186-af75-6566c12c076b
```

Verify that it has:

```text
Storage Blob Data Reader
```

The role should be assigned at the storage-account, container, or appropriate resource-group scope.

Because this is ADLS Gen2, also verify filesystem ACLs on:

```text
cont1/iceberg
```

The service principal needs read and execute access on the directory path. Parent directories require execute permission so the principal can traverse the path.

## Current StarRocks Test Result

StarRocks 4.1.4 starts successfully and can:

- Authenticate to the Databricks Unity Catalog REST endpoint.
- List the `rw_poc` namespace.
- List the Databricks tables.
- Describe `rw_casino_transactions`.

The service-principal and Unity Catalog checks succeed. The actual StarRocks
data scan currently fails with:

```text
HTTP 403: This request is not authorized to perform this operation.
```

This indicates that Unity Catalog authentication is working, but the service principal is not authorized to read the underlying ADLS files, or the storage credential/configuration is not accepted by ADLS.

## Retest After Permissions Are Updated

Recreate the StarRocks services:

```bash
docker compose up -d --force-recreate starrocks starrocks-init
```

Then run:

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SELECT * FROM databricks_uc.rw_poc.rw_casino_transactions LIMIT 5;"
```

A successful result confirms that StarRocks can read the Databricks Iceberg table and the underlying ADLS Parquet files.

## Important Security Note

Do not include the client secret, account key, access tokens, or full `.env` file in email. Since credentials were previously exposed during troubleshooting, revoke and regenerate them before the final test, then update the local `.env` file.
