# Databricks Iceberg Sinks — RisingWave Casino PoC

RisingWave streams casino UC1 (Real Bet Amount) and UC2 (Turnover Percentage) into **Databricks Unity Catalog Iceberg tables** running in parallel to the existing Lakekeeper sinks. This document records the full integration path, every gotcha encountered, and the rationale for each decision.

> **Status: WORKING ✅ (2026-06-05).** Stable continuous commits confirmed over 6 minutes: 4 new commits, zero gRPC crashes, zero silent stalls. Root cause was `type = 'upsert'` — Unity Catalog does not support Iceberg delete files. Switch to `type = 'append-only'` + `force_append_only = 'true'` resolves the issue. See §15 for final working config and §16 for the full investigation journey.

---

## 1. Architecture

```
mv_casino_real_bet     ──► sink_casino_real_bet_databricks     ──► de_dev.rw_poc.rw_casino_real_bet
mv_turnover_percentage ──► sink_turnover_percentage_databricks ──► de_dev.rw_poc.rw_turnover_percentage
```

Both Databricks sinks run **in parallel** with the existing Lakekeeper sinks — the Lakekeeper pipeline is untouched. RisingWave authenticates to Databricks Unity Catalog via the Iceberg REST Catalog (IRC) API using Azure AD OAuth2, and writes Parquet files directly to the PoC Azure storage account using `adlsgen2.account_key`.

---

## 2. New files

| File | Purpose |
|---|---|
| `dbt/models/casino_prd/sink_casino_real_bet_databricks.sql` | UC1 upsert sink to `rw_casino_real_bet` |
| `dbt/models/casino_prd/sink_turnover_percentage_databricks.sql` | UC2 upsert sink to `rw_turnover_percentage` |

Modified:
- `docker-compose.yml` — Databricks env vars passed to both Dagster containers

No `CREATE CONNECTION` is used — Unity Catalog sinks in RisingWave embed all auth parameters directly in `CREATE SINK` (see §3).

---

## 3. Sink SQL (final working version)

`type = 'append-only'` + `force_append_only = 'true'` is required because **Unity Catalog does not support Iceberg delete files** (upsert mode). `force_append_only` tells RisingWave to drop DELETEs and convert UPDATEs to INSERTs from the retract stream.

```sql
CREATE SINK IF NOT EXISTS sink_casino_real_bet_databricks
FROM mv_casino_real_bet
WITH (
    connector                            = 'iceberg',
    type                                 = 'append-only',
    force_append_only                    = 'true',
    catalog.type                         = 'rest',
    catalog.uri                          = '<DBT_DATABRICKS_HOST>/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri            = 'https://login.microsoftonline.com/<TENANT_ID>/oauth2/v2.0/token',
    catalog.credential                   = '<AZURE_CLIENT_ID>:<AZURE_CLIENT_SECRET>',
    catalog.scope                        = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path                       = 'de_dev',
    database.name                        = 'rw_poc',
    table.name                           = 'rw_casino_real_bet',
    adlsgen2.account_name                = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key                 = '<ADLS_ACCOUNT_KEY>',
    commit_checkpoint_interval           = 20,
    compaction.write_parquet_compression = 'zstd'
)
```

**Trade-off:** Duplicate rows accumulate in Databricks (each commit writes current state as new rows). Downstream queries must deduplicate:
```sql
SELECT customer_id, currency_id, event_ts, rolling_1d_real_bet_amount
FROM de_dev.rw_poc.rw_casino_real_bet
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id, currency_id, event_ts
    ORDER BY _commit_timestamp DESC
) = 1
```

---

## 4. Authentication — the full story

### Why not PATs?
Personal Access Tokens are **disabled org-wide**. All approaches relying on `catalog.token = 'dapi...'` are unavailable.

### Why not `catalog.credential` alone (without `catalog.oauth2_server_uri`)?
When `catalog.oauth2_server_uri` is absent, the Apache Iceberg Java library defaults to sending the client credentials to `{catalog.uri}/v1/oauth/tokens`. Databricks UC **does not implement** this Iceberg OAuth2 token exchange endpoint — it returns HTTP 401 (`"Credential was not sent or was of an unsupported type for this API"`).

### Why not the Databricks OIDC endpoint for `catalog.oauth2_server_uri`?
`https://<workspace>/oidc/v1/token` accepts **Databricks-native service principals** only. Our SP (`spn-dtbr-users-dev-crf`) is an **Azure AD service principal** — authenticating against this endpoint returns HTTP 401. Azure AD SPs must use Azure AD's token endpoint.

### What works: Azure AD token endpoint + explicit `adlsgen2` credentials

```
catalog.oauth2_server_uri = 'https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token'
catalog.credential        = '<azure_client_id>:<azure_client_secret>'
catalog.scope             = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default'
adlsgen2.account_name     = 'stkznneurwpoccdddevstd'
adlsgen2.account_key      = '<storage_account_key>'
```

- `catalog.oauth2_server_uri` + `catalog.credential`: RisingWave's Iceberg Java library calls Azure AD's token endpoint to get a Bearer token for the Databricks scope (`2ff814a6-...` is the Databricks application ID in Azure AD, a well-known constant for all Azure Databricks workspaces). This handles Unity Catalog metadata API auth.
- `adlsgen2.account_name` + `adlsgen2.account_key`: RisingWave writes Parquet files **directly** to the dedicated PoC ADLS storage account using the account key, bypassing the vended credentials flow entirely. Shipped in RisingWave v2.7.0 (PR #23350).

### Why `catalog.oauth2_server_uri` not `catalog.oauth2-server-uri`?
The hyphenated form (`oauth2-server-uri`) is not a valid SQL identifier and causes a parse error in RisingWave. The underscore form (`oauth2_server_uri`) is the correct parameter name as documented.

---

## 5. Iceberg REST API endpoint

Databricks deprecated the legacy endpoint. Use the new one:

```
# Wrong (deprecated — returns HTTP 400 "Malformed request: Legacy Iceberg endpoints...")
/api/2.1/unity-catalog/iceberg

# Correct
/api/2.1/unity-catalog/iceberg-rest
```

---

## 6. Unity Catalog permissions required

The service principal (`3b7f531f-db93-4186-af75-6566c12c076b`) needs these grants on the `de_dev` catalog:

```sql
-- Catalog level
GRANT USE_CATALOG   ON CATALOG de_dev TO `3b7f531f-db93-4186-af75-6566c12c076b`;

-- Schema level
GRANT USE_SCHEMA    ON SCHEMA de_dev.risingwave_poc TO `3b7f531f-db93-4186-af75-6566c12c076b`;
GRANT MODIFY        ON SCHEMA de_dev.risingwave_poc TO `3b7f531f-db93-4186-af75-6566c12c076b`;
GRANT SELECT        ON SCHEMA de_dev.risingwave_poc TO `3b7f531f-db93-4186-af75-6566c12c076b`;
```

`CREATE_TABLE` is not needed since tables are pre-created externally. Use the **application ID UUID** as the principal — not the display name (`spn-dtbr-users-dev-crf`), which returns `PRINCIPAL_DOES_NOT_EXIST`.

---

## 7. Azure ADLS Gen2 — the S3FileIO blocker and how `vended_credentials` solves it

### The problem (discovered during investigation)

Without `vended_credentials = true`, RisingWave's Iceberg connector always loads `org.apache.iceberg.aws.s3.S3FileIO`. When Unity Catalog returns an ADLS Gen2 storage location (`abfss://` URI scheme), S3FileIO cannot authenticate to Azure storage — it only understands S3-compatible endpoints. Writes silently time out and no Iceberg snapshots are committed even though the connection and metadata API calls succeed.

The compute node log evidence:
```
INFO: Loading custom FileIO implementation: org.apache.iceberg.aws.s3.S3FileIO
INFO: Table metadata location: abfss://cross-operator@stkznneu...dfs.core.windows.net/...
ERROR: Iceberg error: Failed to finish parquet writer. io operation timeout reached
```

### How `vended_credentials = true` changes things — and why it still doesn't work

With `vended_credentials = true`, RisingWave routes writes through its **Rust OpenDAL** storage layer instead of the JVM S3FileIO. The write path changes:

| Mode | Write layer | Result |
|---|---|---|
| Without `vended_credentials` | JVM `org.apache.iceberg.aws.s3.S3FileIO` | `io operation timeout` on `abfss://` |
| With `vended_credentials = true` | Rust `opendal::layers::retry` | `io operation timeout` on `abfss://` |

Both layers time out. OpenDAL in RisingWave 2.8.4 does not support Azure ADLS Gen2 storage for Iceberg sinks — Azure credentials vended by Unity Catalog cannot be consumed.

The compute node log evidence for `vended_credentials`:
```
WARN opendal::layers::retry: will retry after 1s because:
     Unexpected (temporary) at write, context: { timeout: 10 } => io operation timeout reached
```
This is the Rust OpenDAL layer (not Java), confirming the write path changed but the underlying ADLS incompatibility remains.

`vended_credentials = true` is documented at: https://docs.risingwave.com/iceberg/catalogs/unity as the correct approach for Databricks Unity Catalog. It works when the catalog's storage backend is S3 — it does not work when the backend is Azure ADLS Gen2.

---

## 8. Environment variables

### `.env`

```
DBT_DATABRICKS_HOST:              https://adb-1608121643336927.7.azuredatabricks.net
DATABRICKS_AZURE_CLIENT_ID:       3b7f531f-db93-4186-af75-6566c12c076b
DATABRICKS_AZURE_CLIENT_SECRET:   "<client-secret-from-azure-ad>"
DATABRICKS_AZURE_TENANT_ID:       78395483-9425-447a-ba64-60b90f6bb16e
DATABRICKS_AUTH_TYPE:             azure-client-secret
DATABRICKS_CATALOG:               de_dev
DATABRICKS_SCHEMA:                risingwave_poc
ADLS_ACCOUNT_NAME:                stkznneurwpoccdddevstd
ADLS_ACCOUNT_KEY:                 "<storage-account-key-2-from-platform-team>"
```

### `~/.zshrc` (secrets — single quotes prevent `~` expansion)

```bash
export DATABRICKS_AZURE_CLIENT_ID='3b7f531f-db93-4186-af75-6566c12c076b'
export DATABRICKS_AZURE_CLIENT_SECRET='<client-secret-from-azure-ad>'
export ADLS_ACCOUNT_KEY='<storage-account-key-2-from-platform-team>'
```

### `devbox.json`

```json
"DBT_DATABRICKS_HOST":              "https://adb-1608121643336927.7.azuredatabricks.net",
"DATABRICKS_AZURE_TENANT_ID":       "78395483-9425-447a-ba64-60b90f6bb16e",
"DATABRICKS_AZURE_CLIENT_ID":       "$DATABRICKS_AZURE_CLIENT_ID",
"DATABRICKS_AZURE_CLIENT_SECRET":   "$DATABRICKS_AZURE_CLIENT_SECRET",
"DATABRICKS_AUTH_TYPE":             "azure-client-secret",
"DATABRICKS_CATALOG":               "de_dev",
"DATABRICKS_SCHEMA":                "risingwave_poc",
"ADLS_ACCOUNT_NAME":                "stkznneurwpoccdddevstd",
"ADLS_ACCOUNT_KEY":                 "$ADLS_ACCOUNT_KEY"
```

### `docker-compose.yml` (Dagster containers)

```yaml
- DBT_DATABRICKS_HOST=https://adb-1608121643336927.7.azuredatabricks.net
- DATABRICKS_AZURE_TENANT_ID=78395483-9425-447a-ba64-60b90f6bb16e
- DATABRICKS_AZURE_CLIENT_ID=3b7f531f-db93-4186-af75-6566c12c076b
- DATABRICKS_AZURE_CLIENT_SECRET    # passthrough from host shell
- ADLS_ACCOUNT_NAME=stkznneurwpoccdddevstd
- ADLS_ACCOUNT_KEY                  # passthrough from host shell
```

`DATABRICKS_AZURE_CLIENT_SECRET` and `ADLS_ACCOUNT_KEY` must be exported in the host shell before `docker compose up -d`.

---

## 9. Databricks CLI local setup

`~/.databrickscfg`:
```ini
[DEFAULT]
host                = https://adb-1608121643336927.7.azuredatabricks.net
azure_tenant_id     = 78395483-9425-447a-ba64-60b90f6bb16e
azure_client_id     = 3b7f531f-db93-4186-af75-6566c12c076b
azure_client_secret = <secret>

[dev]
host         = https://adb-1608121643336927.7.azuredatabricks.net
account_id   = 1bd7b286-1792-43ee-bca9-57a410f5566b
workspace_id = 1608121643336927
auth_type    = databricks-cli
```

`DATABRICKS_AUTH_TYPE=azure-client-secret` prevents the CLI from conflicting with `DATABRICKS_AZURE_CLIENT_ID` env vars. Do not set `DATABRICKS_CLIENT_ID` or `DATABRICKS_CLIENT_SECRET` — these map to Databricks-native OAuth fields and conflict with Azure SP auth.

---

## 10. Approaches tried and why they failed

| Approach | Result | Reason |
|---|---|---|
| `catalog.token = '<PAT>'` | Blocked | PATs disabled org-wide |
| `catalog.credential` without `catalog.oauth2_server_uri` | HTTP 401 | Iceberg sends credentials to `{catalog.uri}/v1/oauth/tokens` which Databricks UC doesn't implement |
| `catalog.oauth2_server_uri` = Databricks OIDC endpoint | HTTP 401 | Databricks `/oidc/v1/token` only accepts Databricks-native SPs, not Azure AD SPs |
| `catalog.oauth2-server-uri` (hyphen) | SQL parse error | Hyphen is not a valid identifier character in RisingWave WITH clause |
| `'catalog.oauth2-server-uri'` (single-quoted key) | SQL parse error | WITH clause expects identifiers, not string literals as keys |
| `"catalog.oauth2-server-uri"` (double-quoted key) | `unknown field` | RisingWave 2.8.4 meta service doesn't recognise this field |
| `CREATE CONNECTION` + `catalog.token` (token fetched by Dagster) | No data written | S3FileIO cannot write to `abfss://` ADLS paths — data silently times out |
| `catalog.oauth2_server_uri` (underscore) + `vended_credentials = true` | `io operation timeout` | Write routed through Rust OpenDAL instead of JVM S3FileIO, but OpenDAL also cannot write to ADLS Gen2 without explicit credentials |
| `adlsgen2.account_name` + `adlsgen2.account_key`, `type = 'upsert'` | Commits briefly then stalls | Unity Catalog rejects delete files silently after initial pure-INSERT burst |
| `adlsgen2.account_name` + `adlsgen2.account_key`, `type = 'append-only'` + `force_append_only = 'true'`, `interval = 20`, `JVM_HEAP_SIZE = 2g` | ✅ **WORKS — FINAL CONFIG** | No delete files, no gRPC crashes, stable continuous commits — see §15 |

---

## 11. Known limitations

| Limitation | Detail |
|---|---|
| **No Grafana monitoring** | Local Trino only queries the Lakekeeper catalog. Databricks tables are not wired into Grafana. A Trino connector to Unity Catalog or Databricks SQL Warehouse data source would be needed. |
| **Token handled by Iceberg library** | With `catalog.oauth2_server_uri`, the Azure AD token is fetched and refreshed by the Apache Iceberg Java library inside RisingWave. Token refresh behaviour is library-managed, not controlled at the RisingWave level. |
| **External tables required** | Databricks managed tables use Databricks' own ADLS account (no credentials available). Tables must be created as external Iceberg tables pointing to the PoC storage account — see §12. |

---

## 12. Dedicated PoC storage account — full setup checklist

- **Storage account:** `stkznneurwpoccdddevstd`  
- **Container:** `cont1`  
- **Catalog:** `de_dev` / **Schema:** `risingwave_poc`

---

### Admin steps (requires metastore admin privileges)

**Step 1 — Register storage credential in Unity Catalog:**

```bash
databricks storage-credentials create \
  --json '{
    "name": "rw-poc-adls",
    "azure_storage_credential": {
      "storage_account": "stkznneurwpoccdddevstd",
      "account_key": "<ADLS_ACCOUNT_KEY>"
    }
  }'
```

**Step 2 — Register external location:**

```bash
databricks external-locations create \
  --name rw-poc-cont1 \
  --url 'abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/' \
  --credential-name rw-poc-adls
```

> **Note:** `statement-execution` is not available in this CLI version. Use `databricks api post /api/2.0/sql/statements --json @file.json` with `wait_timeout` between `5s` and `50s`.

**Step 3 — Create schema in `de_dev`** ✅ done:

```bash
databricks api post /api/2.0/sql/statements --json - <<'EOF'
{"statement": "CREATE SCHEMA IF NOT EXISTS de_dev.risingwave_poc", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "30s"}
EOF
```

**Step 4 — Grant SP access on the new catalog/schema** ✅ done:

```bash
for stmt in \
  "GRANT USE_CATALOG ON CATALOG de_dev TO \`3b7f531f-db93-4186-af75-6566c12c076b\`" \
  "GRANT USE_SCHEMA ON SCHEMA de_dev.risingwave_poc TO \`3b7f531f-db93-4186-af75-6566c12c076b\`" \
  "GRANT MODIFY ON SCHEMA de_dev.risingwave_poc TO \`3b7f531f-db93-4186-af75-6566c12c076b\`" \
  "GRANT SELECT ON SCHEMA de_dev.risingwave_poc TO \`3b7f531f-db93-4186-af75-6566c12c076b\`"; do
  echo "{\"statement\": \"$stmt\", \"warehouse_id\": \"4d06eca1e71a9ccc\", \"wait_timeout\": \"30s\"}" | \
    databricks api post /api/2.0/sql/statements --json -
done
```

**Step 5 — Drop any existing managed tables:**

```bash
databricks tables delete de_dev.risingwave_poc.rw_casino_real_bet   2>/dev/null || true
databricks tables delete de_dev.risingwave_poc.rw_turnover_percentage 2>/dev/null || true
```

**Step 6 — Open storage account firewall (admin — Azure Portal):**

The storage account `stkznneurwpoccdddevstd` firewall must allow:
- The **Databricks workspace VNet/subnet** (so SQL Warehouse can create tables)
- The **developer's public IP** (so RisingWave running locally can write data)

Azure Portal → `stkznneurwpoccdddevstd` → Networking → Firewalls and virtual networks → add exceptions. For a PoC, setting "Allow access from: All networks" is simplest.

**Step 7 — Create external Iceberg tables** (run after firewall is open):

```bash
databricks api post /api/2.0/sql/statements --json - <<'EOF'
{"statement": "CREATE TABLE IF NOT EXISTS de_dev.risingwave_poc.rw_casino_real_bet (customer_id INT NOT NULL, currency_id INT NOT NULL, event_ts TIMESTAMP NOT NULL, rolling_1d_real_bet_amount DECIMAL(38,10)) USING ICEBERG LOCATION 'abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/iceberg/rw_casino_real_bet'", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "50s"}
EOF

databricks api post /api/2.0/sql/statements --json - <<'EOF'
{"statement": "CREATE TABLE IF NOT EXISTS de_dev.risingwave_poc.rw_turnover_percentage (customer_id INT NOT NULL, casino_turnover DECIMAL(38,10), sportsbook_turnover DECIMAL(38,10), total_turnover DECIMAL(38,10), casino_ratio DECIMAL(38,10), sportsbook_ratio DECIMAL(38,10)) USING ICEBERG LOCATION 'abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/iceberg/rw_turnover_percentage'", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "50s"}
EOF
```

---

### Your steps (RisingWave side — do before restarting the stack)

1. **`.env`** — ensure these entries are present:
```
DATABRICKS_CATALOG:   de_dev
DATABRICKS_SCHEMA:    risingwave_poc
ADLS_ACCOUNT_NAME:    stkznneurwpoccdddevstd
ADLS_ACCOUNT_KEY:     "<storage-account-key-2-from-platform-team>"
```

2. **`~/.zshrc`** — export secrets (single quotes prevent `~` expansion):
```bash
export DATABRICKS_AZURE_CLIENT_SECRET='<client-secret-from-azure-ad>'
export ADLS_ACCOUNT_KEY='<storage-account-key-2-from-platform-team>'
```

3. **After admin completes steps 1–6:** restart Dagster containers and trigger `casino_prd_full_job`.

---

### Will RisingWave be able to write after these steps?

**Yes, with high confidence.** The two auth concerns are independent:

| Concern | Handled by | Status |
|---|---|---|
| Unity Catalog metadata (load table schema, commit snapshots) | `catalog.oauth2_server_uri` + `catalog.credential` | Already worked in earlier runs |
| ADLS data writes (Parquet files) | `adlsgen2.account_name` + `adlsgen2.account_key` | Direct storage account key — does not go through UC at all |

When RisingWave opens the external table, Unity Catalog returns `table.location = abfss://cont1@stkznneurwpoccdddevstd...`. RisingWave then writes directly to that path using the account key. The UC storage credential (Step 1) is only needed by Databricks to *create* the external table — RisingWave never uses it.

**Known risks / blockers encountered:**

| Issue | When encountered | Resolution |
|---|---|---|
| `storage-credentials create --json azure_storage_credential` returns "Unrecognized storage credential type" | CLI | Admin used the Databricks **UI** (Settings → Unity Catalog → Storage credentials) to create it with the account key |
| `statement-execution` is not a valid CLI command | CLI | Use `databricks api post /api/2.0/sql/statements --json @file.json` with `wait_timeout` between 5s–50s |
| Storage account firewall blocks Databricks SQL Warehouse from creating external tables | Step 6 | Admin must open the storage account firewall: Azure Portal → `stkznneurwpoccdddevstd` → Networking → add the Databricks workspace VNet/subnet, **or** set "Allow access from: All networks" for the PoC. RisingWave (running locally on Mac) also needs firewall access — the developer's public IP must be whitelisted or public access must be enabled. |
| `de_dev` catalog is **Managed Iceberg** — external tables with custom `LOCATION` are not supported | Step 7 | `de_dev` uses Delta with Iceberg compat; all tables are managed and `USING ICEBERG LOCATION '...'` fails with `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`. Admin must create a **new catalog** (standard Unity Catalog, not Managed Iceberg) backed by the PoC storage account. See below. |

#### New catalog requirements (for admin)

The admin needs to create a Unity Catalog catalog that:
- Is **not** a Managed Iceberg catalog
- Uses `stkznneurwpoccdddevstd` as its storage root (or allows external tables pointing there)
- Grants the SP `USE_CATALOG`, `USE_SCHEMA`, `CREATE_TABLE`, `MODIFY`, `SELECT` on the new catalog/schema

Once the catalog name is confirmed, update `DATABRICKS_CATALOG` in `.env`, `devbox.json`, and `docker-compose.yml`.

---

## 13. Delta Lake sink — feasibility analysis

Evaluated as an alternative to the Iceberg approach. **Not recommended.**

### What the Delta Lake connector supports

```sql
CREATE SINK FROM mv WITH (
    connector = 'deltalake',
    type      = 'append-only',
    location  = 's3://bucket/path',   -- s3://, s3a://, gs://, file:// only
    s3.access.key = '...',
    s3.secret.key = '...',
    commit_checkpoint_interval = 10
);
```

Docs: https://docs.risingwave.com/integrations/destinations/delta-lake

### Why it doesn't work for this setup

| Blocker | Detail |
|---|---|
| **No Azure ADLS support** | The Delta Lake connector only supports S3/S3-compatible and GCS. No `adlsgen2.*` equivalent exists — same storage problem we solved for Iceberg with `adlsgen2.account_key`. |
| **No upsert semantics** | Both casino MVs (`mv_casino_real_bet`, `mv_turnover_percentage`) are rolling-window aggregates that produce **updates** on every checkpoint. Delta Lake is append-only — `force_append_only = 'true'` writes every changeset as new rows, producing duplicates and no primary-key deduplication. Downstream queries would need `QUALIFY ROW_NUMBER() OVER (...)` to get current state. |

### One scenario where it could work (not recommended)

Pointing the Delta Lake sink at **MinIO** (already in the stack, S3-compatible) would bypass the ADLS issue:

```sql
connector    = 'deltalake',
location     = 's3a://hummock001/delta/rw_casino_real_bet',
s3.endpoint  = 'http://minio-0:9301',
s3.access.key = 'hummockadmin',
s3.secret.key = 'hummockadmin'
```

Databricks could then read from MinIO via a Delta Sharing connector or external location registration. But this still doesn't solve the upsert problem and introduces network complexity (Databricks on Azure reaching MinIO on a local Mac).

### Conclusion

The Iceberg approach (`type = 'upsert'` + `adlsgen2.account_key`) is the correct tool:
- Solves storage: `adlsgen2.account_key` writes directly to ADLS
- Solves semantics: upsert with primary key gives correct rolling-window state
- Only remaining dependency: admin creates a non-Managed-Iceberg catalog (see §12)

---

## 14. Full journey summary (approaches tried)

See §10 for a complete table. The successful path required discovering:
1. Unity Catalog IRC only supports writes to **managed Iceberg tables** (`USING ICEBERG`) — not Delta+UniForm
2. The schema must have its **MANAGED LOCATION** pointing to a storage account we control (`stkznneurwpoccdddevstd`), otherwise we can't write with `adlsgen2.account_key`
3. The SP must have **`EXTERNAL USE SCHEMA`** grant (separate from `MODIFY`/`SELECT`)
4. **`vended_credentials = true` alone is insufficient** — OpenDAL in RisingWave 2.8.4 cannot consume Azure vended SAS tokens

---

## 15. Working configuration — full reproduction guide

**Confirmed working 2026-06-04.** 364 rows in `rw_casino_real_bet`, 167 rows in `rw_turnover_percentage` after one job run.

---

### What worked and why

| Component | Working value | Why it matters |
|---|---|---|
| Table type | `USING ICEBERG` (managed Iceberg, no LOCATION) | Only managed Iceberg supports External IRC Write. Delta+UniForm is read-only via IRC. |
| Schema storage | `MANAGED LOCATION 'abfss://cont1@stkznneurwpoccdddevstd...'` | Tables land in our storage account so we can write with our account key |
| Storage write | `adlsgen2.account_name` + `adlsgen2.account_key` | Direct ADLS write using account key — works. `vended_credentials` alone doesn't work in RisingWave 2.8.4 |
| Catalog auth | `catalog.oauth2_server_uri` (Azure AD) + `catalog.credential` + `catalog.scope` | Azure AD SP credentials — Databricks OIDC endpoint doesn't support Azure AD SPs |
| Schema grant | `EXTERNAL USE SCHEMA` | Required separately from `MODIFY`/`SELECT` for IRC access |

---

### Step 1 — Databricks setup (admin, run once)

**1a. Register storage credential and external location** (metastore admin required — already done):

```bash
# Storage credential registered as: rw-poc-adls
# External location registered as:  stkznneurwpoccdddevstd_cont1
# covering: abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/
```

**1b. Grant SP `CREATE MANAGED STORAGE` on external location** (already done):
```sql
GRANT CREATE MANAGED STORAGE ON EXTERNAL LOCATION `stkznneurwpoccdddevstd_cont1`
TO `3b7f531f-db93-4186-af75-6566c12c076b`;
```

**1c. Create schema with managed location at PoC storage** (already done):
```bash
databricks api post /api/2.0/sql/statements --json - << 'EOF'
{"statement": "CREATE SCHEMA IF NOT EXISTS de_dev.rw_poc MANAGED LOCATION 'abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/iceberg'", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "30s"}
EOF
```

**1d. Grant SP privileges on schema** (already done):
```bash
for stmt in \
  "GRANT USE_SCHEMA ON SCHEMA de_dev.rw_poc TO \`3b7f531f-db93-4186-af75-6566c12c076b\`" \
  "GRANT MODIFY ON SCHEMA de_dev.rw_poc TO \`3b7f531f-db93-4186-af75-6566c12c076b\`" \
  "GRANT SELECT ON SCHEMA de_dev.rw_poc TO \`3b7f531f-db93-4186-af75-6566c12c076b\`" \
  "GRANT EXTERNAL USE SCHEMA ON SCHEMA de_dev.rw_poc TO \`3b7f531f-db93-4186-af75-6566c12c076b\`"; do
  echo "{\"statement\": \"$stmt\", \"warehouse_id\": \"4d06eca1e71a9ccc\", \"wait_timeout\": \"30s\"}" | \
    databricks api post /api/2.0/sql/statements --json -
done
```

> **`EXTERNAL USE SCHEMA` is required separately from `MODIFY`/`SELECT`** — without it, IRC writes are silently rejected.

**1e. Create managed Iceberg tables** (already done):
```bash
databricks api post /api/2.0/sql/statements --json - << 'EOF'
{"statement": "CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_real_bet (customer_id INT NOT NULL, currency_id INT NOT NULL, event_ts TIMESTAMP NOT NULL, rolling_1d_real_bet_amount DECIMAL(38,10)) USING ICEBERG", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "50s"}
EOF

databricks api post /api/2.0/sql/statements --json - << 'EOF'
{"statement": "CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_turnover_percentage (customer_id INT NOT NULL, casino_turnover DECIMAL(38,10), sportsbook_turnover DECIMAL(38,10), total_turnover DECIMAL(38,10), casino_ratio DECIMAL(38,10), sportsbook_ratio DECIMAL(38,10)) USING ICEBERG", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "50s"}
EOF
```

> **`USING ICEBERG` without `LOCATION`** — Databricks manages storage, places it in the schema's managed location (`stkznneurwpoccdddevstd`).

---

### Step 2 — RisingWave sink SQL (final working version)

```sql
CREATE SINK IF NOT EXISTS sink_casino_real_bet_databricks
FROM mv_casino_real_bet
WITH (
    connector                            = 'iceberg',
    type                                 = 'upsert',
    force_compaction                     = 'true',
    primary_key                          = 'customer_id,currency_id,event_ts',
    enable_compaction                    = 'true',
    compaction_interval_sec              = '60',
    enable_snapshot_expiration           = 'true',
    catalog.type                         = 'rest',
    catalog.uri                          = '<DBT_DATABRICKS_HOST>/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri            = 'https://login.microsoftonline.com/<TENANT_ID>/oauth2/v2.0/token',
    catalog.credential                   = '<AZURE_CLIENT_ID>:<AZURE_CLIENT_SECRET>',
    catalog.scope                        = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path                       = 'de_dev',
    database.name                        = 'rw_poc',
    table.name                           = 'rw_casino_real_bet',
    adlsgen2.account_name                = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key                 = '<ADLS_ACCOUNT_KEY>',
    commit_checkpoint_interval           = 5,
    compaction.trigger_snapshot_count    = '5',
    compaction.write_parquet_compression = 'zstd'
)
```

---

### Step 3 — Environment variables

**`.env`:**
```
DBT_DATABRICKS_HOST:              https://adb-1608121643336927.7.azuredatabricks.net
DATABRICKS_AZURE_CLIENT_ID:       3b7f531f-db93-4186-af75-6566c12c076b
DATABRICKS_AZURE_CLIENT_SECRET:   "<client-secret>"
DATABRICKS_AZURE_TENANT_ID:       78395483-9425-447a-ba64-60b90f6bb16e
DATABRICKS_AUTH_TYPE:             azure-client-secret
DATABRICKS_CATALOG:               de_dev
DATABRICKS_SCHEMA:                rw_poc
ADLS_ACCOUNT_NAME:                stkznneurwpoccdddevstd
ADLS_ACCOUNT_KEY:                 "<storage-account-key-2>"
```

**`~/.zshrc`** (single quotes prevent `~` expansion):
```bash
export DATABRICKS_AZURE_CLIENT_SECRET='<client-secret>'
export ADLS_ACCOUNT_KEY='<storage-account-key-2>'
```

**`docker-compose.yml`** (Dagster services):
```yaml
- DBT_DATABRICKS_HOST=https://adb-1608121643336927.7.azuredatabricks.net
- DATABRICKS_AZURE_TENANT_ID=78395483-9425-447a-ba64-60b90f6bb16e
- DATABRICKS_AZURE_CLIENT_ID=3b7f531f-db93-4186-af75-6566c12c076b
- DATABRICKS_CATALOG=de_dev
- DATABRICKS_SCHEMA=rw_poc
- DATABRICKS_AZURE_CLIENT_SECRET    # passthrough
- ADLS_ACCOUNT_NAME=stkznneurwpoccdddevstd
- ADLS_ACCOUNT_KEY                  # passthrough
```

`DATABRICKS_AZURE_CLIENT_SECRET` and `ADLS_ACCOUNT_KEY` must be exported in the host shell before `docker compose up -d`.

---

### Step 4 — Start and verify

```bash
# Export secrets in the host shell
export DATABRICKS_AZURE_CLIENT_SECRET='<client-secret>'
export ADLS_ACCOUNT_KEY='<storage-account-key-2>'

# Restart Dagster containers to pick up env vars
docker compose up -d dagster-webserver dagster-daemon

# Trigger the job in Dagster UI: casino_prd_full_job

# Verify after ~30 seconds
databricks api post /api/2.0/sql/statements --json - << 'EOF'
{"statement": "SELECT COUNT(*) FROM de_dev.rw_poc.rw_casino_real_bet", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "30s"}
EOF
```

---

### Production notes

- **Token refresh**: `catalog.oauth2_server_uri` causes the Apache Iceberg Java library to fetch and refresh Azure AD tokens automatically — no manual token management needed.
- **Sink recreation**: `casino_prd_full_job` drops and recreates sinks on every run via `drop_prebuild_sinks`. This is safe — managed Iceberg tables are not dropped, only the RisingWave sink objects.
- **Compaction**: `enable_compaction = 'true'` + `compaction.trigger_snapshot_count = '5'` keeps the Iceberg table compact. Databricks will also apply its own auto-optimization.
- **Schema `rw_poc` vs `risingwave_poc`**: `rw_poc` has a custom managed location (`stkznneurwpoccdddevstd`). `risingwave_poc` uses Databricks' managed storage — cannot use `adlsgen2.account_key` for it.
- **Upsert semantics**: `type = 'upsert'` with `primary_key` maintains current state per customer in the Databricks table (rolling window MV produces continuous updates).

---

## 16. Known issue — `force_compaction` stalls on large rolling window state

### What was observed

End-to-end flow confirmed working: 13 Iceberg snapshot commits reached Databricks with 803 rows. However, after a brief period of successful commits (~2 minutes after sink initialization), commits stop permanently. The RisingWave sink actors go silent with no log output and no errors.

### Root cause

`force_compaction = 'true'` instructs RisingWave to buffer ALL current MV state in memory and write a single full compacted Parquet snapshot before each commit. For a rolling window MV consuming a production Kafka topic with high message density, the state grows quickly:

- `source_rate_limit = 1` message/sec × ~38 transactions/message = ~38 rows/sec
- Rolling window: 300 seconds
- Steady-state MV size: ~38 × 300 = **~11,400 rows**

Once the MV accumulates beyond a few hundred rows, the `force_compaction` buffer grows large and the compaction phase takes too long to complete within the checkpoint cycle. The sink actors stall indefinitely in this phase with no error output.

Without `force_compaction = 'false'`, RisingWave writes incremental **positional delete files** (Iceberg v2 row-level deletes). These do not appear to trigger successful commits to the Databricks managed Iceberg table — zero commits observed when `force_compaction` is removed.

### Timeline observed

| Time | Event |
|---|---|
| 13:27:26 | Sinks initialized (fresh stack) |
| 13:28:01–13:29:06 | 6 commits — MV state small (~364 rows) |
| Rate raised to 100 | MV explodes to 230K rows, all subsequent commits blocked |
| Rate dropped to 20, multiple restarts | MV stays large, no new commits |
| Stack restart, rate 1 | 7 more commits (total 13, 803 rows) then sinks stall again |

### Why the first commits worked

At sink initialization, the MV only contained the very first batch of events (a few hundred rows). `force_compaction` completed quickly in this small-state window. As the rolling window filled to steady state (~11K rows), each compaction took longer until commits stopped entirely.

### Behaviour matrix across configurations tested

| Config | Behaviour |
|---|---|
| `force_compaction = true`, `interval = 5` | 6–13 commits on fresh small state, then stalls as MV grows |
| `force_compaction = true`, `interval = 5`, rate 100 | MV explodes to 230K rows, stalls immediately |
| No `force_compaction`, `interval = 5` | Zero commits — positional delete files accumulate but never trigger a commit |
| No `force_compaction`, `interval = 1` | Commits for ~2 minutes then JVM connector crashes: `gRPC: end of request stream` — interval too aggressive |
| No `force_compaction`, `interval = 10` | 1–2 commits after initialization then stalls silently, no errors |

### Pattern

Every configuration tested shows the same lifecycle:
1. Sinks initialize → load latest Iceberg metadata
2. Brief window of commits (seconds to a few minutes)
3. Silent stall — sink actors alive, no errors, no new commits

This is consistently reproducible across all `commit_checkpoint_interval` values and with/without `force_compaction`. The stall happens after the first few commits regardless of settings.

### Open questions for RisingWave support

1. Why do upsert Iceberg sinks targeting Databricks Unity Catalog stall after a brief period of successful commits, with no error output?
2. With `commit_checkpoint_interval = 1`, the JVM connector crashes with `gRPC: end of request stream` — is this a known issue with high commit frequency?
3. Is there a known working `commit_checkpoint_interval` range for Azure ADLS-backed Iceberg sinks?
4. Does the `adlsgen2.account_key` path have any known limitations with Databricks managed Iceberg tables?

### CoW mode (support recommendation) — also stalls

After RisingWave support clarified that Unity Catalog doesn't support Iceberg delete files and recommended `write_mode = 'copy-on-write'`, two CoW variants were tested:

```sql
write_mode               = 'copy-on-write',
enable_compaction        = 'true',
commit_checkpoint_interval = 10,
compaction_interval_sec  = '30'
```

**Result:** Same pattern — sinks initialize, load latest metadata, go completely silent with no commits and no errors. CoW also stalls.

### Complete behaviour matrix

| Config | Behaviour |
|---|---|
| `force_compaction = true`, `interval = 5` | 6–13 commits on small initial state, stalls as MV grows |
| No `force_compaction`, `interval = 5` | Zero commits — positional deletes don't trigger commits |
| No `force_compaction`, `interval = 1` | Commits for ~2 min then JVM connector crashes: `gRPC: end of request stream` |
| No `force_compaction`, `interval = 10` | 1–2 commits after init then silent stall |
| `write_mode = 'copy-on-write'`, `interval = 10` | Silent stall from initialization, zero commits |
| `write_mode = 'copy-on-write'`, `interval = 60`, `compaction_interval_sec = 600` (support recommendation) | Zero commits over 5 minutes of monitoring — same stall |
| MOR (default), no `write_mode`, `interval = 60` | Zero commits over 5 minutes of monitoring — same stall |

### Invariant pattern across ALL configurations

1. Sinks initialize → load latest Iceberg metadata from ADLS ✓
2. Zero to a few commits happen (seconds to minutes)
3. Sink actors go completely silent — no errors, no logs, no commits
4. DESCRIBE HISTORY shows no new versions after the stall

### Definitive diagnosis (parallel monitoring, multiple runs)

**Run 1 — existing table (v30):**

| Side | Observation |
|---|---|
| RisingWave MV | Grew 575 → 1473 rows — pipeline healthy |
| RisingWave sink errors | **Zero** |
| ADLS data files | **891 Parquet files written** — storage working |
| Databricks DESCRIBE HISTORY | **Frozen at v30** — zero new commits |

**Run 2 — fresh table (v0, CREATE TABLE only), testing "stuck table" hypothesis:**

| Side | Observation |
|---|---|
| RisingWave MV | Grew 404 → 1142 rows — pipeline healthy |
| RisingWave sink errors | **Zero** |
| Databricks DESCRIBE HISTORY | **Stuck at v0** — zero WRITE commits on a brand new table |

**"Stuck table" hypothesis: DISPROVEN.** Even a fresh table receives zero commits. Orphaned files and accumulated state are not the cause.

**Run 3 — fresh stack restart + original config (force_compaction, interval=60):**

| Side | Observation |
|---|---|
| RisingWave MV | Grew 415 → 1254 rows — pipeline healthy |
| RisingWave sink errors | **Zero** |
| RisingWave JVM activity | **Zero** — Iceberg Java connector never invoked |
| Databricks DESCRIBE HISTORY | **Stuck at v0** — zero commits over 5 minutes |

**Root cause identified: RisingWave's streaming engine is not delivering commit checkpoints to the sink.**

The JVM being completely silent (zero `metadata location`, zero `OAuth2`, zero `FileIO` log entries) proves the Iceberg Java connector is **never being called** for commit operations. This rules out:
- Unity Catalog API (JVM never reaches it)
- ADLS write path (separate layer, still writing data files)
- Token/auth expiry (JVM never called)
- Table state (fresh table, same result)

**Run 4 — sinks NOT added to `drop_prebuild_sinks` (mimicking original working pattern):**

| Side | Observation |
|---|---|
| RisingWave MV | Grew 496 → 1386 rows — pipeline healthy |
| RisingWave sink errors | **Zero** |
| RisingWave JVM activity | **Zero in 65s monitor window** — but JVM DID fire at sink creation time (04:14) |
| Databricks DESCRIBE HISTORY | **Stuck at v0** — zero commits |

**Key correction:** The JVM IS being invoked at sink creation (metadata loads correctly). The monitoring window (65s back) simply missed the initial load. The stall is not "JVM never called" but "JVM initialises, commits briefly, then stops committing."

**Run 5 — 2026-06-05, enterprise license, `force_compaction=true`, `interval=5`:**

Sinks initialized at 04:23, loaded metadata, produced **2 commits at 04:23:59** (empty/near-empty state, right at initialization), then permanent stall. Monitored for 4+ minutes with zero new commits.

The 2 initial commits are "empty state" commits before real data flows. Once the rolling window MV starts producing rows, `force_compaction` cannot keep up.

**Reason 2026-06-04 got 6 commits vs 2026-06-05 getting 2:** The initial window before the MV state grows is smaller on each successive run because the production Kafka topic has denser recent data. Each restart starts fresh but the source produces more rows/sec, so the compaction buffer fills faster.

**Root cause — RESOLVED 2026-06-05:** Unity Catalog does not support Iceberg positional delete files. `type = 'upsert'` (and any mode that writes delete files) silently stalls after the initial pure-INSERT burst. The fix: `type = 'append-only'` + `force_append_only = 'true'`. Confirmed stable: 4 commits over 6 minutes, zero gRPC crashes.

**Final working configuration:**
```sql
type              = 'append-only',
force_append_only = 'true',
commit_checkpoint_interval = 20
```

**Additional improvement:** `JVM_HEAP_SIZE = "2g"` in compute-node-0 environment — increased JVM heap from default ~286MB to 2GB, reducing gRPC crashes under high commit load.

### RisingWave support ticket — full context

**Environment:**
- RisingWave: v2.8.4 (self-hosted Docker, Mac)
- Databricks: Azure Unity Catalog workspace (`adb-1608121643336927.7.azuredatabricks.net`)
- Table type: Managed Iceberg (`USING ICEBERG`, no LOCATION) in schema with `MANAGED LOCATION abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/iceberg`
- Storage write: `adlsgen2.account_name` + `adlsgen2.account_key` (confirmed writing to ADLS)
- Catalog auth: Azure AD SP via `catalog.oauth2_server_uri` + `catalog.credential`

**What works:**
- ✅ Authentication to Unity Catalog Iceberg REST API
- ✅ Table metadata loading from ADLS
- ✅ Parquet data files written to ADLS (`adlsgen2.account_key`)
- ✅ Iceberg manifest files written to ADLS
- ✅ 31 Iceberg snapshot commits successfully registered in Unity Catalog
- ✅ 1226 rows visible and queryable in Databricks

**What fails:**
- ❌ After the initial commit window (seconds to minutes), all further commits stop permanently
- ❌ No error messages — sink actors alive and processing MV changes but no commit activity
- ❌ Consistent across: `force_compaction`, positional deletes, `write_mode = 'copy-on-write'`, all `commit_checkpoint_interval` values

**Questions for support:**
1. Why do upsert sinks against Databricks Unity Catalog commit briefly then stall permanently with no error output?
2. Is `write_mode = 'copy-on-write'` + `enable_compaction = true` the correct config for Unity Catalog? We tested it and it stalls immediately.
3. Is there a known stable configuration for RisingWave upsert sinks targeting Databricks managed Iceberg tables on Azure?
4. The JVM connector goes silent after initial commits — is there a log level or diagnostic setting to see what it's doing internally?

**Sink SQL used (CoW variant):**
```sql
CREATE SINK sink_casino_real_bet_databricks FROM mv_casino_real_bet
WITH (
    connector              = 'iceberg',
    type                   = 'upsert',
    primary_key            = 'customer_id,currency_id,event_ts',
    write_mode             = 'copy-on-write',
    catalog.type           = 'rest',
    catalog.uri            = 'https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri = 'https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token',
    catalog.credential     = '<client_id>:<client_secret>',
    catalog.scope          = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path         = 'de_dev',
    database.name          = 'rw_poc',
    table.name             = 'rw_casino_real_bet',
    adlsgen2.account_name  = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key   = '<key>',
    enable_compaction      = 'true',
    commit_checkpoint_interval = 10,
    compaction_interval_sec = '30',
    enable_snapshot_expiration = 'true'
)
```
