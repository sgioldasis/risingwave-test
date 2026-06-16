# Databricks Iceberg Sinks — RisingWave Casino PoC

RisingWave streams casino UC1 (Real Bet Amount) and UC2 (Turnover Percentage) into **Databricks Unity Catalog Iceberg tables** running in parallel to the existing Lakekeeper sinks. This document records the full integration path, every gotcha encountered, and the rationale for each decision.

> **Status: WORKING ✅ (2026-06-05).** Stable continuous commits confirmed. Architecture redesigned (2026-06-05): sinks now write **flat event facts with a JSON properties bag** (one table per Kafka topic) rather than aggregated MVs, aligning with lakehouse best practices. Root cause of original stall was `type = 'upsert'` — Unity Catalog does not support Iceberg delete files. Fix: `type = 'append-only'` + `force_append_only = 'true'`. See §3 for final sink SQL, §15 for infrastructure setup, and §16 for the full investigation history. For consumer queries (RisingWave, Databricks SQL, Trino), see `DATABRICKS_ICEBERG_READ.md` §6.
>
> **Update (2026-06-13):** Added the turnover flow (`mv_casino_turnover_90d` → `sink_casino_turnover_90d_databricks` → `rw_casino_turnover_90d`) as a worked example of **upsert semantics without delete files** — append-only sink + read-side `QUALIFY` view `v_casino_turnover_latest` (verified: 1,093 event rows collapse to 569 latest-per-customer). See §1 architecture, §15 step 1e for the table DDL, and `DATABRICKS_ICEBERG_READ.md` §6 for the view.

---

## 1. Architecture

```
Kafka: cronus.casino.out.br  ──► src_casino_prd ──► mv_casino_transactions_full ──► sink_casino_transactions_databricks ──► de_dev.rw_poc.rw_casino_transactions
Kafka: bets-out-br           ──► src_bets_br    ──► mv_sportsbook_bets          ──► sink_sportsbook_bets_databricks     ──► de_dev.rw_poc.rw_sportsbook_bets

mv_casino_transactions_full ──► mv_casino_turnover_90d ──► sink_casino_turnover_90d_databricks ──► de_dev.rw_poc.rw_casino_turnover_90d (append-only)
                                                                                                  └─► view v_casino_turnover_latest (QUALIFY = latest per customer)
```

The turnover flow demonstrates **upsert semantics without Iceberg delete files**: an
upserting MV (`mv_casino_turnover_latest`, one row per customer) cannot sink to UC
directly because UC rejects delete files (§16). Instead the per-event snapshots land
append-only and a read-side view applies the "latest per customer" collapse. See
`DATABRICKS_ICEBERG_READ.md` §6 for the view.

**Design principle — one table per Kafka topic, hybrid schema:**  
RisingWave handles structural transformation only (UNNEST, type casting, protobuf field extraction). No business-logic aggregation happens before the sink. Each Databricks table has:
- **Flat typed columns** for the stable, high-value analytical fields (`customer_id`, timestamps, amounts, IDs)
- **`properties STRING`** — a JSON bag containing all remaining proto fields

This means new fields added to the PROTOBUF automatically appear in `properties` with zero schema changes in Databricks. Common analytical queries hit typed columns; deeper enrichment uses `GET_JSON_OBJECT(properties, '$.field')`.

Both Databricks sinks run **in parallel** with the existing Lakekeeper sinks — the Lakekeeper pipeline is untouched. The UC1/UC2 streaming aggregation chain (`mv_casino_transactions_full`, `mv_casino_real_bet`, etc.) is also untouched.

Both Databricks sinks run **in parallel** with the existing Lakekeeper sinks — the Lakekeeper pipeline is untouched. RisingWave authenticates to Databricks Unity Catalog via the Iceberg REST Catalog (IRC) API using Azure AD OAuth2, and writes Parquet files directly to the PoC Azure storage account using `adlsgen2.account_key`.

---

## 2. New files

| File | Purpose |
|---|---|
| `dbt/models/casino_prd/mv_casino_transactions_full.sql` | Hybrid MV: 6 flat columns + `properties` JSON bag from `src_casino_prd` |
| `dbt/models/casino_prd/mv_sportsbook_bets.sql` | Hybrid MV: 10 flat columns + `properties` JSON bag from `src_bets_br` |
| `dbt/models/casino_prd/sink_casino_transactions_databricks.sql` | Sink: `mv_casino_transactions_full` → `de_dev.rw_poc.rw_casino_transactions` |
| `dbt/models/casino_prd/sink_sportsbook_bets_databricks.sql` | Sink: `mv_sportsbook_bets` → `de_dev.rw_poc.rw_sportsbook_bets` |
| `dbt/models/casino_prd/sink_casino_turnover_90d_databricks.sql` | Append-only sink: `mv_casino_turnover_90d` → `de_dev.rw_poc.rw_casino_turnover_90d` (read-side upsert via view, §16) |
| `orchestration/assets/databricks_turnover_views.py` | Dagster asset: creates the `v_casino_turnover_latest` view (QUALIFY = latest row per customer) after the sink commits |

Modified:
- `docker-compose.yml` — Databricks env vars passed to both Dagster containers

No `CREATE CONNECTION` is used — Unity Catalog sinks in RisingWave embed all auth parameters directly in `CREATE SINK` (see §3).

---

## 3. Sink SQL (final working version)

`type = 'append-only'` + `force_append_only = 'true'` is required because **Unity Catalog does not support Iceberg delete files** (upsert mode). `force_append_only` tells RisingWave to drop DELETEs and convert UPDATEs to INSERTs from the retract stream.

Because the sink sources are flat event MVs (not aggregated state), each row is an immutable fact — **no deduplication is needed** in Databricks.

### `sink_casino_transactions_databricks`

Source: `mv_casino_transactions_full` — one row per casino transaction (all message types, all accounts).

**Hybrid schema** (`de_dev.rw_poc.rw_casino_transactions`):

| Column | Type | Notes |
|---|---|---|
| `customer_id` | INT | |
| `message_type_id` | INT | 1 = real bet, 2 = win, etc. |
| `account_id` | INT | 1 = real-money, 4 = bonus |
| `currency_id` | INT | |
| `transaction_created_at` | TIMESTAMP | |
| `amount_abs` | DECIMAL(20,8) | |
| `properties` | STRING (JSON) | All other proto fields — see `DATABRICKS_ICEBERG_READ.md` §6 |

```sql
CREATE SINK IF NOT EXISTS sink_casino_transactions_databricks
FROM mv_casino_transactions_full
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
    table.name                           = 'rw_casino_transactions',
    adlsgen2.account_name                = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key                 = '<ADLS_ACCOUNT_KEY>',
    commit_checkpoint_interval           = 5,
    compaction.write_parquet_compression = 'zstd'
)
```

### `sink_sportsbook_bets_databricks`

Source: `mv_sportsbook_bets` — one row per sportsbook bet.

**Hybrid schema** (`de_dev.rw_poc.rw_sportsbook_bets`):

| Column | Type | Notes |
|---|---|---|
| `bet_id` | BIGINT | |
| `customer_id` | INT | |
| `customer_segment_id` | INT | |
| `bet_type_id` | INT | |
| `bet_status_id` | INT | |
| `channel_id` | INT | |
| `currency_id` | INT | |
| `placed_at` | TIMESTAMP | |
| `stake_euro` | DECIMAL(20,8) | |
| `stake_local` | DECIMAL(20,8) | |
| `properties` | STRING (JSON) | All other proto fields — see `DATABRICKS_ICEBERG_READ.md` §6 |

```sql
CREATE SINK IF NOT EXISTS sink_sportsbook_bets_databricks
FROM mv_sportsbook_bets
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
    table.name                           = 'rw_sportsbook_bets',
    adlsgen2.account_name                = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key                 = '<ADLS_ACCOUNT_KEY>',
    commit_checkpoint_interval           = 5,
    compaction.write_parquet_compression = 'zstd'
)
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
| **Grafana monitoring** | Wired up via Trino `databricks` catalog — see the "Databricks Iceberg Sinks" row in `casino-uc-metrics.json`. Panels: row counts, data freshness (minutes behind live), Parquet file counts, and snapshot count over time. |
| **UC snapshot accumulation** | Unity Catalog forces `gc.enabled=false` on all managed Iceberg tables, so Iceberg's built-in GC never runs automatically. RisingWave's `enable_snapshot_expiration` sink option has **no effect** on UC tables — it only works with the Lakekeeper catalog. Snapshots accumulate indefinitely; Trino's `$snapshots` metadata table caps at 100 rows, so Grafana shows a flat "100 snapshots" once the table reaches that count. UC tables are created with `TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50')` (see §15.1e); to actually prune, call `CALL rw_poc.system.expire_snapshots(...)` in Databricks SQL or run `VACUUM` manually. |
| **Token handled by Iceberg library** | With `catalog.oauth2_server_uri`, the Azure AD token is fetched and refreshed by the Apache Iceberg Java library inside RisingWave. Token refresh behaviour is library-managed, not controlled at the RisingWave level. |
| **External tables required** | Databricks managed tables use Databricks' own ADLS account (no credentials available). Tables must be created as external Iceberg tables pointing to the PoC storage account — see §12. |
| **Small-file accumulation** | RisingWave commits one Parquet file per checkpoint (~20s). Mitigated by two layers: Predictive Optimization (enabled on `de_dev.rw_poc`, runs automatically) + `databricks_optimize` Dagster step (runs `OPTIMIZE` on-demand before demo queries). See §17. |
| **Trino credential vending not usable on Azure** | The Databricks IRC vends short-lived Azure SAS tokens. Setting `iceberg.rest-catalog.vended-credentials-enabled=true` + `fs.azure.enabled=true` causes Trino to fall back to the Azure SDK `DefaultAzureCredential` chain (environment vars, managed identity, VS Code, CLI) instead of using the vended SAS token — none of these are available in a Docker container. The ADLS account key (`azure.auth-type=ACCESS_KEY`) is the working alternative for this stack. Credential vending would work if Trino ran on an Azure VM or AKS pod with a managed identity that has Storage Blob Data Reader on the ADLS account. |
| **`PARTITIONED BY` incompatible with RisingWave IRC writes** | Pre-creating a Databricks managed Iceberg table with `PARTITIONED BY (col)` and then writing to it via RisingWave's Iceberg sink fails with `Iceberg error: invalid extra partition column index N`. Databricks assigns non-sequential internal Iceberg field IDs to managed table columns; RisingWave's Iceberg connector interprets the partition spec's source field ID as a zero-based column array index, which goes out of range. **Workaround**: do not pre-create the table — let RisingWave create it automatically. RisingWave assigns sequential field IDs from 1, and the resulting table has no partition spec. Predictive Optimization handles clustering automatically. The date columns (`transaction_date`, `placed_date`) added to the MVs are still useful for query filtering even without Iceberg partition pruning. |

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

### Outcome — confirmed working ✅ (2026-06-05)

**Writes are stable and continuous.** The approach that worked differs from the external-table plan above — see §15 for the full reproduction guide.

| Concern | Handled by | Result |
|---|---|---|
| Unity Catalog metadata (load table schema, commit snapshots) | `catalog.oauth2_server_uri` + `catalog.credential` | ✅ Working |
| ADLS data writes (Parquet files) | `adlsgen2.account_name` + `adlsgen2.account_key` | ✅ Working |

**What changed from the plan above:** External tables with a custom `LOCATION` are not supported in `de_dev` (Managed Iceberg catalog) — Step 7 fails with `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`. No new catalog was needed. The fix was to create schema `de_dev.rw_poc` with a custom `MANAGED LOCATION` pointing to the PoC storage account, then create **managed** Iceberg tables (no `LOCATION` override). Tables land in `stkznneurwpoccdddevstd` via the schema's managed location, and `adlsgen2.account_key` writes to them directly.

**Blockers encountered during this investigation:**

| Issue | When encountered | Resolution |
|---|---|---|
| `storage-credentials create --json azure_storage_credential` returns "Unrecognized storage credential type" | CLI | Admin used the Databricks **UI** (Settings → Unity Catalog → Storage credentials) to create it with the account key |
| `statement-execution` is not a valid CLI command | CLI | Use `databricks api post /api/2.0/sql/statements --json @file.json` with `wait_timeout` between 5s–50s |
| Storage account firewall blocks Databricks SQL Warehouse from creating tables | Step 6 | Admin opened the firewall: Azure Portal → `stkznneurwpoccdddevstd` → Networking → allow Databricks workspace VNet/subnet and developer public IP |
| `de_dev` catalog is Managed Iceberg — `USING ICEBERG LOCATION '...'` fails with `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED` | Step 7 | Dropped the external-table plan. Instead: created `de_dev.rw_poc` schema with `MANAGED LOCATION 'abfss://cont1@stkznneurwpoccdddevstd...'` and used managed Iceberg tables. See §15 Step 1c. |

---

## 13. Delta Lake sink — feasibility analysis

Evaluated as an alternative to the Iceberg approach. **Not feasible for this setup** — blocked by missing ADLS Gen2 support.

### What the Delta Lake connector supports

```sql
CREATE SINK IF NOT EXISTS sink_example_deltalake
FROM mv_source
WITH (
    connector                  = 'deltalake',
    type                       = 'append-only',
    force_append_only          = 'true',
    location                   = 's3a://bucket/path/table',
    s3.endpoint                = 'http://minio-host:9301',   -- S3-compatible endpoint
    s3.access.key              = '<access-key>',
    s3.secret.key              = '<secret-key>',
    commit_checkpoint_interval = 5
);
```

**Supported location schemes:** `s3://`, `s3a://` (S3 / MinIO), `gs://` (GCS), `file://` (local).

**Not supported:** Azure ADLS Gen2 (`abfss://`). There are no `adlsgen2.*` parameters in the Delta Lake connector.

Docs: https://docs.risingwave.com/integrations/destinations/delta-lake

### Why it doesn't work for this setup

| Blocker | Detail |
|---|---|
| **No Azure ADLS Gen2 support** | The Delta Lake connector only supports S3-compatible storage and GCS. Our Databricks workspace stores data on Azure ADLS Gen2 (`abfss://cont1@stkznneurwpoccdddevstd...`). There is no `adlsgen2.account_key` equivalent for the Delta Lake connector — unlike the Iceberg connector which added ADLS support in RisingWave v2.7.0. |
| **Databricks cannot reach local MinIO** | The only available S3-compatible target is the MinIO instance running in the local Docker stack. Databricks runs on Azure and has no network path to a developer's local Mac — so Databricks cannot read Delta tables written to local MinIO. |

### What a local MinIO sink would look like (for reference)

If the consumer were a local tool (e.g. Trino querying MinIO directly, or DataFusion), a Delta Lake sink to MinIO would work:

```sql
CREATE SINK IF NOT EXISTS sink_casino_transactions_deltalake
FROM mv_casino_transactions_full
WITH (
    connector                  = 'deltalake',
    type                       = 'append-only',
    force_append_only          = 'true',
    location                   = 's3a://hummock001/delta/rw_casino_transactions',
    s3.endpoint                = 'http://minio-0:9301',
    s3.access.key              = 'hummockadmin',
    s3.secret.key              = 'hummockadmin',
    commit_checkpoint_interval = 5
);
```

This writes valid Delta Lake format Parquet files. The sink sources (`mv_casino_transactions_full`, `mv_sportsbook_bets`) are flat immutable event MVs so `force_append_only` is semantically correct — no deduplication needed. However, since Databricks on Azure cannot reach local MinIO, this is only useful for local analytics.

### Conclusion

For Databricks on Azure, the **Iceberg connector with `adlsgen2.account_key`** is the only viable RisingWave sink path (see §3 and §15). The Delta Lake connector would become an option if:
- RisingWave adds `adlsgen2.*` support to the Delta Lake connector (not available as of v2.8.4), or
- The target storage moves to S3 (AWS or MinIO accessible from Databricks).

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

> **Running SQL**: all statements below can be run directly in the Databricks SQL editor. To run via CLI use `databricks api post /api/2.0/sql/statements --json @file.json` with `{"statement": "...", "warehouse_id": "4d06eca1e71a9ccc", "wait_timeout": "50s"}`.

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

```sql
CREATE SCHEMA IF NOT EXISTS de_dev.rw_poc
MANAGED LOCATION 'abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/iceberg';
```

**1d. Grant SP privileges on schema** (already done):

```sql
GRANT USE_SCHEMA        ON SCHEMA de_dev.rw_poc TO `3b7f531f-db93-4186-af75-6566c12c076b`;
GRANT MODIFY            ON SCHEMA de_dev.rw_poc TO `3b7f531f-db93-4186-af75-6566c12c076b`;
GRANT SELECT            ON SCHEMA de_dev.rw_poc TO `3b7f531f-db93-4186-af75-6566c12c076b`;
GRANT EXTERNAL USE SCHEMA ON SCHEMA de_dev.rw_poc TO `3b7f531f-db93-4186-af75-6566c12c076b`;
```

> **`EXTERNAL USE SCHEMA` is required separately from `MODIFY`/`SELECT`** — without it, IRC writes are silently rejected.

**1e. Create managed Iceberg tables** (already done):

> All five tables are also in `sql/databricks_setup.sql` — the canonical runnable script. The `databricks_uc_tables_setup` Dagster asset (group `casino_prd_setup`) runs these DDLs automatically at the start of `casino_prd_full_job`, creating any missing tables before the dbt build creates the RisingWave sinks.

```sql
CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_transactions (
    customer_id              INT           NOT NULL,
    message_type_id          INT           NOT NULL,
    account_id               INT           NOT NULL,
    currency_id              INT           NOT NULL,
    transaction_created_at   TIMESTAMP     NOT NULL,
    amount_abs               DECIMAL(20,8),
    properties               STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');

CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_sportsbook_bets (
    bet_id               BIGINT        NOT NULL,
    customer_id          INT           NOT NULL,
    customer_segment_id  INT,
    bet_type_id          INT,
    bet_status_id        INT,
    channel_id           INT,
    currency_id          INT,
    placed_at            TIMESTAMP     NOT NULL,
    stake_euro           DECIMAL(20,8),
    stake_local          DECIMAL(20,8),
    properties           STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');

-- Per-event rolling-turnover snapshots (sink_casino_turnover_90d_databricks).
-- Append-only; the "latest per customer" collapse happens in the read-side view
-- v_casino_turnover_latest (see DATABRICKS_ICEBERG_READ.md §6). RisingWave's
-- sink loads this schema and casts to it, so the table MUST exist before the
-- dbt build creates the sink — otherwise: "Table ... not found [ErrorCode: 3000]".
-- rolling_7d_turnover is DECIMAL(38,8): scale 8 matches amount_abs, precision 38
-- prevents overflow when summing a window.
CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_turnover_90d (
    customer_id          INT           NOT NULL,
    event_ts             TIMESTAMP     NOT NULL,
    rolling_7d_turnover  DECIMAL(38,8)
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');
```

-- Raw Protobuf landing tables (sink_casino_landing_databricks, sink_sportsbook_landing_databricks).
-- Payload is binary (undecoded Protobuf bytes); decoding to typed bronze happens via the
-- casino_landing_to_bronze Databricks notebook (casino_landing_to_bronze_job in Dagster).
CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_landing (
    payload          BINARY,
    kafka_key        BINARY,
    kafka_timestamp  TIMESTAMP,
    kafka_partition  STRING,
    kafka_offset     STRING,
    year_month       STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');

CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_sportsbook_landing (
    payload          BINARY,
    kafka_key        BINARY,
    kafka_timestamp  TIMESTAMP,
    kafka_partition  STRING,
    kafka_offset     STRING,
    year_month       STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50');
```

> **`USING ICEBERG`** — required for RisingWave's Iceberg REST Catalog write path. `USING DELTA` creates a plain Delta table that is not Iceberg-compatible via IRC and causes `not an Iceberg compatible table` error. Managed storage is placed in the schema's MANAGED LOCATION (`stkznneurwpoccdddevstd`).

---

### Step 2 — RisingWave sink SQL (final working version)

See §3 for full sink SQL. Key parameters:

```
type                             = 'append-only'
force_append_only                = 'true'
commit_checkpoint_interval       = 5
compaction.write_parquet_compression = 'zstd'
adlsgen2.account_name/key        = PoC storage account credentials
catalog.oauth2_server_uri        = Azure AD token endpoint
```

Sink sources are flat event MVs (`mv_casino_transactions_full`, `mv_sportsbook_bets`) — not aggregated. See §3 for complete SQL.

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

# Verify after ~30 seconds (run in Databricks SQL editor or via CLI)
```

```sql
SELECT COUNT(*), MIN(transaction_created_at), MAX(transaction_created_at)
FROM de_dev.rw_poc.rw_casino_transactions;

SELECT COUNT(*), MIN(placed_at), MAX(placed_at)
FROM de_dev.rw_poc.rw_sportsbook_bets;
```

---

### Production notes

- **Token refresh**: `catalog.oauth2_server_uri` causes the Apache Iceberg Java library to fetch and refresh Azure AD tokens automatically — no manual token management needed.
- **Sink recreation**: `casino_prd_full_job` drops and recreates all sinks on every run via `drop_prebuild_sinks`, including the Databricks sinks. This ensures config changes (e.g. `commit_checkpoint_interval`) take effect on the next run at the cost of a ~5-10s write gap per job run — acceptable for a PoC.
- **Compaction**: Databricks `delta.autoOptimize.autoCompact = true` handles file compaction automatically. No RisingWave-level compaction is needed.
- **Schema `rw_poc` vs `risingwave_poc`**: `rw_poc` has a custom managed location (`stkznneurwpoccdddevstd`). `risingwave_poc` uses Databricks' managed storage — cannot use `adlsgen2.account_key` for it.
- **No deduplication needed**: Sink sources are immutable event MVs (`mv_casino_transactions_full`, `mv_sportsbook_bets`). Every row is a unique event — append-only is semantically correct. UC1/UC2 aggregation logic runs in Databricks SQL, Trino, or RisingWave — see `DATABRICKS_ICEBERG_READ.md` §6.

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

---

## 17. File compaction

### The small-file problem

RisingWave commits one Parquet file per checkpoint cycle (~20s at `commit_checkpoint_interval = 5`). At steady state this produces ~180 files/hour. Many small files degrade scan performance because each file requires a separate ADLS read request.

### Why RisingWave-native compaction doesn't work

`enable_compaction`, `compaction.trigger_snapshot_count`, and `compaction_interval_sec` are implemented in RisingWave's **upsert/retract code path** (`non_append_only_behavior`). Append-only sinks (`type = 'append-only'` + `force_append_only = 'true'`) have `non_append_only_behavior = None` — the compaction options are parsed and stored but no compaction code path is ever reached. Confirmed via compute node logs:

```
# Lakekeeper upsert sinks (compaction works):
Sink executor info sink_id=83 non_append_only_behavior=Some(NonAppendOnlyBehavior { force_compaction: true })

# Databricks append-only sinks (compaction silently ignored):
Sink executor info sink_id=84 non_append_only_behavior=None
```

### Solution: two-layer compaction

**Layer 1 — Databricks Predictive Optimization (continuous, automatic)**

Enabled on `de_dev.rw_poc` on 2026-06-07:

```sql
ALTER SCHEMA de_dev.rw_poc ENABLE PREDICTIVE OPTIMIZATION;
```

Confirmed with:
```sql
DESCRIBE SCHEMA EXTENDED de_dev.rw_poc;
-- Predictive Optimization: ENABLE
```

Databricks automatically runs `OPTIMIZE`, `VACUUM`, and `ANALYZE` on the managed Iceberg tables based on write activity, using serverless compute. No maintenance schedule required.

Prerequisites: Premium plan workspace; Unity Catalog **managed tables** only (external tables are not supported).

To monitor operations:
```sql
SELECT operation_type, operation_status, table_name, start_time, end_time, operation_metrics
FROM system.storage.predictive_optimization_operations_history
WHERE schema_name = 'rw_poc'
ORDER BY start_time DESC
LIMIT 20;
```

Note: the system table has a latency of several hours. Operations may not appear immediately.

**Confirmed in testing (2026-06-07)**

After ~1 hour of continuous RisingWave writes, Predictive Optimization fired automatically on `rw_casino_transactions`, compacting ~100 Parquet files. This was confirmed via the Iceberg `$snapshots` system table — a `replace` snapshot appeared alongside the stream of `append` snapshots:

```sql
-- Run via Trino
SELECT operation, COUNT(*) AS count
FROM databricks.rw_poc."rw_casino_transactions$snapshots"
GROUP BY operation;
-- append  99
-- replace  1
```

A concurrent Trino `ALTER TABLE ... EXECUTE optimize` attempt on the same table failed with a snapshot conflict error (`branch main has changed`) — this was caused by Predictive Optimization committing its own `replace` snapshot at exactly the same moment. The Trino failure was harmless; Predictive Optimization had already done the work.

`rw_sportsbook_bets` was compacted via Trino in the same session (no concurrent conflict), reducing ~917 files to 1. Both tables showed reduced Parquet file counts in Grafana immediately after.

**Layer 2 — Dagster `databricks_optimize` step (on-demand)**

The `databricks_datafusion_job` runs `OPTIMIZE` on all three tables (`rw_casino_transactions`, `rw_sportsbook_bets`, `rw_casino_turnover_90d`) via the Databricks SQL API immediately before demo queries, guaranteeing compacted files regardless of Predictive Optimization timing. Controlled by `orchestration/assets/databricks_optimize.py` (`TABLES` list). The turnover table accumulates files fastest (one commit per ~10s of rolling-window updates), so on-demand `OPTIMIZE` matters most there for a snappy demo.

### Alternative: increase `commit_checkpoint_interval`

Raising `commit_checkpoint_interval` from `5` to a higher value (e.g. `30` or `60`) reduces the file creation rate proportionally at the cost of higher data latency before each commit is visible. Both approaches can be combined.

## 18. Lakekeeper vs Unity Catalog — comparison from PoC experiments

Both catalogs were run in parallel throughout the PoC. The key findings:

| Capability | Lakekeeper (REST + MinIO) | Databricks Unity Catalog |
|---|---|---|
| **Upsert sinks** | ✅ Native — `type = 'upsert'` works out of the box | ❌ Rejects Iceberg delete files; `type = 'upsert'` stalls silently after initial burst |
| **Append-only sinks** | ✅ | ✅ |
| **Upsert workaround** | Not needed | Append-only + read-side `QUALIFY ROW_NUMBER()` VIEW (`v_casino_turnover_latest`) |
| **Snapshot expiration** | ❌ `enable_snapshot_expiration` has no effect with REST catalog (RisingWave 2.7.4–2.8.x) | ❌ `gc.enabled=false` forced on all UC managed tables; manual `CALL system.expire_snapshots(...)` required |
| **File compaction** | ✅ RisingWave-native `force_compaction` works | ❌ Silently ignored; requires external Trino `OPTIMIZE` or Databricks `OPTIMIZE` |
| **Ecosystem / governance** | Local / self-hosted only | Full Databricks compute, Unity Catalog sharing, column-level security, audit logs |
| **Auth complexity** | None (local MinIO) | Azure AD OAuth2 + ADLS Gen2 account key; `vended_credentials` only works for S3 backends |
| **Trino federation** | ✅ via `datalake` catalog | ✅ via `databricks` catalog (separate Trino connector) |

### Summary

Lakekeeper wins on **write semantics**: upsert, compaction, and snapshot expiration all work natively, making it the better choice for streaming sinks where RisingWave continuously updates rows (e.g. rolling-window MVs like `mv_casino_real_bet` and `mv_turnover_percentage`).

Unity Catalog wins on **ecosystem integration**: Databricks SQL, Delta Sharing, Unity Catalog governance, and existing organisational data access controls. It is the right target for **archival / append-only** workloads (raw landing tables like `rw_casino_transactions`, `rw_sportsbook_bets`) where the delete-file limitation does not apply.

The current PoC architecture reflects this split: upsert aggregations go to Lakekeeper; raw event facts go to Unity Catalog.
