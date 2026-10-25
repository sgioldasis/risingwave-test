---
title: StarRocks Shared-Data Mode on Azure — Feasibility Test
description: Standalone proof-of-concept for using Azure (ADLS Gen2) as StarRocks's own table storage — confirmed feasible, currently blocked on Azure credentials
---

<!-- markdownlint-disable-file -->

## What this is

A standalone test of whether StarRocks can use Azure (ADLS Gen2) as its own
table storage — i.e. StarRocks's **shared-data architecture** (compute/storage
separation), not the existing Iceberg-via-REST-catalog integration this
project already has. Those are unrelated: the existing `databricks_uc`/
`lakekeeper_local` catalogs let StarRocks *read* Iceberg tables whose data
happens to live in Azure; shared-data mode is about StarRocks's *own* native
tables writing to Azure instead of local BE disk.

Fully isolated from the main demo stack: separate containers
(`starrocks-shared-fe`, `starrocks-shared-cn`), separate ports, separate
Azure path. Not wired into the dashboard, dbt, or anything else.

## Known caveat beyond the credential blocker: backup/restore is broken

Independent of the credential issue below, there's an open StarRocks bug
where **cluster snapshots (backup/restore) fail on Azure Blob Storage**
(`TYPE = AZBLOB`/`ADLS2`) with an `UnsupportedFileSystemException`
(StarRocks GitHub issue
[#67272](https://github.com/StarRocks/starrocks/issues/67272)). This wasn't
hit in this session's testing (never got past the write-path credential
failure to attempt a snapshot), but it's a real, documented limitation
worth knowing before relying on this for anything where backup/restore
matters -- shared-data on Azure is not yet at full feature parity with the
S3 path for that specific capability.

## Status: confirmed feasible, blocked on credentials

The mechanism works — StarRocks accepted the shared-data configuration and
storage volume definition without complaint. Every write attempt failed at
the Azure authentication layer, not the StarRocks layer. This needs a fresh
Azure credential (see below) before it can be tested end-to-end; nothing
further to do on the StarRocks/config side.

## Why this needs a separate deployment, not a config change

The existing `starrocks` service uses the `starrocks/allin1-ubuntu` image,
which is **shared-nothing only** (FE+BE combined in one process). Shared-data
mode requires the dedicated `starrocks/fe-ubuntu` + `starrocks/cn-ubuntu`
images (CN = Compute Node, replaces BE in shared-data clusters), and
`run_mode` is set at FE bootstrap — not something you flip on a running
shared-nothing cluster. Confirmed via StarRocks's own reference shared-data
docker-compose example
(`https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml`).

## What was built

Two new services added to `docker-compose.yml`, then **commented out** per
this doc (kept for reference, not active) after the credential blocker was
hit:

```yaml
starrocks-shared-fe:
  image: starrocks/fe-ubuntu:4.0-latest
  command: appends `run_mode = shared_data` and
    `cloud_native_storage_type = ADLS2` to fe.conf, then starts with
    `--host_type FQDN`
  ports: 9031 (MySQL), 8031 (HTTP), 9021 (edit log)

starrocks-shared-cn:
  image: starrocks/cn-ubuntu:4.0-latest
  command: registers itself via `ALTER SYSTEM ADD COMPUTE NODE`, then starts
  ports: 8041 (HTTP)
```

Both came up healthy on the first attempt (FE alive, CN registered and
alive) — the shared-data bootstrap itself works cleanly.

### Storage volume

StarRocks shared-data doesn't take object-storage credentials in `fe.conf` —
they're set via a SQL-level `CREATE STORAGE VOLUME`, executed after FE start:

```sql
CREATE STORAGE VOLUME azure_test
TYPE = ADLS2
LOCATIONS = ('adls2://sr-poc-cont1/starrocks-shared-data-test')
PROPERTIES (
    'azure.adls2.endpoint' = 'https://<account>.dfs.core.windows.net',
    'azure.adls2.shared_key' = '<account key>',
    'enabled' = 'true'
);
SET azure_test AS DEFAULT STORAGE VOLUME;
```

This uses the **same** Azure storage account already used for this project's
Iceberg data (`ADLS_ACCOUNT_NAME`/`ADLS_ACCOUNT_KEY` from `.env`), under an
isolated path (`starrocks-shared-data-test`) so it can't collide with
existing Iceberg data in the same container. No new Azure resources needed
if credentials work.

## The actual blocker: every available credential failed

Tried, in order:

1. **`azure.adls2.shared_key`** (the storage account key,
   `ADLS_ACCOUNT_KEY`) — rejected: `403 Server failed to authenticate the
   request. Make sure the value of Authorization header is formed correctly
   including the signature.`
2. **`azure.adls2.sas_token`** (`ADLS_PROTO_SAS_TOKEN`) — same 403, and
   confirmed via a raw `curl` request directly against
   `https://<account>.blob.core.windows.net` (bypassing StarRocks entirely)
   that the token itself is rejected by Azure, independent of any
   StarRocks-side config. This token has since been removed from `.env` as
   stale/invalid.
3. **OAuth2 with `ADLS_CLIENT_ID`/`ADLS_CLIENT_SECRET`/`ADLS_TENANT_ID`**
   (a standard Azure AD app-registration / service-principal credential) —
   **not attempted**, because StarRocks's `CREATE STORAGE VOLUME` for ADLS2
   only supports Managed Identity or Workload Identity for OAuth (confirmed
   by searching the full properties list in StarRocks's own docs:
   `azure.adls2.oauth2_use_managed_identity`,
   `azure.adls2.oauth2_tenant_id`, `azure.adls2.oauth2_client_id`,
   `azure.adls2.oauth2_token_file` -- no `client_secret` property exists at
   all). A plain client-secret service-principal credential genuinely
   cannot be used for this StarRocks feature, in any StarRocks version
   checked. This is a StarRocks limitation, not a missed config option.

**Hypothesis for why both (1) and (2) failed identically**: the storage
account likely has **"Disallow Shared Key access"** enabled in Azure. That
setting blocks both direct shared-key auth *and* any SAS token that was
generated using the account key (which is how ours was almost certainly
made) -- consistent with the main project's own `starrocks` container
defaulting to SAS-token auth rather than shared key
(`STARROCKS_ADLS_AUTH_MODE:-sas` in `docker-compose.yml`), suggesting this
was already known/worked around elsewhere in this project, just not
documented as such. Not independently confirmed via the Azure portal in
this session.

## What would unblock this

One of:
1. A **user-delegation SAS token** (Azure AD-based generation, not
   account-key-based) -- works even with Shared Key access disabled.
   Requires someone with Azure CLI/portal access to the storage account to
   generate it (`az storage container generate-sas --auth-mode login ...`
   or the Azure Portal's SAS generation UI with "User delegation key"
   selected).
2. Disabling "Disallow Shared Key access" on the storage account -- a real
   security-posture change, not something to do without whoever owns that
   Azure resource signing off.
3. Managed/Workload Identity -- only viable if StarRocks is actually running
   on Azure infrastructure with an assigned identity, not from a local
   Docker Compose dev environment.

None of these were pursued further in this session -- paused here per an
explicit decision to stop rather than chase Azure-side credential/policy
changes.

## How to resume

1. Get a working credential (see above).
2. Uncomment the `starrocks-shared-fe` / `starrocks-shared-cn` blocks in
   `docker-compose.yml`.
3. `docker compose up -d starrocks-shared-fe`, wait for healthy, then
   `docker compose up -d starrocks-shared-cn`.
4. Re-run the `CREATE STORAGE VOLUME` statement above with the new
   credential.
5. Retry the test insert:
   ```sql
   CREATE DATABASE IF NOT EXISTS shared_test;
   CREATE TABLE shared_test.demo_table (id INT, name VARCHAR(64))
     DISTRIBUTED BY HASH(id);
   INSERT INTO shared_test.demo_table VALUES (1, 'hello_azure');
   SELECT * FROM shared_test.demo_table;
   ```
   Success looks like: no error, and the row is visible on `SELECT`. To
   confirm data actually landed in Azure (not just StarRocks's own cache),
   check the storage account under `sr-poc-cont1/starrocks-shared-data-test/`
   for new blobs after the insert.
6. Tear down when done testing:
   ```
   docker compose stop starrocks-shared-fe starrocks-shared-cn
   docker compose rm -f starrocks-shared-fe starrocks-shared-cn
   ```
