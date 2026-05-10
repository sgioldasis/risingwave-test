# Plan: Spark Iceberg compaction job for `iceberg_hermes_features` (Dagster on-demand)

TL;DR — Add an on-demand Dagster job (`iceberg_compaction_job`) that opens an in-process Spark 4 session inside the Dagster container, then runs the full Iceberg maintenance suite (`rewrite_data_files` binpack + delete-file-threshold/partial-progress, `rewrite_position_delete_files`, `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`) against `lakekeeper.public.iceberg_hermes_features`. Bake JDK 21 + pyspark + Iceberg jars into `Dockerfile.dagster` so no Ivy download happens at run time. No schedule — user triggers from Dagster UI.

## Phase A — Image & deps (blocks everything)
1. Add `pyspark>=4.0.0,<4.1.0` to `pyproject.toml` (matching Spark 4 migration notes).
2. In `Dockerfile.dagster`, install JDK 21 (`apt-get install -y temurin-21-jdk` via Adoptium repo, or `openjdk-21-jre-headless` from Debian backports — pick whichever the existing base supports cleanly) and set `JAVA_HOME`.
3. Pre-stage the two Iceberg jars into the image at `/opt/iceberg-jars/` so the Spark session uses `spark.jars` (local paths) instead of `spark.jars.packages` (network fetch on every run):
   - `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1`
   - `org.apache.iceberg:iceberg-aws-bundle:1.10.1`
   Download in the Dockerfile via `curl` from Maven Central; checksum-verify.
4. Set env defaults in the container so the asset reads them (overridable):
   - `ICEBERG_REST_URI=http://lakekeeper:8181/catalog`
   - `ICEBERG_WAREHOUSE=risingwave-warehouse`
   - `S3_ENDPOINT=http://minio-0:9301`
   - `S3_ACCESS_KEY=hummockadmin`, `S3_SECRET_KEY=hummockadmin`

## Phase B — Compaction code (parallel with A once design is settled)
5. New module `orchestration/assets/iceberg_compaction.py` with:
   - A small helper `_build_spark_session()` mirroring [scripts/user_activity_flow.py](scripts/user_activity_flow.py#L80-L130) but using container hostnames + local jar paths from Phase A. Driver memory ~2 GB, `master=local[*]`, JDK 17/21 `--add-opens` set per `/memories/repo/spark4-migration.md`. Catalog name: `lakekeeper`.
   - One `@op` per maintenance step so each appears as a separate node in the Dagster run graph and can fail/retry independently:
     - `op_rewrite_data_files` → `CALL lakekeeper.system.rewrite_data_files(table => 'public.iceberg_hermes_features', strategy => 'binpack', options => map('target-file-size-bytes','134217728','min-input-files','3','delete-file-threshold','2','partial-progress.enabled','true','partial-progress.max-commits','10','max-concurrent-file-group-rewrites','4'))`
     - `op_rewrite_position_deletes` → `CALL lakekeeper.system.rewrite_position_delete_files(table => 'public.iceberg_hermes_features', options => map('rewrite-all','true'))`
     - `op_rewrite_manifests` → `CALL lakekeeper.system.rewrite_manifests('public.iceberg_hermes_features')`
     - `op_expire_snapshots` → `CALL lakekeeper.system.expire_snapshots(table => 'public.iceberg_hermes_features', older_than => TIMESTAMP '<now-3d>', retain_last => 5, stream_results => true)`
     - `op_remove_orphan_files` → `CALL lakekeeper.system.remove_orphan_files(table => 'public.iceberg_hermes_features', older_than => TIMESTAMP '<now-7d>')`
   - Each op should reuse a single SparkSession built once at job start (use a Dagster resource `spark_session_resource` with `@resource` returning the session and stopping it on teardown). Avoids JVM cold-start per op.
   - Each op returns the result row count from the procedure (Iceberg returns rewritten/added/deleted file counts) and yields a `MetadataValue` so the Dagster UI surfaces what changed.

6. Wire ops sequentially (each depends on the previous return) inside `@job def iceberg_compaction_job()` in the same module — order matters: data-file rewrite → position-delete rewrite → manifest rewrite → expire snapshots → remove orphan files. Tag the job `{"compaction": "iceberg"}` for filtering.

## Phase C — Dagster registration
7. In [orchestration/definitions.py](orchestration/definitions.py):
   - Import `iceberg_compaction_job` and `spark_session_resource`.
   - Add the job to the `Definitions(jobs=[...])` list near the existing `iceberg_countries_job` (~L368).
   - Add the resource to the `Definitions(resources=...)` mapping.
   - Do **not** add a schedule (user choice: on-demand only).

## Phase D — Verification
8. Build & start: `docker compose build dagster-webserver && bin/1_up.sh`. Confirm container has `java -version` = 21 and `python -c "import pyspark; print(pyspark.__version__)"` = 4.0.x.
9. Pre-state snapshot via Trino (already in stack):
   - `SELECT count(*) FROM datalake.public."iceberg_hermes_features$files"` (active data-file count)
   - `SELECT count(*) FROM datalake.public."iceberg_hermes_features$snapshots"`
   - `SELECT count(*) FROM datalake.public."iceberg_hermes_features$manifests"`
10. Trigger `iceberg_compaction_job` from the Dagster UI (http://localhost:3000). Watch each op succeed; inspect metadata (rewritten / removed counts).
11. Post-state via the same three Trino queries — expect: data-files ↓ significantly, snapshots ↓ to ≤ retain_last + recent, manifests ↓.
12. Sanity-read: `SELECT count(*) FROM datalake.public.iceberg_hermes_features` matches pre-count (no data loss).
13. RisingWave smoke test: confirm the upstream `sink_hermes_features_to_iceberg` is still committing fine (`rw_catalog.rw_event_logs` has no new SINK_FAIL).

## Relevant files
- [Dockerfile.dagster](Dockerfile.dagster) — add JDK + curl-fetch Iceberg jars + JAVA_HOME/env vars.
- [pyproject.toml](pyproject.toml) — add pyspark dep.
- [scripts/user_activity_flow.py](scripts/user_activity_flow.py#L80-L130) — reference for SparkSession config (catalog keys, --add-opens, S3FileIO options). Do not import from it.
- [dbt/models/sink_hermes_features_to_iceberg.sql](dbt/models/sink_hermes_features_to_iceberg.sql) — confirms target table name + that no RW-side compaction is running (so engine-side is meaningful).
- [orchestration/definitions.py](orchestration/definitions.py) — register job + resource.
- [orchestration/assets/iceberg_compaction.py](orchestration/assets/iceberg_compaction.py) — **new file** with resource, ops, and job.
- [trino/catalog/datalake.properties](trino/catalog/datalake.properties) — confirms container-side endpoints for verification queries.
- `/memories/repo/spark4-migration.md` — Spark 4 jar / `--add-opens` / S3FileIO option-name caveats; honour these.
- `/memories/repo/startup-notes.md` — confirms `iceberg_hermes_features` has no native RW compaction (correct gap to fill).

## Decisions
- **Engine**: Spark only (user). Trino procedures intentionally not used despite being lighter, because future extension to sort/z-order is wanted.
- **Scope**: Only `iceberg_hermes_features`. `rw_managed_funnel` continues to be compacted by RisingWave; `iceberg_countries` is static and ignored.
- **Runtime**: pyspark added to the Dagster image (user). Jars pre-baked, not Ivy-fetched.
- **Trigger**: On-demand only — no `ScheduleDefinition`.
- **Out of scope**: sort/z-order rewrite (can be added later by passing `strategy='sort', sort_order=...` to a separate op), maintenance for funnel/countries tables, alerting, metrics export.

## Further considerations
1. **JDK install path** — Debian slim doesn't ship JDK 21 in main repos. Recommendation: use Adoptium Temurin apt repo (clean, official). Alternative: switch base to `eclipse-temurin:21-jre` and reinstall Python via deadsnakes (heavier change). Recommend Adoptium-on-slim first; fall back if image bloat is unacceptable.
2. **Driver memory** — `local[*]` Spark inside the dagster container competes with dagster-webserver memory. Recommendation: cap driver at 2 GB (`spark.driver.memory=2g`) and document that the container's compose memory limit (if any) needs to be ≥ 4 GB. If your `docker-compose.yml` sets a lower limit, raise it for the dagster service.
3. **`remove_orphan_files` lookback** — Iceberg requires `older_than` to be conservative (default 3 days) to avoid racing with concurrent writers. With `commit_checkpoint_interval=2` on the hermes sink, 7 days is safely conservative; user can shorten manually for first run if backlog cleanup is desired.
