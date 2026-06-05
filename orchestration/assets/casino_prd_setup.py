"""Casino production prerequisite assets: fetch, compile, upload proto schemas,
and create Iceberg read sources after sinks have committed their first checkpoint."""
import shutil
import subprocess
import time
from pathlib import Path

import boto3
import httpx
from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset

PROTO_DIR = Path(__file__).parent.parent.parent / "proto"

APICURIO_BASE = "http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts"
CASINO_ARTIFACT = "casinoroundinfo"
BETS_ARTIFACT = "betinfo"

MINIO_ENDPOINT = "http://minio-0:9301"
MINIO_BUCKET = "hummock001"
MINIO_ACCESS_KEY = "hummockadmin"
MINIO_SECRET_KEY = "hummockadmin"


@asset(group_name="casino_prd_setup", description="Fetch .proto files from Apicurio schema registry (native v2)")
def casino_prd_proto_fetch(context: AssetExecutionContext):
    """Fetch casino and bets .proto sources from Apicurio native v2 endpoint.

    Falls back gracefully if the registry is unreachable (e.g. no VPN) and the
    .proto files already exist on disk — the same pattern as casino_prd_proto_compile.
    """
    PROTO_DIR.mkdir(parents=True, exist_ok=True)

    fetched = []
    for artifact, filename in [
        (CASINO_ARTIFACT, "casinoroundinfodto.proto"),
        (BETS_ARTIFACT, "betinfo.proto"),
    ]:
        dest = PROTO_DIR / filename
        url = f"{APICURIO_BASE}/{artifact}"
        try:
            context.log.info(f"Fetching {url} → {dest}")
            response = httpx.get(url, headers={"Accept": "text/plain"}, timeout=30, follow_redirects=True)
            response.raise_for_status()
            dest.write_bytes(response.content)
            context.log.info(f"Saved {dest} ({dest.stat().st_size} bytes)")
        except Exception as e:
            if dest.exists():
                context.log.warning(
                    f"Could not reach Apicurio ({e}) — using existing {dest} ({dest.stat().st_size} bytes). "
                    "Re-run with network access to refresh."
                )
            else:
                raise RuntimeError(
                    f"Failed to fetch {url} and {dest} does not exist locally. "
                    "Ensure VPN is active or place the .proto file manually in proto/."
                ) from e
        fetched.append(str(dest))

    return {"fetched_files": MetadataValue.json(fetched)}


@asset(
    group_name="casino_prd_setup",
    deps=[casino_prd_proto_fetch],
    description="Compile .proto files to binary FileDescriptorSet using protoc",
)
def casino_prd_proto_compile(context: AssetExecutionContext):
    """Compile .proto sources to .pb/.desc FileDescriptorSets for RisingWave."""
    compiled = []

    for proto_file, out_file, extra_args in [
        (
            "casinoroundinfodto.proto",
            "casinoroundinfodto.pb",
            [],
        ),
        (
            "betinfo.proto",
            "betinfo.desc",
            ["--proto_path=/opt/homebrew/include", f"--proto_path={PROTO_DIR}"],
        ),
    ]:
        src = PROTO_DIR / proto_file
        dest = PROTO_DIR / out_file

        if not src.exists():
            raise FileNotFoundError(f"Proto source not found: {src}. Run casino_prd_proto_fetch first.")

        if shutil.which("protoc") is None:
            if dest.exists():
                context.log.warning(f"protoc not on PATH — using existing {dest}")
                compiled.append(str(dest))
                continue
            raise RuntimeError(
                f"protoc not found and {dest} does not exist. "
                "Install protoc or pre-build the descriptor and place it in proto/."
            )

        cmd = [
            "protoc",
            "--include_imports",
            f"--descriptor_set_out={dest}",
            f"--proto_path={PROTO_DIR}",
            *extra_args,
            str(src),
        ]
        context.log.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"protoc failed for {proto_file}:\n{result.stderr}")
        context.log.info(f"Compiled {dest} ({dest.stat().st_size} bytes)")
        compiled.append(str(dest))

    return {"compiled_files": MetadataValue.json(compiled)}


@asset(
    group_name="casino_prd_setup",
    deps=[casino_prd_proto_compile],
    description="Upload compiled proto descriptors to MinIO at s3://hummock001/proto/",
)
def casino_prd_proto_upload(context: AssetExecutionContext):
    """Upload .pb/.desc files to MinIO so RisingWave can fetch them at CREATE TABLE time."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    uploaded = []
    for filename in ["casinoroundinfodto.pb", "betinfo.desc"]:
        local_path = PROTO_DIR / filename
        if not local_path.exists():
            raise FileNotFoundError(f"{local_path} not found. Run casino_prd_proto_compile first.")

        s3_key = f"proto/{filename}"
        context.log.info(f"Uploading {local_path} → s3://{MINIO_BUCKET}/{s3_key}")
        s3.upload_file(str(local_path), MINIO_BUCKET, s3_key)
        uri = f"s3://{MINIO_BUCKET}/{s3_key}"
        context.log.info(f"Uploaded {uri}")
        uploaded.append(uri)

    return {"uploaded_uris": MetadataValue.json(uploaded)}


TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_CATALOG = "datalake"
TRINO_SCHEMA = "public"

# (view_name, DDL select) — dollar-free views over the Iceberg $snapshots
# metadata tables so Grafana (which interprets $ as a variable) can query them.
# Used by the casino dashboard's Snapshot Count and Operations/min panels.
#
# The *_rowcount views sum record_count from the $partitions metadata table —
# a manifest-only lookup, so the dashboard's "Iceberg Rows" panels return
# instantly instead of running a COUNT(*) full scan (merge-on-read over every
# data + equality-delete file) on the upsert tables. ($partitions counts
# data-file records — RisingWave leaves the $snapshots summary map empty, so
# summary['total-records'] is unavailable.)
TRINO_VIEWS = [
    ("casino_real_bet_snapshots",
     'SELECT snapshot_id, operation, CAST(committed_at AS timestamp(6)) AS committed_at '
     'FROM datalake.public."rw_managed_casino_real_bet$snapshots"'),
    ("turnover_pct_snapshots",
     'SELECT snapshot_id, operation, CAST(committed_at AS timestamp(6)) AS committed_at '
     'FROM datalake.public."rw_managed_turnover_percentage$snapshots"'),
    ("casino_real_bet_rowcount",
     "SELECT SUM(record_count) AS iceberg_rows "
     'FROM datalake.public."rw_managed_casino_real_bet$partitions"'),
    ("turnover_pct_rowcount",
     "SELECT SUM(record_count) AS iceberg_rows "
     'FROM datalake.public."rw_managed_turnover_percentage$partitions"'),
    # Live data-file count from the $files metadata table — a manifest-only
    # lookup. This is the current file count (drops after compaction merges small
    # files), NOT a per-snapshot history: RisingWave leaves the $snapshots summary
    # map empty (see note above), so summary['total-data-files'] is unavailable.
    ("casino_real_bet_filecount",
     "SELECT COUNT(*) AS file_count "
     'FROM datalake.public."rw_managed_casino_real_bet$files"'),
    ("turnover_pct_filecount",
     "SELECT COUNT(*) AS file_count "
     'FROM datalake.public."rw_managed_turnover_percentage$files"'),
]


@asset(
    group_name="trino",
    deps=[
        # Depend on the two Iceberg sinks — they create the tables in Lakekeeper
        # that these views read from. The views must be created after the tables exist.
        AssetDep(AssetKey(["public", "sink_casino_real_bet"])),
        AssetDep(AssetKey(["public", "sink_turnover_percentage"])),
    ],
    description=(
        "Create dollar-free Trino views over the Iceberg $snapshots and $files metadata tables. "
        "Grafana panels (snapshot count, live data files) query these views. "
        "Recreated on every run so they survive a stack restart."
    ),
)
def casino_trino_views(context: AssetExecutionContext):
    """Poll Trino until the Iceberg tables are queryable, then CREATE OR REPLACE the metadata views."""
    from trino.dbapi import connect

    def trino_exec(sql: str):
        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="dagster",
                       catalog=TRINO_CATALOG, schema=TRINO_SCHEMA)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()
        finally:
            conn.close()

    # Phase 1: wait for Trino to finish initializing (can take 2-3 min on fresh stack)
    context.log.info("Waiting for Trino to be ready...")
    for attempt in range(60):
        try:
            trino_exec("SELECT 1")
            context.log.info(f"Trino ready after ~{attempt * 5}s")
            break
        except Exception as e:
            if attempt == 59:
                raise RuntimeError(f"Trino did not become ready after 300s: {e}") from e
            time.sleep(5)

    # Phase 2: wait for Iceberg output to be queryable. Gate ONLY on the turnover table
    # (UC2 emits continuously -> commits within ~30-40s). The casino real-bet table can lag
    # far longer if UC1 uses TUMBLE + EMIT ON WINDOW CLOSE (no output until the first window
    # closes, ~20 min), so it is NOT part of the gate and is handled best-effort below. On
    # timeout we warn and proceed rather than raise -- this dashboard-views asset must never
    # hard-fail the pipeline run.
    context.log.info("Waiting for the turnover Iceberg table to be queryable via Trino...")
    for attempt in range(60):
        try:
            trino_exec('SELECT COUNT(*) FROM datalake.public."rw_managed_turnover_percentage$snapshots"')
            context.log.info(f"Iceberg output queryable after ~{attempt * 5}s")
            break
        except Exception as e:
            if attempt == 59:
                context.log.warning(
                    f"Turnover Iceberg table not queryable after 300s: {e}. "
                    "Proceeding best-effort -- views over not-yet-committed tables are skipped."
                )
            else:
                time.sleep(5)

    # Best-effort view creation: a view over an Iceberg table that hasn't committed yet
    # (e.g. a TUMBLE/EOWC casino sink before its first window closes) will fail here. Warn
    # and continue so the asset succeeds; re-materialize it later to backfill skipped views.
    created, skipped = [], []
    for view_name, select_sql in TRINO_VIEWS:
        ddl = f"CREATE OR REPLACE VIEW datalake.public.{view_name} AS {select_sql}"
        try:
            context.log.info(f"Creating view {view_name}")
            trino_exec(ddl)
            created.append(view_name)
        except Exception as e:
            context.log.warning(f"Skipping view {view_name} (underlying table not ready?): {e}")
            skipped.append(view_name)

    return {
        "created_views": MetadataValue.json(created),
        "skipped_views": MetadataValue.json(skipped),
    }
