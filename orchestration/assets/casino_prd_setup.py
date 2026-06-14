"""Casino production prerequisite assets: fetch, compile, upload proto schemas,
and create Iceberg read sources after sinks have committed their first checkpoint."""
import json
import shutil
import subprocess
import time
from pathlib import Path

import boto3
import httpx
import requests
from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset

from .databricks_optimize import (
    CLIENT_ID,
    CLIENT_SECRET,
    DATABRICKS_HOST,
    TENANT_ID,
    _get_token,
    _poll,
    _submit,
)

PROTO_DIR = Path(__file__).parent.parent.parent / "proto"

# UC tables that must exist before RisingWave sinks are created by dbt.
# Each tuple: (fully-qualified table name, CREATE TABLE DDL without TBLPROPERTIES).
# TBLPROPERTIES are appended uniformly below.
_UC_TABLES = [
    (
        "de_dev.rw_poc.rw_casino_transactions",
        """CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_transactions (
    customer_id              INT           NOT NULL,
    message_type_id          INT           NOT NULL,
    account_id               INT           NOT NULL,
    currency_id              INT           NOT NULL,
    transaction_created_at   TIMESTAMP     NOT NULL,
    amount_abs               DECIMAL(20,8),
    properties               STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50')""",
    ),
    (
        "de_dev.rw_poc.rw_sportsbook_bets",
        """CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_sportsbook_bets (
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
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50')""",
    ),
    (
        "de_dev.rw_poc.rw_casino_turnover_90d",
        """CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_turnover_90d (
    customer_id          INT           NOT NULL,
    event_ts             TIMESTAMP     NOT NULL,
    rolling_7d_turnover  DECIMAL(38,8)
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50')""",
    ),
    (
        "de_dev.rw_poc.rw_casino_landing",
        """CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_landing (
    payload          BINARY,
    kafka_key        BINARY,
    kafka_timestamp  TIMESTAMP,
    kafka_partition  STRING,
    kafka_offset     STRING,
    year_month       STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50')""",
    ),
    (
        "de_dev.rw_poc.rw_sportsbook_landing",
        """CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_sportsbook_landing (
    payload          BINARY,
    kafka_key        BINARY,
    kafka_timestamp  TIMESTAMP,
    kafka_partition  STRING,
    kafka_offset     STRING,
    year_month       STRING
) USING ICEBERG
TBLPROPERTIES ('history.expire.min-snapshots-to-keep' = '50')""",
    ),
]


def _run_sql(token: str, statement: str) -> dict:
    data = _submit(token, statement)
    state = data.get("status", {}).get("state", "")
    if state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
        data = _poll(token, data["statement_id"])
    return data


@asset(
    group_name="casino_prd_setup",
    description=(
        "Ensure the three Unity Catalog Iceberg tables exist before dbt creates the "
        "RisingWave sinks that write into them. Creates each table only if absent; "
        "a no-op when all tables already exist. See sql/databricks_setup.sql for the DDL."
    ),
)
def databricks_uc_tables_setup(context: AssetExecutionContext):
    """CREATE TABLE IF NOT EXISTS for each UC table required by the RisingWave sinks."""
    missing_vars = [k for k, v in {
        "DBT_DATABRICKS_HOST":            DATABRICKS_HOST,
        "DATABRICKS_AZURE_TENANT_ID":     TENANT_ID,
        "DATABRICKS_AZURE_CLIENT_ID":     CLIENT_ID,
        "DATABRICKS_AZURE_CLIENT_SECRET": CLIENT_SECRET,
    }.items() if not v]
    if missing_vars:
        raise ValueError(f"Missing required env vars: {missing_vars}")

    token = _get_token()
    created, existed, failed = [], [], []

    for table_fqn, ddl in _UC_TABLES:
        # Probe first so we can report "created" vs "already existed" in metadata.
        probe = _run_sql(token, f"SELECT 1 FROM {table_fqn} LIMIT 0")
        if probe.get("status", {}).get("state") == "SUCCEEDED":
            context.log.info(f"✓ {table_fqn} already exists — skipping")
            existed.append(table_fqn)
            continue

        context.log.info(f"Creating {table_fqn}...")
        result = _run_sql(token, ddl)
        state = result.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            context.log.info(f"✓ Created {table_fqn}")
            created.append(table_fqn)
        else:
            error = result.get("status", {}).get("error", {}).get("message", state)
            context.log.error(f"✗ Failed to create {table_fqn}: {error}")
            failed.append(f"{table_fqn}: {error}")

    if failed:
        raise RuntimeError(f"Failed to create {len(failed)} table(s):\n" + "\n".join(failed))

    return {
        "created":       MetadataValue.json(created),
        "already_existed": MetadataValue.json(existed),
    }

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


REDPANDA_SCHEMA_REGISTRY = "http://redpanda:8081"

# Proto descriptor compiled by casino_prd_proto_compile
CASINO_PROTO_PB = PROTO_DIR / "casinoroundinfodto.pb"
CASINO_ROOT_MESSAGE = "Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto"

# Proto field type → primitive Avro type
_PROTO_SCALAR_TO_AVRO = {
    1:  "double",   # TYPE_DOUBLE
    2:  "float",    # TYPE_FLOAT
    3:  "long",     # TYPE_INT64
    4:  "long",     # TYPE_UINT64
    5:  "int",      # TYPE_INT32
    6:  "long",     # TYPE_FIXED64
    7:  "int",      # TYPE_FIXED32
    8:  "boolean",  # TYPE_BOOL
    9:  "string",   # TYPE_STRING
    12: "bytes",    # TYPE_BYTES
    13: "int",      # TYPE_UINT32
    15: "int",      # TYPE_SFIXED32
    16: "long",     # TYPE_SFIXED64
    17: "int",      # TYPE_SINT32
    18: "long",     # TYPE_SINT64
}
_PROTO_TYPE_MESSAGE = 11
_PROTO_TYPE_ENUM    = 14
_PROTO_LABEL_REPEATED = 3


def _proto_descriptor_to_avro(pb_path: Path, root_message_fqn: str) -> dict:
    """Parse a compiled FileDescriptorSet and return an Avro schema for root_message_fqn.

    All fields are represented as nullable unions (["null", <type>]) with default null.
    Named types (records) are defined only once; subsequent uses are referenced by name
    to satisfy Avro's uniqueness constraint.
    google.protobuf.Timestamp is mapped to a record {seconds: long, nanos: int} — the
    same struct shape RisingWave uses internally when decoding proto timestamps.
    """
    from google.protobuf.descriptor_pb2 import FileDescriptorSet

    with open(pb_path, "rb") as f:
        fds = FileDescriptorSet.FromString(f.read())

    # Build a flat map: fully-qualified message name → descriptor
    msg_by_fqn: dict[str, object] = {}
    for file_proto in fds.file:
        pkg = file_proto.package  # e.g. "Cronus.CasinoService.RoundInfo.Abstractions"
        for msg in file_proto.message_type:
            fqn = f"{pkg}.{msg.name}" if pkg else msg.name
            msg_by_fqn[fqn] = msg

    defined: set[str] = set()  # short names already emitted as full record defs

    def short_name(fqn: str) -> str:
        return fqn.split(".")[-1]

    def convert_message(fqn: str) -> dict | str:
        """Return a full Avro record dict, or just the short name if already defined."""
        sname = short_name(fqn)
        if sname in defined:
            return sname  # Avro back-reference
        defined.add(sname)

        msg = msg_by_fqn.get(fqn) or msg_by_fqn.get(sname)
        if msg is None:
            # Unknown type — treat as opaque string
            return {"type": "record", "name": sname, "fields": []}

        fields = []
        for field in msg.field:
            avro_base = convert_field_type(field)
            # wrap repeated → array
            if field.label == _PROTO_LABEL_REPEATED:
                avro_base = {"type": "array", "items": avro_base, "default": []}
            # all fields nullable
            avro_type = ["null", avro_base]
            fields.append({"name": field.name, "type": avro_type, "default": None})

        return {"type": "record", "name": sname, "fields": fields}

    def convert_field_type(field) -> object:
        if field.type == _PROTO_TYPE_ENUM:
            return "string"  # flatten enums to string for simplicity
        if field.type == _PROTO_TYPE_MESSAGE:
            fqn = field.type_name.lstrip(".")
            return convert_message(fqn)
        return _PROTO_SCALAR_TO_AVRO.get(field.type, "string")

    return convert_message(root_message_fqn)


@asset(
    group_name="casino_prd_setup",
    deps=["casino_prd_proto_compile"],
    description=(
        "Derive a full Avro schema from the CasinoRoundInfoDto Protobuf descriptor "
        "and register it as casino_out_avro-value in Redpanda's built-in schema registry. "
        "Idempotent — skips registration if the subject already exists."
    ),
)
def casino_avro_schema_register(context: AssetExecutionContext):
    """Convert the full CasinoRoundInfoDto proto schema to Avro and register to Redpanda.

    Uses the compiled .pb descriptor (produced by casino_prd_proto_compile) so the schema
    is always derived from the authoritative proto source, not from downstream MV columns.
    The resulting Avro schema preserves the full nested structure — GameInfo, RoundInfo,
    Messages[], Transactions[], Timestamps — mirroring what RisingWave uses internally
    when it decodes the Protobuf Kafka topic.

    RisingWave validates the schema registry subject at CREATE SINK / CREATE TABLE time
    and fails with 40401 if the subject is absent. This asset must run before the dbt
    build step that creates sink_casino_avro_redpanda and src_casino_avro.
    """
    subject = "casino_out_avro-value"

    # --- 1. Check whether schema is already registered ---
    check = requests.get(
        f"{REDPANDA_SCHEMA_REGISTRY}/subjects/{subject}/versions/latest", timeout=10
    )
    if check.status_code == 200:
        info = check.json()
        context.log.info(
            f"Schema already registered for {subject} "
            f"(id={info.get('id')}, version={info.get('version')})"
        )
        return {
            "subject":  MetadataValue.text(subject),
            "action":   MetadataValue.text("skipped — already exists"),
            "schema_id": MetadataValue.int(info.get("id", 0)),
        }

    # --- 2. Derive Avro schema from proto descriptor ---
    if not CASINO_PROTO_PB.exists():
        raise FileNotFoundError(
            f"{CASINO_PROTO_PB} not found. Run casino_prd_proto_compile first."
        )
    avro_schema = _proto_descriptor_to_avro(CASINO_PROTO_PB, CASINO_ROOT_MESSAGE)
    context.log.info(f"Derived Avro schema:\n{json.dumps(avro_schema, indent=2)}")

    # --- 3. Register schema ---
    resp = requests.post(
        f"{REDPANDA_SCHEMA_REGISTRY}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json={"schema": json.dumps(avro_schema), "schemaType": "AVRO"},
        timeout=10,
    )
    resp.raise_for_status()
    schema_id = resp.json().get("id")
    context.log.info(f"Registered Avro schema for {subject}: id={schema_id}")

    return {
        "subject":    MetadataValue.text(subject),
        "action":     MetadataValue.text("registered"),
        "schema_id":  MetadataValue.int(schema_id),
        "avro_schema": MetadataValue.json(avro_schema),
    }


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
    group_name="casino_trino",
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
