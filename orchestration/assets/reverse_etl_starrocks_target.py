"""The demo's StarRocks target: a consumer of the sync's Kafka topic that keeps the source's
current rows in a Primary Key table, fed by a Routine Load (docs/poc/STARROCKS_SERVING_LAYER.md
§12 — the live-tested recipe this asset encodes).

The wire contract is the producer's Debezium Avro envelope, so the ingestion is a mapping
problem, and three lines do the work:

- the primary key comes from `$.after.<key>` for c/u events and falls back to
  `$.before.<key>` for deletes (a Debezium delete event's `after` is null — mapping the
  key from the after-image alone rejects the row before `__op` can apply);
- `__op = if(op = 'd', 1, 0)` converts the Debezium op to StarRocks's delete marker;
- Debezium ZonedTimestamp columns arrive as ISO-8601 strings (Avro has no zone-aware
  timestamp logical type) and are cast to DATETIME with str_to_date — microseconds
  preserved, the source's `Z` offset fixed to UTC.

The asset rebuilds rather than reconciles: the reset job cannot see StarRocks (it drops and
recreates the Databricks source, the topic and the RisingWave/Postgres targets), so a target it
never touches would keep rows the source no longer has — a delete for them was consumed from the
old topic and will never be re-emitted. Every materialization therefore drops the Routine Load
and the table, recreates both, and starts the new load from the topic's beginning: replaying the
topic through the PK table converges on the topic's full state, the same semantics as the
RisingWave target (recreated with earliest-offset on every setup). The seed runs last in the
setup job, so a fresh setup repopulates the table from the seed sync like every other target.
"""

import json
import os
import time

import pymysql
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from kaizen_dagster_reverse_etl.core.delta_reader import get_row_fields
from kaizen_dagster_reverse_etl.core.wire import DATE, ZONED_TIMESTAMP, RowField

from . import reverse_etl_connections as connections
from .reverse_etl_config import StarrocksTargetConfig

# Kafka Connect schema type -> StarRocks type. ZONED_TIMESTAMP lands as DATETIME via the
# str_to_date cast in the COLUMNS mapping below (the VARCHAR-on-the-wire contract keeps the
# raw offset only if a target prefers it — the serving layer wants a typed column).
_STARROCKS_TYPE_BY_CONNECT_TYPE = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "string": "VARCHAR(65533)",
    ZONED_TIMESTAMP: "DATETIME",
    DATE: "DATE",
}

# Debezium ZonedTimestamp: ISO-8601 with microseconds and a trailing zone designator
# ('2026-11-08T15:48:23.247543Z'). StarRocks's implicit cast of that shape is unreliable;
# str_to_date with an explicit format is what the live test validated.
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%i:%s.%fZ"

_TIMEOUT_SECONDS = 60


def _connect():
    """MySQL-wire connection to StarRocks (the shared env-fed helper; the schema-change
    propagation in the sync's hooks uses the same connection)."""
    return connections.starrocks().connect()


def _table_ddl(cfg: StarrocksTargetConfig, row_fields: list[RowField]) -> str:
    columns = ",\n  ".join(
        f"`{name}` {_STARROCKS_TYPE_BY_CONNECT_TYPE[connect_type]}"
        + (" NOT NULL" if name == cfg.key_column else "")
        for name, connect_type, _ in row_fields
    )
    return (
        f"CREATE TABLE `{cfg.starrocks_database}`.`{cfg.starrocks_table}` (\n"
        f"  {columns}\n"
        f")\n"
        f"PRIMARY KEY (`{cfg.key_column}`)\n"
        f"DISTRIBUTED BY HASH(`{cfg.key_column}`)\n"
        f'PROPERTIES ("replication_num" = "1")'
    )


def _routine_load_ddl(cfg: StarrocksTargetConfig, row_fields: list[RowField], partitions: str = "", offsets: str = "") -> str:
    """CREATE ROUTINE LOAD for the Debezium Avro envelope, built from the source's row fields.

    jsonpaths address the envelope's nesting; COLUMNS names each path's value and derives the
    two StarRocks specifics: the key's before-image fallback and the __op delete marker. A
    ZonedTimestamp column reads into a _raw temp column and casts to DATETIME. The key and the
    timestamps read into temp names (never the plain column names) so the derivations below
    can assign the real columns.

    `partitions` (with `offsets`, defaulting to each partition's beginning) pins the start
    offsets in the CREATE itself — the replay path. The alternative, an ALTER after creation,
    is the worse shape on SR 4.1.7 twice over: it is only accepted on PAUSED jobs, and the
    pause/alter/resume churn can orphan the just-queued task (the scheduler drops tasks no
    longer in the job's list and never re-queues them)."""
    key = cfg.key_column
    paths = []
    columns = []
    temps: list[tuple[str, str]] = []  # (temp column, connect type) that need a derivation
    for name, connect_type, _ in row_fields:
        paths.append(f"$.after.{name}")
        if connect_type == ZONED_TIMESTAMP:
            columns.append(f"`{name}_raw`")
            temps.append((name, connect_type))
        elif name == key:
            columns.append(f"`{key}_a`")
        else:
            columns.append(f"`{name}`")
    paths.append("$.op")
    columns.append("`op`")
    paths.append(f"$.before.{key}")
    columns.append(f"`{key}_b`")
    # The derivations: key fallback first (c/u carry the after-image, deletes only the
    # before-image), then the timestamp casts, then the delete marker.
    derived = [f"`{key}` = if(`{key}_a` IS NULL, `{key}_b`, `{key}_a`)"]
    for name, connect_type in temps:
        derived.append(f"`{name}` = str_to_date(`{name}_raw`, '{_TIMESTAMP_FORMAT}')")
    derived.append("`__op` = if(`op` = 'd', 1, 0)")

    offsets = (
        f'  "kafka_partitions" = \'{partitions}\',\n'
        f'  "kafka_offsets" = \'{offsets or ",".join("0" for _ in partitions.split(","))}\',\n'
        if partitions
        else ""
    )
    return (
        f"CREATE ROUTINE LOAD `{cfg.starrocks_database}`.`{cfg.routine_load_name}` "
        f"ON `{cfg.starrocks_table}`\n"
        f"COLUMNS ({', '.join(columns + derived)})\n"
        f'PROPERTIES ("format" = "avro", "jsonpaths" = \'{json.dumps(paths)}\')\n'
        f"FROM KAFKA (\n"
        f'  "kafka_broker_list" = \'{os.environ["KAFKA_OUTPUT_BOOTSTRAP"]}\',\n'
        f'  "kafka_topic" = \'{cfg.kafka_topic}\',\n'
        f'  "confluent.schema.registry.url" = \'{cfg.schema_registry_url}\',\n'
        f"{offsets}"
        f'  "property.security.protocol" = \'SASL_SSL\',\n'
        f'  "property.sasl.mechanism" = \'{os.environ.get("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512")}\',\n'
        f'  "property.sasl.username" = \'{os.environ["KAFKA_OUTPUT_SASL_USERNAME"]}\',\n'
        f'  "property.sasl.password" = \'{os.environ["KAFKA_OUTPUT_SASL_PASSWORD"]}\'\n'
        f")"
    )


def _routine_load_state(cur, cfg: StarrocksTargetConfig) -> dict:
    try:
        cur.execute(f"SHOW ROUTINE LOAD FOR `{cfg.starrocks_database}`.`{cfg.routine_load_name}`")
    except pymysql.err.ProgrammingError as err:
        if 1064 in err.args[:2] and "no running job named" in str(err):
            return {}  # absent job, not a query failure -- the rebuild then creates it
        raise
    rows = cur.fetchall()
    if not rows:
        return {}
    names = [d[0] for d in cur.description]
    return dict(zip(names, rows[0]))


def _stop_and_drop(cur, cfg: StarrocksTargetConfig) -> None:
    """STOP + DROP the Routine Load, tolerating the cosmetic errors SR 4.1.7 raises for jobs
    already stopped or absent (the drop takes effect regardless), then confirm it is gone."""
    for statement in (
        f"STOP ROUTINE LOAD FOR `{cfg.starrocks_database}`.`{cfg.routine_load_name}`",
        f"DROP ROUTINE LOAD FOR `{cfg.starrocks_database}`.`{cfg.routine_load_name}`",
    ):
        try:
            cur.execute(statement)
        except pymysql.err.ProgrammingError:
            pass
        except pymysql.err.OperationalError as err:
            # SR 4.1.7 reports an absent job on STOP/DROP as 5089 OperationalError
            # ("Routine load job [...] not found when checking privilege") — cosmetic
            # like the 1064s above: the drop takes effect either way. Any other
            # OperationalError (connectivity, etc.) is real.
            if 5089 not in err.args[:2]:
                raise
    if _routine_load_state(cur, cfg):
        raise RuntimeError(f"{cfg.routine_load_name}: STOP/DROP ran but the job is still visible")


def _partitions(job: dict) -> str:
    """The comma-separated partition list from the job's own view of the topic."""
    props = json.loads(job.get("DataSourceProperties") or "{}")
    return props.get("currentKafkaPartitions", "")


def _table_columns(cur, cfg: StarrocksTargetConfig) -> set:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (cfg.starrocks_database, cfg.starrocks_table),
    )
    return {row[0] for row in cur.fetchall()}


def _recreate_load(cur, cfg: StarrocksTargetConfig, row_fields: list[RowField], at: str) -> None:
    """Drop and recreate the Routine Load under `row_fields`' mapping (any table-column
    change invalidates it — the COLUMNS mapping names the table's columns). `at` pins the
    fresh job's start offsets: "progress" resumes where the old job got to (schema changes
    that must not replay — a rename's preserved values would be clobbered by pre-rename
    events, whose after-images carry the old name), "beginning" replays the topic (an added
    column's already-consumed events land; their old companions map the new jsonpath to
    NULL, which can only overwrite the column's starting NULLs). A job that never scheduled
    (empty topic) has no progress to pin and the fresh one starts at the topic's end — on
    the empty-topic reset->setup shape that is the beginning anyway."""
    job = _routine_load_state(cur, cfg)
    partitions = _partitions(job)
    offsets = ""
    if partitions:
        if at == "progress":
            progress = json.loads(job.get("Progress") or "{}")
            raw = [str(progress.get(partition, "0")) for partition in partitions.split(",")]
            offsets = ",".join(value if value.isdigit() else "0" for value in raw)
        else:  # beginning
            offsets = ",".join("0" for _ in partitions.split(","))
    _stop_and_drop(cur, cfg)
    cur.execute(_routine_load_ddl(cfg, row_fields, partitions, offsets))


def add_missing_columns_to_starrocks(
    starrocks, cfg: StarrocksTargetConfig, row_fields: list[RowField]
) -> list[str]:
    """Add the source's new columns to the StarRocks table ahead of the messages — the SR
    branch of before_produce, mirroring add_missing_columns for RisingWave. Each new column
    is `ALTER TABLE ... ADD COLUMN` (nullable, its mapped type), and when any was added the
    Routine Load is rebuilt under the new mapping replaying from the topic's beginning, so
    events consumed before the load knew the field land after all. Returns the added
    names."""
    conn = starrocks.connect()
    added: list[str] = []
    try:
        with conn.cursor() as cur:
            existing = _table_columns(cur, cfg)
            if not existing:
                return added
            for name, connect_type, _ in row_fields:
                if name in existing:
                    continue
                cur.execute(
                    f"ALTER TABLE `{cfg.starrocks_database}`.`{cfg.starrocks_table}` "
                    f"ADD COLUMN `{name}` {_STARROCKS_TYPE_BY_CONNECT_TYPE[connect_type]}"
                )
                added.append(name)
            if added:
                _recreate_load(cur, cfg, row_fields, at="beginning")
    finally:
        conn.close()
    return added


def propagate_schema_changes_to_starrocks(
    starrocks,
    cfg: StarrocksTargetConfig,
    renames: list[tuple[str, str]],
    drops: list[str],
    row_fields: list[RowField],
) -> dict:
    """Apply the source's column renames and drops to the StarRocks target — the SR branch of
    the demo hooks' propagation (reverse_etl_demo_hooks), which RisingWave and Postgres get
    inside every sync run.

    A rename is SR's native `ALTER TABLE ... RENAME COLUMN old TO new` (verified live on
    4.1.7, data preserved) — the same semantics as the other targets. A drop is `ALTER TABLE
    ... DROP COLUMN` (the key column is skipped: it anchors the table model). Either change
    invalidates the Routine Load — its COLUMNS mapping names the table's columns — so the
    load is recreated once at the end, pinned to the old job's captured progress
    (kafka_partitions/kafka_offsets) so nothing replays: replaying pre-change events would
    overwrite the renamed column's preserved values with NULLs, because their after-images
    carry the old name. A job that never scheduled (empty topic) has no progress to pin and
    the fresh one starts at the topic's end — on the empty-topic reset->setup shape that is
    the beginning anyway.

    Idempotent per column like the Postgres helper: a rename already applied (old gone, new
    present) is reported but changes nothing — an already-rebuilt load must not be rebuilt —
    and both present is treated as a half-applied earlier run: the old column's values
    backfill only the new column's NULLs before the old column drops. The load is rebuilt
    only when a DDL actually ran. Returns {"renamed": ["old -> new", ...], "dropped": [...]}."""
    conn = starrocks.connect()
    applied: dict = {"renamed": [], "dropped": []}
    changed = False
    try:
        with conn.cursor() as cur:
            existing = _table_columns(cur, cfg)
            if not existing:
                return applied
            for old, new in renames:
                if old not in existing:
                    if new in existing:
                        applied["renamed"].append(f"{old} -> {new}")  # already applied
                    continue
                if new in existing:
                    cur.execute(
                        f"UPDATE `{cfg.starrocks_database}`.`{cfg.starrocks_table}` "
                        f"SET `{new}` = COALESCE(`{new}`, `{old}`)"
                    )
                    cur.execute(
                        f"ALTER TABLE `{cfg.starrocks_database}`.`{cfg.starrocks_table}` DROP COLUMN `{old}`"
                    )
                else:
                    cur.execute(
                        f"ALTER TABLE `{cfg.starrocks_database}`.`{cfg.starrocks_table}` "
                        f"RENAME COLUMN `{old}` TO `{new}`"
                    )
                existing.discard(old)
                existing.add(new)
                applied["renamed"].append(f"{old} -> {new}")
                changed = True
            for name in drops:
                if name not in existing or name == cfg.key_column:
                    continue
                cur.execute(
                    f"ALTER TABLE `{cfg.starrocks_database}`.`{cfg.starrocks_table}` DROP COLUMN `{name}`"
                )
                existing.discard(name)
                applied["dropped"].append(name)
                changed = True

            if changed:
                # Rebuild the load under the new mapping, pinned where the old job got to.
                _recreate_load(cur, cfg, row_fields, at="progress")
    finally:
        conn.close()
    return applied


def _wait_running(cur, cfg: StarrocksTargetConfig) -> str:
    deadline = time.monotonic() + _TIMEOUT_SECONDS
    state = ""
    while time.monotonic() < deadline:
        state = (_routine_load_state(cur, cfg).get("State") or "").upper()
        if state in ("RUNNING", "NEED_SCHEDULE"):
            return state
        time.sleep(2)
    return state


def build_starrocks_target_asset(cfg: StarrocksTargetConfig):
    """The StarRocks target asset: rebuild the PK table and its Routine Load on every run, the
    way the RisingWave and Postgres targets are recreated by each setup. `source_fqn` is the
    same one composition input the RisingWave builder takes (the CREATE reads the source's
    columns and types once). The rebuild is why the reset cannot orphan rows here: the reset
    cannot see StarRocks, so a kept table would hold rows whose deletes were consumed from the
    old topic and never re-emitted. The new load starts from the topic's beginning, so replay
    through the PK table converges on the topic's full state."""

    @asset(
        name=cfg.starrocks_asset,
        group_name=cfg.group_name,
        deps=[AssetKey(dep) for dep in cfg.deps],
        description=(
            f"Rebuild the StarRocks Primary Key table {cfg.starrocks_database}."
            f"{cfg.starrocks_table} and its Routine Load on {cfg.kafka_topic} (Debezium Avro, "
            f"upserts by {cfg.key_column}, deletes via __op + before-image fallback): drop both, "
            "recreate, and start the load from the topic's beginning so the table converges on "
            "the topic's full state. See docs/poc/STARROCKS_SERVING_LAYER.md §12."
        ),
    )
    def starrocks_target(context: AssetExecutionContext):
        if cfg.encoding != "avro":
            raise ValueError(f"the starrocks target supports encoding='avro' only, got {cfg.encoding!r}")

        databricks = connections.databricks()
        token = databricks.token()
        row_fields = get_row_fields(databricks.table_location_cached(token, cfg, cfg.source_fqn))

        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS `{cfg.starrocks_database}`")
                # The old job knows the topic's partition list — capture it before the drop so
                # the fresh CREATE can pin its start offsets to each partition's beginning.
                partitions = _partitions(_routine_load_state(cur, cfg))
                _stop_and_drop(cur, cfg)
                cur.execute(f"DROP TABLE IF EXISTS `{cfg.starrocks_database}`.`{cfg.starrocks_table}`")
                context.log.info(f"rebuilding {cfg.starrocks_database}.{cfg.starrocks_table}")
                cur.execute(_table_ddl(cfg, row_fields))
                cur.execute(_routine_load_ddl(cfg, row_fields, partitions))
                context.log.info(
                    f"created Routine Load {cfg.routine_load_name}"
                    + (f" from the beginning of partitions {partitions}" if partitions else " (no prior partitions known — starts at the topic's end)")
                )

                final_state = _wait_running(cur, cfg)
                cur.execute(
                    f"SELECT COUNT(*) FROM `{cfg.starrocks_database}`.`{cfg.starrocks_table}`"
                )
                count = cur.fetchone()[0]
        finally:
            conn.close()

        if final_state not in ("RUNNING", "NEED_SCHEDULE"):
            raise RuntimeError(f"{cfg.routine_load_name} did not reach RUNNING within {_TIMEOUT_SECONDS}s (state: {final_state})")

        context.add_output_metadata(
            {
                "table": MetadataValue.text(f"{cfg.starrocks_database}.{cfg.starrocks_table}"),
                "routine_load": MetadataValue.text(cfg.routine_load_name),
                "routine_load_state": MetadataValue.text(final_state),
                "row_count_at_setup_time": MetadataValue.int(count),
            }
        )

    return starrocks_target
