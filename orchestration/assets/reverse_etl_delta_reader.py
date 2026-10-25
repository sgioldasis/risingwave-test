"""delta-rs backend for the reverse-ETL syncs: the change feed reconstructed from
snapshot diffs alone -- no CDF read, no Spark, no warehouse scans.

Replaces both former backends -- the Databricks SQL path (table_changes(), DBUs)
and the OSS Spark path (a local JVM reading the change feed) -- with version-range
reconstruction on delta-rs alone:

    for each commit version v in the requested range:
        diff the snapshot at v-1 against the snapshot at v
        -> insert / delete / update_preimage + update_postimage rows

which is exactly what Spark's readChangeFeed does internally for tables without
the CDF table feature -- it reconstructs changes by diffing versions too. The
reconstruction needs no CDF read at all (delta-rs grew a CdfLoadBuilder later,
but per-version snapshot diffing is what makes the file-pair fast path
possible): no warehouse, and no JVM -- per commit version, either just the new
files' rows
(append-only commits) or two column-pruned snapshot scans and a dict diff
(rewrites).
Per-version diffing is required for correctness: a key deleted in one
commit and reinserted in the next must emit both events, which an endpoint diff
over a range would net out to nothing.

Row contract (identical to what table_changes()/readChangeFeed produced, so
_to_debezium_events() and everything downstream is unchanged):
- one dict per CDF row; payload values typed (int/bool/str), every TIMESTAMP
  column rendered as a microsecond UTC string (_MICROSECOND_TIMESTAMP_FORMAT);
- `_change_type` in {insert, update_preimage, update_postimage, delete};
- `_commit_version` int, `_commit_timestamp` formatted the same way;
- rows ordered by `_commit_version` (required: consumers apply events for the
  same key oldest-to-newest); within one version order is irrelevant --
  _to_debezium_events() groups preimages/postimages by key and pairs them
  positionally.

Writes stay out of this module by design: the watermark state lives in Postgres
(reverse_etl_sync_state -- private bookkeeping nothing in Databricks reads), so a
sync has ZERO Databricks warehouse touches: reads (the expensive part) are
delta-rs and free, and the watermark advances through one small Postgres INSERT
(_write_state() in reverse_etl_cdf_setup). The state used to be a Delta table
next to the source -- delta-rs could read but not commit to the warehouse-created
table (this runtime stamps rowTracking and domainMetadata writer features
delta-rs 1.6 refuses), which is why the write was SQL and metered; the Postgres
move deleted that last cost along with the table's protocol constraints.

Memory profile, two paths: an **append-only commit reads only its new files**
(constant memory per version -- the 1M-row bulk load syncs at ~100k rows of
working set per chunk), while a **rewritten-files commit diffs two full
cumulative snapshots** (materialized lazily, then carried forward). For a table
whose history is append-heavy -- the common production shape -- memory stays at
the commit size; a table that rewrites everything in every commit is the honest
limit of snapshot reconstruction, and REVERSE_ETL_CDF_CHUNK_VERSIONS bounds how
many versions accumulate before they stream to Kafka.
"""

import datetime
import io
import json
import re
from typing import Any

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from deltalake import DeltaTable

from .reverse_etl_cdf_setup import (
    _azure_storage_options,
    CHANGE_TYPE_COLUMN,
    COMMIT_TIMESTAMP_COLUMN,
    COMMIT_VERSION_COLUMN,
    ReverseEtlSyncConfig,
    DATE,
    RowField,
    ZONED_TIMESTAMP,
)

# Delta/Spark schema types -> Kafka Connect types, same semantics as the SQL
# path's _CONNECT_TYPE_BY_DATABRICKS_TYPE: anything not listed travels as a
# plain string. Delta's type names match Spark's typeName() for the common
# primitives ('long', 'string', 'timestamp', ...), which is why the same map
# served the Spark backend unchanged.
_CONNECT_TYPE_BY_DELTA_TYPE = {
    "byte": "int8",
    "short": "int16",
    "integer": "int32",
    "long": "int64",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "string": "string",
    "timestamp": ZONED_TIMESTAMP,
    "date": DATE,
}

_CDF_METADATA_COLUMNS = (CHANGE_TYPE_COLUMN, COMMIT_VERSION_COLUMN, COMMIT_TIMESTAMP_COLUMN)

# The SQL path rendered timestamps with the Java pattern
# ("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'" -> "2026-10-18T17:26:36.779212Z"); the
# strftime equivalent for Python datetimes out of delta-rs (always UTC instants,
# hence the literal Z).
_PY_MICROSECOND_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _format_timestamp(value: Any) -> str:
    """One TIMESTAMP value -> the SQL path's exact microsecond UTC string."""
    if not isinstance(value, datetime.datetime):
        return str(value)
    if value.tzinfo is not None:
        value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return value.strftime(_PY_MICROSECOND_TIMESTAMP_FORMAT) + "Z"


def _format_commit_timestamp(timestamp_ms: int) -> str:
    """A commit's timestamp (epoch milliseconds, from the delta log) in the same
    microsecond format -- CDF's _commit_timestamp is the commit's own timestamp,
    and the SQL path rendered it with the same date_format as payload columns."""
    moment = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)
    return moment.strftime(_PY_MICROSECOND_TIMESTAMP_FORMAT) + "Z"


def _open(location: str) -> DeltaTable:
    return DeltaTable(location, storage_options=_azure_storage_options(location))


def get_row_fields(location: str, version: int | None = None) -> list[RowField]:
    """The source table's columns via delta-rs -- (name, connect type, nullable)
    per column, the same contract the information_schema read served.

    `version=None` reads the current snapshot; a version pins the read to that
    commit -- the sync pins it to the same version as the data it read, because
    a commit landing between the data read and the schema read would name the
    envelope's columns with the newer schema and silently null out every value
    under a renamed column (_coerce_row projects by row.get(name)).

    Anything compound (struct/map/array) travels as a plain string, exactly as
    the Spark variant mapped StructType."""
    delta = _version_handle(location, version) if version is not None else _open(location)
    fields = delta.schema().fields
    row_fields: list[RowField] = []
    for field in fields:
        type_name = getattr(field.type, "type", None)  # PrimitiveType carries the Delta name; compound types don't
        row_fields.append((field.name, _CONNECT_TYPE_BY_DELTA_TYPE.get(type_name, "string"), field.nullable))
    if not row_fields:
        raise RuntimeError(f"No columns found in the Delta schema at {location}")
    return row_fields


def read_state(cfg: ReverseEtlSyncConfig, state_location: str) -> tuple[int | None, str | None]:
    """(watermark, last_schema) for this sync's state row -- the delta-rs read
    precheck_versions() does for the no-op case, as the full path's fallback when
    the pre-check thread failed. The table can carry duplicate rows for one
    sync_name; the freshest row wins, not an arbitrary scan order."""
    state = _open(state_location).to_pyarrow_table()
    rows = [row for row in state.to_pylist() if row.get("sync_name") == cfg.sync_name]
    if not rows:
        return None, None
    freshest = max(rows, key=lambda r: int(r["last_commit_version"] or 0))
    version = int(freshest["last_commit_version"]) if freshest["last_commit_version"] is not None else None
    return version, freshest.get("last_schema")


def get_current_version(location: str) -> int:
    """The table's latest committed version (DESCRIBE HISTORY LIMIT 1 equivalent)."""
    return _open(location).version()


_VERSION_LOAD_FAILURE = re.compile(
    "status.*404|does not exist|not found|no valid|Cannot find|cannot load|earliest|truncated|expired|No files in log segment",
    re.IGNORECASE,
)


def get_earliest_available_version(location: str) -> int:
    """Earliest version still loadable from the table's transaction log (full
    DESCRIBE HISTORY equivalent), by bisection on load_as_version().

    What "loadable" means here is exactly what this reader needs: a version the
    reconstruction can read as a snapshot, whether through commit files or a
    checkpoint. 0 means the log's full history is intact and a first-run backfill
    can reconstruct everything; anything greater means history has aged out and
    the sync baselines instead of backfilling a silently incomplete picture."""
    delta = _open(location)
    latest = delta.version()
    if latest <= 0:
        return max(latest, 0)
    lo, hi = 0, latest
    while lo < hi:
        mid = (lo + hi) // 2
        try:
            delta.load_as_version(mid)
        except Exception as e:  # noqa: BLE001 -- any load failure means "not available"; bisect on
            if not _VERSION_LOAD_FAILURE.search(str(e)):
                raise
            lo = mid + 1
        else:
            hi = mid
    delta.load_as_version(latest)  # leave the handle on the current snapshot
    return lo


def _physical_rename_map(delta: DeltaTable) -> dict[str, str]:
    """{physical name: logical name} per column, from the schema's
    delta.columnMapping.physicalName metadata. Empty when the table has no
    column mapping (physical names are the logical names)."""
    return {
        field.metadata["delta.columnMapping.physicalName"]: field.name
        for field in delta.schema().fields
        if field.metadata and "delta.columnMapping.physicalName" in field.metadata
    }


def _version_handle(location: str, version: int) -> DeltaTable:
    """A DeltaTable pinned to exactly this version -- a fresh handle per read.

    Constructing at the version keeps schema, add actions and data consistent
    by construction: everything the versioned reads derive from the handle
    (logical column order, physical rename map, file set, snapshot) comes from
    one snapshot, with no dependence on how a reused handle refreshes its
    cached state across load_as_version() jumps.

    Note the one delta-rs quirk this does NOT fix (see read_changes): history()
    on a version-pinned handle returns the full log with version fields offset
    by the pin -- per-version commit metadata must come from a current handle's
    history(), keyed by each entry's version field."""
    return DeltaTable(location, storage_options=_azure_storage_options(location), version=version)


def _format_rows(rows: list[dict[str, Any]], timestamp_columns: list[str], date_columns: list[str] = ()) -> None:
    """Timestamps become the contract's microsecond strings IN PLACE, before any
    comparison or envelope building, so a datetime-vs-formatted-string mismatch
    can never read as a change. Idempotent (formatted values are not datetimes).
    DATE columns become their ISO strings the same way -- the SQL path's rows
    carried dates as strings (the warehouse JSON), and a datetime.date would
    neither diff cleanly nor serialize."""
    for row in rows:
        for column in timestamp_columns:
            if row.get(column) is not None:
                row[column] = _format_timestamp(row[column])
        for column in date_columns:
            value = row.get(column)
            if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                row[column] = value.isoformat()


def _read_files_as_rows(
    delta: DeltaTable,
    azure: pafs.FileSystem,
    container: str,
    uris: list[str],
    timestamp_columns: list[str],
    masks: dict[str, list[bool]] | None = None,
    partition_values: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Rows of specific parquet data files, with the columnMapping physical->logical
    rename applied. delta-rs 1.6 does not apply the rename for warehouse-written
    column-mapped tables (verified live): to_pyarrow_table() returns the logical
    schema with every value None. Columns the files carry beyond the logical
    schema (CDF's _change_type/__is_cdc sidecars on files written by an UPDATE)
    are dropped. Timestamp formatting is the caller's job (_format_rows).

    `masks` optionally maps a file URI to its deletion-vector keep-mask (one bool
    per row, True = live, padded to the file's row count -- delta-rs's
    deletion_vectors() contract). The mask is applied after the read, so
    DV-deleted rows never reach the diff; files without a DV are absent from
    the mapping and read unfiltered.

    `partition_values` optionally maps a file URI to {partition column: typed
    value} -- partition columns live in the file PATHS, not in the files, so
    raw reads come back without them and the caller injects them (see
    _partition_values)."""
    rename = _physical_rename_map(delta)
    logical_order = [field.name for field in delta.schema().fields]
    tables: list[pa.Table] = []
    for uri in uris:
        path = uri.split(".dfs.core.windows.net/", 1)[1]
        with azure.open_input_stream(f"{container}/{path}") as handle:
            file_table = pq.read_table(io.BytesIO(handle.readall()))
        if rename:
            file_table = file_table.rename_columns([rename.get(name, name) for name in file_table.column_names])
        file_table = file_table.select([name for name in logical_order if name in file_table.column_names])
        if partition_values:
            values = partition_values.get(uri)
            if values:
                for name, value in values.items():
                    if name not in file_table.column_names:
                        file_table = file_table.append_column(
                            name, pa.array([value] * file_table.num_rows)
                        )
        if masks:
            mask = masks.get(uri)
            if mask is not None:
                if len(mask) != file_table.num_rows:
                    raise RuntimeError(
                        f"deletion vector mask for {uri} has {len(mask)} entries but the file has "
                        f"{file_table.num_rows} rows -- the mask and the file are out of sync"
                    )
                file_table = file_table.filter(pa.array(mask))
        tables.append(file_table)
    # Files written by different operations record different column nullability
    # (e.g. the CDF sidecar columns on an UPDATE's rewrite); promote merges them.
    table = pa.concat_tables(tables, promote_options="default") if tables else None
    return table.to_pylist() if table is not None else []


def _snapshot_rows(
    delta: DeltaTable,
    timestamp_columns: list[str],
    key_columns: list[str],
    azure: pafs.FileSystem | None = None,
    container: str = "",
    date_columns: list[str] = (),
) -> dict[tuple, dict[str, Any]]:
    """One version's snapshot as {key tuple: payload row}, timestamps formatted.

    `delta` must be a handle pinned to that version (see _version_handle) --
    schema, add actions and data all read from the same snapshot. Timestamps
    become the contract's microsecond strings BEFORE any comparison, so a
    datetime-vs-formatted-string mismatch can never read as a change."""
    rows = delta.to_pyarrow_table().to_pylist()
    if rows and all(value is None for row in rows for value in row.values()):
        if azure is None:
            raise RuntimeError(
                "delta-rs returned an all-None snapshot and no Azure filesystem is available to re-read the files"
            )
        rows = _read_files_as_rows(delta, azure, container, delta.file_uris(), timestamp_columns)
    _format_rows(rows, timestamp_columns, date_columns)
    return {tuple(row[name] for name in key_columns): row for row in rows}


def _changed_payload_columns(old_row: dict[str, Any], new_row: dict[str, Any], skip: set[str]) -> bool:
    """True when the two rows differ on any non-key column. A column absent from
    one side (added or dropped between the versions) reads as None, so schema
    drift alone never produces an event -- drop propagation handles schema."""
    return any(old_row.get(name) != new_row.get(name) for name in (set(old_row) | set(new_row)) - skip)


def _file_paths(delta: DeltaTable) -> set[str]:
    """The snapshot's data-file paths (relative to the table root) from the add
    actions -- a log-only read, no data."""
    return {
        action["path"]
        for action in pa.table(delta.get_add_actions(flatten=True)).to_pylist()
        if action.get("path")
    }


def _commit_actions(azure: pafs.FileSystem, log_dir: str, version: int) -> tuple[list[dict], list[dict]]:
    """(add actions, remove actions) of one commit, read straight from the
    commit's JSON log entry.

    Path-set comparison cannot see a same-path remove+add pair -- a
    deletion-vector update re-adds the SAME data file with a new DV, which set
    diffing reports as no change at all -- so DV-aware reads take each commit's
    real actions from the log. Every add carries its `deletionVector` reference
    when the file has one; the payloads themselves are decoded by delta-rs
    (_dv_masks), not here."""
    with azure.open_input_stream(f"{log_dir}/{version:020d}.json") as handle:
        content = handle.readall().decode()
    adds, removes = [], []
    for line in content.splitlines():
        obj = json.loads(line)
        if "add" in obj:
            adds.append(obj["add"])
        elif "remove" in obj:
            removes.append(obj["remove"])
    return adds, removes


def _dv_masks(delta: DeltaTable) -> dict[str, list[bool]]:
    """{file URI: keep-mask} for every file carrying a deletion vector in this
    handle's snapshot -- delta-rs's deletion_vectors() decodes the payloads
    (inline or log-referenced) and pads each mask to the file's row count
    (True = keep, verified live: a delete of the middle of three rows reads
    [True, False, True]). Files without a DV are absent. The keys are the same
    abfss URIs the raw file reads use, so callers look up by URI."""
    try:
        reader = delta.deletion_vectors()
    except AttributeError:
        raise RuntimeError(
            "the installed deltalake cannot read deletion vectors; upgrade (read support landed in 1.3)"
        )
    masks: dict[str, list[bool]] = {}
    for row in pa.table(reader.read_all()).to_pylist():
        mask = row.get("selection_vector")
        if mask is not None:
            masks[row["filepath"]] = list(mask)
    return masks


def _partition_values(delta: DeltaTable, root: str) -> dict[str, dict[str, Any]]:
    """{file URI: {partition column: typed value}} for every file in this
    handle's snapshot. Partition columns live in the file PATHS, not in the
    files, so raw file reads come back without them -- this restores them from
    the add actions. delta-rs's flattened add actions carry the values in their
    schema types (partition.<name> columns: a DATE partition arrives as date32,
    verified live), which is exactly the contract the snapshot read produces.
    A file's partition never changes, so one entry per path serves pre- and
    post-images alike."""
    out: dict[str, dict[str, Any]] = {}
    for action in pa.table(delta.get_add_actions(flatten=True)).to_pylist():
        uri = f"{root}/{action['path']}"
        values = {name[len("partition."):]: action[name] for name in action if name.startswith("partition.")}
        if values:
            out[uri] = values
    return out


def _emit_row(change_type: str, row: dict[str, Any], version: int, commit_timestamp: str) -> dict[str, Any]:
    return {
        **row,
        CHANGE_TYPE_COLUMN: change_type,
        COMMIT_VERSION_COLUMN: version,
        COMMIT_TIMESTAMP_COLUMN: commit_timestamp,
    }


def read_changes(
    cfg: ReverseEtlSyncConfig,
    location: str,
    since_version: int,
    until_version: int | None = None,
) -> list[dict[str, Any]]:
    """Reconstructed CDF rows for the inclusive version range [since_version,
    until_version], ordered by `_commit_version`.

    The caller pins `until_version` to the source's current version right before
    reading (the same rule the Spark backend followed), so the read range matches
    exactly what the watermark will advance to.

    Three read paths, chosen per commit:

    - **Append-only commit** (files added, none removed): every row of the NEW
      files is an insert -- read only those, never the cumulative snapshot. This
      is what keeps a 1M-row bulk load constant-memory per version (verified: it
      cut the 1M-row sync's wall time by reading ~1M row-versions instead of
      ~11M). Also covers version 0, whose whole content is inserts.
    - **Rewritten files** (an UPDATE/DELETE/MERGE rewrite, or compaction): diff
      only the REWRITTEN FILE PAIRS. The commit's log names exactly which files
      it removed and added; the changed rows live only in those pairs, rows in
      untouched files cannot have changed, and compaction's pairs diff to
      nothing. The removed files hold the touched rows' pre-image (they were
      still referenced one version earlier), the added files the post-image, so
      memory and I/O are sized by the touched files -- not by the table, which
      is what the previous full-snapshot diff cost (two full scans plus an
      O(all rows) key-dict carried forward).
    - **Deletion vectors** (tables with the DV writer feature): DV-aware, not
      refused. A DV-only commit -- Delta marking rows dead inside an existing
      file -- re-adds the SAME data file, which path-set comparison reports as
      no change, so on DV tables every commit's real actions come from its JSON
      log entry (_commit_actions) instead of the file-set diff. Every touched
      file's rows are filtered through its deletion-vector keep-mask
      (_dv_masks: delta-rs decodes the payloads), and the mask must be the one
      in force at the diffed version -- a file's DV evolves commit by commit,
      so pre-images read the masks of the version-1 handle (cached per
      version). DV tables require the fast path: delta-rs's own scan refuses
      DV tables entirely (to_pyarrow_table raises even on deltalake 1.6), so a
      no-Azure-filesystem DV source is refused with a clear message.
      Partitioned DV tables read raw with their partition columns injected
      from the add actions' typed partition values (_partition_values). The
      demo tables keep DVs off at creation (simpler protocol; the state table
      must stay DV-free regardless, because the no-op pre-check reads it with
      delta-rs).
    - **Fallback for partitioned tables or a missing Azure filesystem**: the
      full snapshot diff, correct at any shape but sized by the table. The
      previous snapshot is materialized lazily -- an append-only history never
      pays for it; the first rewrite scans its own v-1 once and the dict
      carries forward.

    Partitioned tables without DVs disable the fast path: partition values
    live in file paths, not in the files, so added-file reads would come back
    without them (partitioned DV tables read raw with the values injected --
    see the DV bullet)."""
    end = until_version if until_version is not None else get_current_version(location)
    if end < since_version:
        return []

    delta = _open(location)
    features = (delta.protocol().writer_features or []) + (delta.protocol().reader_features or [])
    dv_enabled = "deletionVectors" in features
    if dv_enabled and not hasattr(delta, "deletion_vectors"):
        raise RuntimeError(
            f"{cfg.source_fqn} has deletion vectors enabled but the installed deltalake cannot read them "
            "(read support landed in deltalake 1.3, keep-mask padding in 1.6); upgrade deltalake"
        )

    fields = get_row_fields(location)
    timestamp_columns = [name for name, connect_type, _ in fields if connect_type == ZONED_TIMESTAMP]
    date_columns = [
        field.name for field in delta.schema().fields if getattr(field.type, "type", None) == "date"
    ]
    key_columns = [cfg.key_column]
    skip = set(key_columns) | set(_CDF_METADATA_COLUMNS)
    container = location.split("@")[0].replace("abfss://", "").replace("az://", "")
    azure = None
    if container and "@" in location:
        options = _azure_storage_options(location)
        azure = pafs.AzureFileSystem(
            account_name=location.split("@")[1].split(".")[0],
            client_id=options["client_id"],
            tenant_id=options["tenant_id"],
            client_secret=options["client_secret"],
        )
    root = location.rstrip("/")
    partition_columns = delta.metadata().partition_columns
    fast_path = azure is not None and not partition_columns
    # DV tables take the raw-read fast path even when partitioned: the reads
    # get their partition columns injected from the add actions' typed
    # partition values (a file's partition is fixed, so one lookup per path
    # serves pre- and post-images alike). Without DVs, partitioned tables keep
    # the snapshot fallback -- delta-rs's own scan (which the fallback uses)
    # refuses DV tables, so the fallback cannot serve them.
    use_fast = fast_path or (dv_enabled and azure is not None)
    if dv_enabled and azure is None:
        raise RuntimeError(
            f"{cfg.source_fqn} has deletion vectors enabled but no Azure filesystem is available for "
            "raw file reads: delta-rs's own scan refuses DV tables, so the source cannot be read -- "
            "run where the storage credentials resolve, or read the table another way"
        )
    # The commit JSONs live under the table root's _delta_log; needed only by the
    # DV-aware fast path (which reads each commit's real actions).
    log_dir = None
    if dv_enabled and use_fast:
        root_path = root.split(".dfs.core.windows.net/", 1)[1].lstrip("/")
        log_dir = f"{container}/{root_path}/_delta_log"

    # Deletion-vector keep-masks and typed partition values per version, fetched
    # lazily and cached: the post-image of version v and the pre-image of
    # version v+1 share one read.
    dv_cache: dict[int, dict[str, list[bool]]] = {}
    pv_cache: dict[int, dict[str, dict[str, Any]]] = {}

    def dv_masks_at(v: int) -> dict[str, list[bool]]:
        if v not in dv_cache:
            dv_cache[v] = _dv_masks(_version_handle(location, v)) if v >= 0 else {}
        return dv_cache[v]

    def partition_values_at(v: int, paths: set[str]) -> dict[str, dict[str, Any]]:
        if v not in pv_cache:
            pv_cache[v] = _partition_values(_version_handle(location, v), root) if v >= 0 else {}
        all_values = pv_cache[v]
        wanted = {f"{root}/{path}" for path in paths}
        return {uri: values for uri, values in all_values.items() if uri in wanted}

    rows: list[dict[str, Any]] = []
    if since_version > 0:
        prev_files: set[str] = _file_paths(_version_handle(location, since_version - 1))
    else:
        prev_files = set()
    # Per-version commit timestamps from the log, read through a CURRENT (never
    # version-pinned) handle. delta-rs mislabels history on pinned handles
    # (verified 2026-10-19): pinned below the latest version, history() returns
    # the full log with every entry's version field offset by the pin, and
    # history(1) reports the newest commit's operation and timestamp under the
    # pinned version's label -- so events from a multi-version read would all
    # carry one wrong timestamp instead of their own commit's. On the current
    # handle history() is correct, so the map is keyed by the real version.
    commit_timestamps = {
        entry["version"]: _format_commit_timestamp(entry["timestamp"])
        for entry in _open(location).history()
    }
    # Full snapshots for the fallback diff (no Azure filesystem or partitioned
    # tables), materialized lazily: an append-only history never needs one, the
    # first rewrite scans its own v-1 once.
    prev_rows: dict[tuple, dict[str, Any]] | None = None

    for version in range(since_version, end + 1):
        delta = _version_handle(location, version)
        commit_timestamp = commit_timestamps[version]

        if dv_enabled and use_fast:
            # A DV commit can re-add the SAME data file (a deletion-vector
            # update touches the log, not the parquet), which path-set
            # comparison reports as no change -- so the actions come from the
            # commit's own log entry.
            adds, removes_actions = _commit_actions(azure, log_dir, version)
            removed = {action["path"] for action in removes_actions}
            added = sorted(action["path"] for action in adds)
            added_with_dv = any(action.get("deletionVector") for action in adds)
        else:
            cur_files = _file_paths(delta)
            removed = prev_files - cur_files
            added = sorted(cur_files - prev_files)

        if use_fast:
            # The masks are keyed by file URI; _read_files_as_rows applies the
            # entry of each file it reads, so the whole per-version dict can
            # go in as-is. The post-image uses the DVs of this version, the
            # pre-image the DVs of version-1 (a file's DV evolves commit by
            # commit). Partition values ride along for partitioned DV tables
            # -- the only case where partitioned tables read raw.
            post_masks = dv_masks_at(version) if dv_enabled and added_with_dv else None
            added_pv = partition_values_at(version, set(added)) if partition_columns and dv_enabled else None
            if not removed:
                # Append-only: the changed rows are exactly the new files' rows
                # -- memory and I/O are the commit size, never the table size.
                # A new file can carry a DV already (MERGE outputs do), so its
                # mask filters the inserts.
                new_rows = _read_files_as_rows(
                    delta, azure, container, [f"{root}/{path}" for path in added],
                    timestamp_columns, masks=post_masks, partition_values=added_pv,
                )
                _format_rows(new_rows, timestamp_columns, date_columns)
                for row in new_rows:
                    rows.append(_emit_row("insert", row, version, commit_timestamp))
                if not dv_enabled:
                    prev_files = cur_files
                continue
            # A rewrite: diff only the REWRITTEN FILE PAIRS. The commit's log
            # names exactly which files it removed and added; the changed rows
            # live only in those pairs, rows in untouched files cannot have
            # changed, and compaction's pairs diff to nothing. The removed
            # files hold the touched rows' pre-image (they were still
            # referenced one version earlier), the added files the post-image,
            # so memory and I/O are sized by the touched files -- not by the
            # table, which is what the full-snapshot diff below costs. With
            # DVs the same-path remove+add pair (a DV-only update) reads the
            # one data file twice through different masks, and the keyed diff
            # below emits exactly the DV's deletes/updates.
            pre_masks = dv_masks_at(version - 1) if dv_enabled and version > 0 else None
            removed_pv = (
                partition_values_at(version - 1, removed)
                if partition_columns and dv_enabled and removed and version > 0
                else None
            )
            pre_rows = _read_files_as_rows(
                delta, azure, container, [f"{root}/{path}" for path in sorted(removed)],
                timestamp_columns, masks=pre_masks, partition_values=removed_pv,
            )
            post_rows = _read_files_as_rows(
                delta, azure, container, [f"{root}/{path}" for path in added],
                timestamp_columns, masks=post_masks, partition_values=added_pv,
            )
            _format_rows(pre_rows, timestamp_columns, date_columns)
            _format_rows(post_rows, timestamp_columns, date_columns)
            pre = {tuple(row[name] for name in key_columns): row for row in pre_rows}
            post = {tuple(row[name] for name in key_columns): row for row in post_rows}
            for key, old_row in pre.items():
                new_row = post.get(key)
                if new_row is None:
                    # Deleted (or updated-away): the delete row carries the
                    # full preimage, which is what the Debezium envelope's
                    # before needs.
                    rows.append(_emit_row("delete", old_row, version, commit_timestamp))
                elif _changed_payload_columns(old_row, new_row, skip):
                    # CDF emits an update as preimage + postimage sharing the
                    # key and commit version; _to_debezium_events() pairs them.
                    rows.append(_emit_row("update_preimage", old_row, version, commit_timestamp))
                    rows.append(_emit_row("update_postimage", new_row, version, commit_timestamp))
            for key, new_row in post.items():
                if key not in pre:
                    rows.append(_emit_row("insert", new_row, version, commit_timestamp))
            if not dv_enabled:
                prev_files = cur_files
            continue

        # Fallback (partitioned tables, or no Azure filesystem to read files
        # with): diff the full snapshots, correct at any shape but sized by
        # the table. The first rewrite materializes the diff base (the
        # snapshot one version earlier) with one full scan. (DV tables never
        # reach this path -- refused above, since delta-rs's scan cannot read
        # them and raw reads cannot rebuild partition columns.)
        current = _snapshot_rows(
            delta, timestamp_columns, key_columns, azure, container, date_columns
        )
        if prev_rows is None:
            prev_rows = (
                _snapshot_rows(_version_handle(location, version - 1), timestamp_columns, key_columns, azure, container)
                if version > 0
                else {}
            )
        for key, old_row in prev_rows.items():
            if key not in current:
                # Deleted (or updated-away): CDF's delete row carries the full
                # preimage, which is what the Debezium envelope's before needs.
                rows.append(_emit_row("delete", old_row, version, commit_timestamp))
        for key, new_row in current.items():
            old_row = prev_rows.get(key)
            if old_row is None:
                rows.append(_emit_row("insert", new_row, version, commit_timestamp))
            elif _changed_payload_columns(old_row, new_row, skip):
                # CDF emits an update as preimage + postimage sharing the key
                # and commit version; _to_debezium_events() pairs them.
                rows.append(_emit_row("update_preimage", old_row, version, commit_timestamp))
                rows.append(_emit_row("update_postimage", new_row, version, commit_timestamp))
        prev_rows = current
        prev_files = cur_files

    return rows
