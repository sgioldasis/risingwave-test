"""Jobotron producer (docs/poc/JOBOTRON_CONTRACT_PARITY.md): the upstream publisher on the
Jobotron team's real wire contract -- run the candidate-selection query and publish one JSON
message per selected customer, the validated 13-field PascalCase envelope
(`JobId`/`JobName`/.../`ItemPayload`/`ItemPayloadHash`), keyed by `CustomerId`. The contract
was transcribed from the team's own notebooks (kafka_message_publisher.ipynb + the
CY/GR/RO unverified-customers jobs) on 2026-10-26; one Dagster instance is one query type x
one operator (D9), each with its own folder, query and topic.

The candidate query runs on one of three engines (D10, the `scan_engine` YAML knob):
"databricks_sql" (the default) submits the query verbatim to a SQL warehouse through the
Statement Execution API -- the team's own SQL on the team's own views, no transcription;
"delta_rs" and "delta_extension" keep the zero-DBU path -- delta-rs scans each `sources`
table directly (DuckDB runs the query over the registered aliases) -- and need the
bronze-table transcription because delta-rs cannot scan the sharded UNION views. The
envelope and Kafka path downstream are identical for all three.

`build_messages` (D7) is the pure envelope builder -- no Kafka, no Dagster context -- which
is what makes the contract tests possible without services. In dry-run (the default, D5)
the asset runs the whole path and sends nothing. The full eligible set is published every
run; the Jobotron worker owns deduplication (on `ItemId`) and re-validation, so the
producer stays stateless. The demo instances are read-only scans of `de.voperdb` -- the
only state this component owns is its topic and the demo consumer's mirror table.
"""

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import duckdb
import psycopg2.extras
import pyarrow as pa
import requests
from confluent_kafka import TopicPartition
from confluent_kafka.admin import NewTopic
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Config,
    Definitions,
    In,
    MetadataValue,
    Nothing,
    OpExecutionContext,
    asset,
    define_asset_job,
    in_process_executor,
    job,
    op,
)
from deltalake import DeltaTable

from .databricks_cost_meter import record as meter_record
from .databricks_cost_meter import reset as meter_reset
from .kafka_topics_setup import _DEFAULT_REPLICATION
from .reverse_etl_cdf_setup import (
    DATABRICKS_HOST,
    _azure_storage_options,
    _cost_metadata,
    _poll,
    _rows_as_dicts,
    _with_all_chunks,
)
from . import reverse_etl_connections as connections

BASE = "reverse_etl_jobotron"
SCHEMA_REGISTRY_URL = "http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7"
# Cap on message previews echoed into run metadata (same idea as the sync's MESSAGE_PREVIEW_LIMIT).
PREVIEW_LIMIT = 20
# The wire contract's field set, in transcription order (the JSON lands in this order).
ENVELOPE_FIELDS = (
    "JobId",
    "JobName",
    "JobCreated",
    "RunId",
    "CompanyId",
    "CustomerId",
    "ItemsCount",
    "ItemNumber",
    "ItemId",
    "ItemPayload",
    "ItemPayloadHash",
    "IsDryRun",
    "SchemaVersion",
)
# The two columns every query must emit: the envelope reads both from the row (D2/D4).
REQUIRED_RESULT_COLUMNS = ("CustomerId", "CompanyId")
ENGINES = ("databricks_sql", "delta_rs", "delta_extension")


@dataclass(frozen=True)
class JobotronConfig:
    """One Jobotron producer instance (D9): one query type x one operator. The demo's three
    instances live in orchestration/defs/unverified_customers_{cy,gr,ro}/defs.yaml."""

    query_type: str
    operator: str
    job_name: str
    catalog: str
    schema: str
    query: str
    dry_run: bool = True
    schema_version: int = 1
    encoding: str = "json"
    scan_engine: str = "databricks_sql"
    # Typed when the platform owns the topic; the derived default is the D9 name.
    topic: str = ""
    # alias -> (fqn, scan columns) for the zero-DBU engines: the alias is the name the
    # query uses (and the DuckDB registration); the columns prune the delta-rs scan to
    # what the query selects -- day one has no predicate pushdown, so never scan more
    # than needed. `databricks_sql` needs none (the warehouse resolves the views).
    sources: dict[str, tuple[str, tuple[str, ...]]] = field(default_factory=dict)
    schema_registry_url: str = SCHEMA_REGISTRY_URL

    @property
    def instance_name(self) -> str:
        """The D9 identity: every Dagster object derives from it (`[_<role>`)."""
        return f"jobotron_{self.query_type}_{self.operator}"

    @property
    def derived_topic(self) -> str:
        return self.topic or self.instance_name

    @property
    def group_name(self) -> str:
        return self.instance_name

    @property
    def setup_asset(self) -> str:
        return f"{self.instance_name}_setup"

    @property
    def publish_asset(self) -> str:
        return self.instance_name

    @property
    def consume_asset(self) -> str:
        return f"{self.instance_name}_consume"

    @property
    def setup_job(self) -> str:
        return f"{self.instance_name}_setup_job"

    @property
    def reset_job(self) -> str:
        return f"{self.instance_name}_reset_job"


class JobotronRunMode(Config):
    """The per-run mode switch. None (the default) means the YAML decides; a value
    overrides it for this run only -- the contract's own IsDryRun is a per-run widget, so
    the mode is flippable at launch time, logged and recorded in the metadata. The YAML
    value still governs schedules and blank launches. The launchpad meets this switch
    through _run_mode_for(cfg), which re-defaults it to the instance's YAML value so the
    scaffold shows the effective mode rather than a blank."""

    dry_run: bool | None = None


def _effective_dry_run(cfg: JobotronConfig, config: JobotronRunMode, context) -> bool:
    """The run's mode: the YAML value, or the launch's override (logged when they differ)."""
    if config.dry_run is None:
        return cfg.dry_run
    if config.dry_run != cfg.dry_run:
        context.log.info(f"Run-config override: dry_run={config.dry_run} (the YAML value is {cfg.dry_run})")
    return config.dry_run


def _run_mode_for(cfg: JobotronConfig) -> type:
    """The launchpad-facing mode switch: JobotronRunMode re-defaulted to this instance's
    YAML value. The scaffold button then shows the effective mode (false for a live-pinned
    instance, true for the safe default) instead of a blank, and launching with it lands
    exactly where a blank launch would. Typing null restores the YAML-decides escape
    hatch; the runtime semantics are untouched (_effective_dry_run still arbitrates)."""
    class RunMode(JobotronRunMode):
        dry_run: bool | None = cfg.dry_run

    return RunMode


# --- the envelope (D2-D4, D7) ---------------------------------------------------


def _canonical_json(payload: dict) -> str:
    """D3's canon: compact separators, keys in the dict's order (the query result's column
    order), nulls explicit (json.dumps never drops them), non-ASCII kept as-is."""
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _sort_key(row: dict, columns: list[str]) -> tuple:
    """D4's deterministic total order: CustomerId first, then the remaining payload columns
    in result order (identical to the notebooks' orderBy("CustomerId") whenever a customer
    has one row; stable if a query type ever returns several). Each column sorts as
    (is-null, value): nulls group together, and within one SQL column the values share a
    type, so the pair is always comparable."""
    order = ["CustomerId"] + [column for column in columns if column != "CustomerId"]
    return tuple((row[column] is None, row[column]) for column in order)


def build_messages(
    rows: pa.Table,
    cfg: JobotronConfig,
    *,
    job_created: datetime,
    run_id: str,
    dry_run: bool,
) -> list[dict]:
    """D7: the pure envelope builder -- rows in, the real 13-field PascalCase envelope out,
    one message per row in ItemNumber order. No Kafka, no registry, no Dagster context.
    Fails before any message exists if the result misses the envelope's columns."""
    columns = list(rows.schema.names)
    missing = [name for name in REQUIRED_RESULT_COLUMNS if name not in columns]
    if missing:
        raise ValueError(
            f"The query result is missing the envelope's required column(s) {missing} "
            f"(found {columns}) -- the contract reads CustomerId and CompanyId from every "
            "row; alias them in the SELECT"
        )
    records = rows.to_pylist()
    records.sort(key=lambda row: _sort_key(row, columns))
    created = job_created.astimezone(timezone.utc)
    job_created_text = created.strftime("%Y-%m-%dT%H:%M:%SZ")  # second-truncated UTC
    job_stamp = created.strftime("%Y%m%d%H%M%S")  # the JobId's yyyyMMddHHmmss
    messages = []
    for number, row in enumerate(records, start=1):
        if row["CustomerId"] is None:
            raise ValueError(f"Row {number} has a null CustomerId -- the Kafka record key cannot be null")
        if row["CompanyId"] is None:
            raise ValueError(f"Row {number} has a null CompanyId -- the envelope's CompanyId cannot be null")
        payload = {column: row[column] for column in columns}
        payload_hash = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
        messages.append(
            {
                "JobId": f"{cfg.job_name}_{row['CompanyId']}_{job_stamp}",  # the row's CompanyId
                "JobName": cfg.job_name,
                "JobCreated": job_created_text,
                "RunId": run_id,
                "CompanyId": row["CompanyId"],
                "CustomerId": str(row["CustomerId"]),
                "ItemsCount": len(records),
                "ItemNumber": number,
                "ItemId": f"{payload_hash}-{number}",
                "ItemPayload": payload,
                "ItemPayloadHash": payload_hash,
                "IsDryRun": dry_run,
                "SchemaVersion": cfg.schema_version,
            }
        )
    return messages


# --- Kafka ----------------------------------------------------------------------


def _produce_one(producer, topic: str, key: bytes, value: bytes, delivery_errors: list[str]) -> None:
    """Enqueue one message (key AND value together) with the sync's bounded
    backpressure: produce() only enqueues, and BufferError means librdkafka's
    queue hit its ceiling -- serve the delivery callbacks with poll() and retry
    (demo volumes never hit it, but this loop is the producer's real shape)."""
    def on_delivery(err, msg):
        if err is not None:
            delivery_errors.append(str(err))

    queue_full_deadline = time.monotonic() + 120
    while True:
        try:
            producer.produce(topic=topic, key=key, value=value, on_delivery=on_delivery)
            break
        except BufferError:
            if time.monotonic() > queue_full_deadline:
                raise RuntimeError(f"Kafka producer queue stayed full for 120s (topic {topic})")
            producer.poll(0.1)


# --- the engines (D10) ----------------------------------------------------------


def _submit_statement(token: str, statement: str, warehouse_id: str, catalog: str, schema: str) -> dict:
    """One Statement Execution API submission (same REST shape as databricks_optimize's
    _submit, but the warehouse comes from the caller -- the env contract's value, never a
    module constant): wait up to 50 s inline, poll beyond that. The YAML's catalog and
    schema set the statement's context -- the notebook's `use de.voperdb`; without them
    the query's unqualified names resolve against the warehouse default (verified live
    2026-10-26: cCustomer then fails TABLE_OR_VIEW_NOT_FOUND against de.default)."""
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "statement": statement,
            "warehouse_id": warehouse_id,
            "catalog": catalog,
            "schema": schema,
            "wait_timeout": "50s",
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def _cancel_statement(token: str, statement_id: str, log: Any | None = None) -> None:
    """Best-effort cancellation of a statement the client stopped waiting for: without it
    the warehouse keeps executing (and billing) a statement whose result nobody reads.
    Endpoint note: the documented `/cancellation` form 404s on this workspace's build --
    `/cancel` is the one that works (verified live 2026-10-26). Network errors are
    swallowed; a rejected cancellation is logged, never raised (the original failure
    still propagates)."""
    try:
        resp = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}/cancel",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if not resp.ok and log:
            log.warning(f"Cancellation of statement {statement_id} was rejected: HTTP {resp.status_code}")
    except requests.RequestException:
        pass


def _run_statement(token: str, cfg: JobotronConfig, log: Any | None = None) -> tuple[dict, float]:
    """The databricks_sql engine's statement client: submit the query verbatim on the
    env's warehouse, in the YAML's catalog.schema context, poll to a terminal state, and
    fetch every result chunk (completeness-verified by _with_all_chunks). Returns
    (response, wall seconds). Progress is logged (the submission, then any handoff to
    polling) so a queued statement doesn't read as a hung run; on the polling deadline
    the statement is cancelled before the TimeoutError propagates. A missing
    DATABRICKS_WAREHOUSE_ID fails here, at call time, naming the variable -- the YAML
    never carries it (one defs.yaml still deploys to stg and prd)."""
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        raise ValueError(
            "DATABRICKS_WAREHOUSE_ID is required for scan_engine 'databricks_sql' (the candidate "
            "query runs on a SQL warehouse); set it in the environment -- the YAML never carries it"
        )
    if log:
        log.info(f"Submitting the YAML query on warehouse {warehouse_id} in {cfg.catalog}.{cfg.schema} context (inline wait 50 s)")
    started = time.monotonic()
    try:
        data = _submit_statement(token, cfg.query, warehouse_id, cfg.catalog, cfg.schema)
    except requests.exceptions.HTTPError as e:
        body = e.response.text if e.response is not None else "<no response body>"
        raise RuntimeError(f"Databricks Statement Execution API request failed: {e}\nResponse body: {body}") from e
    state = data.get("status", {}).get("state", "")
    if state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
        if log:
            log.info(f"Statement {data['statement_id']} still {state} after the inline wait -- polling every 5 s (deadline 600 s)")
        try:
            data = _poll(token, data["statement_id"])
        except TimeoutError:
            _cancel_statement(token, data["statement_id"], log)
            raise
    if data.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"Statement failed: {data.get('status', {})}")
    return _with_all_chunks(token, data), round(time.monotonic() - started, 1)


_INT_TYPES = {"int", "integer", "long", "bigint", "smallint", "tinyint", "short", "byte"}


def _coerced(value: Any, type_name: str) -> Any:
    """The JSON_ARRAY wire format renders every value as a string (the smoke test's count
    came back "13"); the manifest's declared column type says what it really is. Unknown
    types keep the raw value."""
    if value is None or not type_name:
        return value
    name = type_name.lower()
    if name in _INT_TYPES:
        return int(value)
    if name.startswith(("decimal", "float", "double", "real")):
        return float(value)
    if name == "boolean":
        return value if isinstance(value, bool) else str(value).lower() == "true"
    return value


def _statement_table(response: dict) -> pa.Table:
    """The statement result as one Arrow table: column names, order and declared types from
    the manifest schema (so an empty result still carries the names for the contract
    check), values coerced back from their JSON string forms."""
    columns = [
        (column["name"], column.get("type", ""))
        for column in response.get("manifest", {}).get("schema", {}).get("columns", [])
    ]
    rows = _rows_as_dicts(response)
    return pa.table({name: [_coerced(row.get(name), type_name) for row in rows] for name, type_name in columns})


def _scan(cfg: JobotronConfig, databricks, token: str, context) -> tuple[pa.Table, dict]:
    """The engine seam (D10): every engine returns the candidate rows as one Arrow table
    plus its engine-specific metadata; the envelope and Kafka path downstream are
    identical for all three."""
    if cfg.scan_engine == "databricks_sql":
        response, seconds = _run_statement(token, cfg, context.log)
        meter_record(seconds)
        table = _statement_table(response)
        warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
        context.log.info(
            f"Statement executed on warehouse {warehouse_id} in {seconds}s: {table.num_rows} row(s)"
        )
        return table, {"warehouse_id": warehouse_id, "statement_seconds": seconds}

    if cfg.scan_engine in ("delta_rs", "delta_extension"):
        if not cfg.sources:
            raise ValueError(
                f"scan_engine '{cfg.scan_engine}' scans Delta tables directly, so it needs `sources` "
                "(the bronze-table transcription of what the query joins -- delta-rs cannot scan the "
                "sharded UNION views); only 'databricks_sql' lets the warehouse resolve the names"
            )
        locations: dict[str, str] = {
            alias: databricks.table_location_cached(token, cfg, fqn) for alias, (fqn, _columns) in cfg.sources.items()
        }
        connection = duckdb.connect()
        if cfg.scan_engine == "delta_extension":
            # Each alias becomes a view over the extension's streaming delta_scan() -- no
            # Arrow snapshot materialization, morsel-driven scans, spill-to-disk joins.
            # The view projects ONLY the allowlisted columns, so a query edit touching a
            # non-allowlisted column fails loudly instead of silently scanning it
            # (payload minimization by construction).
            _ensure_delta_scan_secret(connection, next(iter(locations.values())))
            for alias, (fqn, columns) in cfg.sources.items():
                location = locations[alias]
                version = DeltaTable(location, storage_options=_azure_storage_options(location)).version()
                projection = ", ".join(columns) if columns else "*"
                connection.execute(f"CREATE VIEW {alias} AS SELECT {projection} FROM delta_scan('{location}')")
                context.log.info(f"Scanned {fqn} via delta_scan at version {version} (streaming view): {projection}")
        else:
            # delta-rs scans each source column-pruned into a pyarrow snapshot; DuckDB
            # registers it zero-copy (in-process, the only SQL engine in the whole path).
            for alias, (fqn, columns) in cfg.sources.items():
                delta = DeltaTable(locations[alias], storage_options=_azure_storage_options(locations[alias]))
                snapshot = delta.to_pyarrow_table(columns=list(columns))
                connection.register(alias, snapshot)
                context.log.info(
                    f"Scanned {fqn} at version {delta.version()}: {snapshot.num_rows} rows, "
                    f"columns {list(columns)}"
                )
        # to_arrow_table() materializes the result: duckdb 1.5's .arrow() returns a
        # RecordBatchReader instead of a Table, and duckdb 2.0 dropped fetch_arrow_table() and warns
        # when a DB-API result (execute) is converted, so the query goes through sql().
        result = connection.sql(cfg.query).to_arrow_table()
        connection.close()
        context.log.info(f"Candidate query selected {result.num_rows} row(s) [scan_engine={cfg.scan_engine}]")
        return result, {}

    raise ValueError(f"Unknown scan_engine {cfg.scan_engine!r} (expected one of {ENGINES})")


def _ensure_delta_scan_secret(connection: duckdb.DuckDBPyConnection, location: str) -> None:
    """Azure credentials for delta_scan() over the tables' abfss:// locations, as a DuckDB
    secret on the run's connection: the same service principal _azure_storage_options feeds
    delta-rs, in the extension's PROVIDER service_principal / TENANT_ID parameter form
    (verified live 2026-10-21 -- the OAUTH2_ENDPOINT form is rejected on this version).
    Dropped and recreated per run so a rotated secret can never outlive the job."""
    connection.execute("INSTALL azure")
    connection.execute("LOAD azure")
    account = location.split("@")[1].split(".")[0]
    connection.execute("DROP SECRET IF EXISTS jobotron_delta_scan")
    connection.execute(
        "CREATE SECRET jobotron_delta_scan ("
        "TYPE azure, PROVIDER service_principal, "
        f"ACCOUNT_NAME '{account}', "
        f"CLIENT_ID '{os.environ['DATABRICKS_AZURE_CLIENT_ID']}', "
        f"CLIENT_SECRET '{os.environ['DATABRICKS_AZURE_CLIENT_SECRET']}', "
        f"TENANT_ID '{os.environ['DATABRICKS_AZURE_TENANT_ID']}')"
    )


# --- assets ---------------------------------------------------------------------


def build_setup_asset(cfg: JobotronConfig):
    run_mode = _run_mode_for(cfg)

    @asset(
        name=cfg.setup_asset,
        group_name=cfg.group_name,
        description=(
            f"Create the instance's Kafka topic {cfg.derived_topic} (create-if-not-exists, one "
            "partition). Under dry-run (the YAML default, or this run's mode switch) the asset "
            "is a no-op -- zero side effects, not even a Kafka connection."
        ),
    )
    def setup(context: AssetExecutionContext, config: run_mode):
        dry_run = _effective_dry_run(cfg, config, context)
        if dry_run:
            # Zero side effects in dry-run: not even a Kafka connection. The topic
            # matters only for runs that actually publish.
            context.log.info(
                f"Dry run: nothing created -- launch setup with dry_run: false in the run config "
                f"(or pin it in the YAML) to create the topic {cfg.derived_topic}"
            )
            context.add_output_metadata(
                {
                    "topic": MetadataValue.text(cfg.derived_topic),
                    "created": MetadataValue.bool(False),
                    "dry_run": MetadataValue.bool(True),
                }
            )
            return

        kafka = connections.kafka()
        meter_reset()

        admin = kafka.admin_client()
        existing = admin.list_topics(timeout=20).topics
        if cfg.derived_topic in existing:
            context.log.info(f"Topic {cfg.derived_topic} already exists")
        else:
            admin.create_topics(
                [NewTopic(cfg.derived_topic, num_partitions=1, replication_factor=_DEFAULT_REPLICATION)]
            )
            context.log.info(f"Created topic {cfg.derived_topic}")

        context.add_output_metadata(
            {
                "topic": MetadataValue.text(cfg.derived_topic),
                "created": MetadataValue.bool(True),
                "dry_run": MetadataValue.bool(False),
                **_cost_metadata(),
            }
        )

    return setup


def build_publish_asset(cfg: JobotronConfig):
    run_mode = _run_mode_for(cfg)

    @asset(
        name=cfg.publish_asset,
        group_name=cfg.group_name,
        description=(
            f"Run the candidate-selection query ({cfg.scan_engine}) and publish the real Jobotron "
            f"envelope -- one JSON message per selected customer, keyed by CustomerId -- to "
            f"{cfg.derived_topic}. Under dry-run (the YAML default, or this run's mode switch) "
            "every message is built and hashed and nothing is sent."
        ),
    )
    def publish(context: AssetExecutionContext, config: run_mode):
        dry_run = _effective_dry_run(cfg, config, context)
        databricks, kafka = connections.databricks(), connections.kafka()
        databricks.require_env()
        token = databricks.token()
        meter_reset()
        started = time.monotonic()

        if cfg.encoding != "json":
            raise ValueError(
                f"encoding {cfg.encoding!r} is not implemented -- the validated contract is JSON "
                "(Avro is deferred behind `encoding`, see docs/poc/JOBOTRON_CONTRACT_PARITY.md)"
            )

        # 1. The engine seam: candidate rows as one Arrow table (plus engine metadata).
        result, engine_meta = _scan(cfg, databricks, token, context)

        # 2. The pure envelope builder: the real 13-field contract, ordered and hashed.
        #    A result missing CustomerId/CompanyId fails here, named, before any produce.
        messages = build_messages(
            result, cfg, job_created=datetime.now(timezone.utc), run_id=context.run.run_id, dry_run=dry_run
        )
        previews = [{"key": message["CustomerId"], "value": message} for message in messages[:PREVIEW_LIMIT]]

        def metadata_entries():
            return {
                "items_count": MetadataValue.int(len(messages)),
                "job_name": MetadataValue.text(cfg.job_name),
                "topic": MetadataValue.text(cfg.derived_topic),
                "dry_run": MetadataValue.bool(dry_run),
                "schema_version": MetadataValue.int(cfg.schema_version),
                "candidate_query": MetadataValue.text(cfg.query),
                "message_previews": MetadataValue.json(previews),
                "publish_seconds": MetadataValue.float(round(time.monotonic() - started, 1)),
                **engine_meta,
                **_cost_metadata(),
            }

        # 3. The dry-run gate (D5): the whole path ran -- scan, query, envelope, hashing,
        #    counting -- and nothing is sent. A misconfigured instance cannot pollute the
        #    topic at all.
        if dry_run:
            context.log.info(
                f"Dry run: built {len(messages)} message(s) for {cfg.derived_topic} and sent NOTHING "
                f"(pin dry_run: false to publish)"
            )
            context.add_output_metadata(metadata_entries())
            return

        # 4. Publish: compact canonical JSON on the wire (the hash's exact canon), key =
        #    CustomerId, every message to the instance's topic.
        producer = kafka.producer()
        delivery_errors: list[str] = []
        for message in messages:
            value = _canonical_json(message).encode("utf-8")
            key = message["CustomerId"].encode("utf-8")
            _produce_one(producer, cfg.derived_topic, key, value, delivery_errors)

        # 5. Bounded final flush; any rejected delivery fails the run.
        undelivered = producer.flush(timeout=120)
        if undelivered or delivery_errors:
            raise RuntimeError(
                f"Kafka delivery failed: {len(delivery_errors)} rejected ({delivery_errors[:3]}), "
                f"{undelivered} undelivered after 120s"
            )
        context.log.info(f"Published {len(messages)} message(s) to {cfg.derived_topic}")
        context.add_output_metadata(metadata_entries())

    return publish


def build_consume_asset(cfg: JobotronConfig):
    @asset(
        name=cfg.consume_asset,
        group_name=cfg.group_name,
        description=(
            f"Demo worker stand-in: read every message on {cfg.derived_topic}, decode the JSON "
            "envelope, dedup on ItemId, and keep one mirror row per item in the Postgres table "
            f"{cfg.derived_topic}. Stands in for the RFC's worker, which would act per message -- "
            "ItemId carries the payload hash, so a republished item with new content is a new row."
        ),
    )
    def consume(context: AssetExecutionContext):
        postgres, kafka = connections.state_postgres(), connections.kafka()
        meter_reset()
        started = time.monotonic()

        # Read the topic from the beginning. The full eligible set is republished every
        # run, so the mirror accumulates one row per ItemId ever seen (INSERT ... ON
        # CONFLICT DO NOTHING). Manual partition assign, no commits: the consumer is
        # stateless and re-reads the whole topic per run (demo volumes are a handful of
        # messages; the RFC sizes ~20k/day).
        consumer = kafka.consumer(
            f"{BASE}-demo-consumer", {"auto.offset.reset": "earliest", "enable.auto.commit": "false"}
        )
        values: list[dict] = []
        try:
            consumer.assign([TopicPartition(cfg.derived_topic, 0)])
            # Drain the topic fully, verified against the broker's watermarks. A plain
            # "break on the first quiet window" loses the race on a cold consumer (fresh
            # metadata + fetch handshake) and silently rebuilds the table EMPTY -- verified
            # live. Watermarks make "fully drained" a fact, not a timing guess; the outer
            # cap only guards a pathological loop.
            for _ in range(10):
                while True:
                    batch = consumer.consume(num_messages=500, timeout=5.0)
                    if not batch:
                        break
                    for message in batch:
                        if message.error() or message.topic() != cfg.derived_topic:
                            continue
                        try:
                            values.append(json.loads(message.value()))
                        except Exception as e:  # noqa: BLE001 -- surface decode failures per message
                            context.log.warning(f"Could not decode a message on {message.topic()}: {e}")
                lo, hi = consumer.get_watermark_offsets(TopicPartition(cfg.derived_topic, 0), timeout=10.0, cached=False)
                if hi - lo <= len(values):
                    break
                context.log.info(f"{cfg.derived_topic} not fully drained yet ({len(values)}/{hi - lo}) -- reading again")
        finally:
            consumer.close()

        # Dedup on ItemId (the doc's worker stand-in): a re-delivered item keeps its first
        # mirror row; new content means a new hash and so a new ItemId.
        by_item: dict[str, dict] = {}
        for value in values:
            by_item[value.get("ItemId")] = value
        mirror_rows = [
            {
                "item_id": value.get("ItemId"),
                "item_number": value.get("ItemNumber"),
                "customer_id": value.get("CustomerId"),
                "company_id": None if value.get("CompanyId") is None else str(value.get("CompanyId")),
                "job_name": value.get("JobName"),
                "run_id": value.get("RunId"),
                "is_dry_run": value.get("IsDryRun"),
                "schema_version": value.get("SchemaVersion"),
                "payload": psycopg2.extras.Json(value.get("ItemPayload")),
            }
            for value in by_item.values()
        ]

        conn = postgres.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {cfg.derived_topic} ("
                    "item_id TEXT PRIMARY KEY, item_number BIGINT, customer_id TEXT, company_id TEXT, "
                    "job_name TEXT, run_id TEXT, is_dry_run BOOLEAN, schema_version INTEGER, payload JSONB)"
                )
                cur.executemany(
                    f"INSERT INTO {cfg.derived_topic} (item_id, item_number, customer_id, company_id, "
                    "job_name, run_id, is_dry_run, schema_version, payload) VALUES "
                    "(%(item_id)s, %(item_number)s, %(customer_id)s, %(company_id)s, %(job_name)s, "
                    "%(run_id)s, %(is_dry_run)s, %(schema_version)s, %(payload)s) "
                    "ON CONFLICT (item_id) DO NOTHING",
                    mirror_rows,
                )
            conn.commit()
        finally:
            conn.close()
        context.log.info(f"{cfg.derived_topic}: {len(values)} message(s) read, {len(mirror_rows)} item(s) mirrored")

        context.add_output_metadata(
            {
                "messages_read": MetadataValue.int(len(values)),
                "items_count": MetadataValue.int(len(mirror_rows)),
                "consume_seconds": MetadataValue.float(round(time.monotonic() - started, 1)),
                **_cost_metadata(),
            }
        )

    return consume


# --- reset ----------------------------------------------------------------------


def _assert_reset_allowed(cfg: JobotronConfig) -> None:
    """Refuse to run unless every name this job drops looks like this component's: the
    topic and the mirror table under jobotron_ (the derived default; a typed topic gets
    the same check). Same guard idea as the CDF syncs' reset."""
    bad = [name for name in (cfg.derived_topic,) if not name.startswith("jobotron_")]
    if bad:
        raise RuntimeError(f"Refusing to reset the Jobotron instance: these do not look like jobotron objects: {bad}")


def build_reset_job(cfg: JobotronConfig):
    @op(name=f"{cfg.instance_name}_delete_topic", ins={"start": In(Nothing)})
    def delete_topic(context: OpExecutionContext) -> None:
        kafka = connections.kafka()
        _assert_reset_allowed(cfg)
        admin = kafka.admin_client()
        if cfg.derived_topic in admin.list_topics(timeout=20).topics:
            admin.delete_topics([cfg.derived_topic], operation_timeout=30)[cfg.derived_topic].result()
            context.log.info(f"Delete requested for topic {cfg.derived_topic}")
        else:
            context.log.info(f"Topic {cfg.derived_topic} did not exist")
        # Topic deletes are asynchronous; the setup job must not recreate while listed.
        deadline = time.monotonic() + 90
        while cfg.derived_topic in admin.list_topics(timeout=20).topics:
            if time.monotonic() > deadline:
                raise RuntimeError(f"Topic {cfg.derived_topic} still listed after 90s")
            time.sleep(3)
        context.log.info(f"Topic {cfg.derived_topic} is gone")

    @op(name=f"{cfg.instance_name}_drop_mirror_table", ins={"start": In(Nothing)})
    def drop_mirror_table(context: OpExecutionContext) -> None:
        postgres = connections.state_postgres()
        _assert_reset_allowed(cfg)
        conn = postgres.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {cfg.derived_topic}")
                context.log.info(f"Dropped Postgres table {cfg.derived_topic} (if it existed)")
            conn.commit()
        finally:
            conn.close()

    @job(
        name=cfg.reset_job,
        description=(
            f"Tear down the instance: the Kafka topic {cfg.derived_topic} and the demo consumer's "
            f"Postgres mirror table. The instances own no Databricks tables (read-only scans of "
            f"de.voperdb) and no registry subjects (JSON needs none). Then run {cfg.setup_job} and "
            "the first publish for a fresh demo."
        ),
        executor_def=in_process_executor,
    )
    def reset_job():
        gone = delete_topic()
        drop_mirror_table(gone)

    return reset_job


# --- defs -----------------------------------------------------------------------


def build_jobotron_defs(cfg: JobotronConfig) -> Definitions:
    setup = build_setup_asset(cfg)
    publish = build_publish_asset(cfg)
    consume = build_consume_asset(cfg)
    setup_job = define_asset_job(
        name=cfg.setup_job,
        selection=AssetSelection.assets(AssetKey(cfg.setup_asset)),
        description="Create the instance's topic (create-if-not-exists; a no-op under the dry-run default).",
        executor_def=in_process_executor,
    )
    return Definitions(
        assets=[setup, publish, consume],
        jobs=[setup_job, build_reset_job(cfg)],
    )
