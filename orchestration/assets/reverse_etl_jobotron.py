"""Jobotron producer demo (docs/poc/REVERSE_ETL_JOBOTRON.md): the upstream publisher
described by the Jobotron RFC -- run a candidate-selection query against the lake's
Delta tables and publish one Avro message per selected customer to that customer's
operator topic, schemas in Apicurio. The Jobotron worker itself (dedup, eligibility
re-validation, actions, state) is the RFC's Accounts & Payments scope and is NOT
built here; this component is the data-platform side of that boundary.

No Spark, no JVM, no Databricks compute in the publish path: the tables are scanned
with the service principal, DuckDB -- already a project dependency -- runs the YAML
query, confluent-kafka produces. The scan engine is a per-job knob (scan_engine):
"delta_rs" materializes column-pruned snapshots via delta-rs (day one); "delta_extension"
registers DuckDB views over the delta extension's streaming delta_scan (verified live
on the demo tables 2026-10-21). Either way the cost meter reads 0.0 warehouse seconds
on every publish run.

No CDF anywhere: the query runs against each table's current snapshot, which is
exactly what frees this component from the delta-rs CDF gap that pins the CDF
syncs to Spark. The full eligible set is published every run -- the worker owns
deduplication and re-validation, so the producer stays stateless (no watermark
table, unlike the CDF syncs).

Demo tables (created by the setup asset, plain Delta, deletion vectors off, no
Change Data Feed): customers + kyc_cases in de_dev.sr_poc_external. The demo
query is the RFC's example job, "send PM to customers that remain unverified":
active customers joined to their PENDING KYC case, routed per operator to the
jobotron_demo_<operator> topics. Topics/subjects are demo-prefixed because they
live on the shared staging broker/registry (same convention as the reverse_etl_*
topics).
"""

import os
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any

import duckdb
import pyarrow as pa
from confluent_kafka import TopicPartition
from confluent_kafka.admin import NewTopic
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
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

from .databricks_cost_meter import snapshot as meter_snapshot
from .kafka_topics_setup import _DEFAULT_REPLICATION
from .reverse_etl_cdf_setup import (
    _avro_schema_string,
    _azure_storage_options,
    _cost_metadata,
    _rows_as_dicts,
    _schema_already_registered,
)
from .reverse_etl_resources import (
    DatabricksResource,
    HttpResource,
    KafkaResource,
    PostgresResource,
    SchemaRegistryResource,
)

BASE = "reverse_etl_jobotron"
SCHEMA_REGISTRY_URL = "http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7"
# Cap on message previews echoed into run metadata (same idea as the sync's MESSAGE_PREVIEW_LIMIT).
PREVIEW_LIMIT = 20


@dataclass(frozen=True)
class JobotronConfig:
    """One Jobotron producer job (see the plan doc's YAML contract). The demo's
    single instance lives in orchestration/defs/reverse_etl_jobotron/defs.yaml."""

    job_type: str
    catalog: str
    schema: str
    # alias -> (fqn, scan columns): the alias is the name the query uses (and the
    # DuckDB registration); the columns prune the delta-rs scan to what the query
    # selects -- day one has no predicate pushdown, so never scan more than needed.
    sources: dict[str, tuple[str, tuple[str, ...]]]
    query: str
    operators: tuple[str, ...]
    topic_template: str
    key_columns: tuple[str, ...]
    operator_column: str
    customers_table: str
    kyc_table: str
    group_name: str = BASE
    setup_asset: str = f"{BASE}_setup"
    publish_asset: str = f"{BASE}_publish"
    consume_asset: str = f"{BASE}_consume"
    change_asset: str = f"{BASE}_make_change"
    setup_job: str = f"{BASE}_setup_job"
    reset_job: str = f"{BASE}_reset_job"
    change_job: str = f"{BASE}_make_change_job"
    schema_registry_url: str = SCHEMA_REGISTRY_URL
    # Per-job scan engine (docs/poc/REVERSE_ETL_JOBOTRON.md §8.6 scale ladder): "delta_rs"
    # (day one) materializes column-pruned snapshots via delta-rs and registers them
    # zero-copy; "delta_extension" registers DuckDB views over the delta extension's
    # streaming delta_scan -- same YAML query, no Arrow materialization, spill-to-disk
    # joins (verified live on the demo tables 2026-10-21, §3.1).
    scan_engine: str = "delta_rs"

    @property
    def customers_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.customers_table}"

    @property
    def kyc_fqn(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.kyc_table}"

    @property
    def topics(self) -> dict[str, str]:
        """operator -> topic."""
        return {operator: self.topic_template.format(operator=operator) for operator in self.operators}


# --- Avro ----------------------------------------------------------------------


def _avro_type(pa_type: pa.DataType) -> str:
    """Arrow (DuckDB result) type -> Avro primitive. Timestamps map to string on
    purpose: the demo convention carries them as microsecond-UTC strings (the
    ZonedTimestamp rendering the CDF sync uses), and the demo query selects none."""
    if pa.types.is_integer(pa_type):
        return "long"
    if pa.types.is_boolean(pa_type):
        return "boolean"
    if pa.types.is_floating(pa_type) or pa.types.is_decimal(pa_type):
        return "double"
    return "string"


def _jobotron_schemas(cfg: JobotronConfig, result_schema: pa.Schema) -> tuple[dict, dict]:
    """(key schema, value schema) derived from the query's RESULT shape: the published
    contract is exactly what the YAML author selected (payload minimization by
    construction), wrapped with the run_id/job_type envelope."""
    key_schema = {
        "type": "record",
        "name": "Key",
        "namespace": "kaizen.jobotron",
        "fields": [
            {"name": column, "type": _avro_type(result_schema.field(column).type)} for column in cfg.key_columns
        ],
    }
    value_schema = {
        "type": "record",
        "name": "JobotronItem",
        "namespace": "kaizen.jobotron",
        "fields": [
            {"name": "run_id", "type": "string"},
            {"name": "job_type", "type": "string"},
            # Every payload field is nullable: a NULL in the query result must not
            # fail the run, mirroring the sync's Value schema convention.
            *(
                {"name": field.name, "type": ["null", _avro_type(field.type)], "default": None}
                for field in result_schema
            ),
        ],
    }
    return key_schema, value_schema


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


# --- assets --------------------------------------------------------------------


def build_setup_asset(cfg: JobotronConfig):
    @asset(
        name=cfg.setup_asset,
        group_name=cfg.group_name,
        description=(
            f"Create the Jobotron demo tables {cfg.customers_fqn} and {cfg.kyc_fqn} (plain Delta, "
            "no Change Data Feed -- the producer reads snapshots), the per-operator Kafka topics, "
            "and seed the demo customers. See docs/poc/REVERSE_ETL_JOBOTRON.md."
        ),
    )
    def setup(context: AssetExecutionContext, databricks: DatabricksResource, kafka: KafkaResource):
        databricks.require_env()
        token = databricks.token()
        databricks.reset_meter()

        admin = kafka.admin_client()
        existing = admin.list_topics(timeout=20).topics
        missing = [topic for topic in cfg.topics.values() if topic not in existing]
        if missing:
            # One partition per operator topic: the demo volumes are a handful of
            # messages (the RFC sizes real ones at 3-10 partitions per operator).
            admin.create_topics(
                [NewTopic(topic, num_partitions=1, replication_factor=_DEFAULT_REPLICATION) for topic in missing]
            )
            for topic in missing:
                context.log.info(f"Created topic {topic}")
        else:
            context.log.info(f"Topics already exist: {', '.join(cfg.topics.values())}")

        # Plain Delta on purpose, and NO Change Data Feed: the producer reads each
        # table's current snapshot via delta-rs, so the CDF machinery (and the
        # delta-rs CDF gap that pins the sync component to Spark) never applies.
        # Deletion vectors off keeps the read path uniform with the other demo
        # tables; column mapping is off because this demo has no drop/rename ops.
        for fqn, ddl in (
            (
                cfg.customers_fqn,
                f"""
            CREATE TABLE IF NOT EXISTS {cfg.customers_fqn} (
                customer_id BIGINT NOT NULL,
                email STRING,
                locale STRING,
                operator STRING,
                is_active BOOLEAN
            ) USING DELTA
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
            """,
            ),
            (
                cfg.kyc_fqn,
                f"""
            CREATE TABLE IF NOT EXISTS {cfg.kyc_fqn} (
                customer_id BIGINT NOT NULL,
                status STRING,
                updated_at TIMESTAMP
            ) USING DELTA
            TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
            """,
            ),
        ):
            databricks.run_sql(token, ddl)
            context.log.info(f"{fqn} ready (plain Delta: no CDF, no column mapping)")

        # MERGE so "Set up" twice never duplicates the demo rows (same pattern as
        # the CDF syncs' seed_statements). Seed shape: six customers across two
        # operators; the eligible segment (active AND a PENDING KYC case) starts
        # at three -- 1001/gr, 1004+1005/br -- small enough to read off the page.
        databricks.run_sql(
            token,
            f"""
        MERGE INTO {cfg.customers_fqn} AS target
        USING (
            SELECT 1001 AS customer_id, 'nikos.papadopoulos@example.gr' AS email, 'el-GR' AS locale, 'gr' AS operator, true AS is_active
            UNION ALL SELECT 1002, 'maria.ioannou@example.gr', 'el-GR', 'gr', true
            UNION ALL SELECT 1003, 'georgos.dimopoulos@example.gr', 'el-GR', 'gr', false
            UNION ALL SELECT 1004, 'ana.souza@example.br', 'pt-BR', 'br', true
            UNION ALL SELECT 1005, 'bruno.costa@example.br', 'pt-BR', 'br', true
            UNION ALL SELECT 1006, 'lucia.almeida@example.br', 'pt-BR', 'br', true
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN NOT MATCHED THEN INSERT (customer_id, email, locale, operator, is_active)
            VALUES (source.customer_id, source.email, source.locale, source.operator, source.is_active)
        """,
        )
        databricks.run_sql(
            token,
            f"""
        MERGE INTO {cfg.kyc_fqn} AS target
        USING (
            SELECT 1001 AS customer_id, 'PENDING' AS status, current_timestamp() AS updated_at
            UNION ALL SELECT 1002, 'PENDING', current_timestamp()
            UNION ALL SELECT 1003, 'PENDING', current_timestamp()
            UNION ALL SELECT 1004, 'PENDING', current_timestamp()
            UNION ALL SELECT 1005, 'PENDING', current_timestamp()
            UNION ALL SELECT 1006, 'VERIFIED', current_timestamp()
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN NOT MATCHED THEN INSERT (customer_id, status, updated_at)
            VALUES (source.customer_id, source.status, source.updated_at)
        """,
        )
        context.log.info(f"Seeded {cfg.customers_fqn} and {cfg.kyc_fqn} (segment: 1001+1002/gr, 1004+1005/br)")

        context.add_output_metadata(
            {
                "customers_table": MetadataValue.text(cfg.customers_fqn),
                "kyc_table": MetadataValue.text(cfg.kyc_fqn),
                "topics": MetadataValue.text(", ".join(cfg.topics.values())),
                **_cost_metadata(),
            }
        )

    return setup


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


def build_publish_asset(cfg: JobotronConfig):
    @asset(
        name=cfg.publish_asset,
        group_name=cfg.group_name,
        description=(
            "Run the candidate-selection query from jobotron.yaml over the demo tables and publish "
            f"one Avro message per selected customer to its operator topic ({', '.join(cfg.topics.values())}). "
            f"Scan engine {cfg.scan_engine} + DuckDB query: no Spark, no JVM, zero Databricks compute."
        ),
    )
    def publish(
        context: AssetExecutionContext,
        databricks: DatabricksResource,
        kafka: KafkaResource,
        registry: SchemaRegistryResource,
    ):
        databricks.require_env()
        token = databricks.token()
        databricks.reset_meter()
        started = time.monotonic()

        # 1. Resolve locations (UC REST, cached) -- control-plane reads only, no compute.
        locations: dict[str, str] = {
            alias: databricks.table_location_cached(token, cfg, fqn) for alias, (fqn, _columns) in cfg.sources.items()
        }

        # 2. The scan engine is the per-job YAML knob (§8.6): both branches feed the
        #    SAME YAML query over the SAME aliases -- only each alias's source differs.
        if cfg.scan_engine not in ("delta_rs", "delta_extension"):
            raise ValueError(f"Unknown scan_engine {cfg.scan_engine!r} (expected delta_rs or delta_extension)")
        connection = duckdb.connect()
        if cfg.scan_engine == "delta_extension":
            # Phase-4 lever, verified live on these tables (2026-10-21): each alias
            # becomes a view over the extension's streaming delta_scan() -- no Arrow
            # snapshot materialization, morsel-driven scans, spill-to-disk joins. The
            # view projects ONLY the scan_columns allowlist, so a query edit touching a
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
            # Day-one engine: delta-rs scans each source column-pruned into a pyarrow
            # snapshot; DuckDB registers it zero-copy (in-process, the only SQL engine
            # in the whole path).
            for alias, (fqn, columns) in cfg.sources.items():
                delta = DeltaTable(locations[alias], storage_options=_azure_storage_options(locations[alias]))
                snapshot = delta.to_pyarrow_table(columns=list(columns))
                connection.register(alias, snapshot)
                context.log.info(
                    f"Scanned {fqn} at version {delta.version()}: {snapshot.num_rows} rows, "
                    f"columns {list(columns)}"
                )
        # fetch_arrow_table() materializes the result: duckdb 1.5's .arrow() returns a
        # RecordBatchReader instead of a Table.
        result = connection.execute(cfg.query).fetch_arrow_table()
        connection.close()
        context.log.info(f"Candidate query selected {result.num_rows} customer(s) [scan_engine={cfg.scan_engine}]")

        # 3. Schemas derived from the result shape (an empty result still carries the
        #    schema), ensured per operator subject before any message uses them.
        key_schema, value_schema = _jobotron_schemas(cfg, result.schema)
        client = registry.client(cfg.schema_registry_url)
        for topic in cfg.topics.values():
            for subject, schema in ((f"{topic}-key", key_schema), (f"{topic}-value", value_schema)):
                if not _schema_already_registered(client, subject, schema):
                    client.register_schema(subject, Schema(_avro_schema_string(schema), "AVRO"))
                    context.log.info(f"Registered {subject}")
                else:
                    context.log.info(f"{subject} unchanged")

        # 4. Route by operator and produce one message per customer. An unknown
        #    operator fails the run loudly -- a bad value must never publish
        #    cross-market (operator isolation is the RFC's own selling point).
        serializers = {}
        for topic in cfg.topics.values():
            # All subjects were just ensured above, so the serializers can point at
            # the registered versions instead of re-registering on the first message.
            conf = {"auto.register.schemas": False, "use.latest.version": True}
            serializers[topic] = (
                AvroSerializer(client, _avro_schema_string(key_schema), conf=conf),
                AvroSerializer(client, _avro_schema_string(value_schema), conf=conf),
            )
        producer = kafka.producer()
        delivery_errors: list[str] = []
        per_operator: Counter[str] = Counter()
        previews: list[dict] = []
        for row in result.to_pylist():
            operator = str(row.get(cfg.operator_column) or "")
            topic = cfg.topics.get(operator)
            if topic is None:
                raise RuntimeError(
                    f"Unknown operator {operator!r} in the query result -- refusing to publish "
                    f"cross-market (known operators: {sorted(cfg.topics)})"
                )
            key_serializer, value_serializer = serializers[topic]
            key = {column: row[column] for column in cfg.key_columns}
            value = {"run_id": context.run_id, "job_type": cfg.job_type, **row}
            key_bytes = key_serializer(key, SerializationContext(topic, MessageField.KEY))
            value_bytes = value_serializer(value, SerializationContext(topic, MessageField.VALUE))
            _produce_one(producer, topic, key_bytes, value_bytes, delivery_errors)
            per_operator[operator] += 1
            if len(previews) < PREVIEW_LIMIT:
                previews.append({"topic": topic, "key": key, "value": value})

        # 5. Bounded final flush; any rejected delivery fails the run.
        undelivered = producer.flush(timeout=120)
        if undelivered or delivery_errors:
            raise RuntimeError(
                f"Kafka delivery failed: {len(delivery_errors)} rejected ({delivery_errors[:3]}), "
                f"{undelivered} undelivered after 120s"
            )

        context.add_output_metadata(
            {
                "rows_published": MetadataValue.int(sum(per_operator.values())),
                "per_operator": MetadataValue.json(dict(per_operator)),
                "candidate_query": MetadataValue.text(cfg.query),
                "job_type": MetadataValue.text(cfg.job_type),
                "message_previews": MetadataValue.json(previews),
                "publish_seconds": MetadataValue.float(round(time.monotonic() - started, 1)),
                **_cost_metadata(),
            }
        )

    return publish


def build_consume_asset(cfg: JobotronConfig):
    @asset(
        name=cfg.consume_asset,
        group_name=cfg.group_name,
        description=(
            "Demo consumer: read every message on the operator topics, decode the Avro values, and mirror "
            "the current segment into one Postgres table per operator (one row per customer, table = topic "
            "name). Stands in for the RFC's worker, which would act per message -- this one mirrors state, "
            "so a customer that left the segment disappears from its table at the next consume."
        ),
    )
    def consume(
        context: AssetExecutionContext,
        postgres: PostgresResource,
        databricks: DatabricksResource,
        kafka: KafkaResource,
        registry: SchemaRegistryResource,
    ):
        databricks.reset_meter()
        started = time.monotonic()

        # Read each operator topic from the beginning. Topics APPEND: a customer
        # that left the segment has no new message, so "last message per customer"
        # would keep stale members forever. The current segment is the latest
        # publish's full set -- the messages whose run_id matches the topic's LAST
        # message (highest offset). Manual partition assign, no commits: the
        # consumer is stateless and re-reads the whole topic per run (demo volumes
        # are a handful of messages; the RFC sizes ~20k/day).
        deserializer = AvroDeserializer(registry.client(cfg.schema_registry_url))
        consumer = kafka.consumer(
            f"{BASE}-demo-consumer", {"auto.offset.reset": "earliest", "enable.auto.commit": "false"}
        )
        read: dict[str, list[dict]] = {topic: [] for topic in cfg.topics.values()}
        last_run: dict[str, str] = {}
        last_ts: dict[str, int] = {}
        try:
            consumer.assign([TopicPartition(topic, 0) for topic in cfg.topics.values()])
            # Drain every topic fully, verified against the broker's watermarks. A
            # plain "break on the first quiet window" loses the race on a cold
            # consumer (fresh metadata + fetch handshake) and silently rebuilds the
            # tables EMPTY -- verified live. Watermarks make "fully drained" a fact,
            # not a timing guess; the outer cap only guards a pathological loop.
            for _ in range(10):
                while True:
                    batch = consumer.consume(num_messages=500, timeout=5.0)
                    if not batch:
                        break
                    for message in batch:
                        if message.error() or message.topic() not in read:
                            continue
                        try:
                            value = deserializer(message.value(), SerializationContext(message.topic(), MessageField.VALUE))
                        except Exception as e:  # noqa: BLE001 -- surface decode failures per message
                            context.log.warning(f"Could not decode a message on {message.topic()}: {e}")
                            continue
                        read[message.topic()].append(value)
                        last_run[message.topic()] = value.get("run_id")
                        last_ts[message.topic()] = message.timestamp()[1]
                lagging = []
                for topic in cfg.topics.values():
                    lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10.0, cached=False)
                    if hi - lo > len(read[topic]):
                        lagging.append(f"{topic} ({len(read[topic])}/{hi - lo})")
                if not lagging:
                    break
                context.log.info(f"Topics not fully drained yet: {', '.join(lagging)} -- reading again")
        finally:
            consumer.close()

        current: dict[str, dict[int, dict]] = {}
        # The current segment is the latest PUBLISH's full set. Grouping per topic by
        # that topic's last message breaks when a publish legitimately emits ZERO
        # messages for one operator (empty segment): the topic's last message is then
        # from an OLDER run and its stale snapshot survives forever (a verified 1007
        # did exactly that). The latest publish is the one whose message carries the
        # newest Kafka timestamp across ALL topics -- topics without messages from it
        # correctly rebuild empty.
        stamps = [(last_ts[topic], rid) for topic, rid in last_run.items()]
        latest_run = max(stamps)[1] if stamps else None
        for topic, values in read.items():
            current[topic] = {int(v["customer_id"]): v for v in values if v.get("run_id") == latest_run}
            context.log.info(f"{topic}: latest publish {latest_run} holds {len(current[topic])} customer(s)")

        # One Postgres table per operator topic (same name), rebuilt per consume:
        # the table IS the current segment for that operator. The full-set publish
        # makes this safe -- the topic always reflects the last publish's segment.
        conn = postgres.connect()
        try:
            with conn.cursor() as cur:
                for topic in cfg.topics.values():
                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {topic} ("
                        "customer_id BIGINT PRIMARY KEY, email TEXT, locale TEXT, "
                        "operator TEXT, run_id TEXT, job_type TEXT)"
                    )
                    cur.execute(f"TRUNCATE {topic}")
                    cur.executemany(
                        f"INSERT INTO {topic} (customer_id, email, locale, operator, run_id, job_type) "
                        "VALUES (%(customer_id)s, %(email)s, %(locale)s, %(operator)s, %(run_id)s, %(job_type)s)",
                        list(current[topic].values()),
                    )
                    context.log.info(f"{topic}: {len(current[topic])} row(s)")
            conn.commit()
        finally:
            conn.close()

        context.add_output_metadata(
            {
                "rows_per_operator": MetadataValue.json(
                    {operator: len(current[topic]) for operator, topic in cfg.topics.items()}
                ),
                "consume_seconds": MetadataValue.float(round(time.monotonic() - started, 1)),
                **_cost_metadata(),
            }
        )

    return consume


class JobotronChangeConfig(Config):
    change: str = "add_customer"  # add_customer, verify_customer or deactivate_customer

def _scalar(databricks: DatabricksResource, token: str, sql: str) -> Any:
    rows = _rows_as_dicts(databricks.run_sql(token, sql))
    return next(iter(rows[0].values()))


def _change_statements(cfg: JobotronConfig, change: str) -> list[str]:
    """The SQL for one demo change. add_customer inserts a new active customer with a
    PENDING KYC case (it joins the segment at the next publish); verify_customer and
    deactivate_customer move one eligible customer out of the segment. The full-set
    publish then simply stops carrying them -- no tombstone, which is exactly the
    full-snapshot semantics the plan doc describes."""
    customers, kyc = cfg.customers_fqn, cfg.kyc_fqn
    if change == "add_customer":
        # The id chains off max(); the operator alternates by row count so the
        # segment's routing mix keeps changing across demo runs.
        return [
            f"""
        INSERT INTO {customers} (customer_id, email, locale, operator, is_active)
        SELECT (SELECT max(customer_id) FROM {customers}) + 1,
               'customer' || CAST((SELECT max(customer_id) FROM {customers}) + 1 AS STRING)
                   || '@example.' || (SELECT CASE WHEN count(*) % 2 = 0 THEN 'gr' ELSE 'br' END FROM {customers}),
               (SELECT CASE WHEN count(*) % 2 = 0 THEN 'el-GR' ELSE 'pt-BR' END FROM {customers}),
               (SELECT CASE WHEN count(*) % 2 = 0 THEN 'gr' ELSE 'br' END FROM {customers}),
               true
            """,
            f"""
        INSERT INTO {kyc} (customer_id, status, updated_at)
        SELECT max(customer_id), 'PENDING', current_timestamp() FROM {customers}
            """,
        ]
    if change == "verify_customer":
        return [
            f"""
        UPDATE {kyc} SET status = 'VERIFIED', updated_at = current_timestamp()
        WHERE customer_id = (
            SELECT min(k.customer_id) FROM {kyc} k
            JOIN {customers} c ON c.customer_id = k.customer_id
            WHERE k.status = 'PENDING' AND c.is_active
        )
            """
        ]
    if change == "deactivate_customer":
        return [
            f"""
        UPDATE {customers} SET is_active = false
        WHERE customer_id = (
            SELECT min(c.customer_id) FROM {customers} c
            WHERE c.is_active
              AND c.customer_id IN (SELECT k.customer_id FROM {kyc} k WHERE k.status = 'PENDING')
        )
            """
        ]
    raise ValueError(f"change must be add_customer, verify_customer or deactivate_customer, got {change!r}")


def build_change_asset(cfg: JobotronConfig):
    @asset(
        name=cfg.change_asset,
        description=(
            f"Change the Jobotron demo data in Databricks: add an unverified customer (joins the "
            f"segment), verify a customer, or deactivate one (both leave it). Nothing is published "
            f"until {cfg.publish_asset} runs."
        ),
    )
    def make_change(context: AssetExecutionContext, config: JobotronChangeConfig, databricks: DatabricksResource):
        databricks.require_env()
        token = databricks.token()
        databricks.reset_meter()
        statements = _change_statements(cfg, config.change)

        # A change with nothing to do is a normal demo condition (the segment can be
        # empty): end the run green and tell the page why, like the CDF change ops do.
        skip = None
        if config.change == "verify_customer":
            pending = _scalar(
                databricks,
                token,
                f"SELECT count(*) FROM {cfg.kyc_fqn} k JOIN {cfg.customers_fqn} c ON c.customer_id = k.customer_id "
                "WHERE k.status = 'PENDING' AND c.is_active",
            )
            if not pending:
                skip = "no active customer with a PENDING KYC case left to verify -- run add_customer first"
        elif config.change == "deactivate_customer":
            eligible = _scalar(
                databricks,
                token,
                f"SELECT count(*) FROM {cfg.customers_fqn} c WHERE c.is_active AND c.customer_id IN "
                f"(SELECT k.customer_id FROM {cfg.kyc_fqn} k WHERE k.status = 'PENDING')",
            )
            if not eligible:
                skip = "no eligible customer left to deactivate -- run add_customer first"
        if skip:
            context.log.warning(f"{config.change} skipped: {skip}")
            context.add_output_metadata(
                {
                    "databricks_warehouse_seconds": MetadataValue.float(meter_snapshot()["warehouse_seconds"]),
                    "databricks_statements": MetadataValue.int(0),
                    "databricks_change_sql": MetadataValue.json(
                        {"change": config.change, "sql": [], "skipped": skip}
                    ),
                }
            )
            return

        for statement in statements:
            databricks.run_sql(token, statement)
            context.log.info(f"{config.change}: {' '.join(statement.split())}")
        usage = meter_snapshot()
        context.add_output_metadata(
            {
                "databricks_warehouse_seconds": MetadataValue.float(usage["warehouse_seconds"]),
                "databricks_statements": MetadataValue.int(usage["statements"]),
                # The exact SQL the demo page shows under the run result.
                "databricks_change_sql": MetadataValue.json({"change": config.change, "sql": statements}),
            }
        )

    return make_change


# --- reset ---------------------------------------------------------------------


def _assert_reset_allowed(cfg: JobotronConfig) -> None:
    """Refuse to run unless every name this job drops looks like a demo object: the
    tables under reverse_etl_jobotron_, the topics/subjects under jobotron_demo_,
    and the scratch schema. Same guard idea as the CDF syncs' reset. (The table
    names are checked bare -- the FQNs start with the catalog.)"""
    names = (cfg.customers_table, cfg.kyc_table, *cfg.topics.values())
    bad = [name for name in names if not name.startswith((f"{BASE}_", "jobotron_demo_"))]
    if bad or cfg.schema != "sr_poc_external":
        raise RuntimeError(
            f"Refusing to reset the Jobotron demo: these do not look like demo objects: {bad} "
            f"(schema {cfg.schema!r})"
        )


def build_reset_job(cfg: JobotronConfig):
    @op(name=f"{BASE}_delete_topics", ins={"start": In(Nothing)})
    def delete_topics(context: OpExecutionContext, kafka: KafkaResource) -> None:
        _assert_reset_allowed(cfg)
        admin = kafka.admin_client()
        for topic in cfg.topics.values():
            if topic in admin.list_topics(timeout=20).topics:
                admin.delete_topics([topic], operation_timeout=30)[topic].result()
                context.log.info(f"Delete requested for topic {topic}")
            else:
                context.log.info(f"Topic {topic} did not exist")
        # Topic deletes are asynchronous; the setup job must not recreate while listed.
        deadline = time.monotonic() + 90
        remaining = set(cfg.topics.values())
        while remaining:
            remaining = {topic for topic in cfg.topics.values() if topic in admin.list_topics(timeout=20).topics}
            if not remaining:
                break
            if time.monotonic() > deadline:
                raise RuntimeError(f"Topics still listed after 90s: {sorted(remaining)}")
            time.sleep(3)
        context.log.info("Operator topics are gone")

    @op(name=f"{BASE}_delete_registry_subjects", ins={"start": In(Nothing)})
    def delete_registry_subjects(context: OpExecutionContext, http: HttpResource) -> None:
        _assert_reset_allowed(cfg)
        # Soft delete, then permanent (a soft-deleted subject still blocks re-registering).
        for topic in cfg.topics.values():
            for role in ("key", "value"):
                subject = f"{topic}-{role}"
                for params in ({}, {"permanent": "true"}):
                    resp = http.delete(
                        f"{cfg.schema_registry_url}/subjects/{subject}", params=params, timeout=30
                    )
                    if resp.status_code == 404:
                        context.log.info(f"Registry subject {subject} did not exist ({params or 'soft'})")
                    elif resp.ok:
                        context.log.info(f"Deleted registry subject {subject} ({'permanent' if params else 'soft'})")
                    else:
                        raise RuntimeError(
                            f"Deleting subject {subject} failed ({resp.status_code}): {resp.text[:500]}"
                        )

    @op(name=f"{BASE}_drop_tables", ins={"start": In(Nothing)})
    def drop_tables(context: OpExecutionContext, databricks: DatabricksResource) -> None:
        databricks.require_env()
        token = databricks.token()
        databricks.reset_meter()
        for fqn in (cfg.customers_fqn, cfg.kyc_fqn):
            databricks.run_sql(token, f"DROP TABLE IF EXISTS {fqn}")
            context.log.info(f"Dropped {fqn} (if it existed)")
        # Drop + recreate moves the managed tables to new storage locations; the
        # cached locations would then point at dead tables (the CDF syncs hit this).
        databricks.clear_location_cache()
        context.log.info("Cleared the location cache (the tables moved)")

    @op(name=f"{BASE}_drop_postgres_tables", ins={"start": In(Nothing)})
    def drop_postgres_tables(context: OpExecutionContext, postgres: PostgresResource) -> None:
        _assert_reset_allowed(cfg)
        conn = postgres.connect()
        try:
            with conn.cursor() as cur:
                # The demo consumer's per-operator tables carry the same names as
                # the topics (same guard covers them). Dropping them here means a
                # reset leaves NO stale rows behind even before the first consume
                # rebuilds them (CREATE IF NOT EXISTS + TRUNCATE).
                for topic in cfg.topics.values():
                    cur.execute(f"DROP TABLE IF EXISTS {topic}")
                    context.log.info(f"Dropped Postgres table {topic} (if it existed)")
            conn.commit()
        finally:
            conn.close()

    @job(
        name=cfg.reset_job,
        description=(
            f"Tear down the Jobotron demo: the per-operator Kafka topics ({', '.join(cfg.topics.values())}), "
            f"their Apicurio subjects, the Databricks tables {cfg.customers_fqn} / {cfg.kyc_fqn}, and the "
            f"consumer's Postgres tables. Then run {cfg.setup_job}, the first publish and the first consume "
            "for a fresh demo."
        ),
        executor_def=in_process_executor,
    )
    def reset_job():
        gone = delete_topics()
        deleted = delete_registry_subjects(gone)
        dropped = drop_tables(deleted)
        drop_postgres_tables(dropped)

    return reset_job


# --- defs ----------------------------------------------------------------------


def build_jobotron_defs(cfg: JobotronConfig) -> Definitions:
    setup = build_setup_asset(cfg)
    publish = build_publish_asset(cfg)
    consume = build_consume_asset(cfg)
    change = build_change_asset(cfg)
    setup_job = define_asset_job(
        name=cfg.setup_job,
        selection=AssetSelection.assets(AssetKey(cfg.setup_asset)),
        description="Create the Jobotron demo tables and operator topics, then seed the demo customers.",
        executor_def=in_process_executor,
    )
    change_job = define_asset_job(
        name=cfg.change_job,
        selection=AssetSelection.assets(AssetKey(cfg.change_asset)),
        executor_def=in_process_executor,
    )
    return Definitions(
        assets=[setup, publish, consume, change],
        jobs=[setup_job, build_reset_job(cfg), change_job],
    )
