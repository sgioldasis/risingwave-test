"""The Jobotron producer demo: candidate query over Delta tables, Avro publish per operator topic, the demo consumer,
the change ops, setup and reset. The Delta tables are real (delta-rs, in tmp dirs, synthetic rows); Kafka, the registry,
Postgres and Databricks are fakes.

Run from the repo root:  .venv/bin/python -m pytest
"""

import types
from pathlib import Path

import dagster as dg
import pyarrow as pa
import pytest
import yaml
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from deltalake import DeltaTable as RealDeltaTable
from deltalake import write_deltalake

from orchestration.assets import reverse_etl_jobotron as jobotron
from orchestration.components.reverse_etl_jobotron import JobotronProducer
from tests.fakes import FakeDatabricks, FakeHttp, FakeKafka, FakeSchemaRegistry, error_text, fake_resources

DEFS = Path(__file__).resolve().parent.parent / "orchestration" / "defs" / "reverse_etl_jobotron" / "defs.yaml"


@pytest.fixture
def cfg():
    """The configuration the demo ships with."""
    return JobotronProducer(**yaml.safe_load(DEFS.read_text())["attributes"]).to_config()


# --- the fakes ---------------------------------------------------------------------------------------------------------


class LakeDatabricks(FakeDatabricks):
    """Table locations per table name, and a count for the change ops' SELECT count(*) checks."""

    def __init__(self, calls, locations, count=1):
        super().__init__(calls)
        self.locations, self.count = locations, count

    def table_location_cached(self, token, cfg, fqn):
        return self.locations[fqn]

    def run_sql(self, token, statement):
        self.calls.append(("databricks", " ".join(statement.split())))
        if statement.lstrip().startswith("SELECT count"):
            return {"manifest": {"schema": {"columns": [{"name": "n"}]}}, "result": {"data_array": [[self.count]]}}
        return {}


class FakeAdmin:
    def __init__(self, topics=()):
        self.topics, self.created, self.deleted = set(topics), [], []

    def list_topics(self, timeout=0):
        return types.SimpleNamespace(topics={topic: None for topic in self.topics})

    def create_topics(self, new_topics):
        self.created += [new_topic.topic for new_topic in new_topics]
        self.topics |= {new_topic.topic for new_topic in new_topics}
        return {}

    def delete_topics(self, topics, operation_timeout=0):
        self.deleted += topics
        self.topics -= set(topics)
        return {topic: types.SimpleNamespace(result=lambda: None) for topic in topics}


class FakeConsumer:
    """Serves every message once. With `quiet_first` the first read comes back empty although the broker holds
    messages, the cold-consumer race the real code guards against with watermarks."""

    def __init__(self, messages, quiet_first=False):
        self.messages, self.quiet_first, self.delivered = messages, quiet_first, False
        self.assigned: list = []

    def assign(self, partitions):
        self.assigned = partitions

    def consume(self, num_messages=1, timeout=0):
        if self.quiet_first:
            self.quiet_first = False
            return []
        if self.delivered:
            return []
        self.delivered = True
        return [
            types.SimpleNamespace(
                topic=lambda topic=topic: topic,
                value=lambda value=value: value,
                error=lambda: None,
                timestamp=lambda index=index: (1, 1000 + index),
            )
            for index, (topic, value) in enumerate(self.messages)
        ]

    def get_watermark_offsets(self, partition, timeout=0, cached=True):
        return (0, len([1 for topic, _ in self.messages if topic == partition.topic]))

    def close(self):
        pass


class JobotronKafka(FakeKafka):
    """The producer's messages are what the consumer reads back."""

    def __init__(self, calls, topics=(), quiet_first=False, **options):
        super().__init__(calls, **options)
        self.admin, self.quiet_first, self.extra = FakeAdmin(topics), quiet_first, []

    def admin_client(self):
        return self.admin

    def consumer(self, group_id, extra=None):
        messages = [(topic, value) for topic, _, value in self.recording_producer.messages] + self.extra
        return FakeConsumer(messages, self.quiet_first)


class RecordingPostgres:
    """Statements and the rows of each executemany, in order."""

    def __init__(self):
        self.statements: list = []
        self.commits = 0

    def connect(self):
        stub = self

        class Cursor:
            def execute(self, sql, params=()):
                stub.statements.append(("execute", " ".join(sql.split())))

            def executemany(self, sql, rows):
                stub.statements.append(("executemany", " ".join(sql.split()), rows))

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        class Connection:
            def cursor(self):
                return Cursor()

            def commit(self):
                stub.commits += 1

            def close(self):
                pass

        return Connection()


class RegistryHttp(FakeHttp):
    """Answers each registry DELETE with the status `statuses[(subject, permanent)]`, 200 when it has none."""

    def __init__(self, calls, statuses=None):
        super().__init__(calls)
        self.statuses, self.deletes = statuses or {}, []

    def delete(self, url, params=None, timeout=30):
        permanent = bool(params)
        self.calls.append(("http", url))
        self.deletes.append((url.rsplit("/", 1)[1], permanent))
        status = self.statuses.get((url.rsplit("/", 1)[1], permanent), 200)
        return types.SimpleNamespace(status_code=status, ok=status < 300, text="registry error")


# --- the environment -----------------------------------------------------------------------------------------------------


def customers_table(rows):
    return pa.table(
        {
            "customer_id": pa.array([r[0] for r in rows], pa.int64()),
            "email": pa.array([r[1] for r in rows], pa.string()),
            "locale": pa.array([r[2] for r in rows], pa.string()),
            "operator": pa.array([r[3] for r in rows], pa.string()),
            "is_active": pa.array([r[4] for r in rows], pa.bool_()),
        }
    )


def kyc_table(rows):
    return pa.table({"customer_id": pa.array([r[0] for r in rows], pa.int64()), "status": pa.array([r[1] for r in rows], pa.string())})


# The setup asset's seed: six synthetic customers, four of them active with a PENDING case (1001 and 1002 on gr,
# 1004 and 1005 on br).
SEED_CUSTOMERS = [
    (1001, "c1001@example.gr", "el-GR", "gr", True),
    (1002, "c1002@example.gr", "el-GR", "gr", True),
    (1003, "c1003@example.gr", "el-GR", "gr", False),
    (1004, "c1004@example.br", "pt-BR", "br", True),
    (1005, "c1005@example.br", "pt-BR", "br", True),
    (1006, "c1006@example.br", "pt-BR", "br", True),
]
SEED_KYC = [(1001, "PENDING"), (1002, "PENDING"), (1003, "PENDING"), (1004, "PENDING"), (1005, "PENDING"), (1006, "VERIFIED")]


class Env:
    def __init__(self, cfg, tmp_path, calls):
        self.cfg, self.calls, self.tmp_path = cfg, calls, tmp_path
        self.registry = FakeSchemaRegistry(calls)
        self.kafka = JobotronKafka(calls, topics=cfg.topics.values())
        self.postgres = RecordingPostgres()
        self.http = RegistryHttp(calls)
        self.locations = {
            cfg.customers_fqn: "abfss://lake@testaccount.dfs.core.windows.net/customers",
            cfg.kyc_fqn: "abfss://lake@testaccount.dfs.core.windows.net/kyc_cases",
        }
        self.databricks = LakeDatabricks(calls, self.locations)
        self.write(SEED_CUSTOMERS, SEED_KYC)

    def write(self, customers, kyc):
        write_deltalake(str(self.tmp_path / "customers"), customers_table(customers), mode="overwrite")
        write_deltalake(str(self.tmp_path / "kyc_cases"), kyc_table(kyc), mode="overwrite")

    def resources(self):
        res = fake_resources(self.calls, registry=self.registry)
        res.update(
            kafka=self.kafka, postgres=self.postgres, http=self.http, databricks=self.databricks, registry=self.registry
        )
        return res

    def run(self, asset, **run_config):
        config = {"ops": {asset.key.to_user_string(): {"config": run_config}}} if run_config else None
        return dg.materialize([asset], resources=self.resources(), run_config=config, raise_on_error=False)

    def publish(self):
        return self.run(jobotron.build_publish_asset(self.cfg))

    def consume(self):
        return self.run(jobotron.build_consume_asset(self.cfg))

    def decoded(self, topic=None):
        """(topic, key, value) of every produced message, decoded with the registry."""
        deserializer = AvroDeserializer(self.registry.mock_client)
        out = []
        for message_topic, key, value in self.kafka.recording_producer.messages:
            if topic is None or message_topic == topic:
                out.append(
                    (
                        message_topic,
                        deserializer(key, SerializationContext(message_topic, MessageField.KEY)),
                        deserializer(value, SerializationContext(message_topic, MessageField.VALUE)),
                    )
                )
        return out


@pytest.fixture
def env(cfg, tmp_path, calls, monkeypatch):
    """Delta tables on local disk stand in for the abfss:// ones, with the Azure credentials left out."""

    def local_table(location, storage_options=None, version=None):
        return RealDeltaTable(str(tmp_path / location.rsplit("/", 1)[1]))

    monkeypatch.setattr(jobotron, "DeltaTable", local_table)
    monkeypatch.setattr(jobotron, "_azure_storage_options", lambda location: {})
    return Env(cfg, tmp_path, calls)


def metadata(result):
    (event,) = result.get_asset_materialization_events()
    return event.materialization.metadata


# --- Avro --------------------------------------------------------------------------------------------------------------


class TestAvro:
    @pytest.mark.parametrize(
        "arrow_type, avro_type",
        [
            (pa.int64(), "long"),
            (pa.int32(), "long"),
            (pa.bool_(), "boolean"),
            (pa.float64(), "double"),
            (pa.decimal128(10, 2), "double"),
            (pa.string(), "string"),
            (pa.timestamp("us"), "string"),
        ],
    )
    def test_arrow_types_map_to_avro_primitives(self, arrow_type, avro_type):
        assert jobotron._avro_type(arrow_type) == avro_type

    def test_the_schemas_follow_the_query_result_with_a_nullable_payload_and_a_run_envelope(self, cfg):
        result_schema = pa.schema([("customer_id", pa.int64()), ("email", pa.string())])
        key_schema, value_schema = jobotron._jobotron_schemas(cfg, result_schema)
        assert key_schema["fields"] == [{"name": "customer_id", "type": "long"}]
        assert [field["name"] for field in value_schema["fields"]] == ["run_id", "job_type", "customer_id", "email"]
        payload = value_schema["fields"][2:]
        assert all(field["type"][0] == "null" and field["default"] is None for field in payload)
        assert value_schema["fields"][0]["type"] == "string" and value_schema["fields"][1]["type"] == "string"


class TestProduceOne:
    def test_a_full_queue_is_served_and_retried(self):
        attempts, polls = [], []

        class Producer:
            def produce(self, topic, key, value, on_delivery):
                attempts.append(topic)
                if len(attempts) < 3:
                    raise BufferError("queue full")

            def poll(self, timeout):
                polls.append(timeout)

        jobotron._produce_one(Producer(), "t", b"k", b"v", [])
        assert len(attempts) == 3 and polls == [0.1, 0.1]

    def test_a_queue_that_stays_full_fails_after_the_deadline(self, monkeypatch):
        clock = iter([0, 0, 500])
        monkeypatch.setattr(jobotron.time, "monotonic", lambda: next(clock))

        class Producer:
            def produce(self, **kwargs):
                raise BufferError("queue full")

            def poll(self, timeout):
                pass

        with pytest.raises(RuntimeError, match="stayed full for 120s"):
            jobotron._produce_one(Producer(), "t", b"k", b"v", [])

    def test_a_rejected_delivery_is_collected(self):
        errors = []

        class Producer:
            def produce(self, topic, key, value, on_delivery):
                on_delivery("broker down", None)

        jobotron._produce_one(Producer(), "t", b"k", b"v", errors)
        assert errors == ["broker down"]


# --- publish -----------------------------------------------------------------------------------------------------------


class TestPublish:
    def test_each_selected_customer_goes_to_its_operators_topic(self, env):
        result = env.publish()
        assert result.success
        by_topic = {topic: sorted(key["customer_id"] for _, key, _ in env.decoded(topic)) for topic in env.cfg.topics.values()}
        assert by_topic == {"jobotron_demo_gr": [1001, 1002], "jobotron_demo_br": [1004, 1005]}
        assert metadata(result)["per_operator"].value == {"gr": 2, "br": 2}
        assert metadata(result)["rows_published"].value == 4

    def test_a_message_carries_the_envelope_and_only_the_selected_columns(self, env):
        result = env.publish()
        (value,) = [v for _, key, v in env.decoded("jobotron_demo_gr") if key["customer_id"] == 1001]
        assert value == {
            "run_id": result.run_id,
            "job_type": "kyc.unverified_pm",
            "customer_id": 1001,
            "email": "c1001@example.gr",
            "locale": "el-GR",
            "operator": "gr",
        }

    def test_a_customer_who_left_the_segment_is_no_longer_published(self, env):
        env.write(SEED_CUSTOMERS, [(1001, "VERIFIED")] + SEED_KYC[1:])
        env.publish()
        assert sorted(key["customer_id"] for _, key, _ in env.decoded()) == [1002, 1004, 1005]

    def test_the_four_subjects_are_registered_before_the_first_message(self, env, calls):
        env.publish()
        registered = [detail for kind, detail in calls if kind == "registry"]
        assert sorted(registered) == sorted(
            f"{topic}-{role}" for topic in env.cfg.topics.values() for role in ("key", "value")
        )

    def test_a_second_publish_registers_nothing_new(self, env, calls):
        env.publish()
        before = [detail for kind, detail in calls if kind == "registry"]
        env.publish()
        assert [detail for kind, detail in calls if kind == "registry"] == before

    def test_an_empty_result_still_registers_the_schemas_and_publishes_nothing(self, env, calls):
        env.write(SEED_CUSTOMERS, [(cid, "VERIFIED") for cid, _ in SEED_KYC])
        result = env.publish()
        assert result.success
        assert env.kafka.recording_producer.messages == []
        assert len([1 for kind, _ in calls if kind == "registry"]) == 4
        assert metadata(result)["rows_published"].value == 0

    def test_a_null_in_the_result_is_published_as_null(self, env):
        customers = [(1001, None, "el-GR", "gr", True)]
        env.write(customers, [(1001, "PENDING")])
        assert env.publish().success
        ((_, _, value),) = env.decoded()
        assert value["email"] is None

    def test_an_unknown_operator_stops_the_run_before_the_flush(self, env):
        env.write([(1001, "c@example.gr", "el-GR", "xx", True)], [(1001, "PENDING")])
        result = env.publish()
        assert not result.success
        assert "Unknown operator 'xx'" in error_text(result) and "cross-market" in error_text(result)
        assert env.kafka.recording_producer.flushes == 0

    def test_a_missing_operator_is_unknown_too(self, env):
        env.write([(1001, "c@example.gr", "el-GR", None, True)], [(1001, "PENDING")])
        assert "Unknown operator ''" in error_text(env.publish())

    def test_rejected_deliveries_fail_the_run(self, env):
        env.kafka = JobotronKafka(env.calls, topics=env.cfg.topics.values(), producer_fails=True)
        result = env.publish()
        assert not result.success
        assert "Kafka delivery failed: 4 rejected" in error_text(result)

    def test_an_unknown_scan_engine_is_refused(self, env, cfg):
        import dataclasses

        env.cfg = dataclasses.replace(cfg, scan_engine="spark")
        result = env.publish()
        assert not result.success and "Unknown scan_engine 'spark'" in error_text(result)

    def test_only_the_first_messages_are_echoed_as_previews(self, env):
        many = [(2000 + n, f"c{n}@example.gr", "el-GR", "gr", True) for n in range(25)]
        env.write(many, [(row[0], "PENDING") for row in many])
        result = env.publish()
        assert metadata(result)["rows_published"].value == 25
        assert len(metadata(result)["message_previews"].value) == jobotron.PREVIEW_LIMIT

    def test_a_query_over_a_column_that_is_not_scanned_fails_loudly(self, env, cfg):
        import dataclasses

        pruned = {"customers": (cfg.sources["customers"][0], ("customer_id", "email", "locale", "operator")), "kyc_cases": cfg.sources["kyc_cases"]}
        env.cfg = dataclasses.replace(cfg, sources=pruned)
        result = env.publish()
        assert not result.success and "is_active" in error_text(result)


# --- consume -----------------------------------------------------------------------------------------------------------


def inserts(postgres, topic):
    """The rows written to one operator's table by the last consume."""
    (rows,) = [statement[2] for statement in postgres.statements if statement[0] == "executemany" and topic in statement[1]]
    return sorted((row["customer_id"], row["operator"]) for row in rows)


class TestConsume:
    def test_the_current_segment_is_mirrored_into_one_table_per_operator(self, env):
        env.publish()
        result = env.consume()
        assert result.success
        assert inserts(env.postgres, "jobotron_demo_gr") == [(1001, "gr"), (1002, "gr")]
        assert inserts(env.postgres, "jobotron_demo_br") == [(1004, "br"), (1005, "br")]
        assert metadata(result)["rows_per_operator"].value == {"gr": 2, "br": 2}
        assert env.postgres.commits == 1

    def test_each_table_is_created_truncated_then_filled(self, env):
        env.publish()
        env.consume()
        steps = [statement[1].split(" (")[0].split(" VALUES")[0] for statement in env.postgres.statements if "jobotron_demo_gr" in statement[1]]
        assert steps == [
            "CREATE TABLE IF NOT EXISTS jobotron_demo_gr",
            "TRUNCATE jobotron_demo_gr",
            "INSERT INTO jobotron_demo_gr",
        ]

    def test_a_customer_who_left_the_segment_disappears_at_the_next_consume(self, env):
        env.publish()
        env.write(SEED_CUSTOMERS, [(1001, "VERIFIED")] + SEED_KYC[1:])
        env.publish()
        env.consume()
        assert inserts(env.postgres, "jobotron_demo_gr") == [(1002, "gr")]

    def test_an_operator_with_no_customers_in_the_latest_publish_is_emptied(self, env):
        """The latest publish is the newest across ALL topics: a topic without a message from it rebuilds empty
        instead of keeping an older run's snapshot."""
        env.publish()
        env.write(SEED_CUSTOMERS, [(1001, "VERIFIED"), (1002, "VERIFIED")] + SEED_KYC[2:])
        env.publish()
        result = env.consume()
        assert result.success
        assert metadata(result)["rows_per_operator"].value == {"gr": 0, "br": 2}

    def test_a_cold_consumer_that_reads_nothing_at_first_is_read_again(self, env):
        env.publish()
        env.kafka.quiet_first = True
        result = env.consume()
        assert metadata(result)["rows_per_operator"].value == {"gr": 2, "br": 2}

    def test_a_message_that_cannot_be_decoded_is_skipped(self, env):
        env.publish()
        env.kafka.extra = [("jobotron_demo_gr", b"not avro")]
        result = env.consume()
        assert result.success
        assert metadata(result)["rows_per_operator"].value == {"gr": 2, "br": 2}

    def test_nothing_published_yet_leaves_every_table_empty(self, env):
        result = env.consume()
        assert result.success
        assert metadata(result)["rows_per_operator"].value == {"gr": 0, "br": 0}


# --- the change ops ----------------------------------------------------------------------------------------------------


class TestChangeStatements:
    def test_add_customer_inserts_a_customer_and_a_pending_case(self, cfg):
        customers, kyc = [" ".join(s.split()) for s in jobotron._change_statements(cfg, "add_customer")]
        assert customers.startswith(f"INSERT INTO {cfg.customers_fqn} (customer_id, email, locale, operator, is_active)")
        assert kyc == f"INSERT INTO {cfg.kyc_fqn} (customer_id, status, updated_at) SELECT max(customer_id), 'PENDING', current_timestamp() FROM {cfg.customers_fqn}"

    def test_verify_customer_verifies_the_lowest_eligible_case(self, cfg):
        (statement,) = [" ".join(s.split()) for s in jobotron._change_statements(cfg, "verify_customer")]
        assert statement.startswith(f"UPDATE {cfg.kyc_fqn} SET status = 'VERIFIED'")
        assert "min(k.customer_id)" in statement and "k.status = 'PENDING' AND c.is_active" in statement

    def test_deactivate_customer_deactivates_the_lowest_eligible_customer(self, cfg):
        (statement,) = [" ".join(s.split()) for s in jobotron._change_statements(cfg, "deactivate_customer")]
        assert statement.startswith(f"UPDATE {cfg.customers_fqn} SET is_active = false")
        assert "c.is_active AND c.customer_id IN" in statement

    def test_an_unknown_change_is_refused(self, cfg):
        with pytest.raises(ValueError, match="got 'delete_all'"):
            jobotron._change_statements(cfg, "delete_all")


class TestChangeAsset:
    def change(self, env, name, count=1):
        env.databricks.count = count
        return env.run(jobotron.build_change_asset(env.cfg), change=name)

    def executed(self, calls):
        return [detail for kind, detail in calls if kind == "databricks" and not detail.startswith("SELECT count")]

    @pytest.mark.parametrize("name", ["add_customer", "verify_customer", "deactivate_customer"])
    def test_a_change_runs_its_statements_and_reports_them(self, env, calls, name):
        result = self.change(env, name)
        assert result.success
        sent = self.executed(calls)
        assert sent == [" ".join(s.split()) for s in jobotron._change_statements(env.cfg, name)]
        assert metadata(result)["databricks_change_sql"].value["change"] == name

    @pytest.mark.parametrize(
        "name, reason", [("verify_customer", "no active customer with a PENDING KYC case"), ("deactivate_customer", "no eligible customer")]
    )
    def test_a_change_with_nothing_to_do_ends_green_and_says_why(self, env, calls, name, reason):
        result = self.change(env, name, count=0)
        assert result.success
        report = metadata(result)["databricks_change_sql"].value
        assert report["sql"] == [] and reason in report["skipped"]
        assert self.executed(calls) == []

    def test_adding_a_customer_never_needs_a_check(self, env, calls):
        self.change(env, "add_customer", count=0)
        assert len(self.executed(calls)) == 2


# --- setup -------------------------------------------------------------------------------------------------------------


class TestSetup:
    def test_missing_topics_are_created_and_existing_ones_left(self, env):
        env.kafka.admin.topics = {"jobotron_demo_gr"}
        assert env.run(jobotron.build_setup_asset(env.cfg)).success
        assert env.kafka.admin.created == ["jobotron_demo_br"]

    def test_the_tables_are_plain_delta_without_deletion_vectors_and_the_seed_is_idempotent(self, env, calls):
        assert env.run(jobotron.build_setup_asset(env.cfg)).success
        sql = [detail for kind, detail in calls if kind == "databricks"]
        creates = [s for s in sql if s.startswith("CREATE TABLE IF NOT EXISTS")]
        assert len(creates) == 2 and all("'delta.enableDeletionVectors' = 'false'" in s for s in creates)
        merges = [s for s in sql if s.startswith("MERGE INTO")]
        assert len(merges) == 2 and all("WHEN NOT MATCHED THEN INSERT" in s and "WHEN MATCHED" not in s for s in merges)


# --- reset -------------------------------------------------------------------------------------------------------------


def run_reset(env, cfg=None):
    cfg = cfg or env.cfg
    defs = dg.Definitions(jobs=[jobotron.build_reset_job(cfg)], resources=env.resources())
    return defs.resolve_job_def(cfg.reset_job).execute_in_process(raise_on_error=False)


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(jobotron.time, "sleep", lambda seconds: None)


class TestReset:
    def test_everything_the_demo_made_is_removed(self, env, calls):
        assert run_reset(env).success
        assert sorted(env.kafka.admin.deleted) == ["jobotron_demo_br", "jobotron_demo_gr"]
        assert sorted(env.http.deletes) == sorted(
            (f"{topic}-{role}", permanent)
            for topic in env.cfg.topics.values()
            for role in ("key", "value")
            for permanent in (False, True)
        )
        sql = [detail for kind, detail in calls if kind == "databricks"]
        assert sql == [f"DROP TABLE IF EXISTS {env.cfg.customers_fqn}", f"DROP TABLE IF EXISTS {env.cfg.kyc_fqn}"]
        assert ("cache", "cleared") in calls
        assert [s[1] for s in env.postgres.statements] == [f"DROP TABLE IF EXISTS {t}" for t in env.cfg.topics.values()]
        assert env.postgres.commits == 1

    def test_the_registry_subjects_go_before_the_tables(self, env, calls):
        run_reset(env)
        kinds = [kind for kind, _ in calls if kind in ("http", "databricks")]
        assert kinds == ["http"] * 8 + ["databricks"] * 2

    def test_a_topic_that_does_not_exist_is_fine(self, env):
        env.kafka.admin.topics = set()
        assert run_reset(env).success
        assert env.kafka.admin.deleted == []

    def test_a_subject_that_does_not_exist_is_fine(self, env):
        env.http.statuses = {(f"{topic}-{role}", p): 404 for topic in env.cfg.topics.values() for role in ("key", "value") for p in (False, True)}
        assert run_reset(env).success

    def test_a_registry_failure_stops_the_teardown_before_the_tables(self, env, calls):
        env.http.statuses = {("jobotron_demo_gr-key", False): 500}
        result = run_reset(env)
        assert not result.success and "failed (500)" in error_text(result)
        assert not [1 for kind, _ in calls if kind == "databricks"]

    def test_a_topic_that_stays_listed_fails_after_the_deadline(self, env, monkeypatch):
        env.kafka.admin.delete_topics = lambda topics, operation_timeout=0: {t: types.SimpleNamespace(result=lambda: None) for t in topics}
        clock = iter(range(0, 10_000, 40))
        monkeypatch.setattr(jobotron.time, "monotonic", lambda: next(clock))
        result = run_reset(env)
        assert not result.success and "Topics still listed after 90s" in error_text(result)

    @pytest.mark.parametrize(
        "overrides",
        [{"topic_template": "production_{operator}"}, {"customers_table": "customers"}, {"kyc_table": "kyc_cases"}, {"schema": "gold"}],
    )
    def test_anything_that_does_not_look_like_a_demo_object_is_never_touched(self, env, cfg, calls, overrides):
        import dataclasses

        result = run_reset(env, dataclasses.replace(cfg, **overrides))
        assert not result.success and "Refusing to reset the Jobotron demo" in error_text(result)
        assert env.kafka.admin.deleted == [] and env.http.deletes == [] and env.postgres.statements == []
        assert not [1 for kind, _ in calls if kind == "databricks"]
