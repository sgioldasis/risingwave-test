"""The Jobotron producer on the real contract (docs/poc/JOBOTRON_CONTRACT_PARITY.md): the pure envelope
builder, the publish path (dry-run gate, JSON on the wire), the engines (databricks_sql through a faked
statement client, delta_rs/delta_extension over local Delta tables), the demo consumer, setup, reset and
the three demo defs. The Delta tables are real (delta-rs, in tmp dirs, synthetic rows); Kafka, Postgres,
Databricks and the statement client are fakes.

Run from the repo root:  .venv/bin/python -m pytest
"""

import dataclasses
import hashlib
import json
import types
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import pyarrow as pa
import pytest
import yaml
from deltalake import DeltaTable as RealDeltaTable
from deltalake import write_deltalake

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from orchestration.assets import reverse_etl_jobotron as jobotron
from orchestration.components.reverse_etl_jobotron import JobotronProducer
from tests.fakes import (
    FakeAdmin,
    FakeDatabricks,
    FakeKafka,
    bind_connections,
    error_text,
    fake_resources,
)

DEF_ROOT = Path(__file__).resolve().parent.parent / "orchestration" / "defs"
DEFS = {market: DEF_ROOT / f"unverified_customers_{market}" / "defs.yaml" for market in ("cy", "gr", "ro")}


def demo_attributes(market="cy"):
    return yaml.safe_load(DEFS[market].read_text())["attributes"]


def demo_cfg(market="cy"):
    return JobotronProducer(**demo_attributes(market)).to_config()


# --- the fakes ---------------------------------------------------------------------------------------------------------


class LakeDatabricks(FakeDatabricks):
    """Table locations per fqn for the zero-DBU engines (the statement client is faked at the
    module level, so run_sql is never reached)."""

    def __init__(self, calls, locations):
        super().__init__(calls)
        self.locations = locations

    def table_location_cached(self, token, cfg, fqn):
        return self.locations[fqn]


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
        super().__init__(calls, admin=FakeAdmin(topics), **options)
        self.quiet_first, self.extra = quiet_first, []

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


# The delta engines' demo tables: six synthetic customers, four of them active with a PENDING case
# (1001 and 1002 on gr, 1004 and 1005 on br). The query aliases the envelope's columns.
SEED_CUSTOMERS = [
    (1001, "c1001@example.gr", "el-GR", "gr", True),
    (1002, "c1002@example.gr", "el-GR", "gr", True),
    (1003, "c1003@example.gr", "el-GR", "gr", False),
    (1004, "c1004@example.br", "pt-BR", "br", True),
    (1005, "c1005@example.br", "pt-BR", "br", True),
    (1006, "c1006@example.br", "pt-BR", "br", True),
]
SEED_KYC = [(1001, "PENDING"), (1002, "PENDING"), (1003, "PENDING"), (1004, "PENDING"), (1005, "PENDING"), (1006, "VERIFIED")]

DELTA_QUERY = """
SELECT c.customer_id AS CustomerId, c.operator AS CompanyId, c.email, c.locale
FROM customers AS c
JOIN kyc_cases AS k ON k.customer_id = c.customer_id
WHERE k.status = 'PENDING' AND c.is_active
"""


def statement_response(columns, rows, next_chunk=None, total=None):
    """One SUCCEEDED statement result (the JSON_ARRAY wire shape: manifest schema + data_array).
    `columns` is (name, declared type) pairs; values arrive as strings."""
    return {
        "statement_id": "stmt-1",
        "manifest": {
            "format": "JSON_ARRAY",
            "total_row_count": total if total is not None else len(rows),
            "truncated": False,
            "schema": {"columns": [{"name": name, "type": type_name} for name, type_name in columns]},
        },
        "status": {"state": "SUCCEEDED"},
        "result": {"data_array": rows, "next_chunk_index": next_chunk},
    }


CY_COLUMNS = [("CustomerId", "int"), ("CompanyId", "int"), ("email", "string"), ("locale", "string")]


class Env:
    """The fake environment both engines run against: local Delta tables stand in for the abfss://
    ones (delta engines), and the statement client is faked at the module level (databricks_sql).
    The warehouse id comes from the environment like the real run."""

    def __init__(self, cfg, tmp_path, calls, monkeypatch, delta=False):
        self.cfg, self.calls, self.tmp_path, self.monkeypatch = cfg, calls, tmp_path, monkeypatch
        self.kafka = JobotronKafka(calls, topics=[cfg.derived_topic])
        self.postgres = RecordingPostgres()
        self.locations = {}
        self.delta = delta
        self.statements: list[tuple[str, str]] = []
        self.chunk_responses: dict[int, dict] = {}
        self.submit_error: Exception | None = None
        self.poll_error: Exception | None = None
        if delta:
            # Plain local paths: both zero-DBU engines read them directly (delta_scan()
            # included), no Azure translation needed.
            self.locations = {fqn: str(tmp_path / fqn.rsplit(".", 1)[1]) for fqn, _ in cfg.sources.values()}
            self.write(SEED_CUSTOMERS, SEED_KYC)
        self.databricks = LakeDatabricks(calls, self.locations)
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-123")

    def write(self, customers, kyc):
        write_deltalake(str(self.tmp_path / "reverse_etl_jobotron_customers"), customers_table(customers), mode="overwrite")
        write_deltalake(str(self.tmp_path / "reverse_etl_jobotron_kyc_cases"), kyc_table(kyc), mode="overwrite")

    def fake_statement_client(self, response=None):
        """Route the databricks_sql engine through fakes: _submit_statement records every
        (statement, warehouse_id) and returns `response`; the chunk GETs that _with_all_chunks
        makes are served from chunk_responses; _poll is faked to raise (tests set poll_error)."""

        def fake_submit(token, statement, warehouse_id, catalog, schema):
            self.statements.append((statement, warehouse_id, catalog, schema))
            if self.submit_error:
                raise self.submit_error
            return response or statement_response(CY_COLUMNS, [])

        def fake_chunk_get(url, headers=None, timeout=None):
            index = int(url.rsplit("/", 1)[1])
            chunk = self.chunk_responses[index]
            return types.SimpleNamespace(raise_for_status=lambda: None, json=lambda: chunk)

        def fake_poll(token, statement_id, poll_interval=5, max_wait=600):
            if self.poll_error:
                raise self.poll_error
            raise AssertionError("the tests' responses are already terminal")

        self.monkeypatch.setattr(jobotron, "_submit_statement", fake_submit)
        self.monkeypatch.setattr(cdf_setup.requests, "get", fake_chunk_get)
        self.monkeypatch.setattr(jobotron, "_poll", fake_poll)

    def resources(self):
        res = fake_resources(self.calls)
        res.update(kafka=self.kafka, postgres=self.postgres, databricks=self.databricks)
        return res

    def run(self, asset, **run_config):
        config = {"ops": {asset.key.to_user_string(): {"config": run_config}}} if run_config else None
        bind_connections(self.monkeypatch, self.resources())
        return dg.materialize([asset], run_config=config, raise_on_error=False)

    def publish(self, **run_config):
        return self.run(jobotron.build_publish_asset(self.cfg), **run_config)

    def consume(self):
        return self.run(jobotron.build_consume_asset(self.cfg))

    def decoded(self, topic=None):
        """(topic, key, value) of every produced message, decoded from the wire JSON."""
        out = []
        for message_topic, key, value in self.kafka.recording_producer.messages:
            if topic is None or message_topic == topic:
                out.append((message_topic, key.decode(), json.loads(value)))
        return out

    def produced(self):
        """The producer was never constructed (dry-run / a pre-produce failure) when this is empty."""
        return self.kafka.recording_producer.messages


@pytest.fixture
def cfg():
    """The CY demo instance's configuration, straight from its defs.yaml."""
    return demo_cfg("cy")


@pytest.fixture
def delta_cfg(cfg):
    """The same instance on the zero-DBU engine: sources point at the local demo tables and the
    query aliases the envelope's columns."""
    return dataclasses.replace(
        cfg,
        scan_engine="delta_rs",
        dry_run=False,
        query=DELTA_QUERY,
        sources={
            "customers": (
                f"{cfg.catalog}.{cfg.schema}.reverse_etl_jobotron_customers",
                ("customer_id", "email", "locale", "operator", "is_active"),
            ),
            "kyc_cases": (f"{cfg.catalog}.{cfg.schema}.reverse_etl_jobotron_kyc_cases", ("customer_id", "status")),
        },
    )


@pytest.fixture
def env(cfg, tmp_path, calls, monkeypatch):
    """The databricks_sql engine: a faked statement client, no Delta tables."""

    def local_table(location, storage_options=None, version=None):
        return RealDeltaTable(str(tmp_path / location.rsplit("/", 1)[1]))

    monkeypatch.setattr(jobotron, "DeltaTable", local_table)
    monkeypatch.setattr(jobotron, "_azure_storage_options", lambda location: {})
    return Env(cfg, tmp_path, calls, monkeypatch)


@pytest.fixture
def delta_env(delta_cfg, tmp_path, calls, monkeypatch):
    """The delta_rs engine: real delta-rs over local Delta tables, the faked statement client unused."""

    def local_table(location, storage_options=None, version=None):
        return RealDeltaTable(str(tmp_path / location.rsplit("/", 1)[1]))

    monkeypatch.setattr(jobotron, "DeltaTable", local_table)
    monkeypatch.setattr(jobotron, "_azure_storage_options", lambda location: {})
    return Env(delta_cfg, tmp_path, calls, monkeypatch, delta=True)


def metadata(result):
    (event,) = result.get_asset_materialization_events()
    return event.materialization.metadata


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(jobotron.time, "sleep", lambda seconds: None)


# --- the envelope (the contract) ------------------------------------------------------------------------------------------


def rows_table(columns, rows):
    return pa.table({name: pa.array(row[index] for row in rows) for index, (name, _type) in enumerate(columns)})


def build(rows, cfg=None, **kwargs):
    return jobotron.build_messages(
        rows,
        cfg or demo_cfg(),
        job_created=kwargs.pop("job_created", datetime(2026, 10, 26, 13, 0, 0, tzinfo=timezone.utc)),
        run_id=kwargs.pop("run_id", "run-1"),
        dry_run=kwargs.pop("dry_run", True),
        **kwargs,
    )


CY_ROWS = [("CustomerId", "int"), ("CompanyId", "int"), ("email", "string")]


class TestBuildMessages:
    def test_every_message_carries_exactly_the_13_contract_fields_in_order(self):
        (message,) = build(rows_table(CY_ROWS, [[1001, 7, "c@example.gr"]]))
        assert list(message) == list(jobotron.ENVELOPE_FIELDS)
        assert list(message) == [
            "JobId", "JobName", "JobCreated", "RunId", "CompanyId", "CustomerId", "ItemsCount",
            "ItemNumber", "ItemId", "ItemPayload", "ItemPayloadHash", "IsDryRun", "SchemaVersion",
        ]

    def test_the_hash_is_sha256_over_the_pinned_canonical_json(self):
        """The golden value is computed here from the canon literal, not by the implementation."""
        (message,) = build(rows_table(CY_ROWS, [[1001, 7, "c@example.gr"]]))
        expected = hashlib.sha256(b'{"CustomerId":1001,"CompanyId":7,"email":"c@example.gr"}').hexdigest()
        assert message["ItemPayloadHash"] == expected
        assert message["ItemPayload"] == {"CustomerId": 1001, "CompanyId": 7, "email": "c@example.gr"}
        assert message["ItemId"] == f"{expected}-1"

    def test_the_run_identity_fields_come_from_the_inputs_and_the_row(self):
        (message,) = build(rows_table(CY_ROWS, [[1001, 7, "c@example.gr"]]))
        assert message["JobId"] == "Send_Email_to_NonVerified_CY_Customers_7_20261026130000"
        assert message["JobName"] == "Send_Email_to_NonVerified_CY_Customers"
        assert message["JobCreated"] == "2026-10-26T13:00:00Z"
        assert message["RunId"] == "run-1"
        assert message["CompanyId"] == 7
        assert message["CustomerId"] == "1001"  # the key, cast to string
        assert message["ItemsCount"] == 1
        assert message["ItemNumber"] == 1
        assert message["IsDryRun"] is True
        assert message["SchemaVersion"] == 1

    def test_itemnumber_follows_the_sorted_customer_ids(self):
        messages = build(rows_table(CY_ROWS, [[1003, 7, "c"], [1001, 7, "a"], [1002, 7, "b"]]))
        assert [message["CustomerId"] for message in messages] == ["1001", "1002", "1003"]
        assert [message["ItemNumber"] for message in messages] == [1, 2, 3]
        assert all(message["ItemsCount"] == 3 for message in messages)

    def test_rows_sharing_a_customer_tie_break_on_the_remaining_columns_stably(self):
        rows = rows_table(CY_ROWS, [[1001, 7, "z@example.gr"], [1001, 7, "a@example.gr"]])
        first, second = build(rows), build(rows)
        assert [message["ItemPayload"]["email"] for message in first] == ["a@example.gr", "z@example.gr"]
        assert [message["ItemId"] for message in first] == [message["ItemId"] for message in second]

    def test_a_missing_required_column_fails_naming_it_before_any_message_exists(self):
        table = pa.table({"CustomerId": pa.array([1001])})
        with pytest.raises(ValueError, match=r"missing the envelope's required column\(s\) \['CompanyId'\]"):
            build(table)

    def test_nulls_survive_into_the_payload_and_the_hash(self):
        (message,) = build(rows_table(CY_ROWS, [[1001, 7, None]]))
        assert message["ItemPayload"]["email"] is None
        expected = hashlib.sha256(b'{"CustomerId":1001,"CompanyId":7,"email":null}').hexdigest()
        assert message["ItemPayloadHash"] == expected

    def test_a_null_key_column_is_refused(self):
        with pytest.raises(ValueError, match="null CustomerId"):
            build(rows_table(CY_ROWS, [[None, 7, "c@example.gr"]]))
        with pytest.raises(ValueError, match="null CompanyId"):
            build(rows_table(CY_ROWS, [[1001, None, "c@example.gr"]]))

    def test_an_empty_result_builds_no_messages(self):
        assert build(rows_table(CY_ROWS, [])) == []

    def test_dry_run_false_reaches_the_envelope(self):
        (message,) = build(rows_table(CY_ROWS, [[1001, 7, "c@example.gr"]]), dry_run=False)
        assert message["IsDryRun"] is False

    def test_schema_version_is_the_configured_one(self):
        cfg = dataclasses.replace(demo_cfg(), schema_version=2)
        (message,) = build(rows_table(CY_ROWS, [[1001, 7, "c@example.gr"]]), cfg)
        assert message["SchemaVersion"] == 2


# --- publish on the databricks_sql engine ----------------------------------------------------------------------------------


class TestPublishSql:
    def test_the_query_is_submitted_verbatim_in_the_yaml_context_on_the_env_warehouse(self, env):
        env.fake_statement_client(statement_response(CY_COLUMNS, []))
        result = env.publish()
        assert result.success
        assert env.statements == [(env.cfg.query, "wh-123", "de", "voperdb")]

    def test_the_dry_run_default_builds_everything_and_sends_nothing(self, env):
        env.fake_statement_client(statement_response(CY_COLUMNS, [["1001", "7", "c1@example.gr", "el-GR"], ["1002", "7", "c2@example.gr", "el-GR"]]))
        result = env.publish()
        assert result.success
        assert env.produced() == []
        assert env.kafka.recording_producer.flushes == 0
        assert metadata(result)["items_count"].value == 2
        assert metadata(result)["dry_run"].value is True
        assert metadata(result)["warehouse_id"].value == "wh-123"
        assert "statement_seconds" in metadata(result)
        assert len(metadata(result)["message_previews"].value) == 2

    def test_dry_run_false_publishes_json_keyed_by_customer_id(self, env):
        env.cfg = dataclasses.replace(env.cfg, dry_run=False)
        env.fake_statement_client(statement_response(CY_COLUMNS, [["1001", "7", "c1@example.gr", "el-GR"]]))
        result = env.publish()
        assert result.success
        ((topic, key, value),) = env.decoded()
        assert topic == "jobotron_unverified_customers_cy"
        assert key == "1001"
        assert value["IsDryRun"] is False and value["CustomerId"] == "1001"
        assert value["JobName"] == "Send_Email_to_NonVerified_CY_Customers"
        assert value["RunId"] == result.run_id
        assert metadata(result)["dry_run"].value is False

    def test_the_wire_json_is_the_canon_the_hash_is_taken_over(self, env):
        env.cfg = dataclasses.replace(env.cfg, dry_run=False)
        env.fake_statement_client(statement_response(CY_COLUMNS, [["1001", "7", "c1@example.gr", "el-GR"]]))
        env.publish()
        (_, _, value), = env.decoded()
        assert hashlib.sha256(json.dumps(value["ItemPayload"], separators=(",", ":"), ensure_ascii=False).encode()).hexdigest() == value["ItemPayloadHash"]

    def test_int_columns_arrive_ints_not_json_strings(self, env):
        env.cfg = dataclasses.replace(env.cfg, dry_run=False)
        env.fake_statement_client(statement_response(CY_COLUMNS, [["1001", "7", "c1@example.gr", "el-GR"]]))
        env.publish()
        (_, _, value), = env.decoded()
        assert value["CompanyId"] == 7
        assert value["ItemPayload"]["CompanyId"] == 7

    def test_a_result_missing_the_envelope_columns_fails_before_any_produce(self, env):
        env.cfg = dataclasses.replace(env.cfg, dry_run=False)
        env.fake_statement_client(statement_response([("email", "string")], [["c@example.gr"]]))
        result = env.publish()
        assert not result.success and "CompanyId" in error_text(result) and "CustomerId" in error_text(result)
        assert env.produced() == []

    def test_a_missing_warehouse_id_fails_naming_the_variable(self, env, monkeypatch):
        monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
        result = env.publish()
        assert not result.success and "DATABRICKS_WAREHOUSE_ID" in error_text(result)
        assert env.produced() == []

    def test_a_statement_failure_produces_nothing(self, env):
        env.cfg = dataclasses.replace(env.cfg, dry_run=False)
        env.fake_statement_client(statement_response(CY_COLUMNS, [["1001", "7", "c", "el"]]))
        env.submit_error = RuntimeError("Statement failed: {'state': 'FAILED'}")
        result = env.publish()
        assert not result.success and "Statement failed" in error_text(result)
        assert env.produced() == []

    def test_a_poll_timeout_fails_the_run(self, env):
        env.fake_statement_client(
            {**statement_response(CY_COLUMNS, []), "status": {"state": "PENDING"}}
        )
        env.poll_error = TimeoutError("Statement stmt-1 did not complete within 600s")
        result = env.publish()
        assert not result.success and "did not complete" in error_text(result)
        assert env.produced() == []

    def test_a_poll_timeout_cancels_the_statement_before_failing(self, env):
        env.fake_statement_client(
            {**statement_response(CY_COLUMNS, []), "status": {"state": "RUNNING"}}
        )
        env.poll_error = TimeoutError("Statement stmt-1 did not complete within 600s")
        cancelled = []
        env.monkeypatch.setattr(jobotron, "_cancel_statement", lambda token, statement_id, log=None: cancelled.append(statement_id))
        result = env.publish()
        assert not result.success and "did not complete" in error_text(result)
        assert cancelled == ["stmt-1"]
        assert env.produced() == []

    def test_chunks_assemble_in_order_before_the_envelope_is_built(self, env):
        env.fake_statement_client(
            statement_response(CY_COLUMNS, [["1001", "7", "a@example.gr", "el-GR"]], next_chunk=1, total=3)
        )
        env.chunk_responses = {
            1: {"data_array": [["1002", "7", "b@example.gr", "el-GR"]], "next_chunk_index": 2},
            2: {"data_array": [["1003", "7", "c@example.gr", "el-GR"]], "next_chunk_index": None},
        }
        result = env.publish()
        assert result.success
        assert metadata(result)["items_count"].value == 3
        previews = metadata(result)["message_previews"].value
        assert [preview["key"] for preview in previews] == ["1001", "1002", "1003"]

    def test_only_the_first_messages_are_echoed_as_previews(self, env):
        rows = [[str(2000 + n), "7", f"c{n}@example.gr", "el-GR"] for n in range(25)]
        env.fake_statement_client(statement_response(CY_COLUMNS, rows))
        result = env.publish()
        assert metadata(result)["items_count"].value == 25
        assert len(metadata(result)["message_previews"].value) == jobotron.PREVIEW_LIMIT

    def test_an_unknown_scan_engine_is_refused(self, env):
        env.cfg = dataclasses.replace(env.cfg, scan_engine="spark")
        result = env.publish()
        assert not result.success and "Unknown scan_engine 'spark'" in error_text(result)

    def test_an_unimplemented_encoding_is_refused(self, env):
        env.cfg = dataclasses.replace(env.cfg, encoding="avro")
        result = env.publish()
        assert not result.success and "avro" in error_text(result) and "deferred" in error_text(result)


# --- publish on the zero-DBU engines ---------------------------------------------------------------------------------------


class TestPublishDelta:
    def test_each_selected_customer_is_published_keyed_by_customer_id(self, delta_env):
        result = delta_env.publish()
        assert result.success
        by_key = sorted(key for _, key, _ in delta_env.decoded())
        assert by_key == ["1001", "1002", "1004", "1005"]
        assert metadata(result)["items_count"].value == 4
        assert metadata(result)["dry_run"].value is False

    def test_the_envelope_wraps_the_selected_columns(self, delta_env):
        delta_env.publish()
        (_, key, value), = [m for m in delta_env.decoded() if m[1] == "1001"]
        assert key == "1001"
        assert value["CompanyId"] == "gr"
        assert value["ItemPayload"] == {"CustomerId": 1001, "CompanyId": "gr", "email": "c1001@example.gr", "locale": "el-GR"}
        assert value["JobId"] == f"Send_Email_to_NonVerified_CY_Customers_gr_{value['JobCreated'][:4]}{value['JobCreated'][5:7]}{value['JobCreated'][8:10]}{value['JobCreated'][11:13]}{value['JobCreated'][14:16]}{value['JobCreated'][17:19]}"

    def test_a_customer_who_left_the_segment_is_no_longer_published(self, delta_env):
        delta_env.write(SEED_CUSTOMERS, [(1001, "VERIFIED")] + SEED_KYC[1:])
        delta_env.publish()
        assert sorted(key for _, key, _ in delta_env.decoded()) == ["1002", "1004", "1005"]

    def test_the_dry_run_gate_applies_to_the_delta_engines_too(self, delta_env):
        delta_env.cfg = dataclasses.replace(delta_env.cfg, dry_run=True)
        result = delta_env.publish()
        assert result.success and delta_env.produced() == []
        assert metadata(result)["items_count"].value == 4 and metadata(result)["dry_run"].value is True

    def test_rejected_deliveries_fail_the_run(self, delta_env):
        delta_env.kafka = JobotronKafka(delta_env.calls, topics=[delta_env.cfg.derived_topic], producer_fails=True)
        result = delta_env.publish()
        assert not result.success
        assert "Kafka delivery failed: 4 rejected" in error_text(result)

    def test_a_query_over_a_column_that_is_not_scanned_fails_loudly(self, delta_env):
        delta_env.cfg = dataclasses.replace(
            delta_env.cfg,
            sources={
                "customers": (delta_env.cfg.sources["customers"][0], ("customer_id", "email", "locale", "operator")),
                "kyc_cases": delta_env.cfg.sources["kyc_cases"],
            },
        )
        result = delta_env.publish()
        assert not result.success and "is_active" in error_text(result)

    def test_delta_engines_without_sources_are_refused(self, env):
        env.cfg = dataclasses.replace(env.cfg, scan_engine="delta_rs", sources={})
        result = env.publish()
        assert not result.success and "sources" in error_text(result)

    def test_the_extension_engine_projects_only_the_allowlist(self, delta_env, monkeypatch):
        """delta_extension registers DuckDB views over delta_scan, projecting only the allowlisted
        columns -- the same query over an unlisted column fails at bind time (no Azure needed: the
        tables are local and the secret helper is stubbed)."""
        monkeypatch.setattr(jobotron, "_ensure_delta_scan_secret", lambda connection, location: None)
        delta_env.cfg = dataclasses.replace(delta_env.cfg, scan_engine="delta_extension", dry_run=True)
        result = delta_env.publish()
        assert result.success
        assert metadata(result)["items_count"].value == 4
        delta_env.cfg = dataclasses.replace(
            delta_env.cfg,
            sources={
                "customers": (delta_env.cfg.sources["customers"][0], ("customer_id", "email", "locale", "operator")),
                "kyc_cases": delta_env.cfg.sources["kyc_cases"],
            },
        )
        result = delta_env.publish()
        assert not result.success and "is_active" in error_text(result)


# --- the per-run mode switch ---------------------------------------------------------------------------------------------


class TestRunSwitch:
    def test_a_run_config_overrides_the_yaml_mode_to_publish(self, env):
        env.fake_statement_client(statement_response(CY_COLUMNS, [["1001", "7", "c1@example.gr", "el-GR"]]))
        result = env.publish(dry_run=False)
        assert result.success
        ((_, key, value),) = env.decoded()
        assert key == "1001" and value["IsDryRun"] is False
        assert metadata(result)["dry_run"].value is False

    def test_a_run_config_overrides_the_yaml_mode_to_dry_run(self, delta_env):
        delta_env.cfg = dataclasses.replace(delta_env.cfg, dry_run=False)
        result = delta_env.publish(dry_run=True)
        assert result.success and delta_env.produced() == []
        assert metadata(result)["dry_run"].value is True
        assert metadata(result)["items_count"].value == 4

    def test_a_blank_launch_takes_the_yaml_value(self, env):
        """No config, no override: the YAML default (dry-run) governs -- and schedules see the same."""
        env.fake_statement_client(statement_response(CY_COLUMNS, [["1001", "7", "c1@example.gr", "el-GR"]]))
        result = env.publish()
        assert result.success and env.produced() == []
        assert metadata(result)["dry_run"].value is True

    def test_setup_respects_the_run_switch(self, env):
        env.kafka.admin.topics = set()
        result = env.run(jobotron.build_setup_asset(env.cfg), dry_run=False)
        assert result.success
        assert env.kafka.admin.created == ["jobotron_unverified_customers_cy"]
        assert metadata(result)["dry_run"].value is False

    def test_setup_with_a_run_config_dry_run_is_still_a_no_op(self, delta_env):
        result = delta_env.run(jobotron.build_setup_asset(delta_env.cfg), dry_run=True)
        assert result.success
        assert delta_env.kafka.admin.created == []


# --- consume ---------------------------------------------------------------------------------------------------------------


def mirror_rows(postgres, topic):
    (rows,) = [statement[2] for statement in postgres.statements if statement[0] == "executemany" and topic in statement[1]]
    return sorted(row["customer_id"] for row in rows)


class TestConsume:
    def test_every_item_is_mirrored_with_its_envelope_fields(self, delta_env):
        delta_env.publish()
        result = delta_env.consume()
        assert result.success
        assert mirror_rows(delta_env.postgres, delta_env.cfg.derived_topic) == ["1001", "1002", "1004", "1005"]
        assert metadata(result)["items_count"].value == 4
        (statement,) = [s for s in delta_env.postgres.statements if s[0] == "execute"]
        assert "CREATE TABLE IF NOT EXISTS jobotron_unverified_customers_cy" in statement[1]
        assert "JSONB" in statement[1]

    def test_a_republished_item_is_deduped_on_item_id(self, delta_env):
        """The full eligible set is republished every run; the same item (same hash and number)
        keeps one mirror row."""
        delta_env.publish()
        delta_env.publish()
        result = delta_env.consume()
        assert metadata(result)["messages_read"].value == 8
        assert metadata(result)["items_count"].value == 4

    def test_new_content_is_a_new_item(self, delta_env):
        """ItemId carries the payload hash: a republished customer whose payload changed is a
        second mirror row, while the unchanged customers keep theirs."""
        delta_env.publish()
        changed = [(1001, "moved@example.gr", "el-GR", "gr", True)] + SEED_CUSTOMERS[1:]
        delta_env.write(changed, SEED_KYC)
        delta_env.publish()
        result = delta_env.consume()
        assert metadata(result)["messages_read"].value == 8
        assert metadata(result)["items_count"].value == 5

    def test_a_message_that_cannot_be_decoded_is_skipped(self, delta_env):
        delta_env.publish()
        delta_env.kafka.extra = [(delta_env.cfg.derived_topic, b"not json")]
        result = delta_env.consume()
        assert result.success
        assert metadata(result)["items_count"].value == 4

    def test_nothing_published_yet_leaves_the_table_empty(self, delta_env):
        result = delta_env.consume()
        assert result.success
        assert metadata(result)["items_count"].value == 0

    def test_a_cold_consumer_that_reads_nothing_at_first_is_read_again(self, delta_env):
        delta_env.publish()
        delta_env.kafka.quiet_first = True
        result = delta_env.consume()
        assert metadata(result)["items_count"].value == 4


# --- setup and reset --------------------------------------------------------------------------------------------------------


class TestSetup:
    def test_a_missing_topic_is_created(self, delta_env):
        delta_env.kafka.admin.topics = set()
        assert delta_env.run(jobotron.build_setup_asset(delta_env.cfg)).success
        assert delta_env.kafka.admin.created == ["jobotron_unverified_customers_cy"]

    def test_an_existing_topic_is_left_alone(self, delta_env):
        delta_env.kafka.admin.topics = {"jobotron_unverified_customers_cy"}
        assert delta_env.run(jobotron.build_setup_asset(delta_env.cfg)).success
        assert delta_env.kafka.admin.created == []

    def test_running_setup_twice_changes_nothing(self, delta_env):
        delta_env.kafka.admin.topics = set()
        assert delta_env.run(jobotron.build_setup_asset(delta_env.cfg)).success
        delta_env.kafka.admin.created = []
        assert delta_env.run(jobotron.build_setup_asset(delta_env.cfg)).success
        assert delta_env.kafka.admin.created == []

    def test_dry_run_setup_is_a_no_op_and_never_connects_to_kafka(self, env, calls):
        """Zero side effects under the dry-run default: no topic, not even a Kafka connection."""
        result = env.run(jobotron.build_setup_asset(env.cfg))
        assert result.success
        assert env.kafka.admin.created == []
        assert ("kafka", "admin") not in calls
        assert metadata(result)["created"].value is False


def run_reset(env):
    bind_connections(env.monkeypatch, env.resources())
    defs = dg.Definitions(jobs=[jobotron.build_reset_job(env.cfg)])
    return defs.resolve_job_def(env.cfg.reset_job).execute_in_process(raise_on_error=False)


class TestReset:
    def test_the_topic_and_the_mirror_table_are_removed(self, delta_env, calls):
        assert run_reset(delta_env).success
        assert delta_env.kafka.admin.deleted == ["jobotron_unverified_customers_cy"]
        assert [s[1] for s in delta_env.postgres.statements] == ["DROP TABLE IF EXISTS jobotron_unverified_customers_cy"]
        assert delta_env.postgres.commits == 1

    def test_a_topic_that_does_not_exist_is_fine(self, delta_env):
        delta_env.kafka.admin.topics = set()
        assert run_reset(delta_env).success
        assert delta_env.kafka.admin.deleted == []

    def test_a_topic_that_stays_listed_fails_after_the_deadline(self, delta_env, monkeypatch):
        delta_env.kafka.admin.delete_topics = lambda topics, operation_timeout=0: {t: types.SimpleNamespace(result=lambda: None) for t in topics}
        clock = iter(range(0, 10_000, 40))
        monkeypatch.setattr(jobotron.time, "monotonic", lambda: next(clock))
        result = run_reset(delta_env)
        assert not result.success and "still listed after 90s" in error_text(result)

    def test_anything_that_does_not_look_like_a_jobotron_object_is_never_touched(self, delta_env, calls):
        delta_env.cfg = dataclasses.replace(delta_env.cfg, topic="production_events")
        result = run_reset(delta_env)
        assert not result.success and "Refusing to reset the Jobotron instance" in error_text(result)
        assert delta_env.kafka.admin.deleted == [] and delta_env.postgres.statements == []


# --- the config surface and the three demo defs ------------------------------------------------------------------------------


class TestDefs:
    def test_retired_fields_are_rejected(self):
        attrs = {**demo_attributes(), "job_type": "kyc.unverified_pm"}
        with pytest.raises(Exception):  # pydantic ValidationError: unknown (retired) attribute
            JobotronProducer(**attrs)

    def test_retired_key_columns_and_operators_are_rejected_too(self):
        for retired in ({"operators": ["gr", "br"]}, {"key_columns": ["customer_id"]}, {"topic_template": "x_{operator}"}, {"scan_columns": {"customers": ["customer_id"]}}):
            with pytest.raises(Exception):
                JobotronProducer(**{**demo_attributes(), **retired})

    def test_the_three_demo_defs_load_without_name_collisions(self):
        built = []
        for market in ("cy", "gr", "ro"):
            producer = JobotronProducer(**demo_attributes(market))
            built.append(producer.build_defs(None))
        merged = dg.Definitions.merge(*built)
        dg.Definitions.validate_loadable(merged)
        asset_names = sorted(asset.key.to_user_string() for asset in merged.assets)
        assert asset_names == sorted(
            f"jobotron_unverified_customers_{market}{role}"
            for market in ("cy", "gr", "ro")
            for role in ("", "_setup", "_consume")
        )
        job_names = [job.name for job in merged.jobs]
        assert sorted(job_names) == sorted(
            f"jobotron_unverified_customers_{market}_{role}"
            for market in ("cy", "gr", "ro")
            for role in ("setup_job", "reset_job")
        )

    def test_the_demo_instances_need_nothing_beyond_the_six_typed_facts(self):
        for market in ("cy", "gr", "ro"):
            cfg = demo_cfg(market)
            assert cfg.scan_engine == "databricks_sql"
            assert cfg.encoding == "json"
            assert cfg.dry_run is True
            assert cfg.schema_version == 1
            assert cfg.sources == {}
            assert cfg.derived_topic == f"jobotron_unverified_customers_{market}"
        assert demo_cfg("cy").job_name == "Send_Email_to_NonVerified_CY_Customers"
        assert demo_cfg("gr").job_name == "Send_Email_to_NonVerified_GR_Customers"
        assert demo_cfg("ro").job_name == "Send_Email_to_NonVerified_RO_Customers"

    def test_the_demo_queries_are_the_notebooks_per_market_rules(self):
        assert "ccgGroupID = 1424" in demo_attributes("cy")["query"] and "cusCompanyID = 7" in demo_attributes("cy")["query"]
        assert "cusTypeID not in (2, 5, 6, 10, 11, 15)" in demo_attributes("gr")["query"] and "ccgGroupID = 1425" in demo_attributes("gr")["query"]
        assert "ccgGroupID = 1321" in demo_attributes("ro")["query"] and "cusCompanyID = 4" in demo_attributes("ro")["query"]

    def test_sources_without_a_columns_allowlist_are_refused(self):
        attrs = {**demo_attributes(), "sources": {"customers": {"table": "cCustomer"}}}
        with pytest.raises(ValueError, match="sources.customers.columns is required"):
            JobotronProducer(**attrs).to_config()

    def test_sources_resolve_to_fqns_under_the_instance_catalog(self):
        attrs = {
            **demo_attributes("cy"),
            "scan_engine": "delta_rs",
            "sources": {"cCustomer": {"columns": ["CustomerId"]}, "joined": {"table": "other_table", "columns": ["a"]}},
        }
        cfg = JobotronProducer(**attrs).to_config()
        assert cfg.sources["cCustomer"] == ("de.voperdb.cCustomer", ("CustomerId",))
        assert cfg.sources["joined"] == ("de.voperdb.other_table", ("a",))

    def test_sources_accept_absolute_fqns_for_shards_and_cross_catalog_dimensions(self):
        attrs = {
            **demo_attributes("cy"),
            "scan_engine": "delta_extension",
            "sources": {
                "cCustomer": {"table": "de.voperdb_cy.cCustomer", "columns": ["CustomerId"]},
                "mVerificationStatus": {"table": "de.metadatadb.mVerificationStatus", "columns": ["vstVerificationStatusID"]},
            },
        }
        cfg = JobotronProducer(**attrs).to_config()
        assert cfg.sources["cCustomer"] == ("de.voperdb_cy.cCustomer", ("CustomerId",))
        assert cfg.sources["mVerificationStatus"] == ("de.metadatadb.mVerificationStatus", ("vstVerificationStatusID",))

    def test_a_typed_topic_wins_over_the_derived_one(self):
        attrs = {**demo_attributes(), "topic": "jobotron_platform_owned"}
        assert JobotronProducer(**attrs).to_config().derived_topic == "jobotron_platform_owned"


# --- the produce loop ------------------------------------------------------------------------------------------------------


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
