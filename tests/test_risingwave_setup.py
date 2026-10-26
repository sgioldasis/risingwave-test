"""The RisingWave target: connection and connector settings, the CREATE TABLE statement, and adding, dropping and renaming
columns on a table that already exists.

The database is a small stateful fake: it keeps the table's columns (what information_schema reports) and applies the
ADD COLUMN and DROP COLUMN statements it is sent, so a test can assert the columns left and the order of the statements.

Run from the repo root:  .venv/bin/python -m pytest
"""

import re

import dagster as dg
import pytest

from orchestration.assets import reverse_etl_risingwave_setup as rw
from orchestration.assets.reverse_etl_config import risingwave_target_config
from tests.fakes import FakeSchemaRegistry, error_text, fake_resources, make_config

FIELDS = [
    ("id", "int64", False),
    ("name", "string", True),
    ("seen", rw.ZONED_TIMESTAMP, True),
    ("born", rw.DATE, True),
]


def cfg_for(encoding="json", **overrides):
    """The slim RisingWave config the builders take (release design step 4), composed through
    the demo bridge — the same path the demo's builder uses."""
    return risingwave_target_config(
        make_config(
            source_columns=(), key_column="id", encoding=encoding, schema_registry_url="http://registry.invalid", **overrides
        ).targets_config(),
        source_fqn="de_dev.sr_poc.external",
    )


class StatefulRisingWave:
    """`columns` is {name: declared type}; None means the table does not exist."""

    def __init__(self, columns=None, count=0, calls=None):
        self.columns, self.count, self.calls = columns, count, calls
        self.statements: list[str] = []
        self.commits = 0

    def connect(self):
        database = self

        class Cursor:
            def __init__(self):
                self.result: list = []

            def execute(self, sql, params=()):
                text = " ".join(sql.split())
                database.statements.append(text)
                columns = database.columns or {}
                if text.startswith("SELECT column_name, data_type FROM information_schema.columns"):
                    self.result = list(columns.items())
                elif text.startswith("SELECT column_name FROM information_schema.columns"):
                    self.result = [(name,) for name in columns]
                elif text.startswith("SELECT COUNT(*)"):
                    self.result = [(database.count,)]
                elif match := re.search(r'ADD COLUMN "((?:[^"]|"")+)" (.*)$', text):
                    if database.calls is not None:
                        database.calls.append(("risingwave", "ALTER"))
                    database.columns[match.group(1).replace('""', '"')] = match.group(2)
                elif match := re.search(r'DROP COLUMN "((?:[^"]|"")+)"$', text):
                    del database.columns[match.group(1).replace('""', '"')]

            def fetchall(self):
                return self.result

            def fetchone(self):
                return self.result[0]

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        class Connection:
            def cursor(self):
                return Cursor()

            def commit(self):
                database.commits += 1

            def close(self):
                pass

        return Connection()

    def ddl(self):
        return [s for s in self.statements if s.startswith(("ALTER", "UPDATE", "CREATE"))]


def registrations(calls):
    return [detail for kind, detail in calls if kind == "registry"]


# --- connection and connector settings ---------------------------------------------------------------------------------


class TestConnection:
    def test_the_defaults_and_a_connect_timeout(self, monkeypatch):
        for name in ("RISINGWAVE_HOST", "RISINGWAVE_PORT", "RISINGWAVE_DB", "RISINGWAVE_USER", "RISINGWAVE_PASSWORD"):
            monkeypatch.delenv(name, raising=False)
        seen = {}
        monkeypatch.setattr(rw.psycopg2, "connect", lambda **kwargs: seen.update(kwargs) or "connection")
        assert rw._get_risingwave_connection() == "connection"
        assert seen == {"host": "frontend-node-0", "port": 4566, "database": "dev", "user": "root", "password": "", "connect_timeout": 10}

    def test_the_environment_overrides_every_setting_but_the_timeout(self, monkeypatch):
        monkeypatch.setenv("RISINGWAVE_HOST", "elsewhere")
        monkeypatch.setenv("RISINGWAVE_PORT", "5555")
        monkeypatch.setenv("RISINGWAVE_DB", "other")
        monkeypatch.setenv("RISINGWAVE_USER", "someone")
        monkeypatch.setenv("RISINGWAVE_PASSWORD", "not-a-real-password")
        seen = {}
        monkeypatch.setattr(rw.psycopg2, "connect", lambda **kwargs: seen.update(kwargs))
        rw._get_risingwave_connection()
        assert (seen["host"], seen["port"], seen["database"], seen["user"], seen["connect_timeout"]) == ("elsewhere", 5555, "other", "someone", 10)


class TestConnectorOptions:
    @pytest.fixture(autouse=True)
    def no_sasl(self, monkeypatch):
        for name in ("KAFKA_OUTPUT_SASL_USERNAME", "KAFKA_OUTPUT_SASL_PASSWORD", "KAFKA_OUTPUT_SASL_MECHANISM"):
            monkeypatch.delenv(name, raising=False)

    def test_a_missing_bootstrap_is_refused(self, monkeypatch):
        monkeypatch.delenv("KAFKA_OUTPUT_BOOTSTRAP", raising=False)
        with pytest.raises(ValueError, match="KAFKA_OUTPUT_BOOTSTRAP"):
            rw._kafka_connector_options(cfg_for())

    def test_without_credentials_the_connection_is_plain_and_reads_from_the_start(self, monkeypatch):
        monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")
        options = rw._kafka_connector_options(cfg_for())
        assert "connector = 'kafka'" in options and "scan.startup.mode = 'earliest'" in options
        assert f"topic = '{cfg_for().kafka_topic}'" in options and "properties.bootstrap.server = 'kafka.invalid:9093'" in options
        assert "sasl" not in options

    def test_credentials_turn_on_sasl_ssl_with_scram_sha_512_by_default(self, monkeypatch):
        monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")
        monkeypatch.setenv("KAFKA_OUTPUT_SASL_USERNAME", "fake-user")
        monkeypatch.setenv("KAFKA_OUTPUT_SASL_PASSWORD", "fake-password")
        options = rw._kafka_connector_options(cfg_for())
        assert "properties.security.protocol = 'SASL_SSL'" in options
        assert "properties.sasl.mechanism = 'SCRAM-SHA-512'" in options
        assert "properties.sasl.username = 'fake-user'" in options and "properties.sasl.password = 'fake-password'" in options

    def test_the_mechanism_can_be_chosen(self, monkeypatch):
        monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")
        monkeypatch.setenv("KAFKA_OUTPUT_SASL_USERNAME", "fake-user")
        monkeypatch.setenv("KAFKA_OUTPUT_SASL_MECHANISM", "PLAIN")
        assert "properties.sasl.mechanism = 'PLAIN'" in rw._kafka_connector_options(cfg_for())


# --- column definitions and CREATE TABLE -------------------------------------------------------------------------------


class TestColumnDefinitions:
    def test_an_avro_timestamp_is_read_as_text_with_a_generated_timestamptz_companion(self):
        assert rw._column_defs(cfg_for("avro"), ("seen", rw.ZONED_TIMESTAMP, True)) == [
            '"seen" VARCHAR',
            '"seen_ts" TIMESTAMPTZ AS "seen"::timestamptz',
        ]

    def test_a_json_timestamp_is_a_timestamptz(self):
        assert rw._column_defs(cfg_for("json"), ("seen", rw.ZONED_TIMESTAMP, True)) == ['"seen" TIMESTAMPTZ']

    def test_identifiers_are_quoted_and_their_quotes_doubled(self):
        assert rw._quote('odd"name') == '"odd""name"'


class TestCreateTable:
    def test_the_key_is_the_primary_key_and_json_uses_the_json_format(self, monkeypatch):
        monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")
        sql = " ".join(rw._create_table_sql(cfg_for("json"), FIELDS).split())
        assert f"CREATE TABLE IF NOT EXISTS {cfg_for().risingwave_table}" in sql
        assert '"id" BIGINT PRIMARY KEY, "name" VARCHAR, "seen" TIMESTAMPTZ, "born" DATE' in sql
        assert sql.endswith("FORMAT DEBEZIUM ENCODE JSON")

    def test_avro_names_the_registry_and_adds_the_generated_companion(self, monkeypatch):
        monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")
        sql = " ".join(rw._create_table_sql(cfg_for("avro"), FIELDS).split())
        assert '"seen" VARCHAR, "seen_ts" TIMESTAMPTZ AS "seen"::timestamptz' in sql
        assert sql.endswith("FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://registry.invalid')")


# --- registration fields -----------------------------------------------------------------------------------------------


class TestRegistrationFields:
    def test_columns_the_table_still_has_ride_along_with_their_mapped_type(self):
        existing = {"id": "bigint", "name": "character varying", "old_score": "double precision", "old_flag": "boolean", "odd": "jsonb"}
        extras = rw._registration_fields(cfg_for("avro"), FIELDS, existing)[len(FIELDS):]
        assert extras == [("old_score", "double", True), ("old_flag", "boolean", True), ("odd", "string", True)]

    def test_generated_companions_are_left_out(self):
        existing = {"id": "bigint", "seen": "character varying", "seen_ts": "timestamp with time zone"}
        assert rw._registration_fields(cfg_for("avro"), FIELDS, existing) == FIELDS


# --- adding columns ----------------------------------------------------------------------------------------------------


class TestAddMissingColumns:
    def run(self, table, encoding="json", fields=FIELDS, calls=None):
        calls = calls if calls is not None else []
        registry = FakeSchemaRegistry(calls)
        resources = fake_resources(calls, registry=registry)
        resources["risingwave"] = table
        return rw.add_missing_columns(resources["risingwave"], resources["registry"], cfg_for(encoding), fields), calls

    def test_a_table_that_does_not_exist_yet_is_left_to_the_create_asset(self):
        table = StatefulRisingWave(columns=None)
        added, _ = self.run(table)
        assert added == [] and table.ddl() == []

    def test_a_table_with_every_column_is_left_alone(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar", "seen": "timestamptz", "born": "date"})
        added, calls = self.run(table, "avro")
        assert added == [] and table.ddl() == [] and registrations(calls) == []

    def test_missing_columns_are_added_with_their_types(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar"})
        added, _ = self.run(table)
        assert added == ["seen", "born"]
        assert table.ddl() == [
            f'ALTER TABLE {cfg_for().risingwave_table} ADD COLUMN "seen" TIMESTAMPTZ',
            f'ALTER TABLE {cfg_for().risingwave_table} ADD COLUMN "born" DATE',
        ]
        assert table.commits == 1

    def test_json_registers_nothing(self):
        _, calls = self.run(StatefulRisingWave({"id": "bigint"}))
        assert registrations(calls) == []

    def test_avro_registers_the_new_schema_before_the_first_alter(self):
        calls: list = []
        table = StatefulRisingWave({"id": "bigint", "name": "varchar"}, calls=calls)
        added, _ = self.run(table, "avro", calls=calls)
        assert added == ["seen", "born"]
        assert [kind for kind, _ in calls] == ["registry", "registry", "risingwave", "risingwave", "risingwave"]  # both subjects first

    def test_an_avro_timestamp_adds_its_generated_companion_too(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar", "born": "date"})
        self.run(table, "avro")
        assert [s.split(" ADD COLUMN ")[1] for s in table.ddl()] == ['"seen" VARCHAR', '"seen_ts" TIMESTAMPTZ AS "seen"::timestamptz']


# --- dropping columns --------------------------------------------------------------------------------------------------


class TestDropColumns:
    def run(self, table, dropped):
        resources = fake_resources([])
        resources["risingwave"] = table
        return rw.drop_columns_from_risingwave(resources["risingwave"], cfg_for("avro"), dropped)

    def test_a_table_that_does_not_exist_has_nothing_to_drop(self):
        assert self.run(StatefulRisingWave(columns=None), ["name"]) == []

    def test_the_generated_companion_goes_before_its_column(self):
        table = StatefulRisingWave({"id": "bigint", "seen": "varchar", "seen_ts": "timestamptz", "name": "varchar"})
        assert self.run(table, ["seen"]) == ["seen_ts", "seen"]
        assert table.columns == {"id": "bigint", "name": "varchar"}

    def test_the_key_column_is_never_dropped(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar"})
        assert self.run(table, ["id", "name"]) == ["name"]
        assert "id" in table.columns

    def test_a_column_that_is_already_gone_is_skipped(self):
        table = StatefulRisingWave({"id": "bigint"})
        assert self.run(table, ["name"]) == [] and table.ddl() == []


# --- renaming columns --------------------------------------------------------------------------------------------------


class TestRenameColumns:
    def run(self, table, renames, row_fields, encoding="json", calls=None):
        calls = calls if calls is not None else []
        resources = fake_resources(calls, registry=FakeSchemaRegistry(calls))
        resources["risingwave"] = table
        applied = rw.rename_columns_in_risingwave(
            resources["risingwave"], resources["registry"], cfg_for(encoding), renames, row_fields
        )
        return applied, calls

    TITLE_FIELDS = [("id", "int64", False), ("title", "string", True)]
    table_name = cfg_for().risingwave_table

    def test_a_table_that_does_not_exist_has_nothing_to_rename(self):
        assert self.run(StatefulRisingWave(columns=None), [("name", "title")], self.TITLE_FIELDS)[0] == []

    def test_a_rename_adds_the_new_column_copies_the_data_then_drops_the_old_one(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar"})
        applied, _ = self.run(table, [("name", "title")], self.TITLE_FIELDS)
        assert applied == ["name -> title"]
        assert table.ddl() == [
            f'ALTER TABLE {self.table_name} ADD COLUMN "title" VARCHAR',
            f'UPDATE {self.table_name} SET "title" = "name"',
            f'ALTER TABLE {self.table_name} DROP COLUMN "name"',
        ]
        assert set(table.columns) == {"id", "title"}
        assert table.commits == 3  # each DDL commits before the next one, so a failed run can resume

    def test_an_avro_timestamp_rename_moves_the_companion_with_it(self):
        fields = [("id", "int64", False), ("moment", rw.ZONED_TIMESTAMP, True)]
        table = StatefulRisingWave({"id": "bigint", "seen": "varchar", "seen_ts": "timestamptz"})
        applied, _ = self.run(table, [("seen", "moment")], fields, "avro")
        assert applied == ["seen -> moment"]
        assert set(table.columns) == {"id", "moment", "moment_ts"}
        drops = [s for s in table.ddl() if "DROP COLUMN" in s]
        assert [s.split("DROP COLUMN ")[1] for s in drops] == ['"seen_ts"', '"seen"']  # the companion first

    def test_avro_registers_a_schema_that_still_has_the_old_name(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar"})
        _, calls = self.run(table, [("name", "title")], self.TITLE_FIELDS, "avro")
        assert len(registrations(calls)) == 2

    def test_json_registers_nothing(self):
        _, calls = self.run(StatefulRisingWave({"id": "bigint", "name": "varchar"}), [("name", "title")], self.TITLE_FIELDS)
        assert registrations(calls) == []

    def test_a_rename_that_was_already_applied_is_reported_without_touching_the_table(self):
        table = StatefulRisingWave({"id": "bigint", "title": "varchar"})
        applied, calls = self.run(table, [("name", "title")], self.TITLE_FIELDS, "avro")
        assert applied == ["name -> title"] and table.ddl() == [] and registrations(calls) == []

    def test_a_rename_of_a_column_the_table_never_had_is_skipped(self):
        table = StatefulRisingWave({"id": "bigint"})
        applied, _ = self.run(table, [("name", "title")], self.TITLE_FIELDS)
        assert applied == [] and table.ddl() == []

    def test_a_rename_to_a_name_the_source_does_not_carry_is_skipped(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar"})
        applied, _ = self.run(table, [("name", "title")], [("id", "int64", False)])
        assert applied == [] and table.ddl() == [] and "name" in table.columns

    def test_a_new_column_the_add_path_already_created_empty_is_still_filled_from_the_old_one(self):
        table = StatefulRisingWave({"id": "bigint", "name": "varchar", "title": "varchar"})
        applied, _ = self.run(table, [("name", "title")], self.TITLE_FIELDS)
        assert applied == ["name -> title"]
        assert table.ddl() == [f'UPDATE {self.table_name} SET "title" = "name"', f'ALTER TABLE {self.table_name} DROP COLUMN "name"']

    def test_the_key_column_is_never_dropped_by_a_rename(self):
        fields = [("ident", "int64", False)]
        table = StatefulRisingWave({"id": "bigint"})
        self.run(table, [("id", "ident")], fields)
        assert "id" in table.columns


# --- the table asset ---------------------------------------------------------------------------------------------------


class TestTableAsset:
    @pytest.fixture(autouse=True)
    def kafka_env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")
        monkeypatch.delenv("KAFKA_OUTPUT_SASL_USERNAME", raising=False)
        monkeypatch.setattr(rw, "get_row_fields", lambda location: FIELDS)

    def materialize(self, table, encoding, calls):
        resources = fake_resources(calls, registry=FakeSchemaRegistry(calls))
        resources["risingwave"] = table
        return dg.materialize(
            [rw.build_risingwave_table_asset(cfg_for(encoding))], resources=resources, raise_on_error=False
        )

    def test_the_table_is_created_from_the_sources_columns(self):
        table, calls = StatefulRisingWave(count=7), []
        assert self.materialize(table, "json", calls).success
        (create,) = [s for s in table.ddl() if s.startswith("CREATE TABLE IF NOT EXISTS")]
        assert '"id" BIGINT PRIMARY KEY' in create and create.endswith("FORMAT DEBEZIUM ENCODE JSON")
        assert registrations(calls) == []

    def test_the_table_topic_and_row_count_are_reported_as_metadata(self):
        result = self.materialize(StatefulRisingWave(count=7), "json", [])
        (event,) = result.get_asset_materialization_events()
        assert event.materialization.metadata["row_count_at_setup_time"].value == 7

    def test_avro_registers_its_schemas_before_the_table_exists(self):
        table, calls = StatefulRisingWave(), []
        assert self.materialize(table, "avro", calls).success
        assert len(registrations(calls)) == 2
        assert any("ENCODE AVRO" in s for s in table.ddl())

    def test_an_unreachable_frontend_fails_the_run_with_its_error(self):
        class Failing(StatefulRisingWave):
            def connect(self):
                raise RuntimeError("frontend unreachable")

        result = self.materialize(Failing(), "json", [])
        assert not result.success and "frontend unreachable" in error_text(result)
