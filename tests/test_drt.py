"""The drt comparison syncs: names, the sync-file check, the Postgres target's setup and reset, and the asset's guards.

Run from the repo root:  .venv/bin/python -m pytest
"""

import dataclasses
import os
from pathlib import Path

import dagster as dg
import pytest
import yaml

from orchestration.assets import reverse_etl_drt as drt
from orchestration.assets.reverse_etl_config import ReverseEtlSyncConfig
from tests.fakes import bind_connections, error_text, make_config

COMPANY = ReverseEtlSyncConfig.for_name(
    "company", catalog="de_dev", schema="sr_poc_external", source_columns=("Company_ID BIGINT",), key_column="Company_ID"
)


def sync_file_text(**overrides) -> str:
    doc = {
        "name": "reverse_etl_unittest_drt",
        "model": "SELECT * FROM de_dev.sr_poc_external.reverse_etl_unittest_source",
        "destination": {"table": "reverse_etl_unittest_drt_target", "upsert_key": ["rid"]},
    }
    doc.update(overrides)
    return yaml.safe_dump(doc)


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A drt project directory holding one sync file; returns a function that writes its text."""
    (tmp_path / "syncs").mkdir()
    monkeypatch.setattr(drt, "PROJECT_SOURCE", tmp_path)

    def write(text: str) -> None:
        (tmp_path / "syncs" / "reverse_etl_unittest_drt.yml").write_text(text)

    return write


class TestNames:
    def test_every_name_is_derived_from_the_syncs_name(self):
        names = drt.DrtNames.for_config(make_config())
        assert (names.sync_name, names.asset, names.target_table) == (
            "reverse_etl_unittest_drt",
            "reverse_etl_unittest_drt_to_postgres",
            "reverse_etl_unittest_drt_target",
        )
        assert (names.setup_job, names.reset_job) == ("reverse_etl_unittest_drt_setup_job", "reverse_etl_unittest_drt_reset_job")

    def test_the_sync_file_is_named_after_the_sync_in_the_project(self, project):
        assert drt.DrtNames.for_config(make_config()).sync_file == drt.PROJECT_SOURCE / "syncs" / "reverse_etl_unittest_drt.yml"

    def test_the_shipped_company_sync_file_agrees_with_its_config(self):
        drt._check_sync_file(COMPANY, drt.DrtNames.for_config(COMPANY))


class TestSyncFileCheck:
    def test_a_file_that_agrees_passes(self, project):
        project(sync_file_text())
        cfg = make_config()
        drt._check_sync_file(cfg, drt.DrtNames.for_config(cfg))

    def test_a_missing_file_is_named(self, project):
        cfg = make_config()
        with pytest.raises(RuntimeError, match="reverse_etl_unittest_drt.yml not found"):
            drt._check_sync_file(cfg, drt.DrtNames.for_config(cfg))

    @pytest.mark.parametrize(
        "overrides, message",
        [
            ({"name": "other"}, "name is 'other'"),
            ({"destination": {"table": "elsewhere", "upsert_key": ["rid"]}}, "destination.table is 'elsewhere'"),
            ({"destination": {"table": "reverse_etl_unittest_drt_target", "upsert_key": ["id"]}}, r"upsert_key is \['id'\]"),
            ({"model": "SELECT * FROM somewhere.else"}, "model does not read de_dev.sr_poc_external.reverse_etl_unittest_source"),
        ],
    )
    def test_each_disagreement_is_reported(self, project, overrides, message):
        project(sync_file_text(**overrides))
        cfg = make_config()
        with pytest.raises(RuntimeError, match=message):
            drt._check_sync_file(cfg, drt.DrtNames.for_config(cfg))

    def test_all_disagreements_are_reported_together(self, project):
        project(sync_file_text(name="other", model="x"))
        cfg = make_config()
        with pytest.raises(RuntimeError) as error:
            drt._check_sync_file(cfg, drt.DrtNames.for_config(cfg))
        assert "name is 'other'" in str(error.value) and "model does not read" in str(error.value)


class TestProfile:
    def test_the_profile_holds_the_host_and_warehouse_and_names_the_token_variable_not_the_token(self, tmp_path, monkeypatch):
        monkeypatch.setattr(drt, "DATABRICKS_HOST", "https://adb-1.azuredatabricks.net/")
        monkeypatch.setattr(drt, "WAREHOUSE_ID", "wh123")
        drt._write_profile(tmp_path)
        text = (tmp_path / ".drt" / "profiles.yml").read_text()
        assert "server_hostname: adb-1.azuredatabricks.net\n" in text
        assert "http_path: /sql/1.0/warehouses/wh123\n" in text
        assert f"access_token_env: {drt.TOKEN_ENV}\n" in text
        assert "token:" not in text.replace("access_token_env:", "")


class PostgresStub:
    """Records statements; `keys_table_exists` is what to_regclass reports for drt's key table. With `fail`, opening a
    cursor raises, to show the connection is still closed."""

    def __init__(self, keys_table_exists=True, fail=False):
        self.statements: list[tuple[str, tuple]] = []
        self.keys_table_exists, self.fail, self.commits, self.closed = keys_table_exists, fail, 0, 0

    def connect(self):
        stub = self

        class Cursor:
            def execute(self, sql, params=()):
                stub.statements.append((" ".join(sql.split()), tuple(params)))

            def fetchone(self):
                return ("keys",) if stub.keys_table_exists else (None,)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        class Connection:
            def cursor(self):
                if stub.fail:
                    raise RuntimeError("boom")
                return Cursor()

            def commit(self):
                stub.commits += 1

            def close(self):
                stub.closed += 1

        return Connection()


def run_job(cfg, job_name, monkeypatch, resources, postgres):
    bind_connections(monkeypatch, {**resources(), "postgres": postgres})
    defs = dg.Definitions.merge(drt.build_drt_defs(cfg), dg.Definitions(resources={"drt": FakeDrt()}))
    return defs.resolve_job_def(job_name).execute_in_process(raise_on_error=False)


class TestSetupJob:
    def test_the_target_is_created_with_the_sources_columns_and_the_key_as_primary_key(self, monkeypatch, resources):
        monkeypatch.setattr(
            drt,
            "get_row_fields",
            lambda location: [
                ("Company_ID", "int64", False),
                ("name", "string", True),
                ("founded", "date", True),
                ("odd", "struct", True),
            ],
        )
        postgres = PostgresStub()
        assert run_job(COMPANY, "reverse_etl_company_drt_setup_job", monkeypatch, resources, postgres).success
        assert postgres.statements == [
            (
                'CREATE TABLE IF NOT EXISTS "reverse_etl_company_drt_target" '
                '("Company_ID" bigint PRIMARY KEY, "name" text, "founded" date, "odd" text)',
                (),
            )
        ]
        assert postgres.commits == 1 and postgres.closed == 1

    def test_the_connection_is_closed_when_the_create_fails(self, resources, monkeypatch):
        monkeypatch.setattr(drt, "get_row_fields", lambda location: [("Company_ID", "int64", False)])
        postgres = PostgresStub(fail=True)
        result = run_job(COMPANY, "reverse_etl_company_drt_setup_job", monkeypatch, resources, postgres)
        assert not result.success and "boom" in error_text(result)
        assert postgres.closed == 1


class TestResetJob:
    def test_the_target_is_dropped_and_only_this_syncs_key_rows_are_removed(self, monkeypatch, resources):
        postgres = PostgresStub()
        assert run_job(COMPANY, "reverse_etl_company_drt_reset_job", monkeypatch, resources, postgres).success
        assert postgres.statements == [
            ('DROP TABLE IF EXISTS "reverse_etl_company_drt_target"', ()),
            ("SELECT to_regclass(%s)", (drt.DRT_KEYS_TABLE,)),
            (f'DELETE FROM "{drt.DRT_KEYS_TABLE}" WHERE sync_name = %s', ("reverse_etl_company_drt",)),
        ]
        assert postgres.commits == 1

    def test_without_a_key_table_nothing_is_deleted(self, monkeypatch, resources):
        postgres = PostgresStub(keys_table_exists=False)
        assert run_job(COMPANY, "reverse_etl_company_drt_reset_job", monkeypatch, resources, postgres).success
        assert not [sql for sql, _ in postgres.statements if sql.startswith("DELETE")]

    def test_a_table_outside_the_allowlist_is_never_dropped(self, monkeypatch, resources):
        cfg = dataclasses.replace(COMPANY, reset_name_prefixes=("something_else_",))
        postgres = PostgresStub()
        result = run_job(cfg, "reverse_etl_company_drt_reset_job", monkeypatch, resources, postgres)
        assert not result.success and "Refusing to reset" in error_text(result)
        assert postgres.statements == []


class FakeDrt:
    """Stands in for DagsterDrtResource: `run` yields one result carrying the row counts."""

    def __init__(self, rows_failed=0):
        self.rows_failed, self.ran = rows_failed, 0

    def run(self, context):
        self.ran += 1
        yield dg.MaterializeResult(metadata={"rows_synced": dg.MetadataValue.int(5), "rows_failed": dg.MetadataValue.int(self.rows_failed)})


@pytest.fixture
def isolated_run(tmp_path, monkeypatch):
    """The asset writes a profile under the home directory, copies the project and sets two environment variables."""
    monkeypatch.setattr(drt, "WORKDIR", tmp_path / "workdir")
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    monkeypatch.setenv(drt.TOKEN_ENV, "unset")
    monkeypatch.setenv("PGTZ", "unset")
    return tmp_path


def materialize(monkeypatch, resources, fake_drt):
    bind_connections(monkeypatch, resources())
    asset = drt.build_drt_defs(COMPANY).assets[0]
    return dg.materialize([asset], resources={"drt": fake_drt}, raise_on_error=False)


class TestAsset:
    def test_a_run_gets_a_token_in_the_environment_a_profile_and_a_copy_of_the_project(self, monkeypatch, resources, isolated_run):
        fake = FakeDrt()
        assert materialize(monkeypatch, resources, fake).success
        assert fake.ran == 1
        assert os.environ[drt.TOKEN_ENV] == "token" and os.environ["PGTZ"] == "UTC"
        profile = isolated_run / "home" / ".drt" / "profiles.yml"
        assert profile.exists()
        assert (isolated_run / "workdir" / "syncs" / "reverse_etl_company_drt.yml").exists()
        assert "token" not in profile.read_text().replace("access_token_env", "")

    def test_failed_rows_fail_the_asset_although_drt_reports_success(self, monkeypatch, resources, isolated_run):
        result = materialize(monkeypatch, resources, FakeDrt(rows_failed=2))
        assert not result.success
        assert "failed 2 row(s)" in error_text(result)

    def test_a_sync_file_that_disagrees_stops_the_run_before_drt_starts(self, resources, isolated_run, monkeypatch):
        def disagree(cfg, names):
            raise RuntimeError("disagrees")

        monkeypatch.setattr(drt, "_check_sync_file", disagree)
        fake = FakeDrt()
        result = materialize(monkeypatch, resources, fake)
        assert not result.success and fake.ran == 0
