"""Setup guard, make_change registration and the reset order of the reverse-ETL pipeline.

External systems (Databricks, Postgres, Kafka) are replaced with recorders, so no service is needed.
Run from the repo root:  .venv/bin/python -m unittest discover -s tests -t .
"""

import dataclasses
import unittest
from unittest import mock

from dagster import build_asset_context, build_op_context

from orchestration.assets import reverse_etl_cdf_setup as setup_module
from orchestration.assets import reverse_etl_reset as reset_module
from orchestration.assets.reverse_etl_config import ReverseEtlSyncConfig
from orchestration.assets.reverse_etl_defs import build_reverse_etl_defs
from orchestration.components.reverse_etl_cdf_sync import ReverseEtlCdfSync


def make_config(source_columns=("description STRING",), key_column="rid") -> ReverseEtlSyncConfig:
    return ReverseEtlSyncConfig.for_name(
        "unittest", catalog="de_dev", schema="sr_poc_external", source_columns=source_columns, key_column=key_column
    )


def fake_postgres(calls: list):
    """A get_postgres_connection replacement that records the statements executed."""
    cursor = mock.MagicMock()
    cursor.execute.side_effect = lambda sql, params=None: calls.append(("postgres", " ".join(str(sql).split())))
    connection = mock.MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    return mock.MagicMock(return_value=connection)


class ComponentTests(unittest.TestCase):
    def test_source_columns_are_optional_and_default_to_empty(self):
        cfg = ReverseEtlCdfSync(name="company", catalog="de_dev", schema_name="sr_poc_external").to_config()
        self.assertEqual(cfg.source_columns, ())
        self.assertEqual(cfg.source_table, "reverse_etl_company_source")

    def test_source_columns_are_passed_through_as_a_tuple(self):
        component = ReverseEtlCdfSync(
            name="orders", catalog="de_dev", schema_name="sr_poc_external", source_columns=["id BIGINT NOT NULL", "total DOUBLE"]
        )
        self.assertEqual(component.to_config().source_columns, ("id BIGINT NOT NULL", "total DOUBLE"))


class MakeChangeRegistrationTests(unittest.TestCase):
    @staticmethod
    def job_names(cfg):
        return {job.name for job in build_reverse_etl_defs(cfg).resolve_all_job_defs()}

    @staticmethod
    def asset_names(cfg):
        return {key.to_user_string() for asset in build_reverse_etl_defs(cfg).assets for key in asset.keys}

    def test_a_sync_with_source_columns_gets_the_make_change_job_and_asset(self):
        cfg = make_config()
        self.assertIn("reverse_etl_unittest_make_change_job", self.job_names(cfg))
        self.assertIn("reverse_etl_unittest_make_change", self.asset_names(cfg))

    def test_a_sync_without_source_columns_gets_neither(self):
        cfg = make_config(source_columns=())
        self.assertNotIn("reverse_etl_unittest_make_change_job", self.job_names(cfg))
        self.assertNotIn("reverse_etl_unittest_make_change", self.asset_names(cfg))

    def test_the_other_jobs_exist_either_way(self):
        for cfg in (make_config(), make_config(source_columns=())):
            names = self.job_names(cfg)
            self.assertIn("reverse_etl_unittest_setup_job", names)
            self.assertIn("reverse_etl_unittest_reset_job", names)


class TableSetupGuardTests(unittest.TestCase):
    def run_setup(self, cfg, location_error=None):
        calls: list = []
        with mock.patch.object(setup_module, "_require_databricks_env"), mock.patch.object(
            setup_module, "_get_token", return_value="token"
        ), mock.patch.object(
            setup_module, "_run_sql", side_effect=lambda token, sql: calls.append(("sql", " ".join(sql.split())))
        ), mock.patch.object(
            setup_module,
            "get_table_location",
            side_effect=lambda token, c, fqn: (_ for _ in ()).throw(location_error) if location_error else calls.append(("location", fqn)) or "abfss://x",
        ), mock.patch.object(setup_module, "get_postgres_connection", fake_postgres(calls)):
            error = None
            try:
                setup_module.build_table_setup_asset(cfg)(build_asset_context())
            except Exception as exc:  # the test inspects it
                error = exc
        return calls, error

    def test_without_source_columns_an_existing_table_is_checked_not_created(self):
        cfg = make_config(source_columns=())
        calls, error = self.run_setup(cfg)
        self.assertIsNone(error)
        self.assertIn(("location", cfg.source_fqn), calls)
        self.assertFalse([c for c in calls if c[0] == "sql" and "CREATE TABLE" in c[1]])

    def test_without_source_columns_a_missing_table_fails_instead_of_being_created(self):
        cfg = make_config(source_columns=())
        calls, error = self.run_setup(cfg, location_error=RuntimeError("UC REST lookup failed (404)"))
        self.assertIsInstance(error, Exception)
        self.assertIn("404", str(error))
        self.assertFalse([c for c in calls if c[0] == "sql" and "CREATE TABLE" in c[1]])

    def test_with_source_columns_the_table_is_created_if_absent(self):
        cfg = make_config(source_columns=("description STRING",))
        calls, error = self.run_setup(cfg)
        self.assertIsNone(error)
        creates = [c for c in calls if c[0] == "sql" and "CREATE TABLE IF NOT EXISTS" in c[1]]
        self.assertEqual(len(creates), 1)
        self.assertIn(cfg.source_fqn, creates[0][1])
        self.assertIn("description STRING", creates[0][1])
        self.assertNotIn(("location", cfg.source_fqn), calls)


class ResetOrderTests(unittest.TestCase):
    """The watermark and the location cache are cleared BEFORE any Databricks call, so a failed drop
    cannot leave a stale watermark behind (the recreated source restarts at version 0)."""

    def drop_op(self, cfg):
        job = reset_module.build_reset_job(cfg)
        return next(node for node in job.graph.node_defs if node.name.endswith("_drop_databricks_tables"))

    def run_op(self, cfg, drop_error=None):
        calls: list = []

        def run_sql(token, sql):
            calls.append(("databricks", sql))
            if drop_error:
                raise drop_error

        with mock.patch.object(reset_module, "_require_databricks_env", side_effect=lambda: calls.append(("env", ""))), mock.patch.object(
            reset_module, "_get_token", return_value="token"
        ), mock.patch.object(reset_module, "meter_reset"), mock.patch.object(
            reset_module, "_run_sql", side_effect=run_sql
        ), mock.patch.object(
            reset_module, "clear_control_plane_cache", side_effect=lambda: calls.append(("cache", "cleared"))
        ), mock.patch.object(reset_module, "get_postgres_connection", fake_postgres(calls)):
            error = None
            try:
                self.drop_op(cfg)(build_op_context())
            except Exception as exc:
                error = exc
        return calls, error

    def kinds(self, calls):
        return [kind for kind, _ in calls]

    def test_watermark_and_cache_are_cleared_before_the_first_databricks_call(self):
        calls, error = self.run_op(make_config())
        self.assertIsNone(error)
        kinds = self.kinds(calls)
        self.assertLess(kinds.index("postgres"), kinds.index("databricks"))
        self.assertLess(kinds.index("cache"), kinds.index("databricks"))
        self.assertIn("DELETE FROM reverse_etl_sync_state", calls[kinds.index("postgres")][1])

    def test_both_databricks_tables_are_dropped_on_success(self):
        cfg = make_config()
        calls, _ = self.run_op(cfg)
        drops = [sql for kind, sql in calls if kind == "databricks"]
        self.assertEqual(drops, [f"DROP TABLE IF EXISTS {cfg.source_fqn}", f"DROP TABLE IF EXISTS {cfg.state_fqn}"])

    def test_a_failed_drop_still_leaves_the_watermark_cleared_and_fails_the_run(self):
        calls, error = self.run_op(make_config(), drop_error=RuntimeError("PERMISSION_DENIED: does not have MANAGE"))
        self.assertIsNotNone(error)
        self.assertIn("MANAGE", str(error))
        kinds = self.kinds(calls)
        self.assertIn("postgres", kinds)
        self.assertIn("cache", kinds)
        self.assertEqual(kinds.count("databricks"), 1)  # stopped at the first failing drop


if __name__ == "__main__":
    unittest.main()
