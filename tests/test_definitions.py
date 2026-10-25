"""What a sync registers: the component's config, the make_change asset and job, and valid definitions.

No services needed. Run from the repo root:  .venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.assets.reverse_etl_defs import build_reverse_etl_defs
from orchestration.components.reverse_etl_cdf_sync import ReverseEtlCdfSync
from orchestration.constants import dbt_PROJECT_PATH, dbt_STARROCKS_PROJECT_PATH
from tests.fakes import make_config

RESOURCE_KEYS = ("postgres", "risingwave", "databricks", "kafka", "http", "registry")


def job_names(defs) -> set:
    return {job.name for job in defs.resolve_all_job_defs()}


def asset_names(cfg) -> set:
    return {key.to_user_string() for asset in build_reverse_etl_defs(cfg).assets for key in asset.keys}


def test_source_columns_are_optional_and_default_to_empty():
    cfg = ReverseEtlCdfSync(name="company", catalog="de_dev", schema_name="sr_poc_external").to_config()
    assert cfg.source_columns == ()
    assert cfg.source_table == "reverse_etl_company_source"


def test_source_columns_are_passed_through_as_a_tuple():
    component = ReverseEtlCdfSync(
        name="orders", catalog="de_dev", schema_name="sr_poc_external", source_columns=["id BIGINT NOT NULL", "total DOUBLE"]
    )
    assert component.to_config().source_columns == ("id BIGINT NOT NULL", "total DOUBLE")


def test_a_sync_with_source_columns_gets_the_make_change_job_and_asset(defs_with_fakes):
    cfg = make_config()
    assert "reverse_etl_unittest_make_change_job" in job_names(defs_with_fakes(cfg))
    assert "reverse_etl_unittest_make_change" in asset_names(cfg)


def test_a_sync_without_source_columns_gets_neither(defs_with_fakes):
    cfg = make_config(source_columns=())
    assert "reverse_etl_unittest_make_change_job" not in job_names(defs_with_fakes(cfg))
    assert "reverse_etl_unittest_make_change" not in asset_names(cfg)


@pytest.mark.parametrize("source_columns", [("description STRING",), ()])
def test_the_setup_and_reset_jobs_exist_either_way(defs_with_fakes, source_columns):
    names = job_names(defs_with_fakes(make_config(source_columns=source_columns)))
    assert {"reverse_etl_unittest_setup_job", "reverse_etl_unittest_reset_job"} <= names


def test_a_sync_loads_as_valid_definitions_and_its_jobs_resolve(defs_with_fakes):
    cfg = make_config()
    defs = defs_with_fakes(cfg)
    dg.Definitions.validate_loadable(defs)
    assert defs.resolve_job_def(cfg.reset_job) is not None
    assert defs.resolve_job_def(cfg.setup_job) is not None


DBT_MANIFESTS = [dbt_PROJECT_PATH / "target" / "manifest.json", dbt_STARROCKS_PROJECT_PATH / "target" / "manifest.json"]


@pytest.mark.skipif(
    not all(path.exists() for path in DBT_MANIFESTS),
    reason="importing orchestration.definitions needs the dbt manifests (run `dbt parse` in dbt/ and dbt_starrocks/)",
)
def test_the_project_registers_every_resource_the_reverse_etl_code_asks_for():
    from orchestration.definitions import defs

    dg.Definitions.validate_loadable(defs)
    for key in RESOURCE_KEYS:
        assert key in defs.resources
