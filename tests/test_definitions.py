"""The whole project's definitions: every resource the reverse-ETL code asks for is registered.

The per-component tests live in tests/components/. This one imports orchestration.definitions, which needs the dbt
manifests, so it is skipped where they are missing (CI). No services needed. Run from the repo root:
.venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.constants import dbt_PROJECT_PATH, dbt_STARROCKS_PROJECT_PATH

RESOURCE_KEYS = ("postgres", "risingwave", "databricks", "kafka", "http", "registry")

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
