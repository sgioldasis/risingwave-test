"""Source drops and renames reach both targets through the resources.

Run from the repo root:  .venv/bin/python -m pytest
"""

import json
from unittest import mock

import pytest

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from tests.fakes import make_config

ROW_FIELDS = [("rid", "int64", False), ("keep", "string", True), ("new", "string", True)]
TARGET_COLUMNS = ["rid", "keep", "old", "gone"]


@pytest.fixture
def propagate(resources):
    def run(last_schema, column_ids=None, row_fields=None, propagate_drops=True):
        res = resources(postgres_columns=TARGET_COLUMNS, risingwave_columns=TARGET_COLUMNS)
        return cdf_setup._propagate_column_drops(
            res["postgres"], res["risingwave"], res["http"], res["registry"], make_config(propagate_drops=propagate_drops),
            row_fields or ROW_FIELDS[:2], json.dumps(last_schema), mock.MagicMock(), column_ids,
        )

    return run


def statements(calls, kind) -> list:
    return [detail for k, detail in calls if k == kind]


def test_a_dropped_source_column_is_dropped_from_both_targets(calls, propagate):
    result = propagate([{"name": "rid", "id": 1}, {"name": "keep", "id": 2}, {"name": "gone", "id": 3}])
    assert result["dropped"]["risingwave"] == ["gone"]
    # Postgres drops everything the source no longer mirrors (sorted)
    assert result["dropped"]["postgres"] == ["gone", "old"]
    assert [s for s in statements(calls, "risingwave") if "DROP COLUMN" in s and "gone" in s]
    assert [s for s in statements(calls, "postgres") if 'DROP COLUMN "gone"' in s]


def test_a_renamed_source_column_is_renamed_in_postgres_and_the_sink_task_restarted(calls, propagate):
    result = propagate(
        [{"name": "rid", "id": 1}, {"name": "keep", "id": 2}, {"name": "old", "id": 3}],
        column_ids={"rid": 1, "keep": 2, "new": 3},
        row_fields=ROW_FIELDS,
    )
    assert result["renamed"] == ["old -> new"]
    assert [s for s in statements(calls, "postgres") if 'RENAME COLUMN "old" TO "new"' in s]
    assert [s for s in statements(calls, "risingwave") if "UPDATE" in s and "old" in s]
    assert [s for s in statements(calls, "http") if s.startswith("POST") and "/tasks/0/restart" in s]


def test_nothing_is_touched_when_drop_propagation_is_off(calls, propagate):
    result = propagate([{"name": "gone", "id": 3}], propagate_drops=False)
    assert result["dropped"] == {"risingwave": [], "postgres": []}
    assert calls == []
