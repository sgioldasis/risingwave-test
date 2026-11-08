"""The StarRocks target's schema-change care: the before_produce branch that keeps the table
ahead of the messages (add_missing_columns_to_starrocks, the SR counterpart of the
RisingWave one) and the after_delivery propagation (propagate_schema_changes_to_starrocks,
whose hooks-level behaviour tests/test_propagation.py covers through the fakes).

Run from the repo root:  .venv/bin/python -m pytest
"""

import json

import pytest

from orchestration.assets.reverse_etl_config import StarrocksTargetConfig
from orchestration.assets.reverse_etl_starrocks_target import (
    add_missing_columns_to_starrocks,
    propagate_schema_changes_to_starrocks,
)
from tests.fakes import FakeStarrocks

ROW_FIELDS = [("rid", "int64", False), ("keep", "string", True), ("new", "string", True)]

RUNNING_LOAD = {
    "State": "RUNNING",
    "Progress": json.dumps({"0": "7"}),
    "DataSourceProperties": json.dumps({"currentKafkaPartitions": "0,1,2"}),
}


@pytest.fixture
def sr_env(monkeypatch):
    """The load DDL reads the producer's connection env."""
    monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "broker:9096")
    monkeypatch.setenv("KAFKA_OUTPUT_SASL_USERNAME", "user")
    monkeypatch.setenv("KAFKA_OUTPUT_SASL_PASSWORD", "pw")


def cfg() -> StarrocksTargetConfig:
    return StarrocksTargetConfig(
        label="unittest", sync_name="reverse_etl_unittest", kafka_topic="reverse_etl_unittest_topic",
        key_column="rid", schema_registry_url="http://registry.invalid",
        starrocks_table="reverse_etl_unittest_target", encoding="avro",
        source_fqn="de_dev.sr_poc_external.reverse_etl_unittest_source",
    )


def statements(calls) -> list:
    return [text for kind, text in calls if kind == "starrocks"]


def test_a_new_column_is_added_and_the_load_rebuilt_from_the_beginning(calls, sr_env):
    res = FakeStarrocks(calls, columns=["rid", "keep"], routine_load=RUNNING_LOAD)
    added = add_missing_columns_to_starrocks(res, cfg(), ROW_FIELDS)
    assert added == ["new"]
    sql = statements(calls)
    assert [s for s in sql if "ADD COLUMN `new` VARCHAR(65533)" in s]
    create = [s for s in sql if s.startswith("CREATE ROUTINE LOAD")]
    assert len(create) == 1 and '"kafka_offsets" = \'0,0,0\'' in create[0]  # beginning: the replay lands missed events
    assert '$.after.new' in create[0]


def test_nothing_is_touched_when_the_table_already_mirrors_the_source(calls, sr_env):
    res = FakeStarrocks(calls, columns=["rid", "keep", "new"], routine_load=RUNNING_LOAD)
    assert add_missing_columns_to_starrocks(res, cfg(), ROW_FIELDS) == []
    sql = statements(calls)
    assert not [s for s in sql if "ADD COLUMN" in s]
    assert not [s for s in sql if s.startswith(("DROP ROUTINE LOAD", "CREATE ROUTINE LOAD"))]


def test_a_rename_preserves_data_and_the_load_resumes_at_its_progress(calls, sr_env):
    res = FakeStarrocks(calls, columns=["rid", "keep", "old"], routine_load=RUNNING_LOAD)
    applied = propagate_schema_changes_to_starrocks(res, cfg(), [("old", "new")], [], ROW_FIELDS)
    assert applied == {"renamed": ["old -> new"], "dropped": []}
    sql = statements(calls)
    assert [s for s in sql if "RENAME COLUMN `old` TO `new`" in s]
    create = [s for s in sql if s.startswith("CREATE ROUTINE LOAD")]
    assert len(create) == 1 and '"kafka_offsets" = \'7,0,0\'' in create[0]  # progress: nothing replays
    assert '$.after.new' in create[0]


def test_an_applied_rename_is_skipped_and_touches_no_load(calls, sr_env):
    res = FakeStarrocks(calls, columns=["rid", "keep", "new"], routine_load=RUNNING_LOAD)
    applied = propagate_schema_changes_to_starrocks(res, cfg(), [("old", "new")], [], ROW_FIELDS)
    assert applied == {"renamed": ["old -> new"], "dropped": []}
    sql = statements(calls)
    assert not [s for s in sql if "RENAME COLUMN" in s]
    assert not [s for s in sql if s.startswith(("DROP ROUTINE LOAD", "CREATE ROUTINE LOAD"))]


def test_a_drop_removes_the_column_and_the_key_is_anchored(calls, sr_env):
    res = FakeStarrocks(calls, columns=["rid", "keep", "gone"], routine_load=RUNNING_LOAD)
    applied = propagate_schema_changes_to_starrocks(res, cfg(), [], ["gone", "rid"], ROW_FIELDS)
    assert applied == {"renamed": [], "dropped": ["gone"]}
    sql = statements(calls)
    assert [s for s in sql if "DROP COLUMN `gone`" in s]
    assert not [s for s in sql if "DROP COLUMN `rid`" in s]
