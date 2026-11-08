"""Source drops and renames reach the targets through the connection fakes.

Run from the repo root:  .venv/bin/python -m pytest
"""

import json
from unittest import mock

import pytest

from orchestration.assets.reverse_etl_demo_hooks import _propagate_column_drops
from orchestration.assets.reverse_etl_config import starrocks_target_config
from tests.fakes import make_config

ROW_FIELDS = [("rid", "int64", False), ("keep", "string", True), ("new", "string", True)]
TARGET_COLUMNS = ["rid", "keep", "old", "gone"]


@pytest.fixture
def propagate(resources, monkeypatch):
    # The recreated Routine Load's DDL reads the producer's connection env.
    monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "broker:9096")
    monkeypatch.setenv("KAFKA_OUTPUT_SASL_USERNAME", "user")
    monkeypatch.setenv("KAFKA_OUTPUT_SASL_PASSWORD", "pw")

    def run(last_schema, column_ids=None, row_fields=None, propagate_drops=True, starrocks=False):
        res = resources(
            postgres_columns=TARGET_COLUMNS,
            risingwave_columns=TARGET_COLUMNS,
            starrocks_columns=TARGET_COLUMNS,
            # The routine load row the SR rename path reads its progress from before the rebuild.
            starrocks_routine_load=(
                {
                    "State": "RUNNING",
                    "Progress": json.dumps({"0": "3", "1": "OFFSET_ZERO"}),
                    "DataSourceProperties": json.dumps({"currentKafkaPartitions": "0,1"}),
                }
                if starrocks
                else None
            ),
        )
        cfg = make_config(propagate_drops=propagate_drops)
        starrocks_conn = starrocks_cfg = None
        if starrocks:
            cfg = make_config(
                propagate_drops=propagate_drops, targets=("risingwave", "postgres", "starrocks"),
                starrocks_table="reverse_etl_unittest_target", encoding="avro",
            )
            starrocks_conn = res["starrocks"]
            starrocks_cfg = starrocks_target_config(cfg.targets_config(), source_fqn=cfg.source_fqn)
        return _propagate_column_drops(
            res["postgres"], res["risingwave"], res["http"], res["registry"], cfg,
            row_fields or ROW_FIELDS[:2], json.dumps(last_schema), mock.MagicMock(), column_ids,
            starrocks_conn, starrocks_cfg,
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


def test_a_rename_reaches_starrocks_and_rebuilds_the_load_pinned_at_its_progress(calls, propagate):
    result = propagate(
        [{"name": "rid", "id": 1}, {"name": "keep", "id": 2}, {"name": "old", "id": 3}],
        column_ids={"rid": 1, "keep": 2, "new": 3},
        row_fields=ROW_FIELDS,
        starrocks=True,
    )
    assert result["renamed"] == ["old -> new"]
    sr = statements(calls, "starrocks")
    assert [s for s in sr if "RENAME COLUMN `old` TO `new`" in s]
    create = [s for s in sr if s.startswith("CREATE ROUTINE LOAD")]
    assert len(create) == 1
    # The fresh load carries the NEW field name in its jsonpaths and pins the old job's
    # progress: partition 0 at offset 3, partition 1 (OFFSET_ZERO) at 0 -- nothing replays.
    assert '"kafka_partitions" = \'0,1\'' in create[0]
    assert '"kafka_offsets" = \'3,0\'' in create[0]
    assert '$.after.new' in create[0]
    assert [s for s in sr if s.startswith("DROP ROUTINE LOAD")]


def test_a_drop_reaches_starrocks_but_never_the_key_column(calls, propagate):
    # A recorded column with no id match is a drop everywhere; the key column is the table
    # model's anchor and is skipped.
    result = propagate(
        [{"name": "rid", "id": 1}, {"name": "keep", "id": 2}, {"name": "gone", "id": 3}],
        row_fields=ROW_FIELDS[:2],
        starrocks=True,
    )
    sr = statements(calls, "starrocks")
    assert [s for s in sr if "DROP COLUMN `gone`" in s]
    assert not [s for s in sr if "DROP COLUMN `rid`" in s]
    assert result["dropped"]["starrocks"] == ["gone"]


def test_starrocks_is_untouched_when_it_is_not_selected(calls, propagate):
    propagate(
        [{"name": "rid", "id": 1}, {"name": "keep", "id": 2}, {"name": "old", "id": 3}],
        column_ids={"rid": 1, "keep": 2, "new": 3},
        row_fields=ROW_FIELDS,
    )
    assert statements(calls, "starrocks") == []
