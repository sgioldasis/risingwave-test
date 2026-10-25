"""What the sync asset sends, for every storage mode and both encodings: the first run, later commits, no-ops, resuming, dates, rejected deliveries and a new source column.

Run from the repo root:  .venv/bin/python -m pytest
"""



from tests.fakes import error_text
from tests.sync_asset.support import (
    DAY,
    DAY_NUMBER,
    TOPIC,
    events,
    metadata,
    row,
)



def test_the_first_run_sends_every_row_as_a_create_and_records_the_watermark(source, sync, state_rows):
    source.create(row(1, "a"), row(2, "b"), row(3, "c"))
    result, _, produced = sync()
    assert result.success
    assert sorted(events(produced)) == [("c", 1, None, "a", 0), ("c", 2, None, "b", 0), ("c", 3, None, "c", 0)]
    assert {p.topic for p in produced} == {TOPIC}
    assert [version for version, _ in state_rows] == [0]
    assert metadata(result, "events_synced") == 3


def test_later_commits_arrive_as_insert_update_and_delete_in_commit_order(source, sync, state_rows):
    source.create(row(1, "a"), row(2, "b"))
    sync()
    source.insert(row(4, "d"))  # version 1
    source.update(1, name="a2")  # version 2
    source.delete(2)  # version 3
    result, _, produced = sync()
    assert result.success
    assert events(produced) == [("c", 4, None, "d", 1), ("u", 1, "a", "a2", 2), ("d", 2, "b", None, 3)]
    assert state_rows[-1][0] == 3
    assert metadata(result, "op_counts") == {"c": 1, "u": 1, "d": 1}


def test_a_run_with_no_new_commits_produces_nothing_and_leaves_the_watermark(source, sync, calls, state_rows):
    source.create(row(1, "a"))
    sync()
    watermark, producers = list(state_rows), calls.count(("kafka", "producer"))
    result, _, produced = sync()
    assert result.success
    assert produced == []
    assert calls.count(("kafka", "producer")) == producers  # not even a producer was created
    assert state_rows == watermark
    assert metadata(result, "events_synced") == 0


def test_a_sync_resumes_after_the_stored_watermark(source, sync, state_rows):
    source.create(row(1, "a"), row(2, "b"))  # version 0
    source.update(1, name="a2")  # version 1
    source.delete(2)  # version 2
    state_rows.append((1, None))  # versions 0 and 1 were already synced
    result, _, produced = sync()
    assert result.success
    assert events(produced) == [("d", 2, "b", None, 2)]
    assert state_rows[-1][0] == 2


def test_dates_arrive_as_dates_in_both_encodings(source, sync, encoding):
    source.create(row(1, "a", DAY))
    result, _, produced = sync()
    assert result.success
    born = produced[0].value["after"]["born"]
    if encoding == "json":
        # Connect JSON: epoch days with the Debezium date schema
        after_fields = next(f for f in produced[0].schema["fields"] if f["field"] == "after")["fields"]
        field = next(f for f in after_fields if f["field"] == "born")
        assert (field["type"], field["name"]) == ("int32", "io.debezium.time.Date")
        assert born == DAY_NUMBER
    else:
        # Avro: an int with the date logical type, which the deserializer turns back into a date
        assert born == DAY


def test_a_rejected_delivery_fails_the_run_before_the_watermark_moves(source, sync, state_rows):
    source.create(row(1, "a"))
    result, _, _ = sync(producer_fails=True)
    assert not result.success
    assert "Kafka delivery" in error_text(result)
    assert state_rows == []  # the next run replays the same versions

    result, _, produced = sync()  # the broker is back
    assert result.success
    assert events(produced) == [("c", 1, None, "a", 0)]
    assert [version for version, _ in state_rows] == [0]


def test_a_new_source_column_is_added_to_risingwave_and_carried_in_the_messages(source, sync, calls, registry, encoding):
    source.create(row(1, "a"))
    sync()
    source.insert_with_new_column("email", {"id": 5, "name": "e", "born": None, "email": "e@example.com"})
    result, _, produced = sync()
    assert result.success
    assert [p.value["after"]["email"] for p in produced] == ["e@example.com"]
    added = [sql for kind, sql in calls if kind == "risingwave" and "ADD COLUMN" in sql]
    assert len(added) == 1 and "email" in added[0]
    assert metadata(result, "risingwave_columns_added") == ["email"]
    if encoding == "avro":  # the registry's latest value schema has the column RisingWave was just told about
        assert "email" in registry.mock_client.get_latest_version(f"{TOPIC}-value").schema.schema_str

