"""The sync asset's Avro messages: wire format and schema registration.

Run from the repo root:  .venv/bin/python -m pytest
"""

import struct

import pytest

from tests.sync_asset.support import (
    TOPIC,
    row,
)



@pytest.mark.parametrize("encoding", ["avro"])
def test_avro_messages_use_the_confluent_wire_format(source, sync, registry, encoding):
    source.create(row(1, "a"))
    _, _, produced = sync()
    magic, schema_id = struct.unpack(">bI", produced[0].raw_value[:5])
    assert magic == 0
    assert schema_id == registry.mock_client.get_latest_version(f"{TOPIC}-value").schema_id


@pytest.mark.parametrize("encoding", ["avro"])
def test_each_avro_schema_is_registered_once_across_runs_without_a_schema_change(source, sync, registry, encoding):
    source.create(row(1, "a"))
    sync()
    source.update(1, name="a2")
    sync()
    for subject in (f"{TOPIC}-key", f"{TOPIC}-value"):
        assert registry.mock_client.get_versions(subject) == [1]


@pytest.mark.parametrize("encoding", ["avro"])
def test_the_new_avro_schema_is_registered_before_risingwave_gets_the_column(source, sync, calls, encoding):
    """RisingWave checks a new column against the registry's latest schema, so the registry must come first."""
    source.create(row(1, "a"))
    sync()
    start = len(calls)
    source.insert_with_new_column("email", {"id": 5, "name": "e", "born": None, "email": "e@example.com"})
    sync()
    later = calls[start:]
    registered = next(i for i, c in enumerate(later) if c == ("registry", f"{TOPIC}-value"))
    altered = next(i for i, c in enumerate(later) if c[0] == "risingwave" and "ADD COLUMN" in c[1])
    assert registered < altered

