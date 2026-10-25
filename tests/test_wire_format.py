"""The sync's wire formats, pinned byte for byte against files in tests/golden/.

Consumers (the Debezium JDBC sink, RisingWave, the schema registry) depend on the exact output, so a change that looks
harmless in a field-by-field comparison, such as a reordered field or a different schema string, must show up as a diff
to review. The inputs are fixed: one row field of every type, and an insert, an update and a delete.

- Connect JSON: the key and value of each event, compared as JSON (the key is also compared as text, since it is
  written with sorted keys).
- Avro schemas: compared as exact strings, because the registry matches a schema by its exact text.
- Avro payloads: the bytes after the 5-byte Confluent header (magic byte and schema id), as hex.

After a deliberate change, regenerate with
    UPDATE_GOLDEN=1 .venv/bin/python -m pytest tests/test_wire_format.py
and review the diff of tests/golden/ before committing.
"""

import json
import os
from pathlib import Path

import pytest

from orchestration.assets import reverse_etl_cdf_setup as cdf
from tests.fakes import FakeSchemaRegistry, make_config

GOLDEN = Path(__file__).resolve().parent / "golden"
CFG = make_config(source_columns=(), key_column="id", schema_registry_url="http://registry.invalid")
KEY_COLUMNS = ["id"]
ROW_FIELDS = [
    ("id", "int64", False),
    ("name", "string", True),
    ("score", "double", True),
    ("ratio", "float", True),
    ("flag", "boolean", True),
    ("tiny", "int8", True),
    ("small", "int16", True),
    ("count", "int32", True),
    ("born", cdf.DATE, True),
    ("seen", cdf.ZONED_TIMESTAMP, True),
    ("note", "string", True),
]
SOURCE = {"commit_version": 7, "commit_timestamp": "2026-10-18T17:26:36.779212Z"}


def full_row(name, **more):
    row = {
        "id": 1,
        "name": name,
        "score": 2.5,
        "ratio": 0.25,
        "flag": True,
        "tiny": 7,
        "small": 300,
        "count": 70000,
        "born": "2012-01-01",
        "seen": "2026-10-18T17:26:36.779212Z",
        "note": None,
    }
    return {**row, **more}


EVENTS = {
    "insert": {"before": None, "after": full_row("a"), "op": "c", "source": SOURCE},
    "update": {"before": full_row("a"), "after": full_row("b", score=3.5), "op": "u", "source": SOURCE},
    "delete": {"before": full_row("b", score=3.5), "after": None, "op": "d", "source": SOURCE},
}


def check_golden(name: str, actual: str) -> None:
    path = GOLDEN / name
    if os.environ.get("UPDATE_GOLDEN"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(actual)
    assert path.exists(), f"{path} is missing: run with UPDATE_GOLDEN=1 to create it"
    assert actual == path.read_text(), f"{name} changed: if that is intended, regenerate with UPDATE_GOLDEN=1 and review the diff"


def pretty(raw: bytes) -> str:
    return json.dumps(json.loads(raw), indent=2) + "\n"


@pytest.mark.parametrize("case", EVENTS)
class TestConnectJson:
    def test_the_key(self, case):
        key, _ = cdf._build_connect_json_message(CFG, EVENTS[case], KEY_COLUMNS, ROW_FIELDS)
        check_golden(f"connect_json/{case}.key.json", pretty(key))
        assert key == json.dumps(json.loads(key), default=str, sort_keys=True).encode()  # written with sorted keys

    def test_the_value(self, case):
        _, value = cdf._build_connect_json_message(CFG, EVENTS[case], KEY_COLUMNS, ROW_FIELDS)
        check_golden(f"connect_json/{case}.value.json", pretty(value))


class TestAvroSchemas:
    def test_the_key_schema_string(self):
        key_schema, _ = cdf._avro_schemas(CFG, KEY_COLUMNS, ROW_FIELDS)
        check_golden("avro/key.schema.json", cdf._avro_schema_string(key_schema) + "\n")

    def test_the_value_schema_string(self):
        _, value_schema = cdf._avro_schemas(CFG, KEY_COLUMNS, ROW_FIELDS)
        check_golden("avro/value.schema.json", cdf._avro_schema_string(value_schema) + "\n")

    def test_the_subjects_the_schemas_are_registered_under(self):
        check_golden("avro/subjects.txt", f"{CFG.key_subject}\n{CFG.value_subject}\n")


@pytest.mark.parametrize("case", EVENTS)
class TestAvroPayloads:
    @pytest.fixture
    def message(self, case):
        client = FakeSchemaRegistry().client(CFG.schema_registry_url)
        serializers = cdf._avro_serializers(CFG, KEY_COLUMNS, ROW_FIELDS, client)
        return cdf._build_avro_message(CFG, EVENTS[case], KEY_COLUMNS, ROW_FIELDS, serializers)

    def test_the_wire_format_header_is_the_magic_byte_and_a_schema_id(self, message):
        for part in message:
            assert part[0] == 0 and len(part) > 5

    def test_the_key_payload(self, case, message):
        check_golden(f"avro/{case}.key.hex", message[0][5:].hex() + "\n")

    def test_the_value_payload(self, case, message):
        check_golden(f"avro/{case}.value.hex", message[1][5:].hex() + "\n")
