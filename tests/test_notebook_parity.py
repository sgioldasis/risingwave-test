"""The Databricks notebook must build exactly what the Dagster sync asset builds.

notebooks/reverse_etl_cdf_to_kafka.py repeats the asset's event grouping, value coercion, Connect JSON messages and Avro
schemas (a comment in each says "change both together"), and a notebook cannot be imported: it starts with dbutils and
spark calls. So its pure functions and constants are lifted out of the source with `ast` and executed in a namespace of
their own, then compared with the asset's on the same inputs. Nothing here runs Spark.

If someone renames or removes one of the lifted names the loader says which, so the test is updated rather than skipped.

Run from the repo root:  .venv/bin/python -m pytest
"""

import ast
import types
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from orchestration.assets import reverse_etl_cdf_setup as cdf
from tests.test_properties import change_rows
from tests.test_wire_format import CFG, EVENTS, KEY_COLUMNS, ROW_FIELDS

NOTEBOOK = Path(__file__).resolve().parent.parent / "notebooks" / "reverse_etl_cdf_to_kafka.py"
FUNCTIONS = {
    "row_without_cdf_columns",
    "to_debezium_events",
    "coerce",
    "coerce_row",
    "connect_field",
    "row_schema",
    "build_message",
    "avro_schemas",
    "schema_string",
}
ASSIGNMENTS = {
    "CHANGE_TYPE",
    "COMMIT_VERSION",
    "COMMIT_TIMESTAMP",
    "CDF_COLUMNS",
    "CONNECT_TYPE",
    "ZONED_TIMESTAMP",
    "ZONED_TIMESTAMP_SCHEMA_NAME",
    "DATE",
    "DATE_SCHEMA_NAME",
    "EPOCH",
    "AVRO_TYPE",
    "SPARK_TYPE",
    "FIELD_TYPES",
}
IMPORTS = {"datetime", "json", "struct", "itertools"}


@pytest.fixture(scope="module")
def notebook() -> types.SimpleNamespace:
    """The notebook's pure functions and constants, with the values it would read from its widgets and the live table
    (the row fields, the key columns, the envelope name) taken from the wire-format test inputs."""
    nodes, found = [], set()
    for node in ast.parse(NOTEBOOK.read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS:
            nodes.append(node)
            found.add(node.name)
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and getattr(node.targets[0], "id", None) in ASSIGNMENTS:
            nodes.append(node)
            found.add(node.targets[0].id)
        elif isinstance(node, ast.Import) and {alias.name for alias in node.names} <= IMPORTS:
            nodes.append(node)
        elif isinstance(node, ast.ImportFrom) and node.module in IMPORTS:
            nodes.append(node)
    missing = (FUNCTIONS | ASSIGNMENTS) - found
    assert not missing, f"the notebook no longer defines {sorted(missing)}: update tests/test_notebook_parity.py"

    namespace = {
        "ROW_FIELDS": ROW_FIELDS,
        "KEY_COLUMNS": KEY_COLUMNS,
        "ENVELOPE_SCHEMA_NAME": CFG.envelope_schema_name,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return types.SimpleNamespace(**namespace)


class TestConstants:
    def test_the_sql_type_mapping_is_the_assets(self, notebook):
        assert notebook.CONNECT_TYPE == cdf._CONNECT_TYPE_BY_DATABRICKS_TYPE

    def test_the_special_types_and_their_schema_names_are_the_assets(self, notebook):
        assert (notebook.ZONED_TIMESTAMP, notebook.ZONED_TIMESTAMP_SCHEMA_NAME) == (cdf.ZONED_TIMESTAMP, cdf._ZONED_TIMESTAMP_SCHEMA_NAME)
        assert (notebook.DATE, notebook.DATE_SCHEMA_NAME, notebook.EPOCH) == (cdf.DATE, cdf._DATE_SCHEMA_NAME, cdf._EPOCH)

    def test_the_cdf_column_names_are_the_assets(self, notebook):
        assert notebook.CDF_COLUMNS == cdf.CDF_METADATA_COLUMNS
        assert (notebook.CHANGE_TYPE, notebook.COMMIT_VERSION, notebook.COMMIT_TIMESTAMP) == cdf.CDF_METADATA_COLUMNS

    def test_every_connect_type_has_the_same_avro_type(self, notebook):
        assert set(notebook.AVRO_TYPE) == set(cdf._AVRO_TYPE_BY_CONNECT_TYPE) | {cdf.ZONED_TIMESTAMP, cdf.DATE}
        for connect_type, avro_type in notebook.AVRO_TYPE.items():
            assert avro_type == cdf._avro_value_type(connect_type)

    def test_every_avro_type_has_a_spark_type_to_build_its_dataframe_with(self, notebook):
        assert set(notebook.SPARK_TYPE) == set(notebook.AVRO_TYPE)


@pytest.mark.parametrize("case", EVENTS)
class TestMessages:
    def test_the_connect_json_key_and_value_are_byte_for_byte_the_assets(self, notebook, case):
        asset_key, asset_value = cdf._build_connect_json_message(CFG, EVENTS[case], KEY_COLUMNS, ROW_FIELDS)
        notebook_key, notebook_value = notebook.build_message(EVENTS[case])
        assert notebook_key.encode() == asset_key
        assert notebook_value.encode() == asset_value


class TestAvroSchemas:
    def test_the_key_and_value_schema_strings_are_the_assets(self, notebook):
        asset_key, asset_value = cdf._avro_schemas(CFG, KEY_COLUMNS, ROW_FIELDS)
        notebook_key, notebook_value = notebook.avro_schemas()
        assert notebook.schema_string(notebook_key) == cdf._avro_schema_string(asset_key)
        assert notebook.schema_string(notebook_value) == cdf._avro_schema_string(asset_value)


@settings(deadline=None, max_examples=200)
@given(rows=change_rows)
def test_events_are_grouped_the_same_way(notebook, rows):
    assert notebook.to_debezium_events(rows, ["id"]) == cdf._to_debezium_events(rows, ["id"])


values = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(-(2**40), 2**40),
    st.integers(-(2**40), 2**40).map(str),
    st.floats(allow_nan=False, allow_infinity=False),
    st.floats(allow_nan=False, allow_infinity=False).map(repr),
    st.sampled_from(["true", "True", "FALSE", "x"]),
    st.dates().map(lambda d: d.isoformat()),
    st.dates().map(lambda d: d.isoformat() + "T10:00:00Z"),
    st.text(max_size=8),
)


def outcome(function, *args):
    """The result, or the kind of error: the two implementations must agree on both."""
    try:
        return ("value", function(*args))
    except Exception as error:  # noqa: BLE001 -- what matters is that both fail the same way
        return ("error", type(error).__name__)


@settings(deadline=None, max_examples=300)
@given(value=values, connect_type=st.sampled_from(["int8", "int16", "int32", "int64", "float", "double", "boolean", "string", "date"]))
def test_values_are_coerced_the_same_way(notebook, value, connect_type):
    assert outcome(notebook.coerce, value, connect_type) == outcome(cdf._coerce, value, connect_type)
