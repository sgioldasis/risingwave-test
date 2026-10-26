"""Type mapping of the reverse-ETL pipeline: DATE as a real date, mixed-case names, the sink config.

No services needed. Run from the repo root:  .venv/bin/python -m pytest
"""

import dataclasses
import datetime
import io
import json
import os
from unittest import mock

import fastavro
import pytest

from orchestration.assets import reverse_etl_cdf_setup as cs
from orchestration.assets import reverse_etl_debezium_sink as sink
from orchestration.assets import reverse_etl_delta_reader as reader
from orchestration.assets import reverse_etl_drt as drt
from orchestration.assets import reverse_etl_risingwave_setup as rw
from orchestration.assets.reverse_etl_config import PostgresTargetConfig, ReverseEtlSyncConfig

DAY = datetime.date(2012, 1, 1)
DAY_NUMBER = 15340  # days from 1970-01-01 to 2012-01-01


def types_config(encoding="json", key_column="Company_ID") -> ReverseEtlSyncConfig:
    cfg = ReverseEtlSyncConfig.for_name(
        "unittest", catalog="de_dev", schema="sr_poc_external", source_columns=(), key_column=key_column
    )
    return dataclasses.replace(cfg, encoding=encoding, schema_registry_url="http://registry.invalid/apis/ccompat/v6")


def avro_round_trip(schema, payload):
    parsed = fastavro.parse_schema(schema)
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, parsed, payload)
    buffer.seek(0)
    return fastavro.schemaless_reader(buffer, parsed)


# --- DATE coercion ---------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2012-01-01", DAY_NUMBER),
        ("1970-01-01", 0),
        ("2012-01-01T10:00:00Z", DAY_NUMBER),  # a timestamp-like string keeps only the date
        (None, None),
    ],
)
def test_date_coercion(value, expected):
    assert cs._coerce(value, cs.DATE) == expected


@pytest.mark.parametrize("value, connect_type, expected", [("5", "int32", 5), ("2.5", "double", 2.5), ("x", "string", "x")])
def test_other_types_are_coerced_as_before(value, connect_type, expected):
    assert cs._coerce(value, connect_type) == expected


# --- type maps -------------------------------------------------------------------


def test_databricks_and_delta_date_map_to_the_date_marker():
    assert cs._CONNECT_TYPE_BY_DATABRICKS_TYPE["DATE"] == cs.DATE
    assert reader._CONNECT_TYPE_BY_DELTA_TYPE["date"] == cs.DATE


def test_risingwave_maps_date_both_ways():
    assert rw._RISINGWAVE_TYPE_BY_CONNECT_TYPE[cs.DATE] == "DATE"
    assert rw._CONNECT_TYPE_BY_RW_DATA_TYPE["date"] == cs.DATE


@pytest.mark.parametrize("encoding", ["json", "avro"])
def test_risingwave_column_is_a_plain_date_for_both_encodings(encoding):
    field = ("Company_ReleaseDate", cs.DATE, True)
    assert rw._column_defs(types_config(encoding), field) == ['"Company_ReleaseDate" DATE']


def test_drt_target_gets_a_date_column():
    assert drt._POSTGRES_TYPE_BY_CONNECT_TYPE["date"] == "date"


def test_timestamp_marker_is_unchanged():
    assert cs._CONNECT_TYPE_BY_DATABRICKS_TYPE["TIMESTAMP"] == cs.ZONED_TIMESTAMP
    assert rw._RISINGWAVE_TYPE_BY_CONNECT_TYPE[cs.ZONED_TIMESTAMP] == "TIMESTAMPTZ"


# --- wire formats ----------------------------------------------------------------

ROW_FIELDS = [("Release_Date", cs.DATE, False), ("Name", "string", True), ("N", "int32", True)]
EVENT = {
    "op": "u",
    "before": {"Release_Date": "2012-01-01", "Name": "A", "N": 1},
    "after": {"Release_Date": "2012-01-01", "Name": "A2", "N": 10},
    "source": {"commit_version": 2, "commit_timestamp": "2026-10-25T10:00:00.000000Z"},
}


def test_json_field_names_the_debezium_date_schema():
    field = cs._connect_field("Company_ReleaseDate", cs.DATE, True)
    assert field == {"field": "Company_ReleaseDate", "type": "int32", "name": "io.debezium.time.Date", "optional": True}


def test_json_key_schema_of_a_date_key_names_the_debezium_date_schema():
    cfg = types_config("json", key_column="Release_Date")
    key, value = cs._build_connect_json_message(cfg, EVENT, ["Release_Date"], ROW_FIELDS)
    key = json.loads(key)
    assert key["schema"]["fields"][0]["type"] == "int32"
    assert key["schema"]["fields"][0]["name"] == "io.debezium.time.Date"
    assert key["payload"] == {"Release_Date": DAY_NUMBER}
    assert json.loads(value)["payload"]["after"]["Release_Date"] == DAY_NUMBER


def test_json_key_schema_of_an_integer_key_is_unchanged():
    fields = [("Company_ID", "int32", False), ("Name", "string", True)]
    event = {"op": "c", "before": None, "after": {"Company_ID": 7, "Name": "x"}, "source": {}}
    key, _ = cs._build_connect_json_message(types_config(), event, ["Company_ID"], fields)
    assert json.loads(key)["schema"]["fields"] == [{"field": "Company_ID", "type": "int32", "optional": False}]


def test_avro_date_is_an_int_with_the_date_logical_type():
    assert cs._avro_value_type(cs.DATE) == {"type": "int", "logicalType": "date"}


def test_avro_round_trip_of_a_date_key_and_date_column():
    cfg = types_config("avro", key_column="Release_Date")
    keys = ["Release_Date"]
    key_schema, value_schema = cs._avro_schemas(cfg, keys, ROW_FIELDS)
    key_payload, value_payload = cs._avro_payloads(EVENT, keys, ROW_FIELDS)

    assert avro_round_trip(key_schema, key_payload) == {"Release_Date": DAY}
    decoded = avro_round_trip(value_schema, value_payload)
    assert decoded["after"]["Release_Date"] == DAY
    assert decoded["after"]["Name"] == "A2"
    assert decoded["op"] == "u"


def test_avro_null_date_round_trips_as_null():
    fields = [("Company_ID", "int32", False), ("Company_ReleaseDate", cs.DATE, True)]
    event = {"op": "c", "before": None, "after": {"Company_ID": 2, "Company_ReleaseDate": None}, "source": {}}
    _, value_schema = cs._avro_schemas(types_config("avro"), ["Company_ID"], fields)
    _, payload = cs._avro_payloads(event, ["Company_ID"], fields)
    assert avro_round_trip(value_schema, payload)["after"] == {"Company_ID": 2, "Company_ReleaseDate": None}


# --- sink config and reader ------------------------------------------------------


def test_connector_quotes_identifiers_and_keys_on_the_configured_column():
    with mock.patch.dict(os.environ, {"KAFKA_OUTPUT_BOOTSTRAP": "kafka.invalid:9093"}):
        config = sink._connector_config(types_config("avro"))
    assert config["quote.identifiers"] == "true"
    assert config["primary.key.fields"] == "Company_ID"
    assert config["insert.mode"] == "upsert"
    assert config["delete.enabled"] == "true"
    assert config["value.converter"] == "io.confluent.connect.avro.AvroConverter"


# --- the connector's coordinates: environment only (release design step 6) --------


def sink_config(monkeypatch, **table_overrides):
    """The connector config from the slim postgres target, with every coordinate env var pointed
    at throwaway values — including POSTGRES_PASSWORD, so a test can prove the secret never lands
    in the config dict."""
    monkeypatch.setenv("KAFKA_OUTPUT_BOOTSTRAP", "kafka.invalid:9093")
    monkeypatch.setenv("HOST_POSTGRES_URL", "jdbc:postgresql://db.invalid:5432/theirdb")
    monkeypatch.setenv("POSTGRES_USER", "their_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "their_secret_password")
    cfg = PostgresTargetConfig(
        label="company", kafka_topic="reverse_etl_company_topic", key_column="Company_ID", **table_overrides
    )
    return sink._connector_config(cfg)


def test_the_coordinates_come_from_the_environment_only(monkeypatch):
    config = sink_config(monkeypatch)
    assert config["connection.url"] == "jdbc:postgresql://db.invalid:5432/theirdb"
    assert config["connection.username"] == "their_user"
    assert config["consumer.override.bootstrap.servers"] == "kafka.invalid:9093"


def test_a_schema_qualified_table_flows_through_collection_name_format_verbatim(monkeypatch):
    """Verified live against the Debezium JDBC sink 3.7.0 (2026-10-26): a schema-qualified
    collection name creates the table in the named schema (reverse_etl_check.<table>), rows
    included — so the component needs no postgres_schema field."""
    config = sink_config(monkeypatch, postgres_table="their_schema.dim_company")
    assert config["collection.name.format"] == "their_schema.dim_company"


def test_the_password_is_never_interpolated_into_the_config(monkeypatch):
    config = sink_config(monkeypatch)
    # The placeholder stays verbatim: the Connect worker resolves it, so the secret never
    # passes through Dagster or this process's config dict.
    assert config["connection.password"] == "${env:POSTGRES_PASSWORD}"
    assert "their_secret_password" not in json.dumps(config)


# --- the cost meter degrades gracefully (release design step 7) --------------------


def test_without_the_databricks_env_the_cost_meter_is_a_noop(monkeypatch):
    """Demo-only telemetry: with the Databricks env absent the sync runs unmetered and omits the
    metadata instead of raising — a released sync must not need the demo's cost chips."""
    monkeypatch.delenv("DBT_DATABRICKS_HOST", raising=False)
    with mock.patch.object(cs, "meter_snapshot", side_effect=AssertionError("the meter must not be read")):
        assert cs._cost_metadata() == {}


def test_with_the_databricks_env_the_cost_meter_reports(monkeypatch):
    monkeypatch.setenv("DBT_DATABRICKS_HOST", "adb.invalid")
    with mock.patch.object(cs, "meter_snapshot", return_value={"warehouse_seconds": 1.5, "statements": 3}):
        metadata = cs._cost_metadata()
    assert metadata["databricks_warehouse_seconds"].value == 1.5
    assert metadata["databricks_statements"].value == 3


def test_reader_turns_dates_into_iso_strings_and_keeps_timestamps_formatted():
    rows = [{"d": datetime.date(2012, 1, 1), "t": datetime.datetime(2026, 10, 25, 10, 0, 0, 5)}]
    reader._format_rows(rows, ["t"], ["d"])
    assert rows[0]["d"] == "2012-01-01"
    assert rows[0]["t"] == "2026-10-25T10:00:00.000005Z"
