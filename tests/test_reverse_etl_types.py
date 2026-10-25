"""Type mapping of the reverse-ETL pipeline: DATE as a real date, mixed-case names, the sink config.

No services needed. Run from the repo root:  .venv/bin/python -m unittest discover -s tests -t .
"""

import dataclasses
import datetime
import io
import json
import os
import unittest
from unittest import mock

import fastavro

from orchestration.assets import reverse_etl_cdf_setup as cs
from orchestration.assets import reverse_etl_debezium_sink as sink
from orchestration.assets import reverse_etl_delta_reader as reader
from orchestration.assets import reverse_etl_drt as drt
from orchestration.assets import reverse_etl_risingwave_setup as rw
from orchestration.assets.reverse_etl_config import ReverseEtlSyncConfig

DAY = datetime.date(2012, 1, 1)
DAY_NUMBER = 15340  # days from 1970-01-01 to 2012-01-01


def make_config(encoding="json", key_column="Company_ID") -> ReverseEtlSyncConfig:
    cfg = ReverseEtlSyncConfig.for_name(
        "unittest", catalog="de_dev", schema="sr_poc_external", source_columns=(), key_column=key_column
    )
    return dataclasses.replace(cfg, encoding=encoding, schema_registry_url="http://registry.invalid/apis/ccompat/v6")


class DateCoercionTests(unittest.TestCase):
    def test_iso_string_becomes_days_since_epoch(self):
        self.assertEqual(cs._coerce("2012-01-01", cs.DATE), DAY_NUMBER)
        self.assertEqual(cs._coerce("1970-01-01", cs.DATE), 0)

    def test_timestamp_like_string_keeps_only_the_date(self):
        self.assertEqual(cs._coerce("2012-01-01T10:00:00Z", cs.DATE), DAY_NUMBER)

    def test_none_stays_none(self):
        self.assertIsNone(cs._coerce(None, cs.DATE))

    def test_other_types_are_unchanged(self):
        self.assertEqual(cs._coerce("5", "int32"), 5)
        self.assertEqual(cs._coerce("2.5", "double"), 2.5)
        self.assertEqual(cs._coerce("x", "string"), "x")


class TypeMapTests(unittest.TestCase):
    def test_databricks_and_delta_date_map_to_the_date_marker(self):
        self.assertEqual(cs._CONNECT_TYPE_BY_DATABRICKS_TYPE["DATE"], cs.DATE)
        self.assertEqual(reader._CONNECT_TYPE_BY_DELTA_TYPE["date"], cs.DATE)

    def test_risingwave_maps_date_both_ways(self):
        self.assertEqual(rw._RISINGWAVE_TYPE_BY_CONNECT_TYPE[cs.DATE], "DATE")
        self.assertEqual(rw._CONNECT_TYPE_BY_RW_DATA_TYPE["date"], cs.DATE)

    def test_risingwave_column_is_a_plain_date_for_both_encodings(self):
        field = ("Company_ReleaseDate", cs.DATE, True)
        for encoding in ("json", "avro"):
            self.assertEqual(rw._column_defs(make_config(encoding), field), ['"Company_ReleaseDate" DATE'])

    def test_drt_target_gets_a_date_column(self):
        self.assertEqual(drt._POSTGRES_TYPE_BY_CONNECT_TYPE["date"], "date")

    def test_timestamp_marker_is_unchanged(self):
        self.assertEqual(cs._CONNECT_TYPE_BY_DATABRICKS_TYPE["TIMESTAMP"], cs.ZONED_TIMESTAMP)
        self.assertEqual(rw._RISINGWAVE_TYPE_BY_CONNECT_TYPE[cs.ZONED_TIMESTAMP], "TIMESTAMPTZ")


class WireFormatTests(unittest.TestCase):
    ROW_FIELDS = [("Release_Date", cs.DATE, False), ("Name", "string", True), ("N", "int32", True)]
    EVENT = {
        "op": "u",
        "before": {"Release_Date": "2012-01-01", "Name": "A", "N": 1},
        "after": {"Release_Date": "2012-01-01", "Name": "A2", "N": 10},
        "source": {"commit_version": 2, "commit_timestamp": "2026-10-25T10:00:00.000000Z"},
    }

    def test_json_field_names_the_debezium_date_schema(self):
        field = cs._connect_field("Company_ReleaseDate", cs.DATE, True)
        self.assertEqual(
            field, {"field": "Company_ReleaseDate", "type": "int32", "name": "io.debezium.time.Date", "optional": True}
        )

    def test_json_key_schema_of_a_date_key_names_the_debezium_date_schema(self):
        cfg = make_config("json", key_column="Release_Date")
        key, value = cs._build_connect_json_message(cfg, self.EVENT, ["Release_Date"], self.ROW_FIELDS)
        key = json.loads(key)
        self.assertEqual(key["schema"]["fields"][0]["type"], "int32")
        self.assertEqual(key["schema"]["fields"][0]["name"], "io.debezium.time.Date")
        self.assertEqual(key["payload"], {"Release_Date": DAY_NUMBER})
        after = json.loads(value)["payload"]["after"]
        self.assertEqual(after["Release_Date"], DAY_NUMBER)

    def test_json_key_schema_of_an_integer_key_is_unchanged(self):
        fields = [("Company_ID", "int32", False), ("Name", "string", True)]
        event = {"op": "c", "before": None, "after": {"Company_ID": 7, "Name": "x"}, "source": {}}
        key, _ = cs._build_connect_json_message(make_config(), event, ["Company_ID"], fields)
        self.assertEqual(
            json.loads(key)["schema"]["fields"], [{"field": "Company_ID", "type": "int32", "optional": False}]
        )

    def test_avro_date_is_an_int_with_the_date_logical_type(self):
        self.assertEqual(cs._avro_value_type(cs.DATE), {"type": "int", "logicalType": "date"})

    def test_avro_round_trip_of_a_date_key_and_date_column(self):
        cfg = make_config("avro", key_column="Release_Date")
        keys = ["Release_Date"]
        key_schema, value_schema = cs._avro_schemas(cfg, keys, self.ROW_FIELDS)
        key_payload, value_payload = cs._avro_payloads(self.EVENT, keys, self.ROW_FIELDS)

        def round_trip(schema, payload):
            parsed = fastavro.parse_schema(schema)
            buffer = io.BytesIO()
            fastavro.schemaless_writer(buffer, parsed, payload)
            buffer.seek(0)
            return fastavro.schemaless_reader(buffer, parsed)

        self.assertEqual(round_trip(key_schema, key_payload), {"Release_Date": DAY})
        decoded = round_trip(value_schema, value_payload)
        self.assertEqual(decoded["after"]["Release_Date"], DAY)
        self.assertEqual(decoded["after"]["Name"], "A2")
        self.assertEqual(decoded["op"], "u")

    def test_avro_null_date_round_trips_as_null(self):
        fields = [("Company_ID", "int32", False), ("Company_ReleaseDate", cs.DATE, True)]
        cfg = make_config("avro")
        event = {"op": "c", "before": None, "after": {"Company_ID": 2, "Company_ReleaseDate": None}, "source": {}}
        _, value_schema = cs._avro_schemas(cfg, ["Company_ID"], fields)
        _, payload = cs._avro_payloads(event, ["Company_ID"], fields)
        parsed = fastavro.parse_schema(value_schema)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, parsed, payload)
        buffer.seek(0)
        decoded = fastavro.schemaless_reader(buffer, parsed)
        self.assertEqual(decoded["after"], {"Company_ID": 2, "Company_ReleaseDate": None})


class SinkConfigTests(unittest.TestCase):
    def test_connector_quotes_identifiers_and_keys_on_the_configured_column(self):
        with mock.patch.dict(os.environ, {"KAFKA_OUTPUT_BOOTSTRAP": "kafka.invalid:9093"}):
            config = sink._connector_config(make_config("avro"))
        self.assertEqual(config["quote.identifiers"], "true")
        self.assertEqual(config["primary.key.fields"], "Company_ID")
        self.assertEqual(config["insert.mode"], "upsert")
        self.assertEqual(config["delete.enabled"], "true")
        self.assertEqual(config["value.converter"], "io.confluent.connect.avro.AvroConverter")


class ReaderFormattingTests(unittest.TestCase):
    def test_dates_become_iso_strings_and_timestamps_stay_formatted(self):
        rows = [{"d": datetime.date(2012, 1, 1), "t": datetime.datetime(2026, 10, 25, 10, 0, 0, 5)}]
        reader._format_rows(rows, ["t"], ["d"])
        self.assertEqual(rows[0]["d"], "2012-01-01")
        self.assertEqual(rows[0]["t"], "2026-10-25T10:00:00.000005Z")


if __name__ == "__main__":
    unittest.main()
