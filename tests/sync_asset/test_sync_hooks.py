"""The sync's hook contract: when the hooks run, what they receive and return, and what a sync without hooks does.

The hooks are the demo's extension points (orchestration/assets/reverse_etl_demo_hooks.py); their ordering is the
part the demo's target care depends on — a column must exist before the first message that uses it, and the
drop/rename propagation must finish before the watermark is saved. The reusable component runs with hooks=None, which
is also covered here.

Run from the repo root:  .venv/bin/python -m pytest
"""

import pytest

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from orchestration.assets.reverse_etl_demo_hooks import DemoSyncHooks
import json

import dagster as dg

from tests.fakes import FakeKafka, bind_connections, error_text, kinds, make_config
from tests.sync_asset.support import TOPIC, metadata, row


class RecordingHooks:
    """Records every call into the shared chronological `calls` list (so order can be checked against Kafka and
    Postgres), returns recognisable metadata, and can be told to fail."""

    def __init__(self, calls, fail_in=None):
        self.calls, self.fail_in = calls, fail_in
        self.before_chunks, self.after_schemas, self.idle_calls = [], [], 0

    def before_produce(self, context, cfg, row_fields):
        self.calls.append(("hook", "before_produce"))
        self.before_chunks.append([field[0] for field in row_fields])
        if self.fail_in == "before_produce":
            raise RuntimeError("before_produce fails on purpose")
        return {"hook_before": [len(self.before_chunks)]}

    def after_delivery(self, context, cfg, schema):
        self.calls.append(("hook", "after_delivery"))
        self.after_schemas.append(schema)
        if self.fail_in == "after_delivery":
            raise RuntimeError("after_delivery fails on purpose")
        return {"hook_after": "once"}

    def idle_metadata(self):
        self.idle_calls += 1
        return {"hook_idle": "yes"}


class CallsKafka(FakeKafka):
    """Also records each produce and flush into `calls`, so hook order can be checked against them."""

    def producer(self):
        producer = super().producer()
        produce, flush = producer.produce, producer.flush

        def recorded_produce(topic, key, value, on_delivery=None):
            self.calls.append(("kafka", "produce"))
            return produce(topic, key, value, on_delivery)

        def recorded_flush(timeout=None):
            self.calls.append(("kafka", "flush"))
            return flush(timeout)

        producer.produce, producer.flush = recorded_produce, recorded_flush
        return producer


def position(calls, kind, detail):
    """The first position of a (kind, detail) entry, matching exactly."""
    return next(i for i, (k, d) in enumerate(calls) if k == kind and d == detail)


def positions(calls, kind, detail):
    return [i for i, (k, d) in enumerate(calls) if k == kind and d == detail]


@pytest.fixture
def one_version_per_chunk(monkeypatch):
    """The code default is 10 versions per chunk; one per chunk makes every insert its own event-bearing chunk."""
    monkeypatch.setattr(cdf_setup, "_CDF_CHUNK_VERSIONS", 1)


class TestOrdering:
    def test_before_produce_precedes_the_produce_and_after_delivery_sits_between_flush_and_watermark(
        self, sync, source, calls, state_rows
    ):
        source.create(row(1, "a"))
        hooks = RecordingHooks(calls)
        result, _, produced = sync(hooks=hooks, kafka=CallsKafka(calls, admin_topics=(TOPIC,)))
        assert result.success and len(produced) == 1
        before = position(calls, "hook", "before_produce")
        produce = position(calls, "kafka", "produce")
        flush = position(calls, "kafka", "flush")
        after = position(calls, "hook", "after_delivery")
        write = next(i for i, (k, d) in enumerate(calls) if k == "postgres" and d.startswith("INSERT INTO reverse_etl_sync_state"))
        assert before < produce < flush < after < write

    @pytest.mark.usefixtures("one_version_per_chunk")
    def test_before_produce_runs_once_per_event_bearing_chunk(self, sync, source, calls):
        source.create(row(1, "a"))
        sync()  # watermark the table's first version
        source.insert(row(2, "b"))  # version 1: its own chunk
        source.insert(row(3, "c"))  # version 2: its own chunk
        hooks = RecordingHooks(calls)
        _, _, produced = sync(hooks=hooks, kafka=CallsKafka(calls, admin_topics=(TOPIC,)))
        assert hooks.before_chunks == [["id", "name", "born"], ["id", "name", "born"]]
        before, produce = positions(calls, "hook", "before_produce"), positions(calls, "kafka", "produce")
        assert before[0] < produce[0] < before[1] < produce[1]  # each chunk's hook precedes its own produce
        assert [p.value["after"]["name"] for p in produced] == ["b", "c"]

    @pytest.mark.usefixtures("one_version_per_chunk")
    def test_after_delivery_runs_once_per_run_even_over_several_chunks(self, sync, source, calls):
        source.create(row(1, "a"))
        sync()
        source.insert(row(2, "b"))
        source.insert(row(3, "c"))
        hooks = RecordingHooks(calls)
        sync(hooks=hooks)
        assert len(hooks.after_schemas) == 1


class TestWhatHooksReceive:
    def test_after_delivery_gets_the_final_schema_the_previous_state_and_the_event_era_schema(
        self, sync, source, calls
    ):
        source.create(row(1, "a"))
        hooks = RecordingHooks(calls)
        sync(hooks=hooks)
        schema = hooks.after_schemas[0]
        assert [field[0] for field in schema["row_fields"]] == ["id", "name", "born"]
        assert schema["last_schema"] is None  # first run: nothing recorded yet
        assert isinstance(schema["column_ids"], dict)
        assert [field[0] for field in schema["event_row_fields"]] == ["id", "name", "born"]
        assert schema["event_fields_version"] is not None
        assert schema["source_location"] == source.location


class TestWhatHooksReturn:
    @pytest.mark.usefixtures("one_version_per_chunk")
    def test_lists_merge_across_chunks_and_scalars_overwrite(self, sync, source, calls):
        source.create(row(1, "a"))
        sync()
        source.insert(row(2, "b"))
        source.insert(row(3, "c"))
        hooks = RecordingHooks(calls)
        result, _, _ = sync(hooks=hooks)
        assert metadata(result, "hook_before") == [1, 2]  # one entry per event-bearing chunk, in order
        assert metadata(result, "hook_after") == "once"


class TestFailure:
    def test_a_failing_before_produce_produces_nothing_and_moves_no_watermark(self, sync, source, calls, state_rows):
        source.create(row(1, "a"))
        result, _, produced = sync(hooks=RecordingHooks(calls, fail_in="before_produce"))
        assert not result.success and produced == [] and state_rows == []
        assert "before_produce fails on purpose" in error_text(result)

    def test_a_failing_after_delivery_keeps_the_messages_delivered_but_the_watermark_unmoved(
        self, sync, source, calls, state_rows
    ):
        source.create(row(1, "a"))
        result, _, produced = sync(hooks=RecordingHooks(calls, fail_in="after_delivery"))
        assert not result.success and len(produced) == 1 and state_rows == []
        assert "after_delivery fails on purpose" in error_text(result)


class TestTargetSelection:
    """The demo hooks' target care follows the sync's target selection (plan section 12, option A): a
    sync whose targets exclude the warehouse tables touches neither, and propagate_drops — which cares
    for BOTH targets — refuses a half selection instead of half-propagating a schema change."""

    def test_a_duckdb_only_sync_touches_neither_risingwave_nor_the_postgres_targets(self, sync, source, calls, tmp_path):
        source.create(row(1, "a"))
        result, _, produced = sync(
            hooks=DemoSyncHooks(), targets=("duckdb",), state_backend="duckdb",
            state_path=str(tmp_path / "state.duckdb"), duckdb_target_path=str(tmp_path / "mirror.duckdb"),
        )
        assert result.success and len(produced) == 1
        assert "risingwave" not in kinds(calls) and "http" not in kinds(calls) and "postgres" not in kinds(calls)
        assert metadata(result, "risingwave_columns_added") == []
        assert metadata(result, "columns_dropped") == {} and metadata(result, "columns_renamed") == []

    def test_propagate_drops_with_a_half_selection_fails_loudly(self, sync, source, calls):
        source.create(row(1, "a"))
        result, _, _ = sync(hooks=DemoSyncHooks(), propagate_drops=True, targets=("risingwave",))
        assert not result.success
        assert "propagate_drops needs both the risingwave and postgres targets" in error_text(result)


class TestEmptyRun:
    def test_an_empty_run_reports_idle_metadata_and_calls_no_hook(self, sync, source, calls):
        source.create(row(1, "a"))
        sync()
        hooks = RecordingHooks(calls)
        result, _, produced = sync(hooks=hooks)
        assert produced == []
        assert hooks.before_chunks == [] and hooks.after_schemas == [] and hooks.idle_calls == 1
        assert metadata(result, "hook_idle") == "yes"


class TestWithoutHooks:
    def test_the_core_touches_neither_risingwave_nor_http_and_reports_no_demo_metadata(self, sync, source, calls):
        source.create(row(1, "a"))
        result, _, produced = sync(hooks=None)
        assert result.success and len(produced) == 1
        assert "risingwave" not in kinds(calls) and "http" not in kinds(calls)
        with pytest.raises(KeyError):
            metadata(result, "columns_dropped")
        with pytest.raises(KeyError):
            metadata(result, "risingwave_columns_added")

    def test_the_core_runs_without_the_demo_connections(self, monkeypatch, source, calls, resources, state_rows, registry, encoding):
        source.create(row(1, "a"))
        cfg = make_config(source_columns=(), key_column="id", encoding=encoding, schema_registry_url="http://registry.invalid")
        res = resources(table_location=source.location, state_rows=state_rows, registry=registry, kafka_admin_topics=(TOPIC,))
        bind_connections(monkeypatch, res)
        result = dg.materialize([cdf_setup.build_sync_asset(cfg)], raise_on_error=False)
        assert result.success

    def test_the_bare_core_declares_no_upstream_assets_and_no_resources(self):
        asset = cdf_setup.build_sync_asset(make_config(source_columns=(), key_column="id"))
        (spec,) = asset.specs
        assert {dep.asset_key.to_user_string() for dep in spec.deps} == set()
        assert set(asset.required_resource_keys) - {"io_manager"} == set()

    def test_the_core_still_writes_its_watermark_and_schema(self, sync, source, calls, state_rows):
        source.create(row(1, "a"))
        sync(hooks=None)
        assert len(state_rows) == 1
        assert [column["name"] for column in json.loads(state_rows[-1][1])] == ["id", "name", "born"]
