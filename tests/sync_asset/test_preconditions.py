"""The core's preconditions: the Kafka topic and the watermark state table are the team's to provide,
and the sync fails fast with what to create when either is missing — it never creates either one
(decoupling plan D1, D5). The demo's table_setup and topic assets keep creating both. The watermark
guard (release design) is the third precondition: an empty watermark for this sync's key must not
mean a silent fresh start when the source already has one under another key.

Run from the repo root:  .venv/bin/python -m pytest
"""

from tests.fakes import error_text
from tests.sync_asset.support import TOPIC, row

# make_config's source: the guard only fires for rows recorded against THIS source table.
SOURCE_FQN = "de_dev.sr_poc_external.reverse_etl_unittest_source"


class TestTopic:
    def test_a_missing_topic_fails_before_anything_else_runs(self, sync, source, calls):
        source.create(row(1, "a"))
        result, _, produced = sync(kafka_admin_topics=())
        assert not result.success and produced == []
        assert TOPIC in error_text(result)
        # The check is the run's first act: the admin lookup is all that happened.
        assert calls == [("kafka", "admin")]

    def test_a_listed_topic_passes_the_check(self, sync, source):
        source.create(row(1, "a"))
        result, _, produced = sync()
        assert result.success and len(produced) == 1


class TestStateTable:
    def test_a_missing_state_table_fails_with_the_ddl_and_writes_no_watermark(self, sync, source, calls, state_rows):
        source.create(row(1, "a"))
        result, _, produced = sync(state_table_exists=False)
        assert not result.success and produced == [] and state_rows == []
        assert "CREATE TABLE IF NOT EXISTS reverse_etl_sync_state" in error_text(result)
        # Checked after the topic, before any read: still no side effects beyond the two lookups.
        assert calls == [("kafka", "admin"), ("postgres", "SELECT to_regclass('reverse_etl_sync_state')")]


class TestWatermarkGuard:
    """The rename guard, wired into the run: a derived sync_name that lost its watermark to a
    label rename fails loudly instead of silently restarting the sync."""

    def test_a_renamed_label_fails_with_the_pin_message(self, sync, source, state_rows):
        source.create(row(1, "a"))
        state_rows.append((5, None))  # the old watermark, under the old derived key
        result, _, produced = sync(
            state_identities=[("reverse_etl_unittest", SOURCE_FQN, "unittest")],
            cfg_overrides={"sync_name": "reverse_etl_unittest_v2"},  # the renamed label's derivation
        )
        assert not result.success and produced == []
        text = error_text(result)
        assert "sync_name='reverse_etl_unittest' (label 'unittest')" in text
        assert "the label was renamed" in text
        assert "Pin sync_name in the YAML to keep the watermark, or delete the old rows to start fresh." in text

    def test_a_derived_name_colliding_with_a_deliberate_sync_fails_with_the_explicit_fix(
        self, sync, source, state_rows
    ):
        source.create(row(1, "a"))
        state_rows.append((5, None))
        result, _, produced = sync(
            state_identities=[("orders_watermark", SOURCE_FQN, "orders")],  # not derived-looking
        )
        assert not result.success and produced == []
        assert "collides with a deliberate sync; set sync_name explicitly" in error_text(result)

    def test_an_explicitly_pinned_sync_warns_and_proceeds(self, sync, source, state_rows):
        source.create(row(1, "a"))
        state_rows.append((5, None))  # another sync's watermark on the same source
        result, _, produced = sync(
            state_identities=[("reverse_etl_unittest", SOURCE_FQN, "unittest")],
            cfg_overrides={"sync_name": "pinned_key", "sync_name_explicit": True},
        )
        # A deliberate identity: the sync starts fresh rather than failing — a pinned rename or
        # a legitimate second sync on one table.
        assert result.success and len(produced) == 1
