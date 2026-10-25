"""The core's preconditions: the Kafka topic and the watermark state table are the team's to provide,
and the sync fails fast with what to create when either is missing — it never creates either one
(decoupling plan D1, D5). The demo's table_setup and topic assets keep creating both.

Run from the repo root:  .venv/bin/python -m pytest
"""

from tests.fakes import error_text
from tests.sync_asset.support import TOPIC, row


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
