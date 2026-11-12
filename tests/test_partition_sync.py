"""The Jobotron partition-sync host piece (orchestration/assets/jobotron_partition_sync.py):
the yaml scan that discovers the source-based instances, and the add/remove diff that
keeps a dynamic partition set equal to its enumeration. The warehouse call and the
Dagster instance are the wheel's own machinery and dagster's, not retested here."""

from pathlib import Path

from orchestration.assets.jobotron_partition_sync import (
    partition_delta,
    scan_source_based_instances,
)

DEF_ROOT = Path(__file__).resolve().parent.parent / "orchestration" / "defs"


class TestScan:
    def test_exactly_the_source_based_instances_are_discovered(self):
        instances = {entry["name"]: entry for entry in scan_source_based_instances(DEF_ROOT)}
        assert set(instances) == {
            "verified_customers_with_expired_id",
            "notify_unverified_customers_over_deposit_limit",
            "limit_unverified_customer_rights",
        }

    def test_the_partitions_def_name_matches_the_component_derivation(self):
        (entry,) = [
            e for e in scan_source_based_instances(DEF_ROOT)
            if e["name"] == "verified_customers_with_expired_id"
        ]
        assert entry["partitions_name"] == (
            "jobotron_verified_customers_with_expired_id_companies"
        )
        assert entry["source"] == "de.metadatadb.mcompany"

    def test_the_exclude_list_is_carried_as_written(self):
        (entry,) = [
            e for e in scan_source_based_instances(DEF_ROOT)
            if e["name"] == "verified_customers_with_expired_id"
        ]
        # the notebooks' inactive list, as exact comCodes
        assert entry["exclude"] == ["B2B", "BTN", "ENBTN", "KZ", "ATBTN", "DEKSN", "ATKSN"]

    def test_static_instances_are_not_served(self, tmp_path: Path):
        static = tmp_path / "static_one" / "defs.yaml"
        static.parent.mkdir()
        static.write_text(
            "type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent\n"
            "attributes:\n"
            "  name: static_one\n"
            "  asset_key_prefix: [jobotron]\n"
            "  companies: {include: [CYSTX]}\n"
        )
        assert scan_source_based_instances(tmp_path) == []


class TestDelta:
    def test_arrivals_are_added_and_stable_keys_touched_not(self):
        additions, removals, kept = partition_delta(
            ["CYSTX", "GRSTX", "ROBTN"], {"CYSTX", "GRSTX"}, set()
        )
        assert additions == ["ROBTN"]
        assert removals == []
        assert kept == []

    def test_a_departed_company_is_removed_only_when_idle(self):
        additions, removals, kept = partition_delta(
            ["CYSTX"], {"CYSTX", "ROBTN"}, in_flight={"ROBTN"}
        )
        assert additions == []
        assert removals == []
        assert kept == ["ROBTN"]

    def test_an_idle_departure_is_removed(self):
        additions, removals, kept = partition_delta(
            ["CYSTX"], {"CYSTX", "ROBTN"}, in_flight=set()
        )
        assert additions == []
        assert removals == ["ROBTN"]
        assert kept == []
