"""The Delta federation setup (scripts/setup_delta_federation.py): the shapes it
builds and the live-found quirks they bake in — the serde `path` parameter SR
reads the location from, the `deltalake` type string, and the drop+create
re-pointing after a rebuild.

Run from the repo root:  .venv/bin/python -m pytest
"""

import pytest

from scripts.setup_delta_federation import (
    CATALOG,
    HMS_DATABASE,
    SOURCE_TABLE,
    SOURCES,
    catalog_ddl,
    get_table_request,
    hms_location,
    hms_table,
    load_env,
)

LOCATION = "abfss://sr-poc-cont1@account.dfs.core.windows.net/sr_poc_external/tables/uuid-1"


class TestHmsTable:
    def test_the_location_lives_in_the_serde_path_parameter(self):
        table = hms_table(LOCATION)
        # SR's toTableLocation reads SerDeInfo.parameters["path"] for
        # Delta-provider tables; sd.location alone yields a null scan path.
        assert table.sd.serdeInfo.parameters["path"] == LOCATION
        assert table.sd.location == LOCATION

    def test_the_table_is_registered_as_a_delta_provider(self):
        table = hms_table(LOCATION)
        assert table.parameters["spark.sql.sources.provider"] == "delta"
        assert table.tableType == "EXTERNAL_TABLE"
        assert table.dbName == HMS_DATABASE and table.tableName == SOURCE_TABLE

    def test_the_columns_are_empty_because_sr_reads_the_delta_log(self):
        # verified live: a fresh registration with bogus columns still DESCs
        # with the real schema — SR takes each table's schema from its
        # _delta_log, so the registration carries none (and none can go stale).
        table = hms_table(LOCATION)
        assert table.sd.cols == []

    def test_the_table_name_is_parameterizable(self):
        table = hms_table(LOCATION, "reverse_etl_avro_source")
        assert table.tableName == "reverse_etl_avro_source"

    def test_every_demo_sync_source_is_registered(self):
        assert SOURCES == [
            f"de_dev.sr_poc_external.reverse_etl_{label}_source"
            for label in ("avro", "cdf", "orders", "delta", "company")
        ]


class TestHmsLocation:
    def test_the_serde_path_wins_for_a_registered_table(self):
        assert hms_location(hms_table(LOCATION)) == LOCATION

    def test_sd_location_is_the_fallback_and_none_means_unregistered(self):
        # A table registered without the serde `path` parameter (the pre-fix
        # convention) still reports its sd.location; no table at all is None.
        class Serde:
            parameters = {}
        class SD:
            serdeInfo = Serde()
            location = LOCATION
        class Tbl:
            sd = SD()
        assert hms_location(Tbl()) == LOCATION
        assert hms_location(None) is None


class TestGetTableRequest:
    def test_the_request_constructs_with_the_camelcase_kwargs(self):
        # hmsclient's thrift GetTableRequest takes dbName/tblName (camelCase),
        # unlike the rest of its API (dbname=/tbl_name= on drop_table); a
        # snake_case typo raises TypeError at construction — which, swallowed,
        # is indistinguishable from "table not registered" and made every
        # registration look stale.
        req = get_table_request(SOURCE_TABLE)
        assert req.dbName == HMS_DATABASE and req.tblName == SOURCE_TABLE


class TestCheckFederationGuard:
    def test_only_the_delta_sync_has_a_federation(self):
        # The demo server imports cleanly without the stack (its checks are
        # lazy); the guard runs before any thrift/pymysql work.
        from scripts.reverse_etl_demo_server import check_federation
        d = check_federation("company")
        assert d["error"] and "delta" in d["error"]


class TestCatalogDdl:
    def env(self):
        return {
            "DATABRICKS_AZURE_CLIENT_ID": "client-id",
            "DATABRICKS_AZURE_CLIENT_SECRET": "client-secret",
            "DATABRICKS_AZURE_TENANT_ID": "tenant-id",
        }

    def test_the_type_string_is_deltalake_not_the_documented_delta(self):
        # live 4.1.7: "[type : delta] is not supported"
        assert '"type"="deltalake"' in catalog_ddl(self.env())

    def test_the_metastore_goes_through_the_published_port(self):
        # the docker DNS name breaks the FE's Java URI parser (search domain
        # with an underscore)
        assert '"hive.metastore.uris"="thrift://host.docker.internal:9083"' in catalog_ddl(self.env())

    def test_the_service_principal_becomes_the_adls_credentials(self):
        ddl = catalog_ddl(self.env())
        assert '"azure.adls2.oauth2_client_id"="client-id"' in ddl
        assert '"azure.adls2.oauth2_client_secret"="client-secret"' in ddl
        assert "login.microsoftonline.com/tenant-id/oauth2/token" in ddl
        assert CATALOG in ddl


class TestLoadEnv:
    def test_quotes_are_stripped_and_process_env_wins(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text('A_KEY="quoted"\nB_KEY=plain\n# comment\n\n')
        monkeypatch.setenv("B_KEY", "from-process")
        env = load_env(str(env_file))
        assert env["A_KEY"] == "quoted"
        assert env["B_KEY"] == "from-process"

    def test_a_missing_colon_style_line_is_skipped_silently(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("NO_EQUALS_SIGN\n")
        env = load_env(str(env_file))
        assert env == {}

    def test_missing_secret_fails_the_ddl_loudly(self):
        with pytest.raises(KeyError):
            catalog_ddl({"DATABRICKS_AZURE_CLIENT_ID": "id"})
