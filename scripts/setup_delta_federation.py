"""Set up the StarRocks Delta federation demo: the Databricks Delta sources read
in place by StarRocks through an HMS shim (serving-layer doc §13). Idempotent —
re-run after every reset/rebuild, because UC mints a new storage path (a UUID)
for each source table every time.

Three steps, each independent and safe to repeat:
  1. discover each demo source table's abfss location (databricks CLI, --profile
     personal — the only profile this project's CLI calls may use),
  2. register the HMS database + one table per source under that location
     (thrift, drop + create so a moved location is re-pointed),
  3. create the StarRocks external catalog with the sync's service principal
     (drop + create), then verify each table with a COUNT.

The SR 4.1.7 quirks this script bakes in (all found live):
  - the catalog type string is "deltalake", not the documented "delta";
  - the FE cannot use the docker DNS name for the metastore (its Java URI
    parser appends the compose network's search domain, whose underscore is
    an illegal hostname character) — thrift://host.docker.internal:9083
    instead;
  - SR reads a Delta-provider table's location from SerDeInfo.parameters
    ["path"] (the Databricks HMS convention), NOT from sd.location;
  - the HMS column list is decorative: SR reads each table's schema from its
    _delta_log at query time (verified live — a fresh registration with bogus
    columns still DESCs with the real schema), so registrations carry none.

Run from the repo root:  ./bin/3_setup_delta_federation.sh
Needs: the hms container up (docker compose up -d hms), StarRocks up, the
devbox venv (hmsclient, pymysql) and the databricks CLI.
"""

from __future__ import annotations

import os
import sys

CATALOG = "delta_federation"
HMS_DATABASE = "delta_fed"
# Every demo sync's source (orchestration/defs/reverse_etl_*/defs.yaml) — one
# HMS name -> location mapping each. The delta source stays the federation
# button's showcase (DEFAULT_SOURCE_FQN below).
SYNC_LABELS = ("avro", "cdf", "orders", "delta", "company")
SOURCES = [f"de_dev.sr_poc_external.reverse_etl_{label}_source" for label in SYNC_LABELS]
DEFAULT_SOURCE_FQN = "de_dev.sr_poc_external.reverse_etl_delta_source"
SOURCE_TABLE = "reverse_etl_delta_source"
STARROCKS_HOST = os.environ.get("STARROCKS_HOST", "127.0.0.1")
STARROCKS_PORT = int(os.environ.get("STARROCKS_PORT", "9030"))
HMS_HOST = os.environ.get("HMS_HOST", "127.0.0.1")
HMS_PORT = int(os.environ.get("HMS_PORT", "9083"))

PROFILE = "--profile personal"


def load_env(path: str = ".env") -> dict[str, str]:
    """The repo's .env (KEY=value) into a dict, with the literal quotes the
    values carry stripped. Existing process env wins, like the demo assets."""
    env: dict[str, str] = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key, value.strip("'\""))
            env[key] = os.environ[key]
    return env


def table_location(fqn: str = DEFAULT_SOURCE_FQN) -> str:
    """A source table's abfss location, straight from UC (which owns it)."""
    import json
    import subprocess

    result = subprocess.run(
        ["databricks", *PROFILE.split(), "tables", "get", fqn, "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    location = json.loads(result.stdout).get("storage_location") or ""
    if not location.startswith("abfss://"):
        raise RuntimeError(f"{fqn}: no abfss storage_location ({location!r})")
    return location


def hms_location(tbl) -> str | None:
    """The location an HMS-registered table carries, read back the way SR reads
    it: the serde's `path` parameter first (the value scans actually use),
    sd.location as the fallback. None (no table) returns None."""
    if tbl is None:
        return None
    serde = getattr(tbl.sd, "serdeInfo", None)
    params = getattr(serde, "parameters", None) or {}
    return params.get("path") or getattr(tbl.sd, "location", None)


def hms_table(location: str, table_name: str = SOURCE_TABLE) -> dict:
    """The HMS Table as hmsclient's thrift types want it. The serde's `path`
    parameter carries the location — for Delta-provider tables StarRocks reads
    the location from there (Databricks HMS convention), sd.location alone is
    ignored. The column list is left empty on purpose: SR takes each table's
    schema from its _delta_log, the registration's cols are never consulted."""
    from hmsclient.genthrift.hive_metastore import ttypes as t

    serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    return t.Table(
        dbName=HMS_DATABASE,
        tableName=table_name,
        tableType="EXTERNAL_TABLE",
        parameters={"spark.sql.sources.provider": "delta", "EXTERNAL": "TRUE"},
        sd=t.StorageDescriptor(
            cols=[],
            location=location,
            inputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            outputFormat="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serdeInfo=t.SerDeInfo(
                name="delta",
                serializationLib=serde,
                parameters={"path": location, "serialization.format": "1"},
            ),
        ),
    )


def _hms_client():
    """An HMS thrift client with its transport opened (hmsclient leaves the
    transport closed on construction — calls fail with 'Transport not open')."""
    from hmsclient import HMSClient
    from thrift.protocol import TBinaryProtocol
    from thrift.transport import TSocket, TTransport

    transport = TTransport.TBufferedTransport(TSocket.TSocket(HMS_HOST, HMS_PORT))
    proto = TBinaryProtocol.TBinaryProtocol(transport)
    transport.open()
    return HMSClient(iprot=proto, oprot=proto), transport


def register_in_hms(location: str, table_name: str = SOURCE_TABLE) -> None:
    """Drop + create the HMS table under `location` (a moved location — UC's
    new UUID after a rebuild — is re-pointed)."""
    from hmsclient.genthrift.hive_metastore import ttypes as t

    client, transport = _hms_client()
    try:
        try:
            client.create_database(t.Database(
                name=HMS_DATABASE,
                description="Databricks UC Delta sources, read in place by StarRocks",
            ))
        except Exception:
            pass  # exists
        try:
            client.drop_table(dbname=HMS_DATABASE, name=table_name, deleteData=False)
        except Exception:
            pass  # not there yet
        client.create_table(hms_table(location, table_name))
    finally:
        transport.close()


def get_table_request(table_name: str):
    """The thrift GetTableRequest for one registered table. The kwargs are
    camelCase (dbName, tblName), unlike the rest of hmsclient's API (dbname=,
    tbl_name= on drop_table) — a snake_case typo raises TypeError at
    construction, which looks like "table not registered" if swallowed."""
    from hmsclient.genthrift.hive_metastore import ttypes as t

    return t.GetTableRequest(dbName=HMS_DATABASE, tblName=table_name)


def registered_location(table_name: str = SOURCE_TABLE) -> str | None:
    """The location the HMS registration currently carries (None if the table
    is not registered or HMS is unreachable at the thrift level). Compared
    against UC's current path so a moved table can be re-pointed without
    dropping the registration blindly."""
    client, transport = _hms_client()
    try:
        try:
            resp = client.get_table_req(get_table_request(table_name))
        except Exception:
            return None  # not registered (or HMS unreachable for the table)
        return hms_location(resp.table)
    finally:
        transport.close()


def catalog_ddl(env: dict[str, str]) -> str:
    """CREATE EXTERNAL CATALOG with the sync's service principal. The tenant
    becomes the OAuth2 token endpoint (v1) StarRocks asks for. The type string
    is "deltalake" — 4.1.7's enum, not the documented "delta"."""
    return (
        f'CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES ('
        '"type"="deltalake", '
        '"hive.metastore.uris"="thrift://host.docker.internal:9083", '
        f'"azure.adls2.oauth2_client_id"="{env["DATABRICKS_AZURE_CLIENT_ID"]}", '
        f'"azure.adls2.oauth2_client_secret"="{env["DATABRICKS_AZURE_CLIENT_SECRET"]}", '
        f'"azure.adls2.oauth2_client_endpoint"="https://login.microsoftonline.com/'
        f'{env["DATABRICKS_AZURE_TENANT_ID"]}/oauth2/token")'
    )


def create_catalog(env: dict[str, str]) -> None:
    import pymysql

    conn = pymysql.connect(
        host=STARROCKS_HOST, port=STARROCKS_PORT, user="root", connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP CATALOG IF EXISTS {CATALOG}")
            cur.execute(catalog_ddl(env))
    finally:
        conn.close()


def verify(table_name: str = SOURCE_TABLE) -> int:
    """COUNT(*) through the federation — the proof the whole chain works."""
    import pymysql

    conn = pymysql.connect(
        host=STARROCKS_HOST, port=STARROCKS_PORT, user="root", connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {CATALOG}.{HMS_DATABASE}.{table_name}")
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def main() -> int:
    load_env()
    # Discover every source's location; a table UC does not know (never set up,
    # or renamed) is skipped with a warning, not fatal — the rest still register.
    locations: dict[str, str] = {}
    for fqn in SOURCES:
        try:
            locations[fqn] = table_location(fqn)
        except Exception as e:
            print(f"skip {fqn}: {type(e).__name__}: {e}")
    for fqn, location in locations.items():
        register_in_hms(location, fqn.rpartition(".")[2])
        print(f"HMS: {HMS_DATABASE}.{fqn.rpartition('.')[2]} registered ({location})")
    create_catalog(load_env())
    print(f"StarRocks: catalog {CATALOG} (re)created")
    for fqn in locations:
        table_name = fqn.rpartition(".")[2]
        count = verify(table_name)
        print(f"verified: SELECT COUNT(*) on {table_name} -> {count:,} rows read from Databricks storage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
