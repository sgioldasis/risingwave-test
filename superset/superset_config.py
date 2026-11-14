# Superset's MySQL engine spec imports MySQLdb (mysqlclient) unconditionally when it reads result
# column types. The StarRocks connections use mysql+pymysql, so let pymysql stand in for MySQLdb.
import json

import pymysql

pymysql.install_as_MySQLdb()


# When RisingWave is stopped, charts backed by the `risingwave` JDBC catalog
# fail with StarRocks's raw analysis error (e.g. `Error: (5501, "Getting
# analyzing error. Detail message: Unknown database 'public.'")`) or a raw
# PG connection error. A view can't catch its own analysis failure, so
# rewrite the known signatures into one generic, human-readable message in
# the JSON response instead. Markers are deliberately narrow: "Unknown
# database 'public.'" is unique to the RW JDBC catalog (StarRocks's own DB
# is sr_local_db, and RW objects are always referenced as
# risingwave.public.*), and port 4566 is RisingWave's PG-wire port.
RW_ERROR_MARKERS = (
    "Unknown database 'public'",
    "port 4566",
    # FE-side Hikari pool bootstrap failure when the JDBC catalog can't reach
    # RisingWave (the fast-fail path since connectTimeout=2 on the catalog).
    # Safe to attribute to RW: no chart's dataset touches the other PG JDBC
    # catalog (postgres_demo backs no datasets).
    "Failed to initialize pool",
)
RW_ERROR_GENERIC = "RisingWave not available"


def _rewrite_rw_errors(node):
    if isinstance(node, dict):
        return {
            k: (
                RW_ERROR_GENERIC
                if isinstance(v, str) and any(m in v for m in RW_ERROR_MARKERS)
                else _rewrite_rw_errors(v)
            )
            for k, v in node.items()
        }
    if isinstance(node, list):
        return [_rewrite_rw_errors(x) for x in node]
    return node


def FLASK_APP_MUTATOR(app):
    from flask import request

    @app.after_request
    def _generic_rw_errors(response):
        if (
            response.status_code >= 400
            and (response.mimetype or "").startswith("application/json")
            and request.path.startswith("/api/v1/")
        ):
            try:
                payload = response.get_json(silent=True)
                rewritten = _rewrite_rw_errors(payload)
                if rewritten != payload:
                    response.data = json.dumps(rewritten)
                    response.content_length = len(response.data)
            except Exception:
                pass  # never let the rewrite break the original error response
        return response
