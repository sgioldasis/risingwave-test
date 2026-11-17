"""StarRocks serving-feature demos (computed columns, ASOF JOIN, UNIQUE KEY
model, workload groups) -- the data side of the "StarRocks Serving Features"
dashboard.

Four pieces, all idempotent so demo resets can re-run the asset freely:

1. funnel_window_stats -- a PRIMARY KEY table carrying the funnel's
   view_to_cart_rate / cart_to_buy_rate as COMPUTED COLUMNS (single
   definition, zero drift: every consumer reads the table's own column
   instead of re-deriving `carters / viewers` in view/MV/query SQL).
   Refreshed every minute by the scheduled task funnel_window_stats_refresh
   (SUBMIT TASK body is an INSERT; the PK table upserts on window_start, so
   re-inserting every window each tick is idempotent). Computed columns are
   auto-derived -- the INSERT lists only the base columns. They need an
   EXPLICIT type in the DDL (untyped `c AS expr` fails with "Column's type
   is null" on 4.1.7).

2. country_scores -- a deterministic per-country risk-score publication feed
   (hourly, PRIMARY KEY (country, ts)) for the ASOF JOIN demo: each page
   view is matched to the most recent score published for its country at or
   before the event. ASOF needs a REAL-column equi predicate alongside the
   temporal >= (confirmed live: equi on a constant/aliased column through a
   subquery -- or any subquery-derived right side -- is rejected as a
   "correlated condition"; both sides must be real tables). u.country is
   the equi key, which is exactly what makes the demo semantically real:
   country comes from dim_users, not from the raw events.

3. wallet_type_totals_unique -- the UNIQUE KEY table model (the one model
   the Wallet dashboard's DUPLICATE / AGGREGATE / PRIMARY KEY trio doesn't
   show). Same per-key upsert semantics as PRIMARY KEY, but whole-row
   REPLACE: an INSERT re-writing a key replaces the entire row (no
   partial-column update). Stores per-type wallet totals, refreshed by the
   same 1-minute INSERT-SELECT pattern as the AGGREGATE KEY twin
   (wallet_type_totals_agg) -- AGGREGATE maintains SUM incrementally at
   ingest, UNIQUE does full-row upsert per key at refresh time.

4. Resource-group isolation for the demo dashboard: every chart on the
   Serving Features dashboard connects as user superset_demo (SELECT-only,
   no password -- it can only read), which a resource-group classifier
   pins to rg_superset_demo (cpu_weight 80 vs default_wg's 100 and
   default_mv_wg's 1). That is the workload-isolation story: dashboard
   SELECTs get their own CPU share no matter what the MV-refresh tasks are
   doing (the jam documented in docs/SR_POC_QUERY_REWRITE_DEMO.md).
   Scope note (re-verified 2026-11-17 on shared-data 4.1.7 -- the earlier
   "shared-nothing" attribution was wrong; this cluster has been
   shared-data on MinIO since 2026-09-08): per-resource-group query
   queueing is not available here. The 3.x queue properties
   (max_concurrency / max_queue_size / queue_timeout) no longer exist --
   the current property is concurrency_limit. And since v4.1 the FE runs
   query queue v2 by default (enable_query_queue_v2), under which
   group-level queuing via concurrency_limit is NOT implemented (it was a
   v1-only feature): three concurrent SELECT SLEEP(5) calls against a
   concurrency_limit=2 group all ran, regardless of
   enable_group_level_query_queue / enable_query_queue_select. What
   concurrency_limit DOES do (verified live): with
   enable_group_level_query_queue=false (the default), the third-plus
   concurrent query FAILS with "Exceed concurrency limit" -- admission
   control, not queueing. Disabling v2 (enable_query_queue_v2=false) does
   NOT bring queueing back either -- the v1 path it would activate is inert
   on this build (probed exhaustively 2026-11-17: SHOW USAGE RESOURCE GROUPS
   showed 3 concurrent queries in a concurrency_limit=2 group with no queue
   and no error; StarRocks PR #78425, unmerged, confirms v2 has no
   resource-group logic and that enable_group_level_query_queue=true makes
   the limit unenforced everywhere). So the demo ships weight isolation +
   classifier pinning, with fail-fast admission control via
   concurrency_limit=10 on rg_superset_demo (over the 11th concurrent
   query, never tripped by a dashboard render).
   NOTE: db-level grants do NOT cover views on 4.1.7 (GRANT ... ON db.*
   passes tables only, and an explicit view grant fails with "cannot find
   table") -- all superset_demo datasets query base tables directly.
"""

import os

from dagster import AssetDep, AssetExecutionContext, asset
from sqlalchemy import create_engine, text

from .sql_loader import load_sql

SCHEMA = "sr_local_db"

# All DDL for this asset lives in sql/starrocks/ (extracted 2026-11-17),
# one file per object, each carrying its own verified-gotcha commentary:
#   1. computed columns -- funnel_window_stats.sql,
#      funnel_window_stats_refresh_task.sql, funnel_window_stats_backfill.sql
#   2. ASOF JOIN feed -- country_scores.sql, country_scores_seed.sql
#   3. UNIQUE KEY model -- wallet_type_totals_unique.sql (+_refresh_task,
#      _backfill)
#   4. workload isolation -- superset_demo_user.sql, superset_demo_grant.sql,
#      rg_superset_demo.sql, rg_superset_demo_alter.sql,
#      resource_group_snapshot.sql (the live-weights INSERT itself stays
#      below -- it parses SHOW RESOURCE GROUPS ALL output at runtime).
FUNNEL_WINDOW_STATS_SQL = load_sql("funnel_window_stats", schema=SCHEMA)
FUNNEL_WINDOW_STATS_TASK_SQL = load_sql("funnel_window_stats_refresh_task", schema=SCHEMA)
FUNNEL_WINDOW_STATS_BACKFILL_SQL = load_sql("funnel_window_stats_backfill", schema=SCHEMA)
COUNTRY_SCORES_SQL = load_sql("country_scores", schema=SCHEMA)
COUNTRY_SCORES_SEED_SQL = load_sql("country_scores_seed", schema=SCHEMA)
WALLET_TYPE_TOTALS_UNIQUE_SQL = load_sql("wallet_type_totals_unique", schema=SCHEMA)
WALLET_TYPE_TOTALS_UNIQUE_TASK_SQL = load_sql("wallet_type_totals_unique_refresh_task", schema=SCHEMA)
WALLET_TYPE_TOTALS_UNIQUE_BACKFILL_SQL = load_sql("wallet_type_totals_unique_backfill", schema=SCHEMA)
DEMO_USER_SQL = load_sql("superset_demo_user")
DEMO_USER_GRANT_SQL = load_sql("superset_demo_grant", schema=SCHEMA)
RESOURCE_GROUP_SQL = load_sql("rg_superset_demo")
RESOURCE_GROUP_ALTER_SQL = load_sql("rg_superset_demo_alter")
RESOURCE_GROUP_SNAPSHOT_SQL = load_sql("resource_group_snapshot", schema=SCHEMA)


@asset(
    name="starrocks_feature_demo_setup",
    group_name="starrocks_unified_funnel",
    # wallet_transactions_direct_kafka_setup (renamed from
    # ..._direct_kafka 2026-11-17): the bare table name is the dbt model's
    # asset key now; the setup asset (routine load) transitively covers
    # the dbt-created table shell.
    deps=[AssetDep("funnel_events_direct_kafka"), AssetDep("wallet_transactions_direct_kafka_setup")],
    description=(
        "Data side of the StarRocks Serving Features dashboard: "
        "funnel_window_stats (computed columns + 1-min refresh task), "
        "country_scores (per-country risk feed for the ASOF JOIN demo), "
        "wallet_type_totals_unique (the UNIQUE KEY table-model demo), the "
        "superset_demo user + rg_superset_demo workload group that "
        "isolate the dashboard's queries from MV-refresh traffic, and the "
        "resource_group_snapshot table the dashboard charts (live weights "
        "read from SHOW RESOURCE GROUPS ALL)."
    ),
)
def starrocks_feature_demo_setup(context: AssetExecutionContext) -> dict:
    starrocks_url = os.environ.get(
        "STARROCKS_URL",
        "mysql+pymysql://root@starrocks:9030",
    )
    engine = create_engine(
        starrocks_url,
        connect_args={"connect_timeout": 5},
        pool_pre_ping=True,
        isolation_level="AUTOCOMMIT",
    )

    with engine.connect() as connection:
        connection.execute(text(FUNNEL_WINDOW_STATS_SQL))
        context.log.info("Table %s.funnel_window_stats ready", SCHEMA)

        connection.execute(text(
            f"DROP TASK IF EXISTS {SCHEMA}.funnel_window_stats_refresh"
        ))
        connection.execute(text(FUNNEL_WINDOW_STATS_TASK_SQL))
        context.log.info("Task %s.funnel_window_stats_refresh recreated", SCHEMA)

        connection.execute(text(COUNTRY_SCORES_SQL))
        connection.execute(text(COUNTRY_SCORES_SEED_SQL))
        score_rows = connection.execute(text(
            f"SELECT COUNT(*) FROM {SCHEMA}.country_scores"
        )).scalar()
        context.log.info("Score feed %s.country_scores ready (%d rows)", SCHEMA, score_rows)

        connection.execute(text(WALLET_TYPE_TOTALS_UNIQUE_SQL))
        connection.execute(text(
            f"DROP TASK IF EXISTS {SCHEMA}.wallet_type_totals_unique_refresh"
        ))
        connection.execute(text(WALLET_TYPE_TOTALS_UNIQUE_TASK_SQL))
        connection.execute(text(WALLET_TYPE_TOTALS_UNIQUE_BACKFILL_SQL))
        unique_rows = connection.execute(text(
            f"SELECT COUNT(*) FROM {SCHEMA}.wallet_type_totals_unique"
        )).scalar()
        context.log.info(
            "UNIQUE KEY table %s.wallet_type_totals_unique ready (%d rows)",
            SCHEMA, unique_rows,
        )

        connection.execute(text(FUNNEL_WINDOW_STATS_BACKFILL_SQL))

        connection.execute(text(DEMO_USER_SQL))
        connection.execute(text(DEMO_USER_GRANT_SQL))
        connection.execute(text(RESOURCE_GROUP_SQL))
        connection.execute(text(RESOURCE_GROUP_ALTER_SQL))
        groups = connection.execute(text(
            "SHOW RESOURCE GROUPS ALL"
        )).fetchall()
        group_names = [g[0] for g in groups]
        context.log.info("Resource groups: %s",
                         [(g[0], g[2]) for g in groups])

        # Snapshot the LIVE group weights into resource_group_snapshot so the
        # dashboard can chart them (no information_schema table exists). PK
        # upsert on group_name keeps this idempotent.
        connection.execute(text(RESOURCE_GROUP_SNAPSHOT_SQL))
        for group in groups:
            name = group[0]
            # SHOW RESOURCE GROUPS ALL columns: name, id, cpu_weight_percent,
            # exclusive_cpu_percent, mem_limit, ...
            cpu_weight = int(group[2]) if group[2] is not None else 0
            mem_limit = str(group[4] or "").replace("%", "")
            connection.execute(text(
                f"INSERT INTO {SCHEMA}.resource_group_snapshot "
                f"(group_name, cpu_weight, mem_limit, updated_at) "
                f"VALUES ('{name}', {cpu_weight}, '{mem_limit}', now())"
            ))
        context.log.info(
            "Resource group snapshot upserted: %s", group_names
        )

        stats_rows = connection.execute(text(
            f"SELECT COUNT(*) FROM {SCHEMA}.funnel_window_stats"
        )).scalar()
        context.log.info("funnel_window_stats rows: %d", stats_rows)

    return {
        "computed_columns_table": "funnel_window_stats",
        "refresh_task": "funnel_window_stats_refresh",
        "country_scores_rows": score_rows,
        "unique_key_rows": unique_rows,
        "resource_group": "rg_superset_demo",
    }
