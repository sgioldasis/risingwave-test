-- WHAT: funnel per-minute window stats carrying view_to_cart_rate /
--   cart_to_buy_rate as COMPUTED COLUMNS.
-- FEATURE: computed columns -- auto-derived column expressions the engine
--   maintains on every write (they need an EXPLICIT type on 4.1.7, see the
--   gotcha below).
-- WHY: the rates are defined ONCE here -- every consumer reads the table's
--   own columns instead of re-deriving carters / viewers in view/MV/query
--   SQL, removing the drift risk that comparisons against
--   funnel_summary_direct exposed. This table is also the computed-columns
--   feature demo itself.
-- WHERE: StarRocks Serving Features dashboard; created by the
--   starrocks_feature_demo_setup Dagster asset.

-- PRIMARY KEY table carrying the funnel's view_to_cart_rate /
-- cart_to_buy_rate as COMPUTED COLUMNS (single definition, zero drift:
-- every consumer reads the table's own column instead of re-deriving
-- `carters / viewers` in view/MV/query SQL). Computed columns are
-- auto-derived -- the INSERT (see funnel_window_stats_refresh_task.sql)
-- lists only the base columns. They need an EXPLICIT type in the DDL
-- (untyped `c AS expr` fails with "Column's type is null" on 4.1.7).
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
CREATE TABLE IF NOT EXISTS {schema}.funnel_window_stats (
    window_start DATETIME NOT NULL,
    viewers BIGINT NOT NULL,
    carters BIGINT NOT NULL,
    purchasers BIGINT NOT NULL,
    view_to_cart_rate DOUBLE AS round(carters / nullif(viewers, 0), 2),
    cart_to_buy_rate DOUBLE AS round(purchasers / nullif(carters, 0), 2)
) PRIMARY KEY (window_start)
DISTRIBUTED BY HASH(window_start)
