# Spark Caching Fix Plan

## Problem
Spark notebook charts are slow to refresh. Data appears cached and requires pressing "Run" multiple times before new data is visible.

## Root Causes
1. Spark SQL caches query plans and table metadata
2. Iceberg connector maintains its own metadata cache
3. DataFrames may be implicitly cached
4. No delay after cache operations for propagation

## Solution

### 1. Spark Session Configuration Changes ✅ DONE
Added configurations to minimize caching:
- `spark.sql.files.openCostInBytes=0` - Reduce file open overhead
- `spark.sql.files.maxPartitionBytes=134217728` - Control partition size

### 2. Cache Clearing Helper Function ✅ DONE
Updated `clear_and_retry_query()` function to:
- Clear Spark catalog cache with `spark.catalog.clearCache()`
- Clear SQL cache with `spark.sql("CLEAR CACHE")`
- Refreshes table metadata with `spark.sql("REFRESH TABLE")` for all Iceberg tables
- Adds a small delay (0.5s) for cache propagation

### 3. Apply Cache Clearing to All Data Query Cells ✅ DONE
All cells that query data now use the cache clearing function:
- ✅ Funnel metrics cell
- ✅ Time series chart cell
- ✅ Sample data preview cells

### 4. Changes Applied to `scripts/user_activity_flow.py`

1. **Spark Session Builder (lines 110-116)** - Added file caching configs
2. **`clear_and_retry_query()` function (lines 145-178)** - Added REFRESH TABLE calls and delay
3. **Sample data preview cell (lines 489-495)** - Now uses cache clearing helper

## Result
Spark charts should now refresh properly when new data is available in Iceberg tables.
