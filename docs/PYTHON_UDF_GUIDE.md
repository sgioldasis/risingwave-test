# RisingWave Python UDF Integration Guide

## Overview

This guide explains how to create and use Python UDFs (User-Defined Functions) in RisingWave for the funnel demo.

## Key Finding: Proper Python UDF Syntax

RisingWave embedded Python UDFs **must be defined as proper Python functions** using the `def` keyword:

```sql
CREATE FUNCTION my_function(arg1 type1, arg2 type2)
RETURNS return_type
LANGUAGE python AS $$
def my_function(arg1, arg2):
    # Function body here
    return result
$$;
```

### ❌ Incorrect (Bare Code Block)

```sql
-- This will fail with: SyntaxError: 'return' outside function
CREATE FUNCTION conversion_category(rate float)
RETURNS varchar
LANGUAGE python AS $$
if rate >= 0.5:       -- ERROR: 'return' outside function
    return 'excellent'
$$;
```

### ✅ Correct (Function Definition)

```sql
-- This works correctly
CREATE FUNCTION conversion_category(rate float)
RETURNS varchar
LANGUAGE python AS $$
def conversion_category(rate):  -- Proper function definition
    if rate >= 0.5:
        return 'excellent'
    return 'other'
$$;
```

## UDFs Created

The following Python UDFs are available for the funnel demo:

| Function | Description |
|----------|-------------|
| `conversion_category(rate)` | Categorizes conversion rates into tiers |
| `calculate_funnel_score(viewers, carters, purchasers)` | Weighted funnel performance score |
| `format_rate_with_emoji(rate)` | Visual rate formatting with emojis and text |
| `revenue_tier(amount)` | Revenue classification |
| `calculate_funnel_health(v2c_rate, c2b_rate)` | Overall funnel health status |

### conversion_category(rate float) → varchar

Categorizes conversion rates into performance tiers:
```python
def conversion_category(rate):
    if rate is None:
        return 'unknown'
    if rate >= 0.5:
        return 'excellent'
    elif rate >= 0.3:
        return 'good'
    elif rate >= 0.1:
        return 'average'
    else:
        return 'needs_improvement'
```

| Rate Range | Category |
|------------|----------|
| ≥ 0.5 | `excellent` |
| ≥ 0.3 | `good` |
| ≥ 0.1 | `average` |
| < 0.1 | `needs_improvement` |

### calculate_funnel_score(viewers bigint, carters bigint, purchasers bigint) → float

Calculates a weighted funnel performance score (0.0 - 1.0):
```python
def calculate_funnel_score(viewers, carters, purchasers):
    if viewers is None or viewers == 0:
        return 0.0
    view_to_cart = carters / float(viewers) if carters else 0.0
    cart_to_buy = purchasers / float(carters) if carters else 0.0
    return round(view_to_cart * 0.4 + cart_to_buy * 0.6, 2)
```

**Note:** The function signature uses `bigint` for type compatibility. Use `::bigint` casts when calling from SQL:
```sql
SELECT calculate_funnel_score(viewers::bigint, carters::bigint, purchasers::bigint);
```

### format_rate_with_emoji(rate float) → varchar

Formats conversion rates with emoji indicators and descriptive text:
```python
def format_rate_with_emoji(rate):
    if rate is None:
        return '⚪ N/A'
    percentage = round(rate * 100, 1)
    if rate >= 0.5:
        return f'🟢 High {percentage}%'
    elif rate >= 0.3:
        return f'🟡 Medium {percentage}%'
    elif rate >= 0.1:
        return f'🟠 Low {percentage}%'
    else:
        return f'🔴 Critical {percentage}%'
```

| Rate Range | Output |
|------------|--------|
| None | `⚪ N/A` |
| ≥ 0.5 | `🟢 High XX.X%` |
| ≥ 0.3 | `🟡 Medium XX.X%` |
| ≥ 0.1 | `🟠 Low XX.X%` |
| < 0.1 | `🔴 Critical XX.X%` |

### revenue_tier(amount numeric) → varchar

Classifies revenue amounts into tiers:
```python
def revenue_tier(amount):
    if amount is None:
        return 'unknown'
    if amount >= 1000:
        return 'premium'
    elif amount >= 500:
        return 'high'
    elif amount >= 100:
        return 'medium'
    else:
        return 'low'
```

| Amount Range | Tier |
|--------------|------|
| ≥ 1000 | `premium` |
| ≥ 500 | `high` |
| ≥ 100 | `medium` |
| < 100 | `low` |

### calculate_funnel_health(view_to_cart_rate float, cart_to_buy_rate float) → varchar

Calculates overall funnel health based on both conversion rates:
```python
def calculate_funnel_health(view_to_cart_rate, cart_to_buy_rate):
    if view_to_cart_rate is None or cart_to_buy_rate is None:
        return 'unknown'
    if view_to_cart_rate >= 0.3 and cart_to_buy_rate >= 0.3:
        return 'strong'
    elif view_to_cart_rate >= 0.2 or cart_to_buy_rate >= 0.2:
        return 'moderate'
    else:
        return 'weak'
```

| Condition | Health Status |
|-----------|---------------|
| Both rates ≥ 30% | `strong` |
| Either rate ≥ 20% | `moderate` |
| Both rates < 20% | `weak` |
| Either rate is NULL | `unknown` |

**Type Handling:** The function handles multiple type signatures. DROP statements include:
- `calculate_funnel_health(float, float)`
- `calculate_funnel_health(double precision, double precision)`
- `calculate_funnel_health(numeric, numeric)`

## How to Create UDFs

### Via Dagster Asset (Recommended)

1. Open Dagster UI at http://localhost:3000
2. Navigate to **Asset Catalog**
3. Find `risingwave_python_udfs`
4. Click **Materialize** to create the UDFs

### Via SQL File

The UDF definitions are stored in [`sql/create_udfs.sql`](sql/create_udfs.sql).

```bash
# Copy and execute from dagster container
docker cp sql/create_udfs.sql dagster-daemon:/tmp/
docker exec dagster-daemon psql -h frontend-node-0 -p 4566 -U root -d dev -f /tmp/create_udfs.sql
```

### Direct Python Execution

```python
import psycopg2

conn = psycopg2.connect(host="frontend-node-0", port=4566, database="dev", user="root")
with conn.cursor() as cur:
    cur.execute("""
        CREATE FUNCTION my_udf(x int) RETURNS int
        LANGUAGE python AS $$
        def my_udf(x):
            return x * 2
        $$;
    """)
    conn.commit()
conn.close()
```

## Using UDFs in Views

The `funnel_enriched` view uses all five UDFs:

```sql
SELECT
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate,
    revenue,
    conversion_category(view_to_cart_rate) AS view_to_cart_category,
    conversion_category(cart_to_buy_rate) AS cart_to_buy_category,
    format_rate_with_emoji(view_to_cart_rate) AS view_to_cart_emoji,
    format_rate_with_emoji(cart_to_buy_rate) AS cart_to_buy_emoji,
    calculate_funnel_score(viewers::bigint, carters::bigint, purchasers::bigint) AS funnel_score,
    revenue_tier(revenue) AS revenue_tier,
    calculate_funnel_health(view_to_cart_rate, cart_to_buy_rate) AS funnel_health
FROM funnel_summary;
```

**Important:** When calling `calculate_funnel_score`, ensure you cast numeric columns to `bigint`:
```sql
-- Correct
SELECT calculate_funnel_score(viewers::bigint, carters::bigint, purchasers::bigint);

-- May cause type errors
SELECT calculate_funnel_score(viewers, carters, purchasers);
```

## Troubleshooting

### "'return' outside function" Error

This means the Python code is not wrapped in a proper function definition. Ensure your UDF looks like:

```sql
CREATE FUNCTION name(args) RETURNS type LANGUAGE python AS $$
def name(args):
    # code with return
    return value
$$;
```

### Function Does Not Exist

If you see errors like:
```
function calculate_funnel_score(numeric, numeric, numeric) does not exist
```

This means the function signature doesn't match. Check:
1. The SQL uses correct types (e.g., `bigint` not `numeric`)
2. Use explicit type casts: `column::bigint`

### Function Already Exists with Different Signature

If you see:
```
function with name calculate_funnel_health(double precision,double precision) exists
```

The UDF exists with a different type signature. The Dagster asset handles this by dropping all common signatures before creating:

```python
drop_commands = [
    "DROP FUNCTION IF EXISTS calculate_funnel_health(float, float) CASCADE",
    "DROP FUNCTION IF EXISTS calculate_funnel_health(double precision, double precision) CASCADE",
    "DROP FUNCTION IF EXISTS calculate_funnel_health(numeric, numeric) CASCADE",
]
```

### UDF Not Found

Run the Dagster asset `risingwave_python_udfs` to recreate UDFs after RisingWave restart:

```bash
# Or manually from psql
psql -h frontend-node-0 -p 4566 -U root -d dev -f sql/create_udfs.sql
```

### View Uses Corrupted UDF

If a view was created with a corrupted UDF, drop and recreate it:

```sql
DROP VIEW IF EXISTS funnel_enriched;
DROP MATERIALIZED VIEW IF EXISTS funnel_enriched;
-- Then re-materialize via Dagster or dbt
```

## Configuration

Python UDFs are enabled in [`risingwave.toml`](risingwave.toml):

```toml
[udf]
enable_embedded_python_udf = true
enable_embedded_javascript_udf = true
enable_embedded_wasm_udf = true
```

## File Structure

| File | Purpose |
|------|---------|
| [`sql/create_udfs.sql`](sql/create_udfs.sql) | Main UDF definitions (5 Python UDFs) |
| [`orchestration/assets/risingwave_udfs.py`](orchestration/assets/risingwave_udfs.py) | Dagster asset for creating UDFs |
| [`dbt/macros/create_udfs.sql`](dbt/macros/create_udfs.sql) | dbt macro to run UDF creation from SQL file |
| [`dbt/models/funnel_enriched.sql`](dbt/models/funnel_enriched.sql) | Materialized view using all UDFs |

### Removed Files (Cleanup)

The following files were removed as duplicates/iterations:
- ~~`scripts/create_udfs_clean.py`~~ - Intermediate iteration
- ~~`scripts/create_udfs_final.py`~~ - Intermediate iteration

## References

- [RisingWave Embedded Python UDFs Documentation](https://docs.risingwave.com/sql/udfs/embedded-python-udfs)
- [RisingWave CREATE FUNCTION](https://docs.risingwave.com/sql/commands/sql-create-function)
