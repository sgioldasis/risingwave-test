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

- `conversion_category(rate)` - Categorizes conversion rates
- `calculate_funnel_score(viewers, carters, purchasers)` - Weighted funnel score
- `format_rate_with_emoji(rate)` - Visual rate formatting
- `revenue_tier(amount)` - Revenue classification
- `calculate_funnel_health(v2c_rate, c2b_rate)` - Overall health status

### conversion_category(rate float) → varchar

Categorizes conversion rates into performance tiers:
- `excellent` - rate >= 0.5
- `good` - rate >= 0.3
- `average` - rate >= 0.1
- `needs_improvement` - rate < 0.1

### calculate_funnel_score(viewers, carters, purchasers) → float

Calculates a weighted funnel performance score (0.0 - 1.0):
```python
def calculate_funnel_score(viewers, carters, purchasers):
    view_to_cart = carters / viewers
    cart_to_buy = purchasers / carters
    return view_to_cart * 0.4 + cart_to_buy * 0.6
```

### format_rate_with_emoji(rate float) → varchar

Formats conversion rates with emoji indicators and descriptive text:
- 🟢 High ≥50%
- 🟡 Medium ≥30%
- 🟠 Low ≥10%
- 🔴 Critical <10%

### revenue_tier(amount numeric) → varchar

Classifies revenue amounts into tiers:
- `premium` - amount >= 1000
- `high` - amount >= 500
- `medium` - amount >= 100
- `low` - amount < 100

### calculate_funnel_health(view_to_cart_rate float, cart_to_buy_rate float) → varchar

Calculates overall funnel health based on both conversion rates:
- `strong` - both rates >= 0.3 (30%)
- `moderate` - either rate >= 0.2 (20%)
- `weak` - both rates < 0.2 (20%)

Used to categorize overall funnel performance in the enriched view.

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

The `funnel_enriched` view uses these UDFs:

```sql
SELECT 
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    conversion_category(view_to_cart_rate) AS view_to_cart_category,
    format_rate_with_emoji(cart_to_buy_rate) AS cart_to_buy_emoji,
    calculate_funnel_score(viewers, carters, purchasers) AS funnel_score
FROM funnel_summary;
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

### UDF Not Found

Run the Dagster asset `risingwave_python_udfs` to recreate UDFs after RisingWave restart.

### View Uses Corrupted UDF

If a view was created with a corrupted UDF, drop and recreate it:

```sql
DROP VIEW IF EXISTS funnel_enriched;
-- Then re-materialize via Dagster
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

- [`sql/create_udfs.sql`](sql/create_udfs.sql) - UDF definitions
- [`orchestration/assets/risingwave_udfs.py`](orchestration/assets/risingwave_udfs.py) - Dagster asset
- [`dbt/models/funnel_enriched.sql`](dbt/models/funnel_enriched.sql) - View using UDFs

## References

- [RisingWave Embedded Python UDFs Documentation](https://docs.risingwave.com/sql/udfs/embedded-python-udfs)
- [RisingWave CREATE FUNCTION](https://docs.risingwave.com/sql/commands/sql-create-function)
